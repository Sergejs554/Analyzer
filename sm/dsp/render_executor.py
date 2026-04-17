# sm/dsp/render_executor.py

from __future__ import annotations

import json
import math
import os
import shlex
import subprocess
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import librosa
import numpy as np
import soundfile as sf


CustomBackend = Callable[[List[Dict[str, Any]], str, str, Dict[str, Any]], str]


def _run(cmd: str) -> tuple[str, str]:
    p = subprocess.run(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if p.returncode != 0:
        raise RuntimeError(p.stderr.decode("utf-8", errors="ignore")[:4000])
    return (
        p.stdout.decode("utf-8", errors="ignore"),
        p.stderr.decode("utf-8", errors="ignore"),
    )


def _db_to_linear(gain_db: float) -> float:
    return math.pow(10.0, gain_db / 20.0)


def _safe_name(value: str) -> str:
    out = []
    for ch in str(value or "node"):
        if ch.isalnum() or ch in ("_", "-"):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)


def _tmp_wav_path(td: str, name: str) -> str:
    return os.path.join(td, f"{_safe_name(name)}.wav")


def _quote(path: str) -> str:
    return shlex.quote(path)


def _ensure_2d_audio(audio: np.ndarray) -> np.ndarray:
    audio = np.asarray(audio, dtype=np.float32)
    if audio.ndim == 1:
        return audio[:, None]
    return audio.astype(np.float32, copy=False)


def _match_length(x: np.ndarray, target_len: int) -> np.ndarray:
    if len(x) == target_len:
        return x
    if len(x) > target_len:
        return x[:target_len]
    pad = np.zeros((target_len - len(x),) + x.shape[1:], dtype=x.dtype)
    return np.concatenate([x, pad], axis=0)


def _load_audio(path: str) -> tuple[np.ndarray, int]:
    audio, sr = sf.read(path, always_2d=True)
    return _ensure_2d_audio(audio), int(sr)


def _write_wav(path: str, audio: np.ndarray, sr: int) -> None:
    sf.write(path, np.asarray(audio, dtype=np.float32), sr, format="WAV", subtype="PCM_24")


def _resample_audio(audio: np.ndarray, orig_sr: int, target_sr: int) -> np.ndarray:
    audio = _ensure_2d_audio(audio)
    if orig_sr == target_sr:
        return audio.copy()

    channels = []
    for ch in range(audio.shape[1]):
        y = librosa.resample(
            audio[:, ch],
            orig_sr=orig_sr,
            target_sr=target_sr,
            res_type="soxr_hq",
        )
        channels.append(np.asarray(y, dtype=np.float32))

    min_len = min(len(ch) for ch in channels)
    return np.stack([ch[:min_len] for ch in channels], axis=1).astype(np.float32)


def _smooth_gain_samples(
    gain: np.ndarray,
    attack_ms: float,
    release_ms: float,
    sr: int,
) -> np.ndarray:
    gain = np.asarray(gain, dtype=np.float32)
    if gain.size == 0:
        return gain

    attack_ms = max(float(attack_ms), 0.05)
    release_ms = max(float(release_ms), attack_ms)

    attack_coeff = math.exp(-1.0 / max(sr * attack_ms * 0.001, 1e-9))
    release_coeff = math.exp(-1.0 / max(sr * release_ms * 0.001, 1e-9))

    out = np.zeros_like(gain, dtype=np.float32)
    prev = float(gain[0])

    for i, v in enumerate(gain):
        v = float(gain[i])

        if v < prev:
            if attack_ms <= 0.5:
                prev = v
            else:
                prev = attack_coeff * prev + (1.0 - attack_coeff) * v
        else:
            prev = release_coeff * prev + (1.0 - release_coeff) * v

        out[i] = prev

    return out


def _probe_audio(path: str) -> Dict[str, Any]:
    cmd = (
        f"ffprobe -v error -print_format json -show_streams -show_format {_quote(path)}"
    )
    out, _ = _run(cmd)
    data = json.loads(out or "{}")

    streams = data.get("streams", []) or []
    audio_stream = None
    for stream in streams:
        if stream.get("codec_type") == "audio":
            audio_stream = stream
            break

    sample_rate = 48000
    channels = 2
    channel_layout = "stereo"

    if audio_stream:
        sample_rate = int(audio_stream.get("sample_rate") or 48000)
        channels = int(audio_stream.get("channels") or 2)
        channel_layout = audio_stream.get("channel_layout") or ("mono" if channels == 1 else "stereo")

    duration = float((data.get("format") or {}).get("duration") or 0.0)
    if duration <= 0.0 and audio_stream:
        duration = float(audio_stream.get("duration") or 0.0)

    return {
        "sample_rate": sample_rate,
        "channels": channels,
        "channel_layout": channel_layout,
        "duration": max(duration, 0.0),
    }


def _channel_layout_from_count(channels: int, fallback: str = "stereo") -> str:
    if channels == 1:
        return "mono"
    if channels == 2:
        return "stereo"
    return fallback or "stereo"


def _copy_audio_like(input_path: str, output_path: str) -> str:
    cmd = (
        f"ffmpeg -y -hide_banner -i {_quote(input_path)} "
        f"-map a:0 -ar 48000 -ac 2 -c:a pcm_s24le {_quote(output_path)}"
    )
    _run(cmd)
    return output_path


def _create_silence_like(reference_path: str, output_path: str) -> str:
    meta = _probe_audio(reference_path)
    sample_rate = int(meta["sample_rate"])
    channels = int(meta["channels"])
    duration = float(meta["duration"])
    channel_layout = _channel_layout_from_count(channels, meta.get("channel_layout", "stereo"))

    dur = max(duration, 0.01)
    cmd = (
        f"ffmpeg -y -hide_banner -f lavfi -i anullsrc=r={sample_rate}:cl={channel_layout} "
        f"-t {dur:.6f} -ar {sample_rate} -ac {channels} -c:a pcm_s24le {_quote(output_path)}"
    )
    _run(cmd)
    return output_path


def _ffmpeg_filter_for_op(op: Dict[str, Any]) -> Optional[str]:
    primitive_class = op.get("primitive_class")
    params = op.get("params") or {}

    if primitive_class in {"static_eq_cut", "presence_contour"}:
        freq_hz = params.get("freq_hz")
        gain_db = params.get("gain_db")
        q = params.get("q")
        if freq_hz is None or gain_db is None:
            return None
        if q is None:
            q = 0.707
        return (
            f"equalizer=f={float(freq_hz):.6f}:"
            f"width_type=q:width={float(q):.6f}:g={float(gain_db):.6f}"
        )

    if primitive_class in {"output_trim", "final_balance"}:
        gain_db = params.get("gain_db")
        if gain_db is None:
            return None
        return f"volume={float(gain_db):.6f}dB"

    return None


def _apply_ffmpeg_ops(input_path: str, output_path: str, ops: Sequence[Dict[str, Any]]) -> str:
    filters: List[str] = []
    for op in ops:
        filt = _ffmpeg_filter_for_op(op)
        if filt:
            filters.append(filt)

    if not filters:
        return _copy_audio_like(input_path, output_path)

    filter_chain = ",".join(filters)
    cmd = (
        f"ffmpeg -y -hide_banner -i {_quote(input_path)} "
        f'-af "{filter_chain}" -ar 48000 -ac 2 -c:a pcm_s24le {_quote(output_path)}'
    )
    _run(cmd)
    return output_path


def _execute_output_trim_file(input_path: str, output_path: str, op: Dict[str, Any]) -> str:
    audio, sr = _load_audio(input_path)
    params = dict(op.get("params") or {})
    gain_db = float(params.get("gain_db", 0.0))

    processed = audio * _db_to_linear(gain_db)
    processed = np.clip(processed, -1.0, 1.0)
    _write_wav(output_path, processed, sr)
    return output_path


def _execute_true_peak_limiter_file(input_path: str, output_path: str, op: Dict[str, Any]) -> str:
    audio, sr = _load_audio(input_path)
    params = dict(op.get("params") or {})

    ceiling_db = float(params.get("gain_db", -1.0))
    threshold_db = float(params.get("threshold_db", -4.5))
    attack_ms = float(params.get("attack_ms", 0.25))
    release_ms = float(params.get("release_ms", 45.0))

    ceiling_lin = _db_to_linear(ceiling_db)
    threshold_lin = _db_to_linear(threshold_db)

    oversample_factor = 4
    os_sr = sr * oversample_factor
    audio_os = _resample_audio(audio, sr, os_sr)

    linked_peak = np.max(np.abs(audio_os), axis=1)
    engage_level = min(threshold_lin, ceiling_lin)

    desired_gain = np.ones_like(linked_peak, dtype=np.float32)
    active = linked_peak > engage_level
    if np.any(active):
        desired_gain[active] = np.minimum(
            1.0,
            ceiling_lin / np.maximum(linked_peak[active], 1e-8),
        ).astype(np.float32)

    smoothed_gain = _smooth_gain_samples(
        desired_gain,
        attack_ms=attack_ms,
        release_ms=release_ms,
        sr=os_sr,
    )

    limited_os = audio_os * smoothed_gain[:, None]
    limited_os = np.clip(limited_os, -ceiling_lin, ceiling_lin)

    limited = _resample_audio(limited_os, os_sr, sr)
    limited = _match_length(limited, len(audio))
    limited = np.clip(limited, -ceiling_lin, ceiling_lin)
    limited = np.clip(limited, -1.0, 1.0)

    _write_wav(output_path, limited, sr)
    return output_path


def _execute_delivery_backend_group(
    input_path: str,
    output_path: str,
    ops: Sequence[Dict[str, Any]],
    td: str,
    stack_name: str,
) -> Tuple[str, List[Dict[str, Any]], bool]:
    working_path = input_path
    unresolved: List[Dict[str, Any]] = []
    any_audio_change = False

    for idx, op in enumerate(ops):
        op_kind = str(op.get("op_kind") or "").lower()
        is_last = idx == len(ops) - 1
        step_output = output_path if is_last else _tmp_wav_path(
            td,
            f"{stack_name}__delivery_grp_{idx}",
        )

        if op_kind == "output_trim":
            working_path = _execute_output_trim_file(working_path, step_output, op)
            any_audio_change = True
            continue

        if op_kind == "true_peak_limiter":
            working_path = _execute_true_peak_limiter_file(working_path, step_output, op)
            any_audio_change = True
            continue

        unresolved.append(
            {
                "primitive_name": op.get("primitive_name"),
                "instance_name": op.get("instance_name"),
                "backend_hint": op.get("backend_hint") or "delivery_backend",
                "stack_name": stack_name,
            }
        )

    return working_path, unresolved, any_audio_change


def _group_ops_by_backend(ops: Sequence[Dict[str, Any]]) -> List[Tuple[str, List[Dict[str, Any]]]]:
    groups: List[Tuple[str, List[Dict[str, Any]]]] = []
    current_backend: Optional[str] = None
    current_ops: List[Dict[str, Any]] = []

    for op in ops:
        backend = op.get("backend_hint") or "custom_dsp_required"
        if current_backend is None:
            current_backend = backend
            current_ops = [op]
            continue
        if backend == current_backend:
            current_ops.append(op)
            continue
        groups.append((current_backend, current_ops))
        current_backend = backend
        current_ops = [op]

    if current_backend is not None:
        groups.append((current_backend, current_ops))

    return groups


def _stack_is_parallel(stack: Dict[str, Any]) -> bool:
    return stack.get("path_type") in {"parallel", "finish"} or stack.get("render_mode") in {
        "parallel_return",
        "parallel_assist_return",
        "finish_micro_return",
    }


def _execute_stack(
    stack: Dict[str, Any],
    source_path: str,
    output_path: str,
    td: str,
    *,
    custom_backend: Optional[CustomBackend] = None,
    fail_on_custom: bool = False,
) -> Dict[str, Any]:
    ops = list(stack.get("ops") or [])
    working_path = source_path
    executed_groups: List[Dict[str, Any]] = []
    unresolved_custom_ops: List[Dict[str, Any]] = []
    any_audio_change = False

    for group_index, (backend_hint, group_ops) in enumerate(_group_ops_by_backend(ops)):
        step_output = _tmp_wav_path(td, f"{stack.get('stack_name')}__grp_{group_index}")

        if backend_hint == "ffmpeg_safe":
            working_path = _apply_ffmpeg_ops(working_path, step_output, group_ops)
            any_audio_change = True
            executed_groups.append(
                {
                    "backend": backend_hint,
                    "status": "executed",
                    "op_names": [op.get("primitive_name") for op in group_ops],
                    "output_path": working_path,
                }
            )
            continue

        if backend_hint == "custom_dsp_required":
            if custom_backend is not None:
                working_path = custom_backend(group_ops, working_path, step_output, stack)
                any_audio_change = True
                executed_groups.append(
                    {
                        "backend": backend_hint,
                        "status": "executed",
                        "op_names": [op.get("primitive_name") for op in group_ops],
                        "output_path": working_path,
                    }
                )
                continue

            unresolved = [
                {
                    "primitive_name": op.get("primitive_name"),
                    "instance_name": op.get("instance_name"),
                    "backend_hint": backend_hint,
                    "stack_name": stack.get("stack_name"),
                }
                for op in group_ops
            ]
            unresolved_custom_ops.extend(unresolved)
            executed_groups.append(
                {
                    "backend": backend_hint,
                    "status": "unresolved",
                    "op_names": [op.get("primitive_name") for op in group_ops],
                }
            )
            if fail_on_custom:
                names = ", ".join(op["primitive_name"] for op in unresolved)
                raise RuntimeError(
                    f"Custom DSP backend required for stack={stack.get('stack_name')} ops={names}"
                )
            continue

        if backend_hint == "delivery_backend":
            delivery_output = step_output
            working_path, unresolved, delivery_changed = _execute_delivery_backend_group(
                input_path=working_path,
                output_path=delivery_output,
                ops=group_ops,
                td=td,
                stack_name=str(stack.get("stack_name") or "delivery"),
            )

            if delivery_changed:
                any_audio_change = True
                executed_groups.append(
                    {
                        "backend": backend_hint,
                        "status": "executed" if not unresolved else "partial",
                        "op_names": [op.get("primitive_name") for op in group_ops],
                        "output_path": working_path,
                    }
                )
            else:
                executed_groups.append(
                    {
                        "backend": backend_hint,
                        "status": "unresolved",
                        "op_names": [op.get("primitive_name") for op in group_ops],
                    }
                )

            if unresolved:
                unresolved_custom_ops.extend(unresolved)
                if fail_on_custom:
                    names = ", ".join(op["primitive_name"] for op in unresolved)
                    raise RuntimeError(
                        f"Delivery backend required for stack={stack.get('stack_name')} ops={names}"
                    )
            continue

        unresolved = [
            {
                "primitive_name": op.get("primitive_name"),
                "instance_name": op.get("instance_name"),
                "backend_hint": backend_hint,
                "stack_name": stack.get("stack_name"),
            }
            for op in group_ops
        ]
        unresolved_custom_ops.extend(unresolved)
        executed_groups.append(
            {
                "backend": backend_hint,
                "status": "unresolved",
                "op_names": [op.get("primitive_name") for op in group_ops],
            }
        )
        if fail_on_custom:
            names = ", ".join(op["primitive_name"] for op in unresolved)
            raise RuntimeError(
                f"Unknown backend required for stack={stack.get('stack_name')} ops={names}"
            )

    if not ops:
        if _stack_is_parallel(stack):
            _create_silence_like(source_path, output_path)
        else:
            _copy_audio_like(source_path, output_path)
    elif any_audio_change:
        _copy_audio_like(working_path, output_path)
    else:
        if _stack_is_parallel(stack):
            _create_silence_like(source_path, output_path)
        else:
            _copy_audio_like(source_path, output_path)

    return {
        "stack_name": stack.get("stack_name"),
        "output_path": output_path,
        "executed_groups": executed_groups,
        "unresolved_custom_ops": unresolved_custom_ops,
        "any_audio_change": any_audio_change,
    }


def _amix_to_target(
    source_paths: Sequence[str],
    output_path: str,
    *,
    weights: Optional[Sequence[float]] = None,
) -> str:
    if not source_paths:
        raise RuntimeError("amix requires at least one source path")

    if len(source_paths) == 1:
        return _copy_audio_like(source_paths[0], output_path)

    inputs = " ".join(f"-i {_quote(path)}" for path in source_paths)
    if weights is None:
        weights = [1.0] * len(source_paths)

    weight_str = " ".join(f"{float(w):.8f}" for w in weights)
    filter_chain = f"amix=inputs={len(source_paths)}:weights='{weight_str}':normalize=0"
    cmd = (
        f"ffmpeg -y -hide_banner {inputs} "
        f'-filter_complex "{filter_chain}" -ar 48000 -ac 2 -c:a pcm_s24le {_quote(output_path)}'
    )
    _run(cmd)
    return output_path


def _execute_recombine_plan(
    recombine: Dict[str, Any],
    node_registry: Dict[str, str],
    td: str,
) -> str:
    source_nodes = list(recombine.get("source_nodes") or [])
    source_paths = [node_registry[node] for node in source_nodes]
    target_node = recombine.get("target_node")
    target_path = _tmp_wav_path(td, target_node)

    kind = recombine.get("render_recombine_kind") or recombine.get("recombine_mode")
    blend = float(recombine.get("blend", 1.0) or 0.0)
    gain_db = float(recombine.get("gain_db", 0.0) or 0.0)
    linear = _db_to_linear(gain_db) * blend

    if kind in {"passthrough_or_sum", "sum"}:
        if len(source_paths) == 1:
            return _copy_audio_like(source_paths[0], target_path)
        weights = [1.0] * len(source_paths)
        return _amix_to_target(source_paths, target_path, weights=weights)

    if kind in {"guarded_parallel_sum", "assist_blend_sum", "finish_blend_sum"}:
        if len(source_paths) == 1:
            return _copy_audio_like(source_paths[0], target_path)

        weights: List[float] = []
        for idx, _ in enumerate(source_paths):
            if idx == 0 and len(source_paths) > 1 and kind != "guarded_parallel_sum":
                weights.append(1.0)
            elif idx == 0 and len(source_paths) == 2 and source_nodes[0].endswith("_out") and source_nodes[1].endswith("_bus"):
                weights.append(1.0)
            else:
                weights.append(linear)

        if kind == "guarded_parallel_sum" and len(source_paths) == 2 and source_nodes[1].endswith("_bus"):
            weights = [1.0, linear]
        elif kind == "guarded_parallel_sum" and len(source_paths) > 1 and not source_nodes[1].endswith("_bus"):
            weights = [1.0] * len(source_paths)

        return _amix_to_target(source_paths, target_path, weights=weights)

    raise RuntimeError(f"Unknown recombine kind: {kind}")


def execute_sm_render_plan(
    render_plan: Dict[str, Any],
    prepared_input_path: str,
    td: str,
    *,
    custom_backend: Optional[CustomBackend] = None,
    fail_on_custom: bool = False,
) -> Dict[str, Any]:
    os.makedirs(td, exist_ok=True)

    node_registry: Dict[str, str] = {
        render_plan["prepared_input_node"]: prepared_input_path,
    }
    stage_results: List[Dict[str, Any]] = []
    unresolved_custom_ops: List[Dict[str, Any]] = []

    for stage in render_plan.get("stages", []):
        stage_input_node = stage["input_node"]
        if stage_input_node not in node_registry:
            raise RuntimeError(f"Missing stage input node: {stage_input_node}")
        stage_input_path = node_registry[stage_input_node]

        stage_stack_results: List[Dict[str, Any]] = []
        for stack in stage.get("stacks", []):
            tap_node = stack.get("tap_point") or stage_input_node
            if tap_node not in node_registry:
                raise RuntimeError(
                    f"Missing tap node '{tap_node}' for stack '{stack.get('stack_name')}'"
                )
            stack_source_path = node_registry[tap_node]
            stack_output_node = stack.get("output_node")
            stack_output_path = _tmp_wav_path(td, stack_output_node)

            stack_result = _execute_stack(
                stack,
                stack_source_path,
                stack_output_path,
                td,
                custom_backend=custom_backend,
                fail_on_custom=fail_on_custom,
            )
            node_registry[stack_output_node] = stack_output_path
            stage_stack_results.append(stack_result)
            unresolved_custom_ops.extend(stack_result["unresolved_custom_ops"])

        stage_recombine_results: List[Dict[str, Any]] = []
        for recombine in stage.get("recombine", []):
            recombined_path = _execute_recombine_plan(recombine, node_registry, td)
            target_node = recombine.get("target_node")
            node_registry[target_node] = recombined_path
            stage_recombine_results.append(
                {
                    "recombine_name": recombine.get("recombine_name"),
                    "target_node": target_node,
                    "output_path": recombined_path,
                }
            )

        stage_output_node = stage.get("output_node")
        if stage_output_node not in node_registry:
            if stage_stack_results:
                last_stack_output = stage_stack_results[-1]["output_path"]
                final_stage_output = _tmp_wav_path(td, stage_output_node)
                _copy_audio_like(last_stack_output, final_stage_output)
                node_registry[stage_output_node] = final_stage_output
            else:
                fallback_output = _tmp_wav_path(td, stage_output_node)
                _copy_audio_like(stage_input_path, fallback_output)
                node_registry[stage_output_node] = fallback_output

        stage_results.append(
            {
                "stage_name": stage.get("stage_name"),
                "stage_kind": stage.get("stage_kind"),
                "input_node": stage_input_node,
                "input_path": stage_input_path,
                "output_node": stage_output_node,
                "output_path": node_registry[stage_output_node],
                "stack_results": stage_stack_results,
                "recombine_results": stage_recombine_results,
                "requires_custom_dsp": bool(stage.get("requires_custom_dsp", False)),
            }
        )

    final_output_node = render_plan["final_output_node"]
    if final_output_node not in node_registry:
        raise RuntimeError(f"Final output node missing after execution: {final_output_node}")

    return {
        "prepared_input_node": render_plan.get("prepared_input_node"),
        "prepared_input_path": prepared_input_path,
        "final_output_node": final_output_node,
        "final_output_path": node_registry[final_output_node],
        "node_registry": node_registry,
        "stage_results": stage_results,
        "unresolved_custom_ops": unresolved_custom_ops,
        "has_unresolved_custom_ops": bool(unresolved_custom_ops),
        "notes": [
            "render_executor_attached",
            "stage_by_stage_execution_completed",
            "ffmpeg_safe_ops_executed",
            "custom_dsp_ops_routed_or_marked_unresolved",
            "delivery_backend_supported",
        ],
    }

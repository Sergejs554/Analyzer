from __future__ import annotations

import os
import subprocess
from dataclasses import asdict, dataclass, field
from typing import Any


def _obj_get(value: Any, key: str, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _safe_name(value: str) -> str:
    text = (value or "item").strip().lower()
    out = []
    for ch in text:
        if ch.isalnum():
            out.append(ch)
        else:
            out.append("_")
    return "".join(out).strip("_") or "item"


def _run(cmd: list[str]) -> tuple[str, str]:
    p = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    stdout = p.stdout.decode("utf-8", errors="ignore")
    stderr = p.stderr.decode("utf-8", errors="ignore")
    if p.returncode != 0:
        raise RuntimeError(stderr[:4000] or stdout[:4000] or "ffmpeg/ffprobe failed")
    return stdout, stderr


def _probe_duration_sec(path: str) -> float:
    stdout, _ = _run([
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        path,
    ])
    try:
        return max(0.0, float(stdout.strip()))
    except Exception:
        return 0.0


def _ensure_wav_copy(src_path: str, dst_path: str) -> str:
    _run([
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-i", src_path,
        "-ar", "48000",
        "-ac", "2",
        "-c:a", "pcm_s16le",
        dst_path,
    ])
    return dst_path


def _copy_audio(src_path: str, dst_path: str) -> str:
    return _ensure_wav_copy(src_path, dst_path)


def _make_silence_like(src_path: str, dst_path: str) -> str:
    duration = max(0.01, _probe_duration_sec(src_path))
    _run([
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-f", "lavfi",
        "-i", f"anullsrc=r=48000:cl=stereo:d={duration:.6f}",
        "-c:a", "pcm_s16le",
        dst_path,
    ])
    return dst_path


def _apply_volume(src_path: str, dst_path: str, gain_db: float) -> str:
    if abs(gain_db) < 1e-9:
        return _copy_audio(src_path, dst_path)
    _run([
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-i", src_path,
        "-af", f"volume={gain_db}dB",
        "-ar", "48000",
        "-ac", "2",
        "-c:a", "pcm_s16le",
        dst_path,
    ])
    return dst_path


def _ffmpeg_equalizer_filter(freq_hz: float, gain_db: float, q: float) -> str:
    freq_hz = max(20.0, float(freq_hz or 1000.0))
    q = max(0.1, float(q or 1.0))
    gain_db = float(gain_db or 0.0)
    return f"equalizer=f={freq_hz}:width_type=q:w={q}:g={gain_db}"


def _apply_static_eq(src_path: str, dst_path: str, params: dict[str, Any]) -> str:
    filt = _ffmpeg_equalizer_filter(
        params.get("freq_hz"),
        params.get("gain_db"),
        params.get("q"),
    )
    _run([
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-i", src_path,
        "-af", filt,
        "-ar", "48000",
        "-ac", "2",
        "-c:a", "pcm_s16le",
        dst_path,
    ])
    return dst_path


def _apply_parallel_eq_fill(src_path: str, dst_path: str, params: dict[str, Any]) -> str:
    mix = max(0.0, min(1.0, float(params.get("mix") or 0.0)))
    filt = _ffmpeg_equalizer_filter(
        params.get("freq_hz"),
        params.get("gain_db"),
        params.get("q"),
    )
    chain = f"{filt},volume={mix}"
    _run([
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-i", src_path,
        "-af", chain,
        "-ar", "48000",
        "-ac", "2",
        "-c:a", "pcm_s16le",
        dst_path,
    ])
    return dst_path


def _apply_parallel_compressor(src_path: str, dst_path: str, params: dict[str, Any]) -> str:
    mix = max(0.0, min(1.0, float(params.get("mix") or 0.0)))
    ratio = max(1.0, float(params.get("ratio") or 1.2))
    attack_ms = max(0.1, float(params.get("attack_ms") or 10.0))
    release_ms = max(1.0, float(params.get("release_ms") or 100.0))
    threshold_db = float(params.get("threshold_db") or -18.0)
    filt = (
        "acompressor="
        f"threshold={threshold_db}dB:"
        f"ratio={ratio}:"
        f"attack={attack_ms}:"
        f"release={release_ms},"
        f"volume={mix}"
    )
    _run([
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-i", src_path,
        "-af", filt,
        "-ar", "48000",
        "-ac", "2",
        "-c:a", "pcm_s16le",
        dst_path,
    ])
    return dst_path


@dataclass
class UnsupportedRenderOp:
    stage_name: str
    stack_name: str
    instance_name: str
    primitive_name: str
    op_kind: str
    backend_hint: str
    reason: str


@dataclass
class StackExecutionReport:
    stage_name: str
    stack_name: str
    render_mode: str
    input_path: str
    output_path: str
    applied_ops: list[str] = field(default_factory=list)
    skipped_ops: list[str] = field(default_factory=list)
    partial: bool = False
    requires_custom_dsp: bool = False


@dataclass
class StageExecutionReport:
    stage_name: str
    input_node: str
    output_node: str
    input_path: str
    output_path: str
    stack_reports: list[StackExecutionReport] = field(default_factory=list)
    recombine_reports: list[dict[str, Any]] = field(default_factory=list)
    partial: bool = False
    requires_custom_dsp: bool = False


@dataclass
class RenderExecutionResult:
    ok: bool
    status: str
    final_output_path: str | None
    node_paths: dict[str, str] = field(default_factory=dict)
    stage_reports: list[StageExecutionReport] = field(default_factory=list)
    unsupported_ops: list[UnsupportedRenderOp] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_debug_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "status": self.status,
            "final_output_path": self.final_output_path,
            "node_paths": dict(self.node_paths),
            "stage_reports": [asdict(x) for x in self.stage_reports],
            "unsupported_ops": [asdict(x) for x in self.unsupported_ops],
            "notes": list(self.notes),
        }


class RenderExecutionError(RuntimeError):
    pass


def _normalize_op(op: Any) -> dict[str, Any]:
    params = dict(_obj_get(op, "params", {}) or {})
    return {
        "instance_name": _obj_get(op, "instance_name", "op"),
        "primitive_name": _obj_get(op, "primitive_name", "unknown_primitive"),
        "op_kind": _obj_get(op, "op_kind", "unknown"),
        "backend_hint": _obj_get(op, "backend_hint", "custom_dsp_required"),
        "channel_mode": (_obj_get(op, "channel_mode", "stereo") or "stereo"),
        "params": params,
        "enabled": bool(_obj_get(op, "enabled", True)),
    }


def _supported_op_executor(op: dict[str, Any]):
    op_kind = op["op_kind"]
    channel_mode = (op.get("channel_mode") or "stereo").lower().strip()

    if op_kind in {"static_eq", "broad_eq"} and channel_mode == "stereo":
        return _apply_static_eq, None
    if op_kind == "parallel_eq_fill" and channel_mode == "stereo":
        return _apply_parallel_eq_fill, None
    if op_kind == "parallel_compressor" and channel_mode == "stereo":
        return _apply_parallel_compressor, None

    if op_kind in {"static_eq", "broad_eq"} and channel_mode != "stereo":
        return None, f"channel_mode={channel_mode} is not implemented safely for ffmpeg eq"
    return None, "custom dsp backend required"


def _stack_input_path(node_paths: dict[str, str], stage_input_path: str, stack: Any) -> str:
    tap_point = _obj_get(stack, "tap_point")
    if tap_point and tap_point in node_paths:
        return node_paths[tap_point]
    output_node = _obj_get(stack, "output_node")
    if output_node and output_node in node_paths:
        return node_paths[output_node]
    return stage_input_path


def _temp_wav(td: str, prefix: str, suffix: str) -> str:
    name = f"{_safe_name(prefix)}__{_safe_name(suffix)}.wav"
    return os.path.join(td, name)


def _execute_stack(
    *,
    stage_name: str,
    stack: Any,
    node_paths: dict[str, str],
    stage_input_path: str,
    td: str,
    unsupported_ops: list[UnsupportedRenderOp],
    allow_partial_custom_dsp: bool,
) -> tuple[str, StackExecutionReport, bool]:
    stack_name = _obj_get(stack, "stack_name", "stack")
    render_mode = _obj_get(stack, "render_mode", "serial_inplace")
    output_node = _obj_get(stack, "output_node", f"{stack_name}_out")
    input_path = _stack_input_path(node_paths, stage_input_path, stack)
    ops = [_normalize_op(op) for op in _as_list(_obj_get(stack, "ops")) if bool(_obj_get(op, "enabled", True))]

    report = StackExecutionReport(
        stage_name=stage_name,
        stack_name=stack_name,
        render_mode=render_mode,
        input_path=input_path,
        output_path="",
        applied_ops=[],
        skipped_ops=[],
        partial=False,
        requires_custom_dsp=False,
    )

    if render_mode in {"parallel_return", "parallel_assist_return"}:
        current_path = _temp_wav(td, stack_name, "branch_input")
        _copy_audio(input_path, current_path)
    else:
        current_path = _temp_wav(td, stack_name, "serial_input")
        _copy_audio(input_path, current_path)

    any_supported = False
    partial = False

    for index, op in enumerate(ops):
        op_exec, unsupported_reason = _supported_op_executor(op)
        if op_exec is None:
            report.requires_custom_dsp = True
            report.skipped_ops.append(op["primitive_name"])
            unsupported_ops.append(
                UnsupportedRenderOp(
                    stage_name=stage_name,
                    stack_name=stack_name,
                    instance_name=op["instance_name"],
                    primitive_name=op["primitive_name"],
                    op_kind=op["op_kind"],
                    backend_hint=op["backend_hint"],
                    reason=unsupported_reason or "unsupported op",
                )
            )
            if not allow_partial_custom_dsp:
                raise RenderExecutionError(
                    f"{stage_name}/{stack_name}: unsupported op {op['primitive_name']} ({unsupported_reason})"
                )
            partial = True
            continue

        next_path = _temp_wav(td, stack_name, f"op_{index:02d}_{op['primitive_name']}")
        op_exec(current_path, next_path, dict(op["params"]))
        current_path = next_path
        any_supported = True
        report.applied_ops.append(op["primitive_name"])

    if render_mode in {"parallel_return", "parallel_assist_return"} and not any_supported:
        current_path = _temp_wav(td, stack_name, "silent_branch")
        _make_silence_like(input_path, current_path)
        partial = partial or bool(ops)

    final_path = _temp_wav(td, stack_name, "out")
    _copy_audio(current_path, final_path)

    report.output_path = final_path
    report.partial = partial
    node_paths[output_node] = final_path
    return final_path, report, partial


def _mix_many(
    *,
    source_paths: list[str],
    target_path: str,
    gain_db: float = 0.0,
    second_input_blend: float | None = None,
) -> str:
    if not source_paths:
        raise RenderExecutionError("mix_many called without sources")

    if len(source_paths) == 1:
        return _apply_volume(source_paths[0], target_path, gain_db)

    cmd = ["ffmpeg", "-y", "-hide_banner"]
    for src in source_paths:
        cmd.extend(["-i", src])

    if second_input_blend is not None and len(source_paths) >= 2:
        weights = ["1.0", str(float(second_input_blend))]
        for _ in range(len(source_paths) - 2):
            weights.append("1.0")
        amix = f"amix=inputs={len(source_paths)}:normalize=0:weights={' '.join(weights)}"
    else:
        amix = f"amix=inputs={len(source_paths)}:normalize=0"

    if abs(gain_db) > 1e-9:
        amix = f"{amix},volume={gain_db}dB"

    cmd.extend([
        "-filter_complex", amix,
        "-ar", "48000",
        "-ac", "2",
        "-c:a", "pcm_s16le",
        target_path,
    ])
    _run(cmd)
    return target_path


def _execute_recombine(
    *,
    recombine: Any,
    node_paths: dict[str, str],
    td: str,
) -> tuple[str, dict[str, Any]]:
    recombine_name = _obj_get(recombine, "recombine_name", "recombine")
    target_node = _obj_get(recombine, "target_node", "recombine_out")
    source_nodes = list(_as_list(_obj_get(recombine, "source_nodes")))
    gain_db = float(_obj_get(recombine, "gain_db", 0.0) or 0.0)
    render_kind = _obj_get(recombine, "render_recombine_kind", "passthrough_or_sum")
    blend = _obj_get(recombine, "blend", None)

    source_paths = []
    for node in source_nodes:
        if node not in node_paths:
            raise RenderExecutionError(f"recombine {recombine_name}: missing source node {node}")
        source_paths.append(node_paths[node])

    out_path = _temp_wav(td, recombine_name, "out")

    if render_kind == "assist_blend_sum":
        _mix_many(
            source_paths=source_paths,
            target_path=out_path,
            gain_db=gain_db,
            second_input_blend=float(blend or 0.0),
        )
    else:
        _mix_many(
            source_paths=source_paths,
            target_path=out_path,
            gain_db=gain_db,
            second_input_blend=None,
        )

    node_paths[target_node] = out_path
    info = {
        "recombine_name": recombine_name,
        "target_node": target_node,
        "source_nodes": source_nodes,
        "source_paths": source_paths,
        "output_path": out_path,
        "render_recombine_kind": render_kind,
        "gain_db": gain_db,
        "blend": blend,
    }
    return out_path, info


def execute_dsp_render_plan(
    *,
    input_path: str,
    render_plan: Any,
    td: str,
    allow_partial_custom_dsp: bool = True,
) -> RenderExecutionResult:
    os.makedirs(td, exist_ok=True)

    node_paths: dict[str, str] = {}
    prepared_input_node = _obj_get(render_plan, "prepared_input_node", "prepared_input")
    prepared_input_path = _temp_wav(td, prepared_input_node, "source")
    _copy_audio(input_path, prepared_input_path)
    node_paths[prepared_input_node] = prepared_input_path

    stage_reports: list[StageExecutionReport] = []
    unsupported_ops: list[UnsupportedRenderOp] = []
    notes = list(_as_list(_obj_get(render_plan, "notes")))

    for stage in _as_list(_obj_get(render_plan, "stages")):
        stage_name = _obj_get(stage, "stage_name", "stage")
        input_node = _obj_get(stage, "input_node", prepared_input_node)
        output_node = _obj_get(stage, "output_node", f"{stage_name}_out")

        if input_node not in node_paths:
            raise RenderExecutionError(f"{stage_name}: missing input node {input_node}")

        stage_input_path = node_paths[input_node]
        stack_reports: list[StackExecutionReport] = []
        recombine_reports: list[dict[str, Any]] = []
        stage_partial = False
        stage_requires_custom = bool(_obj_get(stage, "requires_custom_dsp", False))

        for stack in _as_list(_obj_get(stage, "stacks")):
            _, stack_report, partial = _execute_stack(
                stage_name=stage_name,
                stack=stack,
                node_paths=node_paths,
                stage_input_path=stage_input_path,
                td=td,
                unsupported_ops=unsupported_ops,
                allow_partial_custom_dsp=allow_partial_custom_dsp,
            )
            stage_partial = stage_partial or partial
            stage_requires_custom = stage_requires_custom or stack_report.requires_custom_dsp
            stack_reports.append(stack_report)

        for recombine in _as_list(_obj_get(stage, "recombine")):
            out_path, info = _execute_recombine(
                recombine=recombine,
                node_paths=node_paths,
                td=td,
            )
            recombine_reports.append(info)
            if info.get("target_node") == output_node:
                node_paths[output_node] = out_path

        if output_node not in node_paths:
            if stack_reports:
                node_paths[output_node] = stack_reports[-1].output_path
            else:
                passthrough_path = _temp_wav(td, stage_name, "passthrough")
                _copy_audio(stage_input_path, passthrough_path)
                node_paths[output_node] = passthrough_path

        stage_reports.append(
            StageExecutionReport(
                stage_name=stage_name,
                input_node=input_node,
                output_node=output_node,
                input_path=stage_input_path,
                output_path=node_paths[output_node],
                stack_reports=stack_reports,
                recombine_reports=recombine_reports,
                partial=stage_partial,
                requires_custom_dsp=stage_requires_custom,
            )
        )

    final_output_node = _obj_get(render_plan, "final_output_node", "final_output")
    if final_output_node not in node_paths:
        raise RenderExecutionError(f"missing final output node {final_output_node}")

    final_output_path = node_paths[final_output_node]
    status = "ok"
    if unsupported_ops:
        status = "partial_custom_dsp_pending"

    return RenderExecutionResult(
        ok=True,
        status=status,
        final_output_path=final_output_path,
        node_paths=node_paths,
        stage_reports=stage_reports,
        unsupported_ops=unsupported_ops,
        notes=notes,
    )


def build_render_execution_report(
    *,
    input_path: str,
    render_plan: Any,
    td: str,
    allow_partial_custom_dsp: bool = True,
) -> dict[str, Any]:
    result = execute_dsp_render_plan(
        input_path=input_path,
        render_plan=render_plan,
        td=td,
        allow_partial_custom_dsp=allow_partial_custom_dsp,
    )
    return result.to_debug_dict()

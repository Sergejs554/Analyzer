from __future__ import annotations

import json
import os
import shlex
import subprocess
from typing import Any, Dict, Optional

from .analysis import analyze_sm_input


SM_METRIC_KEYS = [
    "integrated_lufs",
    "true_peak_dbtp",
    "sample_peak_dbfs",
    "rms_dbfs",
    "crest_db",
    "plr_proxy_db",
    "lra_ebu",
    "punch_proxy",
    "transient_index",
    "limiter_stress_proxy",
    "near_clip_ratio",
    "momentary_to_integrated_gap_db",
    "short_term_to_integrated_gap_db",

    "body_150_400_db",
    "low_body_150_300_db",
    "lowmid_120_300_db",
    "lowmid_buildup_200_400_db",
    "mud_200_500_db",
    "mud_to_body_db",
    "lowmid_buildup_ratio_db",

    "bass_60_120_db",
    "bass_to_body_db",
    "low_foundation_50_100_db",
    "low_foundation_ratio_db",
    "sub_to_body_db",

    "mid_1k_2k_db",
    "presence_2k_5k_db",
    "presence_to_body_db",

    "harsh_2p5k_6k_db",
    "harsh_to_mid_db",
    "harshness_index",
    "sibilance_5k_9k_db",
    "sibilance_index",

    "air_8k_12k_db",
    "air_8k_16k_db",
    "air16_to_body_db",
    "air_ratio_db",
    "tilt_indicator_db",
]


DELTA_KEYS = [
    "integrated_lufs",
    "true_peak_dbtp",
    "sample_peak_dbfs",
    "rms_dbfs",
    "crest_db",
    "plr_proxy_db",
    "lra_ebu",
    "punch_proxy",
    "transient_index",
    "limiter_stress_proxy",
    "near_clip_ratio",

    "body_150_400_db",
    "low_body_150_300_db",
    "lowmid_120_300_db",
    "lowmid_buildup_200_400_db",
    "mud_200_500_db",
    "mud_to_body_db",
    "lowmid_buildup_ratio_db",

    "bass_to_body_db",
    "low_foundation_ratio_db",
    "sub_to_body_db",

    "mid_1k_2k_db",
    "presence_2k_5k_db",
    "presence_to_body_db",

    "harshness_index",
    "sibilance_index",
]


def _run(cmd: str, timeout_sec: int = 420) -> tuple[str, str]:
    p = subprocess.run(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_sec,
    )
    if p.returncode != 0:
        raise RuntimeError(p.stderr.decode("utf-8", errors="ignore")[:4000])
    return (
        p.stdout.decode("utf-8", errors="ignore"),
        p.stderr.decode("utf-8", errors="ignore"),
    )


def _read(obj: Any, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _to_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _to_int(x, default=None):
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _round(x: Any, ndigits: int = 6):
    value = _to_float(x)
    if value is None:
        return None
    return round(value, ndigits)


def _normalize_format(x: str) -> str:
    x = (x or "wav16").lower().strip()
    if x in ("wav", "wav16"):
        return "wav16"
    if x in ("wav24",):
        return "wav24"
    if x in ("flac",):
        return "flac"
    if x in ("mp3", "mp3_320"):
        return "mp3_320"
    if x in ("aiff", "aif"):
        return "aiff"
    return "wav16"


def _format_ext(fmt: str) -> str:
    fmt = _normalize_format(fmt)
    if fmt == "wav16":
        return "wav"
    if fmt == "wav24":
        return "wav"
    if fmt == "flac":
        return "flac"
    if fmt == "mp3_320":
        return "mp3"
    if fmt == "aiff":
        return "aiff"
    return "wav"


def _ensure_file(path: str) -> None:
    if not path:
        raise RuntimeError("final_output_path missing")
    if not os.path.isfile(path):
        raise RuntimeError(f"final output file not found: {path}")
    if os.path.getsize(path) <= 0:
        raise RuntimeError(f"final output file empty: {path}")


def _file_exists(path: Optional[str]) -> bool:
    return bool(path and os.path.isfile(path) and os.path.getsize(path) > 0)


def _ffprobe_json(path: str) -> Dict[str, Any]:
    cmd = (
        f"ffprobe -v error "
        f"-show_entries format=format_name,duration,size,bit_rate "
        f"-show_entries stream=index,codec_type,codec_name,sample_rate,channels,bits_per_sample "
        f"-of json {shlex.quote(path)}"
    )
    stdout, _ = _run(cmd, timeout_sec=120)
    try:
        return json.loads(stdout)
    except Exception:
        return {}


def _first_audio_stream(probe: Dict[str, Any]) -> Dict[str, Any]:
    streams = probe.get("streams") or []
    for stream in streams:
        if stream.get("codec_type") == "audio":
            return stream
    return {}


def _collect_sm_metrics(analysis: Any) -> Dict[str, Any]:
    metrics = _read(analysis, "metrics")
    out: Dict[str, Any] = {}

    for key in SM_METRIC_KEYS:
        out[key] = _round(_read(metrics, key))

    return out


def _extract_analysis_flags(analysis: Any) -> Dict[str, Any]:
    flags = _read(analysis, "global_flags", {}) or {}
    out: Dict[str, Any] = {}

    for key in [
        "dense_behavior_candidate",
        "thin_behavior_candidate",
        "punch_fragile_candidate",
        "top_risk_candidate",
        "neutral_preclean_applied",
        "neutral_preclean_afftdn",
        "tone",
        "intensity",
        "fmt",
    ]:
        out[key] = _read(flags, key)

    return out


def inspect_audio_state(path: Optional[str], label: str, strict: bool = False) -> Dict[str, Any]:
    if not path:
        if strict:
            raise RuntimeError(f"{label}_path_missing")
        return {
            "status": "missing",
            "label": label,
            "path": path,
            "reason": "path_missing",
            "metrics": {},
        }

    if not _file_exists(path):
        if strict:
            raise RuntimeError(f"{label}_file_missing_or_empty: {path}")
        return {
            "status": "missing",
            "label": label,
            "path": path,
            "reason": "file_missing_or_empty",
            "metrics": {},
        }

    probe = _ffprobe_json(path)
    audio_stream = _first_audio_stream(probe)
    fmt = probe.get("format") or {}

    analysis = analyze_sm_input(path)
    metrics = _collect_sm_metrics(analysis)

    return {
        "status": "ok",
        "label": label,
        "path": path,
        "probe": probe,
        "metrics": metrics,
        "analysis_flags": _extract_analysis_flags(analysis),
        "duration_sec": _to_float(fmt.get("duration"), 0.0),
        "filesize_bytes": _to_int(fmt.get("size"), 0),
        "bitrate_bps": _to_int(fmt.get("bit_rate")),
        "format_name": fmt.get("format_name"),
        "codec_name": audio_stream.get("codec_name"),
        "sample_rate_hz": _to_int(audio_stream.get("sample_rate")),
        "channels": _to_int(audio_stream.get("channels")),
        "bits_per_sample": _to_int(audio_stream.get("bits_per_sample")),
    }


def inspect_render_output(final_output_path: str) -> Dict[str, Any]:
    state = inspect_audio_state(final_output_path, "post_delivery", strict=True)

    return {
        "status": "ok",
        "final_output_path": final_output_path,
        "probe": state.get("probe", {}),
        "delivery_metrics": state.get("metrics", {}),
        "duration_sec": state.get("duration_sec"),
        "filesize_bytes": state.get("filesize_bytes"),
        "bitrate_bps": state.get("bitrate_bps"),
        "format_name": state.get("format_name"),
        "codec_name": state.get("codec_name"),
        "sample_rate_hz": state.get("sample_rate_hz"),
        "channels": state.get("channels"),
        "bits_per_sample": state.get("bits_per_sample"),
    }


def _metric(state: Dict[str, Any], key: str):
    return _to_float((state.get("metrics") or {}).get(key))


def _delta_value(before_state: Dict[str, Any], after_state: Dict[str, Any], key: str):
    before = _metric(before_state, key)
    after = _metric(after_state, key)
    if before is None or after is None:
        return None
    return round(after - before, 6)


def _build_pair_delta(before_state: Dict[str, Any], after_state: Dict[str, Any]) -> Dict[str, Any]:
    if before_state.get("status") != "ok" or after_state.get("status") != "ok":
        return {
            "available": False,
            "reason": "missing_stage",
            "deltas": {},
        }

    deltas: Dict[str, Any] = {}
    for key in DELTA_KEYS:
        deltas[key] = _delta_value(before_state, after_state, key)

    return {
        "available": True,
        "before": before_state.get("label"),
        "after": after_state.get("label"),
        "deltas": deltas,
    }


def build_delta_report(
    input_state: Dict[str, Any],
    pre_delivery_state: Dict[str, Any],
    post_delivery_state: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "input_to_pre": _build_pair_delta(input_state, pre_delivery_state),
        "pre_to_post": _build_pair_delta(pre_delivery_state, post_delivery_state),
        "input_to_post": _build_pair_delta(input_state, post_delivery_state),
    }


def _delta(delta_report: Dict[str, Any], pair: str, key: str):
    block = delta_report.get(pair) or {}
    if not block.get("available"):
        return None
    return _to_float((block.get("deltas") or {}).get(key))


def infer_material_context(metrics: Dict[str, Any]) -> Dict[str, Any]:
    integrated_lufs = _to_float(metrics.get("integrated_lufs"))
    true_peak = _to_float(metrics.get("true_peak_dbtp"))
    crest = _to_float(metrics.get("crest_db"))
    punch = _to_float(metrics.get("punch_proxy"))
    plr = _to_float(metrics.get("plr_proxy_db"))
    body = _to_float(metrics.get("body_150_400_db"))
    low_body = _to_float(metrics.get("low_body_150_300_db"))
    mud_to_body = _to_float(metrics.get("mud_to_body_db"))
    buildup_ratio = _to_float(metrics.get("lowmid_buildup_ratio_db"))
    bass_to_body = _to_float(metrics.get("bass_to_body_db"))
    presence_to_body = _to_float(metrics.get("presence_to_body_db"))
    harshness = _to_float(metrics.get("harshness_index"))
    sibilance = _to_float(metrics.get("sibilance_index"))
    near_clip = _to_float(metrics.get("near_clip_ratio"))
    limiter_stress = _to_float(metrics.get("limiter_stress_proxy"))

    flags: list[str] = []

    if integrated_lufs is not None and integrated_lufs <= -12.5:
        flags.append("quiet_integrated_level")

    if true_peak is not None and true_peak > -0.70:
        flags.append("peak_headroom_blocked")

    if near_clip is not None and near_clip > 0.001:
        flags.append("near_clip_activity")

    if limiter_stress is not None and limiter_stress >= 1.20:
        flags.append("limiter_stress_candidate")

    if harshness is not None and harshness > -8.0:
        flags.append("top_harshness_risk")

    if sibilance is not None and sibilance > 2.0:
        flags.append("sibilance_risk")

    if crest is not None and crest >= 11.5 and punch is not None and punch >= 12.0:
        flags.append("transient_structure_present")

    if plr is not None and plr >= 10.0:
        flags.append("plr_structure_present")

    if body is not None and body >= 30.0 and low_body is not None and low_body >= 30.0:
        flags.append("body_foundation_present")

    if mud_to_body is not None and mud_to_body >= -0.20:
        flags.append("mud_close_to_body")

    if buildup_ratio is not None and buildup_ratio >= 16.0:
        flags.append("lowmid_buildup_candidate")

    if bass_to_body is not None and bass_to_body >= 8.0:
        flags.append("strong_bass_to_body_weight")

    if presence_to_body is not None and presence_to_body > -15.0:
        flags.append("forward_presence_already_present")

    material_class = "unknown"

    if "quiet_integrated_level" in flags and "peak_headroom_blocked" in flags:
        material_class = "quiet_peak_blocked"
    elif "quiet_integrated_level" in flags:
        material_class = "quiet_restore"
    elif (
        "body_foundation_present" in flags
        and "mud_close_to_body" in flags
        and "lowmid_buildup_candidate" in flags
    ):
        material_class = "dirty_dense"
    elif (
        "body_foundation_present" in flags
        and "transient_structure_present" in flags
        and "plr_structure_present" in flags
        and (mud_to_body is None or mud_to_body <= 0.35)
    ):
        material_class = "studio_dense"
    elif "top_harshness_risk" in flags or "sibilance_risk" in flags:
        material_class = "top_risky"

    return {
        "material_class": material_class,
        "flags": flags,
        "source": "post_render_metric_inference",
    }


def _find_delivery_stage(render_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(render_context, dict):
        return {}

    stage_reports = render_context.get("stage_reports") or []
    for stage in stage_reports:
        if (
            stage.get("stage_kind") == "delivery"
            or stage.get("stage_name") == "delivery_protect"
        ):
            return stage

    stages = render_context.get("stages") or []
    for stage in stages:
        if (
            stage.get("stage_kind") == "delivery"
            or stage.get("stage_name") == "delivery_protect"
        ):
            return stage

    return {}


def _extract_delivery_report_from_render_context(render_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    stage = _find_delivery_stage(render_context)
    report: Dict[str, Any] = {
        "source": "render_context",
        "available": bool(stage),
        "output_gain_trim_db": None,
        "limiter_threshold_db": None,
        "limiter_gain_db": None,
        "limiter_mix": None,
        "limiter_attack_ms": None,
        "limiter_release_ms": None,
        "active_clamps": [],
        "ops": [],
    }

    if not stage:
        return report

    report["active_clamps"] = stage.get("active_clamps", []) or []

    stack_reports = stage.get("stack_reports") or []
    role_stacks = stage.get("role_stacks") or stage.get("stacks") or []

    ops: list[Dict[str, Any]] = []

    for stack in stack_reports:
        for op in stack.get("ops") or []:
            ops.append(op)

    for stack in role_stacks:
        for op in stack.get("ops") or stack.get("primitive_instances") or []:
            ops.append(op)

    report["ops"] = ops

    for op in ops:
        primitive_name = op.get("primitive_name")
        op_kind = op.get("op_kind")
        params = op.get("params") or {}

        if primitive_name == "output_gain_trim" or op_kind == "output_trim":
            report["output_gain_trim_db"] = _round(params.get("gain_db"))

        if primitive_name == "true_peak_limiter" or op_kind == "true_peak_limiter":
            report["limiter_threshold_db"] = _round(params.get("threshold_db"))
            report["limiter_gain_db"] = _round(params.get("gain_db"))
            report["limiter_mix"] = _round(params.get("mix"))
            report["limiter_attack_ms"] = _round(params.get("attack_ms"))
            report["limiter_release_ms"] = _round(params.get("release_ms"))

    return report


def _has_real_tp_emergency(pre_metrics: Dict[str, Any], input_metrics: Dict[str, Any]) -> bool:
    pre_tp = _to_float(pre_metrics.get("true_peak_dbtp"))
    pre_peak = _to_float(pre_metrics.get("sample_peak_dbfs"))
    pre_near_clip = _to_float(pre_metrics.get("near_clip_ratio"))
    pre_limiter_stress = _to_float(pre_metrics.get("limiter_stress_proxy"))

    input_tp = _to_float(input_metrics.get("true_peak_dbtp"))
    input_near_clip = _to_float(input_metrics.get("near_clip_ratio"))

    if pre_tp is not None and pre_tp > -0.70:
        return True
    if input_tp is not None and input_tp > -0.40:
        return True
    if pre_peak is not None and pre_peak >= -0.25:
        return True
    if pre_near_clip is not None and pre_near_clip > 0.002:
        return True
    if input_near_clip is not None and input_near_clip > 0.002:
        return True
    if pre_limiter_stress is not None and pre_limiter_stress >= 1.25:
        return True

    return False


def build_delivery_audit(
    input_state: Dict[str, Any],
    pre_delivery_state: Dict[str, Any],
    post_delivery_state: Dict[str, Any],
    delta_report: Dict[str, Any],
    delivery_report: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    input_metrics = input_state.get("metrics") or {}
    pre_metrics = pre_delivery_state.get("metrics") or {}
    post_metrics = post_delivery_state.get("metrics") or {}

    delivery_report = delivery_report or {}
    real_tp_emergency = _has_real_tp_emergency(pre_metrics, input_metrics)

    output_gain_trim_db = _to_float(delivery_report.get("output_gain_trim_db"))
    limiter_threshold_db = _to_float(delivery_report.get("limiter_threshold_db"))
    limiter_gain_db = _to_float(delivery_report.get("limiter_gain_db"))

    pre_to_post_lufs = _delta(delta_report, "pre_to_post", "integrated_lufs")
    pre_to_post_crest = _delta(delta_report, "pre_to_post", "crest_db")
    pre_to_post_punch = _delta(delta_report, "pre_to_post", "punch_proxy")
    pre_to_post_plr = _delta(delta_report, "pre_to_post", "plr_proxy_db")

    diagnosis: list[str] = []
    warnings: list[str] = []
    failures: list[str] = []

    if output_gain_trim_db is not None and output_gain_trim_db < -0.20 and not real_tp_emergency:
        failures.append("delivery_negative_trim_without_real_tp_emergency")
        diagnosis.append("delivery_handbrake_candidate")

    if pre_to_post_lufs is not None and pre_to_post_lufs < -0.35 and not real_tp_emergency:
        failures.append("delivery_loudness_loss_without_real_tp_emergency")
        diagnosis.append("delivery_reduced_loudness_after_core")

    if pre_to_post_crest is not None and pre_to_post_crest < -0.55:
        if real_tp_emergency:
            warnings.append("delivery_crest_loss_under_tp_pressure")
        else:
            failures.append("delivery_crest_damage")
        diagnosis.append("delivery_reduced_crest")

    if pre_to_post_punch is not None and pre_to_post_punch < -0.75:
        if real_tp_emergency:
            warnings.append("delivery_punch_loss_under_tp_pressure")
        else:
            failures.append("delivery_punch_damage")
        diagnosis.append("delivery_reduced_punch")

    if pre_to_post_plr is not None and pre_to_post_plr < -0.65:
        if real_tp_emergency:
            warnings.append("delivery_plr_loss_under_tp_pressure")
        else:
            failures.append("delivery_plr_damage")
        diagnosis.append("delivery_reduced_plr")

    if limiter_threshold_db is not None and limiter_threshold_db < -2.20:
        warnings.append("delivery_limiter_threshold_aggressive")

    if limiter_gain_db is not None and limiter_gain_db < -1.20:
        warnings.append("delivery_limiter_gain_reduction_risk")

    mode = "preserve"
    if real_tp_emergency:
        mode = "protect"
    elif pre_to_post_lufs is not None and pre_to_post_lufs > 0.20:
        mode = "lift"
    elif pre_to_post_lufs is not None and pre_to_post_lufs < -0.20:
        mode = "handbrake_candidate"

    return {
        "status": "ok",
        "mode": mode,
        "real_tp_emergency": real_tp_emergency,
        "input_true_peak_dbtp": _round(input_metrics.get("true_peak_dbtp")),
        "pre_delivery_true_peak_dbtp": _round(pre_metrics.get("true_peak_dbtp")),
        "post_delivery_true_peak_dbtp": _round(post_metrics.get("true_peak_dbtp")),
        "output_gain_trim_db": _round(output_gain_trim_db),
        "limiter_threshold_db": _round(limiter_threshold_db),
        "limiter_gain_db": _round(limiter_gain_db),
        "limiter_mix": _round(delivery_report.get("limiter_mix")),
        "limiter_attack_ms": _round(delivery_report.get("limiter_attack_ms")),
        "limiter_release_ms": _round(delivery_report.get("limiter_release_ms")),
        "pre_to_post_lufs_delta": _round(pre_to_post_lufs),
        "pre_to_post_crest_delta": _round(pre_to_post_crest),
        "pre_to_post_punch_delta": _round(pre_to_post_punch),
        "pre_to_post_plr_delta": _round(pre_to_post_plr),
        "warnings": warnings,
        "failures": failures,
        "diagnosis": diagnosis,
        "raw_delivery_report": delivery_report,
    }


def _encode_audio(src_path: str, dst_path: str, fmt: str) -> None:
    fmt = _normalize_format(fmt)
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

    if fmt == "wav16":
        cmd = (
            f"ffmpeg -y -hide_banner -i {shlex.quote(src_path)} "
            f"-af aresample=48000:dither_method=triangular "
            f"-ac 2 -c:a pcm_s16le {shlex.quote(dst_path)}"
        )
    elif fmt == "wav24":
        cmd = (
            f"ffmpeg -y -hide_banner -i {shlex.quote(src_path)} "
            f"-ar 48000 -ac 2 -c:a pcm_s24le {shlex.quote(dst_path)}"
        )
    elif fmt == "flac":
        cmd = (
            f"ffmpeg -y -hide_banner -i {shlex.quote(src_path)} "
            f"-ar 48000 -ac 2 -c:a flac {shlex.quote(dst_path)}"
        )
    elif fmt == "aiff":
        cmd = (
            f"ffmpeg -y -hide_banner -i {shlex.quote(src_path)} "
            f"-ar 48000 -ac 2 -c:a pcm_s24be -f aiff {shlex.quote(dst_path)}"
        )
    elif fmt == "mp3_320":
        cmd = (
            f"ffmpeg -y -hide_banner -i {shlex.quote(src_path)} "
            f"-ar 48000 -ac 2 -c:a libmp3lame -b:a 320k {shlex.quote(dst_path)}"
        )
    else:
        raise RuntimeError(f"unsupported output format: {fmt}")

    _run(cmd)


def build_post_render_derivatives(
    final_output_path: str,
    td: str,
    requested_format: str = "wav16",
) -> Dict[str, Any]:
    _ensure_file(final_output_path)

    requested_format = _normalize_format(requested_format)
    out_dir = os.path.join(td, "sm_post_render")
    os.makedirs(out_dir, exist_ok=True)

    master_download_path = os.path.join(
        out_dir,
        f"sm_master.{_format_ext(requested_format)}",
    )
    preview_mp3_path = os.path.join(out_dir, "sm_preview.mp3")
    telegram_preview_mp3_path = os.path.join(out_dir, "sm_telegram_preview.mp3")

    _encode_audio(final_output_path, master_download_path, requested_format)
    _encode_audio(final_output_path, preview_mp3_path, "mp3_320")

    cmd_tg = (
        f"ffmpeg -y -hide_banner -i {shlex.quote(final_output_path)} "
        f"-ar 44100 -ac 2 -c:a libmp3lame -b:a 192k "
        f"{shlex.quote(telegram_preview_mp3_path)}"
    )
    _run(cmd_tg)

    created_files = []
    for path in [master_download_path, preview_mp3_path, telegram_preview_mp3_path]:
        created_files.append(
            {
                "path": path,
                "bytes": os.path.getsize(path) if os.path.isfile(path) else 0,
            }
        )

    return {
        "status": "ok",
        "output_dir": out_dir,
        "requested_format": requested_format,
        "master_download_path": master_download_path,
        "preview_mp3_path": preview_mp3_path,
        "telegram_preview_mp3_path": telegram_preview_mp3_path,
        "created_files": created_files,
    }


def inspect_derivatives(derivatives_report: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    paths = {
        "master_download": derivatives_report.get("master_download_path"),
        "preview_mp3": derivatives_report.get("preview_mp3_path"),
        "telegram_preview_mp3": derivatives_report.get("telegram_preview_mp3_path"),
    }

    for label, path in paths.items():
        try:
            out[label] = inspect_audio_state(path, label, strict=False)
        except Exception as exc:
            out[label] = {
                "status": "error",
                "label": label,
                "path": path,
                "error": str(exc)[:1000],
                "metrics": {},
            }

    return out


def _append_if_missing(items: list[str], value: str) -> None:
    if value not in items:
        items.append(value)


def build_post_render_verdict(
    inspect_report: Dict[str, Any],
    stage_states: Optional[Dict[str, Any]] = None,
    delta_report: Optional[Dict[str, Any]] = None,
    material_context: Optional[Dict[str, Any]] = None,
    delivery_audit: Optional[Dict[str, Any]] = None,
    derivative_states: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    warnings: list[str] = []
    technical_failures: list[str] = []
    musical_failures: list[str] = []
    root_cause_hints: list[str] = []

    stage_states = stage_states or {}
    delta_report = delta_report or {}
    material_context = material_context or {"material_class": "unknown", "flags": []}
    delivery_audit = delivery_audit or {}
    derivative_states = derivative_states or {}

    duration_sec = _to_float(inspect_report.get("duration_sec"), 0.0) or 0.0
    filesize_bytes = _to_int(inspect_report.get("filesize_bytes"), 0) or 0

    post_metrics = inspect_report.get("delivery_metrics") or {}
    input_state = stage_states.get("input") or {}
    pre_state = stage_states.get("pre_delivery") or {}

    input_metrics = input_state.get("metrics") or {}
    pre_metrics = pre_state.get("metrics") or {}

    material_class = material_context.get("material_class") or "unknown"

    integrated_lufs = _to_float(post_metrics.get("integrated_lufs"))
    true_peak_dbtp = _to_float(post_metrics.get("true_peak_dbtp"))
    sample_peak_dbfs = _to_float(post_metrics.get("sample_peak_dbfs"))
    crest_db = _to_float(post_metrics.get("crest_db"))
    lra_ebu = _to_float(post_metrics.get("lra_ebu"))
    near_clip_ratio = _to_float(post_metrics.get("near_clip_ratio"))
    harshness_index = _to_float(post_metrics.get("harshness_index"))
    sibilance_index = _to_float(post_metrics.get("sibilance_index"))

    if filesize_bytes <= 0:
        technical_failures.append("empty_output_file")

    if duration_sec <= 0.25:
        technical_failures.append("invalid_or_too_short_duration")

    if true_peak_dbtp is None:
        warnings.append("true_peak_missing")
    else:
        if true_peak_dbtp > 0.30:
            technical_failures.append("true_peak_above_safe_fail_zone")
        elif true_peak_dbtp > -1.00:
            warnings.append("true_peak_above_distribution_margin")

    if sample_peak_dbfs is not None and sample_peak_dbfs >= 0.0:
        warnings.append("sample_peak_at_or_above_zero")

    if integrated_lufs is not None:
        if integrated_lufs > -7.0:
            warnings.append("very_hot_master")
        elif integrated_lufs < -16.0:
            warnings.append("very_quiet_master")

    if crest_db is not None and crest_db < 7.0:
        warnings.append("low_crest_possible_overflattening")

    if lra_ebu is not None and lra_ebu < 2.0:
        warnings.append("low_lra_macro_dynamics_limited")

    if near_clip_ratio is not None and near_clip_ratio > 0.01:
        warnings.append("elevated_near_clip_ratio")

    if harshness_index is not None and harshness_index > -6.0:
        warnings.append("elevated_harshness_risk")

    if sibilance_index is not None and sibilance_index > -4.0:
        warnings.append("elevated_sibilance_risk")

    if input_state.get("status") != "ok":
        warnings.append("missing_input_reference_for_delta_qc")

    if pre_state.get("status") != "ok":
        warnings.append("missing_pre_delivery_reference_for_delivery_qc")

    input_to_post_lufs = _delta(delta_report, "input_to_post", "integrated_lufs")
    input_to_post_crest = _delta(delta_report, "input_to_post", "crest_db")
    input_to_post_punch = _delta(delta_report, "input_to_post", "punch_proxy")
    input_to_post_plr = _delta(delta_report, "input_to_post", "plr_proxy_db")
    input_to_post_body = _delta(delta_report, "input_to_post", "body_150_400_db")
    input_to_post_low_body = _delta(delta_report, "input_to_post", "low_body_150_300_db")
    input_to_post_mud = _delta(delta_report, "input_to_post", "mud_200_500_db")
    input_to_post_mud_to_body = _delta(delta_report, "input_to_post", "mud_to_body_db")
    input_to_post_bass_bridge = _delta(delta_report, "input_to_post", "bass_to_body_db")
    input_to_post_harsh = _delta(delta_report, "input_to_post", "harshness_index")
    input_to_post_sibilance = _delta(delta_report, "input_to_post", "sibilance_index")

    input_to_pre_crest = _delta(delta_report, "input_to_pre", "crest_db")
    input_to_pre_punch = _delta(delta_report, "input_to_pre", "punch_proxy")
    input_to_pre_plr = _delta(delta_report, "input_to_pre", "plr_proxy_db")

    pre_to_post_lufs = _delta(delta_report, "pre_to_post", "integrated_lufs")
    pre_to_post_crest = _delta(delta_report, "pre_to_post", "crest_db")
    pre_to_post_punch = _delta(delta_report, "pre_to_post", "punch_proxy")
    pre_to_post_plr = _delta(delta_report, "pre_to_post", "plr_proxy_db")

    real_tp_emergency = bool(delivery_audit.get("real_tp_emergency"))

    if input_to_post_lufs is not None and input_to_post_lufs < -0.55 and not real_tp_emergency:
        musical_failures.append("unnecessary_output_loudness_loss")
        root_cause_hints.append("global_gain_or_delivery_handbrake_candidate")

    if pre_to_post_lufs is not None and pre_to_post_lufs < -0.35 and not real_tp_emergency:
        musical_failures.append("delivery_stage_reduced_loudness_without_real_tp_emergency")
        root_cause_hints.append("delivery_handbrake")

    if input_to_post_crest is not None and input_to_post_crest < -0.80:
        musical_failures.append("crest_damage_input_to_output")
        if input_to_pre_crest is not None and input_to_pre_crest < -0.55:
            root_cause_hints.append("core_stage_crest_loss")
        if pre_to_post_crest is not None and pre_to_post_crest < -0.55:
            root_cause_hints.append("delivery_stage_crest_loss")

    if input_to_post_punch is not None and input_to_post_punch < -0.85:
        musical_failures.append("punch_damage_input_to_output")
        if input_to_pre_punch is not None and input_to_pre_punch < -0.60:
            root_cause_hints.append("core_stage_punch_loss")
        if pre_to_post_punch is not None and pre_to_post_punch < -0.70:
            root_cause_hints.append("delivery_stage_punch_loss")

    if input_to_post_plr is not None and input_to_post_plr < -0.80:
        musical_failures.append("plr_damage_input_to_output")
        if input_to_pre_plr is not None and input_to_pre_plr < -0.55:
            root_cause_hints.append("core_stage_plr_loss")
        if pre_to_post_plr is not None and pre_to_post_plr < -0.65:
            root_cause_hints.append("delivery_stage_plr_loss")

    if material_class == "studio_dense":
        if input_to_post_lufs is not None and input_to_post_lufs < -0.35 and not real_tp_emergency:
            musical_failures.append("studio_dense_unnecessary_loudness_loss")
        if input_to_post_crest is not None and input_to_post_crest < -0.65:
            musical_failures.append("studio_dense_crest_damage")
        if input_to_post_punch is not None and input_to_post_punch < -0.70:
            musical_failures.append("studio_dense_punch_damage")
        if input_to_post_plr is not None and input_to_post_plr < -0.70:
            musical_failures.append("studio_dense_plr_damage")

    if material_class in ("quiet_restore", "quiet_peak_blocked"):
        if input_to_post_lufs is not None and input_to_post_lufs < 0.60:
            musical_failures.append("quiet_track_no_meaningful_lift")
            if real_tp_emergency or material_class == "quiet_peak_blocked":
                root_cause_hints.append("quiet_lift_blocked_by_peak_headroom")
            else:
                root_cause_hints.append("delivery_or_gain_planner_failed_to_lift_quiet_track")

        if input_to_post_crest is not None and input_to_post_crest < -1.10:
            musical_failures.append("quiet_restore_lift_bought_by_crest_loss")

        if input_to_post_punch is not None and input_to_post_punch < -1.10:
            musical_failures.append("quiet_restore_lift_bought_by_punch_loss")

    if material_class == "dirty_dense":
        mud_improved = input_to_post_mud_to_body is not None and input_to_post_mud_to_body < -0.25
        body_damaged = (
            (input_to_post_body is not None and input_to_post_body < -0.50)
            or (input_to_post_low_body is not None and input_to_post_low_body < -0.50)
        )

        if body_damaged and not mud_improved:
            musical_failures.append("cleanup_removed_body_not_mud")
            root_cause_hints.append("cleanup_mud_body_discrimination_failure")

        if input_to_post_mud is not None and input_to_post_mud > -0.20:
            warnings.append("dirty_dense_mud_not_reduced_enough")

    if input_to_post_bass_bridge is not None and input_to_post_bass_bridge < -0.70:
        musical_failures.append("bass_to_body_bridge_damage")
        root_cause_hints.append("bridge_or_cleanup_broke_low_body_continuity")

    if input_to_post_harsh is not None and input_to_post_harsh > 1.25:
        musical_failures.append("harshness_increased_by_processing")
        root_cause_hints.append("projection_or_spark_bought_forwardness_with_harshness")

    if input_to_post_sibilance is not None and input_to_post_sibilance > 1.25:
        warnings.append("sibilance_increased_by_processing")
        root_cause_hints.append("spark_or_projection_sibilance_control_insufficient")

    for failure in delivery_audit.get("failures", []) or []:
        _append_if_missing(musical_failures, failure)

    for warning in delivery_audit.get("warnings", []) or []:
        _append_if_missing(warnings, warning)

    for hint in delivery_audit.get("diagnosis", []) or []:
        _append_if_missing(root_cause_hints, hint)

    for label, state in derivative_states.items():
        if state.get("status") != "ok":
            warnings.append(f"{label}_inspection_unavailable")
            continue

        m = state.get("metrics") or {}
        tp = _to_float(m.get("true_peak_dbtp"))
        sp = _to_float(m.get("sample_peak_dbfs"))
        ncr = _to_float(m.get("near_clip_ratio"))

        if tp is not None and tp > 0.30:
            if label == "master_download":
                technical_failures.append("master_derivative_true_peak_above_safe_fail_zone")
            else:
                warnings.append(f"{label}_true_peak_above_safe_fail_zone")

        elif tp is not None and tp > -1.00:
            warnings.append(f"{label}_true_peak_above_distribution_margin")

        if sp is not None and sp >= 0.0:
            warnings.append(f"{label}_sample_peak_at_or_above_zero")

        if ncr is not None and ncr > 0.01:
            warnings.append(f"{label}_elevated_near_clip_ratio")

    if technical_failures:
        verdict = "fail"
    elif musical_failures:
        verdict = "fail"
    elif warnings:
        verdict = "warn"
    else:
        verdict = "ok"

    legacy_failures = list(technical_failures)
    legacy_warnings = list(warnings)

    return {
        "status": "ok",
        "verdict": verdict,
        "warnings": legacy_warnings,
        "failures": legacy_failures,
        "technical_failures": technical_failures,
        "musical_failures": musical_failures,
        "root_cause_hints": root_cause_hints,
        "material_class": material_class,
        "material_flags": material_context.get("flags", []),
    }


def build_post_render_manifest(
    inspect_report: Dict[str, Any],
    derivatives_report: Dict[str, Any],
    verdict_report: Dict[str, Any],
    stage_states: Optional[Dict[str, Any]] = None,
    delta_report: Optional[Dict[str, Any]] = None,
    material_context: Optional[Dict[str, Any]] = None,
    delivery_audit: Optional[Dict[str, Any]] = None,
    derivative_states: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    stage_states = stage_states or {}
    delta_report = delta_report or {}
    material_context = material_context or {}
    delivery_audit = delivery_audit or {}
    derivative_states = derivative_states or {}

    return {
        "status": "ok",
        "verdict": verdict_report.get("verdict", "warn"),
        "warnings": list(verdict_report.get("warnings", []) or []),
        "failures": list(verdict_report.get("failures", []) or []),
        "technical_failures": list(verdict_report.get("technical_failures", []) or []),
        "musical_failures": list(verdict_report.get("musical_failures", []) or []),
        "root_cause_hints": list(verdict_report.get("root_cause_hints", []) or []),

        "material_class": verdict_report.get("material_class") or material_context.get("material_class"),
        "material_flags": verdict_report.get("material_flags") or material_context.get("flags", []),
        "material_context": material_context,

        "paths": {
            "input_path": _read(stage_states.get("input"), "path"),
            "pre_delivery_path": _read(stage_states.get("pre_delivery"), "path"),
            "final_output_path": inspect_report.get("final_output_path"),
            "master_download_path": derivatives_report.get("master_download_path"),
            "preview_mp3_path": derivatives_report.get("preview_mp3_path"),
            "telegram_preview_mp3_path": derivatives_report.get("telegram_preview_mp3_path"),
        },

        "metrics": {
            "input": _read(stage_states.get("input"), "metrics", {}),
            "pre_delivery": _read(stage_states.get("pre_delivery"), "metrics", {}),
            "post_delivery": inspect_report.get("delivery_metrics", {}),
            "master_download": _read(derivative_states.get("master_download"), "metrics", {}),
            "preview_mp3": _read(derivative_states.get("preview_mp3"), "metrics", {}),
            "telegram_preview_mp3": _read(derivative_states.get("telegram_preview_mp3"), "metrics", {}),
        },

        "deltas": delta_report,
        "delivery_audit": delivery_audit,

        "master_download_path": derivatives_report.get("master_download_path"),
        "preview_mp3_path": derivatives_report.get("preview_mp3_path"),
        "telegram_preview_mp3_path": derivatives_report.get("telegram_preview_mp3_path"),
        "final_output_path": inspect_report.get("final_output_path"),
        "requested_format": derivatives_report.get("requested_format"),
        "duration_sec": inspect_report.get("duration_sec"),
        "filesize_bytes": inspect_report.get("filesize_bytes"),
        "sample_rate_hz": inspect_report.get("sample_rate_hz"),
        "channels": inspect_report.get("channels"),
        "codec_name": inspect_report.get("codec_name"),
        "delivery_metrics": inspect_report.get("delivery_metrics", {}),
    }


def run_post_render_stage(
    final_output_path: str,
    td: str,
    requested_format: str = "wav16",
    input_path: Optional[str] = None,
    pre_delivery_path: Optional[str] = None,
    material_class: Optional[str] = None,
    delivery_report: Optional[Dict[str, Any]] = None,
    render_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    try:
        post_delivery_state = inspect_audio_state(
            final_output_path,
            "post_delivery",
            strict=True,
        )

        input_state = inspect_audio_state(
            input_path,
            "input",
            strict=False,
        )

        pre_delivery_state = inspect_audio_state(
            pre_delivery_path,
            "pre_delivery",
            strict=False,
        )

        inspect_report = {
            "status": "ok",
            "final_output_path": final_output_path,
            "probe": post_delivery_state.get("probe", {}),
            "delivery_metrics": post_delivery_state.get("metrics", {}),
            "duration_sec": post_delivery_state.get("duration_sec"),
            "filesize_bytes": post_delivery_state.get("filesize_bytes"),
            "bitrate_bps": post_delivery_state.get("bitrate_bps"),
            "format_name": post_delivery_state.get("format_name"),
            "codec_name": post_delivery_state.get("codec_name"),
            "sample_rate_hz": post_delivery_state.get("sample_rate_hz"),
            "channels": post_delivery_state.get("channels"),
            "bits_per_sample": post_delivery_state.get("bits_per_sample"),
        }

        derivatives_report = build_post_render_derivatives(
            final_output_path=final_output_path,
            td=td,
            requested_format=requested_format,
        )

        derivative_states = inspect_derivatives(derivatives_report)

        delta_report = build_delta_report(
            input_state=input_state,
            pre_delivery_state=pre_delivery_state,
            post_delivery_state=post_delivery_state,
        )

        input_metrics_for_class = input_state.get("metrics") or post_delivery_state.get("metrics") or {}
        material_context = infer_material_context(input_metrics_for_class)

        if material_class:
            material_context["material_class"] = material_class
            material_context["source"] = "explicit"

        if delivery_report is None:
            delivery_report = _extract_delivery_report_from_render_context(render_context)

        delivery_audit = build_delivery_audit(
            input_state=input_state,
            pre_delivery_state=pre_delivery_state,
            post_delivery_state=post_delivery_state,
            delta_report=delta_report,
            delivery_report=delivery_report,
        )

        stage_states = {
            "input": input_state,
            "pre_delivery": pre_delivery_state,
            "post_delivery": post_delivery_state,
        }

        verdict_report = build_post_render_verdict(
            inspect_report=inspect_report,
            stage_states=stage_states,
            delta_report=delta_report,
            material_context=material_context,
            delivery_audit=delivery_audit,
            derivative_states=derivative_states,
        )

        manifest = build_post_render_manifest(
            inspect_report=inspect_report,
            derivatives_report=derivatives_report,
            verdict_report=verdict_report,
            stage_states=stage_states,
            delta_report=delta_report,
            material_context=material_context,
            delivery_audit=delivery_audit,
            derivative_states=derivative_states,
        )

        return {
            "status": "ok",
            "verdict": verdict_report.get("verdict"),
            "inspect_report": inspect_report,
            "derivatives_report": derivatives_report,
            "derivative_inspect_report": derivative_states,
            "delta_report": delta_report,
            "material_context": material_context,
            "delivery_audit": delivery_audit,
            "verdict_report": verdict_report,
            "manifest": manifest,
        }

    except Exception as exc:
        return {
            "status": "error",
            "verdict": "fail",
            "error": str(exc)[:2000],
        }


def run_post_render_from_execution_report(
    render_execution_report: Dict[str, Any],
    td: str,
    requested_format: str = "wav16",
) -> Dict[str, Any]:
    final_output_path = _read(render_execution_report, "final_output_path")

    node_paths = _read(render_execution_report, "node_paths", {}) or {}

    input_path = (
        _read(render_execution_report, "input_path")
        or _read(render_execution_report, "render_input_path")
        or node_paths.get("prepared_input")
    )

    pre_delivery_path = (
        _read(render_execution_report, "pre_delivery_path")
        or node_paths.get("finish_stage_out")
        or node_paths.get("delivery_input")
    )

    material_class = (
        _read(render_execution_report, "material_class")
        or _read(render_execution_report, "input_material_class")
    )

    delivery_report = _extract_delivery_report_from_render_context(render_execution_report)

    return run_post_render_stage(
        final_output_path=final_output_path,
        td=td,
        requested_format=requested_format,
        input_path=input_path,
        pre_delivery_path=pre_delivery_path,
        material_class=material_class,
        delivery_report=delivery_report,
        render_context=render_execution_report,
    )

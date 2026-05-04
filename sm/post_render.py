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


STAGE_CHAIN_NODES = [
    "prepared_input",
    "cleanup_core_out",
    "guard_core_out",
    "support_stage_out",
    "projection_stage_out",
    "finish_stage_out",
    "final_output",
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


def _bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x

    if isinstance(x, (int, float)):
        return bool(x)

    if isinstance(x, str):
        return x.strip().lower() in {"1", "true", "yes", "y", "on"}

    return bool(x)


def _append_if_missing(items: list[str], value: str) -> None:
    if value and value not in items:
        items.append(value)


def _extend_unique(items: list[str], values: list[str]) -> None:
    for value in values:
        _append_if_missing(items, value)


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
            "before": before_state.get("label"),
            "after": after_state.get("label"),
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


def _classify_loudness(integrated_lufs: Optional[float]) -> str:
    if integrated_lufs is None:
        return "unknown"

    if integrated_lufs <= -13.5:
        return "quiet"

    if integrated_lufs <= -12.0:
        return "moderately_quiet"

    if integrated_lufs >= -8.0:
        return "hot"

    return "normal"


def _classify_peak(
    true_peak: Optional[float],
    sample_peak: Optional[float],
    near_clip: Optional[float],
    limiter_stress: Optional[float],
) -> str:
    if true_peak is None and sample_peak is None:
        return "unknown"

    if true_peak is not None and true_peak >= 0.30:
        return "unsafe_over"

    if sample_peak is not None and sample_peak >= 0.0:
        return "sample_over"

    if true_peak is not None and true_peak > -0.70:
        return "peak_blocked"

    if near_clip is not None and near_clip > 0.002:
        return "near_clip"

    if limiter_stress is not None and limiter_stress >= 1.25:
        return "limiter_stress_candidate"

    return "safe"


def _classify_density(
    body: Optional[float],
    low_body: Optional[float],
    mud_to_body: Optional[float],
    buildup_ratio: Optional[float],
    crest: Optional[float],
    plr: Optional[float],
) -> str:
    body_present = (
        body is not None
        and body >= 30.0
        and low_body is not None
        and low_body >= 30.0
    )

    transient_present = (
        crest is not None
        and crest >= 11.5
        and plr is not None
        and plr >= 10.0
    )

    dirty = (
        body_present
        and mud_to_body is not None
        and mud_to_body >= -0.20
        and buildup_ratio is not None
        and buildup_ratio >= 16.0
    )

    studio_dense = (
        body_present
        and transient_present
        and (mud_to_body is None or mud_to_body <= 0.35)
    )

    if dirty:
        return "dirty_dense"

    if studio_dense:
        return "studio_dense"

    if body is not None and body < 28.0:
        return "thin_or_body_light"

    if body_present:
        return "body_dense"

    return "unknown"


def _classify_body(body: Optional[float], low_body: Optional[float]) -> str:
    if body is None or low_body is None:
        return "unknown"

    if body < 28.0 or low_body < 28.0:
        return "body_weak"

    if body >= 34.5 and low_body >= 34.5:
        return "body_heavy"

    if body >= 30.0 and low_body >= 30.0:
        return "body_present"

    return "body_mid"


def _classify_bridge(
    bass_to_body: Optional[float],
    low_foundation_ratio: Optional[float],
    sub_to_body: Optional[float],
) -> str:
    if bass_to_body is None:
        return "unknown"

    if bass_to_body < 3.5:
        return "bridge_light_or_disconnected"

    if bass_to_body >= 8.0 or (low_foundation_ratio is not None and low_foundation_ratio >= 9.0):
        return "bridge_heavy"

    if sub_to_body is not None and sub_to_body >= 11.5:
        return "sub_body_heavy"

    return "connected"


def _classify_top(harshness: Optional[float], sibilance: Optional[float], air_ratio: Optional[float]) -> str:
    if harshness is None and sibilance is None:
        return "unknown"

    if harshness is not None and harshness > -6.0:
        return "harsh_risk"

    if sibilance is not None and sibilance > -4.0:
        return "sibilance_watch"

    if air_ratio is not None and air_ratio < -25.0:
        return "air_missing"

    return "safe"


def _classify_dynamics(crest: Optional[float], punch: Optional[float], plr: Optional[float]) -> str:
    if crest is None and punch is None and plr is None:
        return "unknown"

    if crest is not None and crest < 7.0:
        return "overflattened"

    if crest is not None and crest >= 11.5 and punch is not None and punch >= 12.0:
        if plr is not None and plr >= 10.0:
            return "transient_rich"
        return "punch_present"

    if punch is not None and punch < 10.0:
        return "punch_fragile"

    return "normal"


def infer_material_context(metrics: Dict[str, Any]) -> Dict[str, Any]:
    integrated_lufs = _to_float(metrics.get("integrated_lufs"))
    true_peak = _to_float(metrics.get("true_peak_dbtp"))
    sample_peak = _to_float(metrics.get("sample_peak_dbfs"))
    crest = _to_float(metrics.get("crest_db"))
    punch = _to_float(metrics.get("punch_proxy"))
    plr = _to_float(metrics.get("plr_proxy_db"))
    body = _to_float(metrics.get("body_150_400_db"))
    low_body = _to_float(metrics.get("low_body_150_300_db"))
    mud_to_body = _to_float(metrics.get("mud_to_body_db"))
    buildup_ratio = _to_float(metrics.get("lowmid_buildup_ratio_db"))
    bass_to_body = _to_float(metrics.get("bass_to_body_db"))
    low_foundation_ratio = _to_float(metrics.get("low_foundation_ratio_db"))
    sub_to_body = _to_float(metrics.get("sub_to_body_db"))
    presence_to_body = _to_float(metrics.get("presence_to_body_db"))
    harshness = _to_float(metrics.get("harshness_index"))
    sibilance = _to_float(metrics.get("sibilance_index"))
    air_ratio = _to_float(metrics.get("air_ratio_db"))
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

    if sibilance is not None and sibilance > -4.0:
        flags.append("sibilance_watch")

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

    axes = {
        "loudness_context": _classify_loudness(integrated_lufs),
        "peak_context": _classify_peak(true_peak, sample_peak, near_clip, limiter_stress),
        "density_context": _classify_density(body, low_body, mud_to_body, buildup_ratio, crest, plr),
        "body_context": _classify_body(body, low_body),
        "bridge_context": _classify_bridge(bass_to_body, low_foundation_ratio, sub_to_body),
        "top_context": _classify_top(harshness, sibilance, air_ratio),
        "dynamics_context": _classify_dynamics(crest, punch, plr),
    }

    material_class = "unknown"

    if axes["loudness_context"] in {"quiet", "moderately_quiet"} and axes["peak_context"] in {
        "peak_blocked",
        "near_clip",
        "unsafe_over",
        "sample_over",
    }:
        material_class = "quiet_peak_blocked"
    elif axes["loudness_context"] in {"quiet", "moderately_quiet"}:
        material_class = "quiet_restore"
    elif axes["density_context"] == "dirty_dense":
        material_class = "dirty_dense"
    elif axes["density_context"] == "studio_dense":
        material_class = "studio_dense"
    elif axes["top_context"] in {"harsh_risk", "sibilance_watch"}:
        material_class = "top_risky"
    elif axes["body_context"] == "body_weak":
        material_class = "body_light"

    return {
        "material_class": material_class,
        "flags": flags,
        "axes": axes,
        "source": "post_render_metric_inference_v2",
    }


def _find_delivery_stage(render_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(render_context, dict):
        return {}

    stage_reports = render_context.get("stage_reports") or []

    for stage in stage_reports:
        if stage.get("stage_kind") == "delivery" or stage.get("stage_name") == "delivery_protect":
            return stage

    stages = render_context.get("stages") or []

    for stage in stages:
        if stage.get("stage_kind") == "delivery" or stage.get("stage_name") == "delivery_protect":
            return stage

    return {}


def _op_extra(op: Dict[str, Any]) -> Dict[str, Any]:
    debug = op.get("debug") or {}

    if isinstance(debug, dict):
        extra = debug.get("op_extra") or {}
        if isinstance(extra, dict):
            return extra

    return {}


def _flatten_delivery_ops_from_stage(stage: Dict[str, Any]) -> list[Dict[str, Any]]:
    if not stage:
        return []

    stack_reports = stage.get("stack_reports") or []
    ops: list[Dict[str, Any]] = []

    for stack in stack_reports:
        for op in stack.get("ops") or []:
            if isinstance(op, dict):
                ops.append(op)

    if ops:
        return ops

    role_stacks = stage.get("role_stacks") or stage.get("stacks") or []

    for stack in role_stacks:
        for op in stack.get("ops") or stack.get("primitive_instances") or []:
            if isinstance(op, dict):
                ops.append(op)

    return ops


def _extract_delivery_report_from_render_context(render_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    stage = _find_delivery_stage(render_context)

    report: Dict[str, Any] = {
        "source": "render_context_actuals_first",
        "available": bool(stage),
        "active_clamps": [],
        "ops": [],

        "output_gain_trim_db": None,
        "output_gain_requested_db": None,
        "output_gain_applied_db": None,
        "output_gain_positive_blocked": None,
        "output_gain_positive_allowed": None,

        "limiter_threshold_db": None,
        "limiter_ceiling_db": None,
        "limiter_gain_db": None,
        "limiter_mix": None,
        "limiter_attack_ms": None,
        "limiter_release_ms": None,

        "limiter_max_gain_reduction_db": None,
        "limiter_active_gain_reduction_ratio": None,
        "limiter_final_safety_trim_db": None,
        "limiter_before_true_peak_dbtp": None,
        "limiter_after_true_peak_dbtp": None,
        "limiter_before_rms_dbfs": None,
        "limiter_after_rms_dbfs": None,
        "limiter_rms_delta_db": None,
        "limiter_mode": None,
        "limiter_no_rms_makeup": None,
        "limiter_no_creative_compression": None,
    }

    if not stage:
        return report

    report["active_clamps"] = stage.get("active_clamps", []) or []

    ops = _flatten_delivery_ops_from_stage(stage)
    report["ops"] = ops

    for op in ops:
        primitive_name = op.get("primitive_name")
        op_kind = op.get("op_kind")
        params = op.get("params") or {}
        extra = _op_extra(op)

        is_trim = primitive_name == "output_gain_trim" or op_kind in {
            "output_trim",
            "ceiling_trim",
            "final_balance_guard",
        }

        is_limiter = primitive_name == "true_peak_limiter" or op_kind == "true_peak_limiter"

        if is_trim:
            planned_gain = _to_float(params.get("gain_db"))
            requested_gain = _to_float(extra.get("requested_gain_db"), planned_gain)
            applied_gain = _to_float(extra.get("applied_gain_db"), planned_gain)

            report["output_gain_trim_db"] = _round(applied_gain)
            report["output_gain_requested_db"] = _round(requested_gain)
            report["output_gain_applied_db"] = _round(applied_gain)
            report["output_gain_positive_blocked"] = _bool(extra.get("positive_gain_blocked", False))
            report["output_gain_positive_allowed"] = _bool(extra.get("positive_gain_allowed", False))

        if is_limiter:
            report["limiter_threshold_db"] = _round(
                extra.get("threshold_db", params.get("threshold_db"))
            )

            report["limiter_ceiling_db"] = _round(
                extra.get("ceiling_db", params.get("gain_db"))
            )

            report["limiter_gain_db"] = _round(
                extra.get("ceiling_db", params.get("gain_db"))
            )

            report["limiter_mix"] = _round(params.get("mix"))
            report["limiter_attack_ms"] = _round(params.get("attack_ms"))
            report["limiter_release_ms"] = _round(params.get("release_ms"))

            report["limiter_max_gain_reduction_db"] = _round(extra.get("max_gain_reduction_db"))
            report["limiter_active_gain_reduction_ratio"] = _round(extra.get("active_gain_reduction_ratio"))
            report["limiter_final_safety_trim_db"] = _round(extra.get("final_safety_trim_db"))
            report["limiter_before_true_peak_dbtp"] = _round(extra.get("before_true_peak_dbtp"))
            report["limiter_after_true_peak_dbtp"] = _round(extra.get("after_true_peak_dbtp"))
            report["limiter_before_rms_dbfs"] = _round(extra.get("before_rms_dbfs"))
            report["limiter_after_rms_dbfs"] = _round(extra.get("after_rms_dbfs"))
            report["limiter_rms_delta_db"] = _round(extra.get("rms_delta_db"))
            report["limiter_mode"] = extra.get("limiter_mode")
            report["limiter_no_rms_makeup"] = extra.get("no_rms_makeup")
            report["limiter_no_creative_compression"] = extra.get("no_creative_compression")

    return report


def _delivery_ceiling(delivery_report: Dict[str, Any]) -> float:
    ceiling = _to_float(delivery_report.get("limiter_ceiling_db"))

    if ceiling is not None:
        return ceiling

    gain = _to_float(delivery_report.get("limiter_gain_db"))

    if gain is not None:
        return gain

    return -1.05


def _has_peak_catch_needed(
    pre_metrics: Dict[str, Any],
    input_metrics: Dict[str, Any],
    delivery_report: Dict[str, Any],
) -> bool:
    ceiling = _delivery_ceiling(delivery_report)

    pre_tp = _to_float(pre_metrics.get("true_peak_dbtp"))
    pre_peak = _to_float(pre_metrics.get("sample_peak_dbfs"))
    pre_near_clip = _to_float(pre_metrics.get("near_clip_ratio"))
    pre_limiter_stress = _to_float(pre_metrics.get("limiter_stress_proxy"))

    input_tp = _to_float(input_metrics.get("true_peak_dbtp"))
    input_near_clip = _to_float(input_metrics.get("near_clip_ratio"))

    if pre_tp is not None and pre_tp > ceiling - 0.10:
        return True

    if input_tp is not None and input_tp > ceiling + 0.25:
        return True

    if pre_peak is not None and pre_peak > ceiling - 0.05:
        return True

    if pre_near_clip is not None and pre_near_clip > 0.002:
        return True

    if input_near_clip is not None and input_near_clip > 0.002:
        return True

    if pre_limiter_stress is not None and pre_limiter_stress >= 1.25:
        return True

    return False


def _has_severe_peak_emergency(
    pre_metrics: Dict[str, Any],
    input_metrics: Dict[str, Any],
    delivery_report: Dict[str, Any],
) -> bool:
    ceiling = _delivery_ceiling(delivery_report)

    pre_tp = _to_float(pre_metrics.get("true_peak_dbtp"))
    pre_peak = _to_float(pre_metrics.get("sample_peak_dbfs"))
    input_tp = _to_float(input_metrics.get("true_peak_dbtp"))
    max_gr = _to_float(delivery_report.get("limiter_max_gain_reduction_db"))
    final_trim = _to_float(delivery_report.get("limiter_final_safety_trim_db"))

    if pre_tp is not None and pre_tp > ceiling + 0.90:
        return True

    if input_tp is not None and input_tp > ceiling + 1.10:
        return True

    if pre_peak is not None and pre_peak > 0.35:
        return True

    if max_gr is not None and max_gr >= 1.65:
        return True

    if final_trim is not None and final_trim <= -0.90:
        return True

    return False


def _is_limiter_point_catch(delivery_report: Dict[str, Any]) -> bool:
    max_gr = _to_float(delivery_report.get("limiter_max_gain_reduction_db"))
    active_ratio = _to_float(delivery_report.get("limiter_active_gain_reduction_ratio"))
    final_trim = _to_float(delivery_report.get("limiter_final_safety_trim_db"))

    if max_gr is None and active_ratio is None and final_trim is None:
        return False

    max_gr_ok = max_gr is None or max_gr <= 1.10
    active_ok = active_ratio is None or active_ratio <= 0.080
    trim_ok = final_trim is None or final_trim >= -0.35

    return bool(max_gr_ok and active_ok and trim_ok)


def _is_limiter_global_handbrake(delivery_report: Dict[str, Any], delta_report: Dict[str, Any]) -> bool:
    max_gr = _to_float(delivery_report.get("limiter_max_gain_reduction_db"))
    active_ratio = _to_float(delivery_report.get("limiter_active_gain_reduction_ratio"))
    final_trim = _to_float(delivery_report.get("limiter_final_safety_trim_db"))
    pre_to_post_lufs = _delta(delta_report, "pre_to_post", "integrated_lufs")

    if active_ratio is not None and active_ratio > 0.180:
        return True

    if max_gr is not None and max_gr > 1.50:
        return True

    if final_trim is not None and final_trim < -0.60:
        return True

    if pre_to_post_lufs is not None and pre_to_post_lufs < -0.35:
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

    peak_catch_needed = _has_peak_catch_needed(
        pre_metrics=pre_metrics,
        input_metrics=input_metrics,
        delivery_report=delivery_report,
    )

    severe_peak_emergency = _has_severe_peak_emergency(
        pre_metrics=pre_metrics,
        input_metrics=input_metrics,
        delivery_report=delivery_report,
    )

    limiter_point_catch = _is_limiter_point_catch(delivery_report)
    limiter_global_handbrake = _is_limiter_global_handbrake(delivery_report, delta_report)

    output_gain_requested_db = _to_float(delivery_report.get("output_gain_requested_db"))
    output_gain_applied_db = _to_float(delivery_report.get("output_gain_applied_db"))
    output_gain_positive_blocked = _bool(delivery_report.get("output_gain_positive_blocked", False))
    output_gain_positive_allowed = _bool(delivery_report.get("output_gain_positive_allowed", False))

    limiter_threshold_db = _to_float(delivery_report.get("limiter_threshold_db"))
    limiter_ceiling_db = _to_float(delivery_report.get("limiter_ceiling_db"))
    limiter_gain_db = _to_float(delivery_report.get("limiter_gain_db"))
    limiter_max_gr = _to_float(delivery_report.get("limiter_max_gain_reduction_db"))
    limiter_active_ratio = _to_float(delivery_report.get("limiter_active_gain_reduction_ratio"))
    limiter_final_trim = _to_float(delivery_report.get("limiter_final_safety_trim_db"))

    pre_to_post_lufs = _delta(delta_report, "pre_to_post", "integrated_lufs")
    pre_to_post_crest = _delta(delta_report, "pre_to_post", "crest_db")
    pre_to_post_punch = _delta(delta_report, "pre_to_post", "punch_proxy")
    pre_to_post_plr = _delta(delta_report, "pre_to_post", "plr_proxy_db")

    diagnosis: list[str] = []
    warnings: list[str] = []
    failures: list[str] = []

    if output_gain_requested_db is not None and output_gain_requested_db > 0.0:
        if output_gain_positive_blocked:
            warnings.append("delivery_positive_gain_attempt_blocked")
            diagnosis.append("delivery_loudness_purchase_attempt_blocked")
        elif not output_gain_positive_allowed:
            failures.append("delivery_positive_gain_applied_without_permission")
            diagnosis.append("delivery_bought_loudness")
        else:
            warnings.append("delivery_positive_gain_explicitly_allowed")
            diagnosis.append("delivery_loudness_lift_explicitly_allowed")

    if output_gain_applied_db is not None and output_gain_applied_db < -0.20 and not peak_catch_needed:
        failures.append("delivery_negative_trim_without_peak_need")
        diagnosis.append("delivery_handbrake_candidate")

    if limiter_point_catch:
        diagnosis.append("limiter_point_catch_ok")

    if limiter_global_handbrake:
        diagnosis.append("limiter_global_handbrake")

        if severe_peak_emergency:
            warnings.append("limiter_global_pressure_under_severe_peak_emergency")
        else:
            failures.append("limiter_global_handbrake_without_severe_peak_emergency")

    if limiter_max_gr is not None and limiter_max_gr > 1.20:
        if severe_peak_emergency:
            warnings.append("limiter_gain_reduction_high_under_peak_emergency")
        else:
            failures.append("limiter_gain_reduction_too_high_for_terminal_protection")
        diagnosis.append("limiter_excessive_gain_reduction")

    if limiter_active_ratio is not None and limiter_active_ratio > 0.120:
        if severe_peak_emergency:
            warnings.append("limiter_active_ratio_high_under_peak_emergency")
        else:
            failures.append("limiter_active_ratio_too_high")
        diagnosis.append("limiter_touched_too_much_material")

    if limiter_final_trim is not None and limiter_final_trim < -0.45:
        if severe_peak_emergency:
            warnings.append("limiter_safety_trim_used_under_peak_emergency")
        else:
            failures.append("limiter_final_safety_trim_too_large")
        diagnosis.append("limiter_global_safety_trim_used")

    if pre_to_post_lufs is not None and pre_to_post_lufs < -0.35 and not peak_catch_needed:
        failures.append("delivery_loudness_loss_without_peak_need")
        diagnosis.append("delivery_reduced_loudness_after_core")

    if pre_to_post_crest is not None and pre_to_post_crest < -0.65:
        if severe_peak_emergency:
            warnings.append("delivery_crest_loss_under_severe_peak_emergency")
        else:
            failures.append("delivery_crest_damage")
        diagnosis.append("delivery_reduced_crest")

    if pre_to_post_punch is not None and pre_to_post_punch < -0.85:
        if severe_peak_emergency:
            warnings.append("delivery_punch_loss_under_severe_peak_emergency")
        else:
            failures.append("delivery_punch_damage")
        diagnosis.append("delivery_reduced_punch")

    if pre_to_post_plr is not None and pre_to_post_plr < -0.80:
        if severe_peak_emergency:
            warnings.append("delivery_plr_loss_under_severe_peak_emergency")
        else:
            failures.append("delivery_plr_damage")
        diagnosis.append("delivery_reduced_plr")

    if limiter_threshold_db is not None and limiter_ceiling_db is not None:
        if limiter_threshold_db < limiter_ceiling_db - 1.30:
            warnings.append("delivery_limiter_threshold_aggressive")

    mode = "preserve"

    if severe_peak_emergency:
        mode = "severe_peak_protect"
    elif peak_catch_needed:
        mode = "point_peak_catch" if limiter_point_catch else "peak_protect_watch"
    elif output_gain_applied_db is not None and output_gain_applied_db > 0.05:
        mode = "lift"
    elif output_gain_applied_db is not None and output_gain_applied_db < -0.05:
        mode = "trim"

    return {
        "status": "ok",
        "mode": mode,

        "peak_catch_needed": peak_catch_needed,
        "severe_peak_emergency": severe_peak_emergency,
        "limiter_point_catch": limiter_point_catch,
        "limiter_global_handbrake": limiter_global_handbrake,

        "input_true_peak_dbtp": _round(input_metrics.get("true_peak_dbtp")),
        "pre_delivery_true_peak_dbtp": _round(pre_metrics.get("true_peak_dbtp")),
        "post_delivery_true_peak_dbtp": _round(post_metrics.get("true_peak_dbtp")),

        "output_gain_trim_db": _round(output_gain_applied_db),
        "output_gain_requested_db": _round(output_gain_requested_db),
        "output_gain_applied_db": _round(output_gain_applied_db),
        "output_gain_positive_blocked": output_gain_positive_blocked,
        "output_gain_positive_allowed": output_gain_positive_allowed,

        "limiter_threshold_db": _round(limiter_threshold_db),
        "limiter_ceiling_db": _round(limiter_ceiling_db),
        "limiter_gain_db": _round(limiter_gain_db),
        "limiter_mix": _round(delivery_report.get("limiter_mix")),
        "limiter_attack_ms": _round(delivery_report.get("limiter_attack_ms")),
        "limiter_release_ms": _round(delivery_report.get("limiter_release_ms")),
        "limiter_max_gain_reduction_db": _round(limiter_max_gr),
        "limiter_active_gain_reduction_ratio": _round(limiter_active_ratio),
        "limiter_final_safety_trim_db": _round(limiter_final_trim),

        "pre_to_post_lufs_delta": _round(pre_to_post_lufs),
        "pre_to_post_crest_delta": _round(pre_to_post_crest),
        "pre_to_post_punch_delta": _round(pre_to_post_punch),
        "pre_to_post_plr_delta": _round(pre_to_post_plr),

        "warnings": warnings,
        "failures": failures,
        "diagnosis": diagnosis,
        "raw_delivery_report": delivery_report,
    }


def _stage_state_key(node_name: str) -> str:
    if node_name == "prepared_input":
        return "input"

    if node_name == "finish_stage_out":
        return "pre_delivery"

    if node_name == "final_output":
        return "post_delivery"

    return node_name


def _node_paths_from_render_context(render_context: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not isinstance(render_context, dict):
        return {}

    node_paths = render_context.get("node_paths") or {}

    if not isinstance(node_paths, dict):
        return {}

    return {str(k): str(v) for k, v in node_paths.items() if v}


def build_stage_chain_report(render_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    node_paths = _node_paths_from_render_context(render_context)

    if not node_paths:
        return {
            "status": "missing",
            "reason": "node_paths_missing",
            "states": {},
            "pairs": {},
            "diagnosis": [],
            "root_cause_hints": [],
        }

    states: Dict[str, Any] = {}

    for node in STAGE_CHAIN_NODES:
        path = node_paths.get(node)
        label = _stage_state_key(node)

        try:
            states[node] = inspect_audio_state(path, label, strict=False)
        except Exception as exc:
            states[node] = {
                "status": "error",
                "label": label,
                "path": path,
                "error": str(exc)[:1000],
                "metrics": {},
            }

    pairs: Dict[str, Any] = {}

    for before_node, after_node in zip(STAGE_CHAIN_NODES[:-1], STAGE_CHAIN_NODES[1:]):
        before_state = states.get(before_node) or {}
        after_state = states.get(after_node) or {}
        pair_name = f"{before_node}_to_{after_node}"
        pairs[pair_name] = _build_pair_delta(before_state, after_state)

    diagnosis: list[str] = []
    root_cause_hints: list[str] = []

    cleanup_delta = pairs.get("prepared_input_to_cleanup_core_out") or {}
    guard_delta = pairs.get("cleanup_core_out_to_guard_core_out") or {}
    support_delta = pairs.get("guard_core_out_to_support_stage_out") or {}
    projection_delta = pairs.get("support_stage_out_to_projection_stage_out") or {}
    finish_delta = pairs.get("projection_stage_out_to_finish_stage_out") or {}
    delivery_delta = pairs.get("finish_stage_out_to_final_output") or {}

    def pd(pair: Dict[str, Any], key: str):
        if not pair.get("available"):
            return None
        return _to_float((pair.get("deltas") or {}).get(key))

    cleanup_body = pd(cleanup_delta, "body_150_400_db")
    cleanup_low_body = pd(cleanup_delta, "low_body_150_300_db")
    cleanup_mud_to_body = pd(cleanup_delta, "mud_to_body_db")
    cleanup_mud = pd(cleanup_delta, "mud_200_500_db")

    if (
        (cleanup_body is not None and cleanup_body < -0.35)
        or (cleanup_low_body is not None and cleanup_low_body < -0.35)
    ):
        if cleanup_mud_to_body is None or cleanup_mud_to_body > -0.15:
            diagnosis.append("cleanup_body_loss_without_clear_mud_improvement")
            root_cause_hints.append("cleanup_mud_body_discrimination_failure")

    if cleanup_mud is not None and cleanup_mud > 0.20:
        diagnosis.append("cleanup_added_or_failed_to_reduce_mud")
        root_cause_hints.append("cleanup_not_cleaning_buildup")

    guard_body = pd(guard_delta, "body_150_400_db")
    guard_bridge = pd(guard_delta, "bass_to_body_db")

    if guard_body is not None and guard_body < -0.35:
        diagnosis.append("guard_removed_body")
        root_cause_hints.append("guard_overcut_body_transition")

    if guard_bridge is not None and guard_bridge < -0.35:
        diagnosis.append("guard_weakened_bass_body_bridge")
        root_cause_hints.append("guard_broke_bridge")

    support_bridge = pd(support_delta, "bass_to_body_db")
    support_body = pd(support_delta, "body_150_400_db")
    support_tp = pd(support_delta, "true_peak_dbtp")
    support_peak = pd(support_delta, "sample_peak_dbfs")

    if support_bridge is not None and support_bridge < -0.35:
        diagnosis.append("support_weakened_bridge")
        root_cause_hints.append("support_stage_bridge_failure")

    if support_body is not None and support_body < -0.35:
        diagnosis.append("support_failed_body_anchor")
        root_cause_hints.append("anchor_support_underbuilt_or_wrong_polarity")

    if (support_tp is not None and support_tp > 0.45) or (support_peak is not None and support_peak > 0.45):
        diagnosis.append("support_created_peak_pressure")
        root_cause_hints.append("support_recombine_or_parallel_gain_peak_pressure")

    projection_presence = pd(projection_delta, "presence_to_body_db")
    projection_harsh = pd(projection_delta, "harshness_index")
    projection_tp = pd(projection_delta, "true_peak_dbtp")

    if projection_presence is not None and projection_presence < 0.05:
        diagnosis.append("projection_did_not_move_forward_enough")
        root_cause_hints.append("projection_underbuilt")

    if projection_harsh is not None and projection_harsh > 0.85:
        diagnosis.append("projection_bought_forwardness_with_harshness")
        root_cause_hints.append("projection_deharsh_insufficient")

    if projection_tp is not None and projection_tp > 0.50:
        diagnosis.append("projection_created_peak_pressure")
        root_cause_hints.append("projection_peak_budget_overflow")

    finish_sibilance = pd(finish_delta, "sibilance_index")
    finish_harsh = pd(finish_delta, "harshness_index")
    finish_tp = pd(finish_delta, "true_peak_dbtp")

    if finish_sibilance is not None and finish_sibilance > 0.65:
        diagnosis.append("spark_increased_sibilance")
        root_cause_hints.append("spark_deess_or_air_safety_insufficient")

    if finish_harsh is not None and finish_harsh > 0.65:
        diagnosis.append("spark_increased_harshness")
        root_cause_hints.append("spark_top_texture_too_hot")

    if finish_tp is not None and finish_tp > 0.35:
        diagnosis.append("spark_created_peak_pressure")
        root_cause_hints.append("spark_peak_budget_overflow")

    delivery_crest = pd(delivery_delta, "crest_db")
    delivery_punch = pd(delivery_delta, "punch_proxy")
    delivery_plr = pd(delivery_delta, "plr_proxy_db")

    if delivery_crest is not None and delivery_crest < -0.65:
        diagnosis.append("delivery_reduced_crest")
        root_cause_hints.append("delivery_stage_crest_loss")

    if delivery_punch is not None and delivery_punch < -0.85:
        diagnosis.append("delivery_reduced_punch")
        root_cause_hints.append("delivery_stage_punch_loss")

    if delivery_plr is not None and delivery_plr < -0.80:
        diagnosis.append("delivery_reduced_plr")
        root_cause_hints.append("delivery_stage_plr_loss")

    return {
        "status": "ok",
        "states": states,
        "pairs": pairs,
        "diagnosis": diagnosis,
        "root_cause_hints": root_cause_hints,
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


def build_derivative_safety_report(derivative_states: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    derivative_states = derivative_states or {}

    warnings: list[str] = []
    failures: list[str] = []
    codec_margin_pressure = False

    for label, state in derivative_states.items():
        if state.get("status") != "ok":
            warnings.append(f"{label}_inspection_unavailable")
            continue

        metrics = state.get("metrics") or {}

        tp = _to_float(metrics.get("true_peak_dbtp"))
        sp = _to_float(metrics.get("sample_peak_dbfs"))
        ncr = _to_float(metrics.get("near_clip_ratio"))

        if tp is not None and tp > 0.30:
            if label == "master_download":
                failures.append("master_derivative_true_peak_above_safe_fail_zone")
            else:
                warnings.append(f"{label}_true_peak_above_safe_fail_zone")
                codec_margin_pressure = True

        elif tp is not None and tp > -1.00:
            warnings.append(f"{label}_true_peak_above_distribution_margin")
            if label != "master_download":
                codec_margin_pressure = True

        if sp is not None and sp >= 0.0:
            warnings.append(f"{label}_sample_peak_at_or_above_zero")
            if label != "master_download":
                codec_margin_pressure = True

        if ncr is not None and ncr > 0.01:
            warnings.append(f"{label}_elevated_near_clip_ratio")
            if label != "master_download":
                codec_margin_pressure = True

    return {
        "status": "ok",
        "codec_margin_pressure": codec_margin_pressure,
        "warnings": warnings,
        "failures": failures,
    }


def build_post_render_verdict(
    inspect_report: Dict[str, Any],
    stage_states: Optional[Dict[str, Any]] = None,
    delta_report: Optional[Dict[str, Any]] = None,
    material_context: Optional[Dict[str, Any]] = None,
    delivery_audit: Optional[Dict[str, Any]] = None,
    derivative_states: Optional[Dict[str, Any]] = None,
    stage_chain_report: Optional[Dict[str, Any]] = None,
    derivative_safety_report: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    warnings: list[str] = []
    technical_failures: list[str] = []
    musical_failures: list[str] = []
    root_cause_hints: list[str] = []

    stage_states = stage_states or {}
    delta_report = delta_report or {}
    material_context = material_context or {"material_class": "unknown", "flags": [], "axes": {}}
    delivery_audit = delivery_audit or {}
    derivative_states = derivative_states or {}
    stage_chain_report = stage_chain_report or {}
    derivative_safety_report = derivative_safety_report or {}

    duration_sec = _to_float(inspect_report.get("duration_sec"), 0.0) or 0.0
    filesize_bytes = _to_int(inspect_report.get("filesize_bytes"), 0) or 0

    post_metrics = inspect_report.get("delivery_metrics") or {}
    input_state = stage_states.get("input") or {}
    pre_state = stage_states.get("pre_delivery") or {}

    material_class = material_context.get("material_class") or "unknown"
    material_axes = material_context.get("axes") or {}

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

    input_to_post_sibilance = _delta(delta_report, "input_to_post", "sibilance_index")
    input_to_post_harsh = _delta(delta_report, "input_to_post", "harshness_index")

    if harshness_index is not None and harshness_index > -6.0:
        if input_to_post_harsh is not None and input_to_post_harsh <= 0.0:
            warnings.append("absolute_harshness_watch_but_not_worsened")
        else:
            warnings.append("elevated_harshness_risk")

    if sibilance_index is not None and sibilance_index > -4.0:
        if input_to_post_sibilance is not None and input_to_post_sibilance <= 0.0:
            warnings.append("absolute_sibilance_watch_but_not_worsened")
        else:
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

    input_to_pre_crest = _delta(delta_report, "input_to_pre", "crest_db")
    input_to_pre_punch = _delta(delta_report, "input_to_pre", "punch_proxy")
    input_to_pre_plr = _delta(delta_report, "input_to_pre", "plr_proxy_db")

    pre_to_post_lufs = _delta(delta_report, "pre_to_post", "integrated_lufs")
    pre_to_post_crest = _delta(delta_report, "pre_to_post", "crest_db")
    pre_to_post_punch = _delta(delta_report, "pre_to_post", "punch_proxy")
    pre_to_post_plr = _delta(delta_report, "pre_to_post", "plr_proxy_db")

    peak_catch_needed = bool(delivery_audit.get("peak_catch_needed"))
    severe_peak_emergency = bool(delivery_audit.get("severe_peak_emergency"))

    if input_to_post_lufs is not None and input_to_post_lufs < -0.55 and not peak_catch_needed:
        musical_failures.append("unnecessary_output_loudness_loss")
        root_cause_hints.append("global_gain_or_delivery_handbrake_candidate")

    if pre_to_post_lufs is not None and pre_to_post_lufs < -0.35 and not peak_catch_needed:
        musical_failures.append("delivery_stage_reduced_loudness_without_peak_need")
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

    if material_class == "studio_dense" or material_axes.get("density_context") == "studio_dense":
        if input_to_post_lufs is not None and input_to_post_lufs < -0.35 and not peak_catch_needed:
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

            if peak_catch_needed or material_class == "quiet_peak_blocked":
                root_cause_hints.append("quiet_lift_blocked_by_peak_headroom")
            else:
                root_cause_hints.append("gain_planner_failed_to_lift_quiet_track")

        if input_to_post_crest is not None and input_to_post_crest < -1.10:
            musical_failures.append("quiet_restore_lift_bought_by_crest_loss")

        if input_to_post_punch is not None and input_to_post_punch < -1.10:
            musical_failures.append("quiet_restore_lift_bought_by_punch_loss")

    if material_class == "dirty_dense" or material_axes.get("density_context") == "dirty_dense":
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

    for hint in stage_chain_report.get("root_cause_hints", []) or []:
        _append_if_missing(root_cause_hints, hint)

    for diag in stage_chain_report.get("diagnosis", []) or []:
        if diag in {
            "cleanup_body_loss_without_clear_mud_improvement",
            "support_weakened_bridge",
            "projection_bought_forwardness_with_harshness",
            "spark_increased_sibilance",
            "delivery_reduced_crest",
            "delivery_reduced_punch",
            "delivery_reduced_plr",
        }:
            _append_if_missing(warnings, diag)

    for failure in derivative_safety_report.get("failures", []) or []:
        _append_if_missing(technical_failures, failure)

    for warning in derivative_safety_report.get("warnings", []) or []:
        _append_if_missing(warnings, warning)

    if severe_peak_emergency:
        _append_if_missing(warnings, "severe_peak_emergency_detected")

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
        "material_axes": material_axes,
    }


def build_repair_request(
    verdict_report: Dict[str, Any],
    delivery_audit: Dict[str, Any],
    stage_chain_report: Dict[str, Any],
    material_context: Dict[str, Any],
    derivative_safety_report: Dict[str, Any],
) -> Dict[str, Any]:
    verdict = verdict_report.get("verdict")
    musical_failures = list(verdict_report.get("musical_failures", []) or [])
    technical_failures = list(verdict_report.get("technical_failures", []) or [])
    warnings = list(verdict_report.get("warnings", []) or [])
    root_hints = list(verdict_report.get("root_cause_hints", []) or [])

    retry_recommended = verdict == "fail"
    publishable = verdict != "fail"

    repair_domain = "none"
    repair_target = "none"
    reason = "no_repair_needed"
    do_not_fix_in_delivery = True
    suggested_actions: list[str] = []

    if technical_failures:
        repair_domain = "technical_output_safety"
        repair_target = "delivery_or_derivative_safety"
        reason = technical_failures[0]
        do_not_fix_in_delivery = False

        suggested_actions = [
            "verify final true peak ceiling",
            "verify derivative encoding margin",
            "rerender derivative or final safety stage only if master core is musically valid",
        ]

    elif any(x in root_hints for x in [
        "delivery_stage_crest_loss",
        "delivery_stage_punch_loss",
        "delivery_stage_plr_loss",
        "delivery_handbrake",
    ]) or any(x in musical_failures for x in [
        "delivery_crest_damage",
        "delivery_punch_damage",
        "delivery_plr_damage",
        "limiter_global_handbrake_without_severe_peak_emergency",
        "limiter_gain_reduction_too_high_for_terminal_protection",
        "limiter_active_ratio_too_high",
        "limiter_final_safety_trim_too_large",
    ]):
        repair_domain = "pre_delivery_peak_budget"
        repair_target = "rerender_before_delivery"
        reason = "delivery_damage_or_peak_budget_overflow"
        do_not_fix_in_delivery = True

        suggested_actions = [
            "keep delivery positive gain blocked",
            "keep limiter as point-catch only",
            "reduce pre-delivery peak pressure before limiter",
            "check support/projection/spark peak budget",
            "rerender from musical graph instead of pushing delivery harder",
        ]

    elif "quiet_lift_blocked_by_peak_headroom" in root_hints:
        repair_domain = "quiet_peak_blocked_headroom"
        repair_target = "musical_graph_peak_budget"
        reason = "quiet material needs lift but peak headroom blocks delivery"
        do_not_fix_in_delivery = True

        suggested_actions = [
            "do not buy loudness in delivery",
            "reduce isolated peak pressure earlier in the chain",
            "keep musical projection but lower peak-heavy support or spark contribution",
            "rerender with more headroom before terminal limiter",
        ]

    elif any(x in musical_failures for x in [
        "bass_to_body_bridge_damage",
    ]) or "bridge_or_cleanup_broke_low_body_continuity" in root_hints:
        repair_domain = "bass_body_bridge"
        repair_target = "cleanup_support_bridge"
        reason = "bass_to_body_bridge_damage"
        do_not_fix_in_delivery = True

        suggested_actions = [
            "reduce cleanup around body/bridge transition",
            "restore bridge support around low-body handoff",
            "avoid broad low-mid removal",
            "keep bass-to-body continuity alive before projection",
        ]

    elif any(x in musical_failures for x in [
        "cleanup_removed_body_not_mud",
    ]) or "cleanup_mud_body_discrimination_failure" in root_hints:
        repair_domain = "cleanup_mud_body_discrimination"
        repair_target = "cleanup_core"
        reason = "cleanup removed useful body instead of mud"
        do_not_fix_in_delivery = True

        suggested_actions = [
            "reduce cleanup amount",
            "narrow cleanup band",
            "raise body protection",
            "only cut buildup when mud-to-body actually improves",
        ]

    elif any(x in musical_failures for x in [
        "harshness_increased_by_processing",
    ]) or "projection_or_spark_bought_forwardness_with_harshness" in root_hints:
        repair_domain = "projection_top_safety"
        repair_target = "projection_or_spark"
        reason = "forwardness was bought with harshness"
        do_not_fix_in_delivery = True

        suggested_actions = [
            "reduce projection presence lift",
            "increase projection-local deharsh",
            "reduce spark air or top texture",
            "keep projection body-linked, not brightness-led",
        ]

    elif derivative_safety_report.get("codec_margin_pressure"):
        repair_domain = "derivative_codec_margin"
        repair_target = "preview_derivatives_only"
        reason = "preview codec margin pressure"
        do_not_fix_in_delivery = True

        suggested_actions = [
            "do not change master download for preview-only issue",
            "apply preview-only codec safety margin",
            "keep master path untouched if master true peak is safe",
        ]

    elif retry_recommended:
        repair_domain = "general_musical_qc"
        repair_target = "controller_rerender"
        reason = musical_failures[0] if musical_failures else "post_render_fail"
        do_not_fix_in_delivery = True

        suggested_actions = [
            "inspect stage_chain_report root cause",
            "rerender only the responsible musical block",
            "do not solve musical failure inside delivery",
        ]

    return {
        "status": "ok",
        "retry_recommended": retry_recommended,
        "publishable": publishable,
        "repair_domain": repair_domain,
        "repair_target": repair_target,
        "reason": reason,
        "do_not_fix_in_delivery": do_not_fix_in_delivery,
        "suggested_actions": suggested_actions,
        "source_failures": {
            "technical_failures": technical_failures,
            "musical_failures": musical_failures,
            "warnings": warnings,
            "root_cause_hints": root_hints,
        },
        "delivery_constraints": {
            "delivery_terminal_only": True,
            "delivery_does_not_buy_loudness": True,
            "delivery_limiter_point_catch_only": True,
            "delivery_should_not_fix_musical_graph": True,
            "positive_delivery_gain_blocked_by_default": True,
        },
        "material_context": material_context,
        "delivery_audit_summary": {
            "mode": delivery_audit.get("mode"),
            "peak_catch_needed": delivery_audit.get("peak_catch_needed"),
            "severe_peak_emergency": delivery_audit.get("severe_peak_emergency"),
            "limiter_point_catch": delivery_audit.get("limiter_point_catch"),
            "limiter_global_handbrake": delivery_audit.get("limiter_global_handbrake"),
            "output_gain_requested_db": delivery_audit.get("output_gain_requested_db"),
            "output_gain_applied_db": delivery_audit.get("output_gain_applied_db"),
            "output_gain_positive_blocked": delivery_audit.get("output_gain_positive_blocked"),
            "limiter_max_gain_reduction_db": delivery_audit.get("limiter_max_gain_reduction_db"),
            "limiter_active_gain_reduction_ratio": delivery_audit.get("limiter_active_gain_reduction_ratio"),
            "limiter_final_safety_trim_db": delivery_audit.get("limiter_final_safety_trim_db"),
        },
        "stage_chain_root_cause_hints": list(stage_chain_report.get("root_cause_hints", []) or []),
    }


def build_publish_gate(
    verdict_report: Dict[str, Any],
    repair_request: Dict[str, Any],
    derivative_safety_report: Dict[str, Any],
) -> Dict[str, Any]:
    verdict = verdict_report.get("verdict")

    technical_failures = list(verdict_report.get("technical_failures", []) or [])
    musical_failures = list(verdict_report.get("musical_failures", []) or [])

    derivative_failures = list(derivative_safety_report.get("failures", []) or [])
    derivative_warnings = list(derivative_safety_report.get("warnings", []) or [])

    publishable = verdict != "fail"
    quarantine = verdict == "fail"

    download_ready = publishable and not any(
        failure.startswith("master_derivative") for failure in derivative_failures
    )

    player_ready = publishable and not any(
        warning.startswith("preview_mp3_true_peak_above_safe_fail_zone")
        or warning.startswith("telegram_preview_mp3_true_peak_above_safe_fail_zone")
        for warning in derivative_warnings
    )

    return {
        "status": "ok",
        "verdict": verdict,
        "publishable": publishable,
        "player_ready": player_ready,
        "download_ready": download_ready,
        "quarantine": quarantine,
        "requires_rerender": bool(repair_request.get("retry_recommended")),
        "technical_failures": technical_failures,
        "musical_failures": musical_failures,
        "derivative_failures": derivative_failures,
        "derivative_warnings": derivative_warnings,
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
    stage_chain_report: Optional[Dict[str, Any]] = None,
    derivative_safety_report: Optional[Dict[str, Any]] = None,
    repair_request: Optional[Dict[str, Any]] = None,
    publish_gate: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    stage_states = stage_states or {}
    delta_report = delta_report or {}
    material_context = material_context or {}
    delivery_audit = delivery_audit or {}
    derivative_states = derivative_states or {}
    stage_chain_report = stage_chain_report or {}
    derivative_safety_report = derivative_safety_report or {}
    repair_request = repair_request or {}
    publish_gate = publish_gate or {}

    return {
        "status": "ok",
        "verdict": verdict_report.get("verdict", "warn"),
        "publish_gate": publish_gate,
        "repair_request": repair_request,

        "warnings": list(verdict_report.get("warnings", []) or []),
        "failures": list(verdict_report.get("failures", []) or []),
        "technical_failures": list(verdict_report.get("technical_failures", []) or []),
        "musical_failures": list(verdict_report.get("musical_failures", []) or []),
        "root_cause_hints": list(verdict_report.get("root_cause_hints", []) or []),

        "material_class": verdict_report.get("material_class") or material_context.get("material_class"),
        "material_flags": verdict_report.get("material_flags") or material_context.get("flags", []),
        "material_axes": verdict_report.get("material_axes") or material_context.get("axes", {}),
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
        "derivative_safety_report": derivative_safety_report,
        "stage_chain_report": stage_chain_report,

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
        derivative_safety_report = build_derivative_safety_report(derivative_states)

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

        stage_chain_report = build_stage_chain_report(render_context)

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
            stage_chain_report=stage_chain_report,
            derivative_safety_report=derivative_safety_report,
        )

        repair_request = build_repair_request(
            verdict_report=verdict_report,
            delivery_audit=delivery_audit,
            stage_chain_report=stage_chain_report,
            material_context=material_context,
            derivative_safety_report=derivative_safety_report,
        )

        publish_gate = build_publish_gate(
            verdict_report=verdict_report,
            repair_request=repair_request,
            derivative_safety_report=derivative_safety_report,
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
            stage_chain_report=stage_chain_report,
            derivative_safety_report=derivative_safety_report,
            repair_request=repair_request,
            publish_gate=publish_gate,
        )

        return {
            "status": "ok",
            "verdict": verdict_report.get("verdict"),
            "publish_gate": publish_gate,
            "repair_request": repair_request,
            "inspect_report": inspect_report,
            "derivatives_report": derivatives_report,
            "derivative_inspect_report": derivative_states,
            "derivative_safety_report": derivative_safety_report,
            "delta_report": delta_report,
            "stage_chain_report": stage_chain_report,
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
            "publish_gate": {
                "status": "error",
                "verdict": "fail",
                "publishable": False,
                "player_ready": False,
                "download_ready": False,
                "quarantine": True,
                "requires_rerender": True,
            },
            "repair_request": {
                "status": "error",
                "retry_recommended": True,
                "publishable": False,
                "repair_domain": "post_render_runtime_error",
                "repair_target": "post_render_stage",
                "reason": str(exc)[:1000],
                "do_not_fix_in_delivery": True,
                "suggested_actions": [
                    "inspect post_render exception",
                    "do not publish failed render",
                    "fix runtime path or analysis failure before retry",
                ],
            },
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

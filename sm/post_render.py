from __future__ import annotations

import json
import os
import shlex
import subprocess
from typing import Any, Dict

from .analysis import analyze_sm_input


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


def _read(obj: Any, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


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


def _ffprobe_json(path: str) -> Dict[str, Any]:
    cmd = (
        f"ffprobe -v error "
        f"-show_entries format=format_name,duration,size,bit_rate "
        f"-show_entries stream=index,codec_type,codec_name,sample_rate,channels,bits_per_sample "
        f"-of json {shlex.quote(path)}"
    )
    stdout, _ = _run(cmd)
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


def _to_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default


def _to_int(x, default=None):
    try:
        return int(x)
    except Exception:
        return default


def _collect_delivery_metrics(analysis: Any) -> Dict[str, Any]:
    metrics = _read(analysis, "metrics")

    keys = [
        "integrated_lufs",
        "true_peak_dbtp",
        "sample_peak_dbfs",
        "crest_db",
        "plr_proxy_db",
        "lra_ebu",
        "punch_proxy",
        "harshness_index",
        "sibilance_index",
        "near_clip_ratio",
        "momentary_to_integrated_gap_db",
        "short_term_to_integrated_gap_db",
        "bass_to_body_db",
        "presence_to_body_db",
        "mud_to_body_db",
    ]

    out: Dict[str, Any] = {}
    for key in keys:
        out[key] = _read(metrics, key)

    return out


def inspect_render_output(final_output_path: str) -> Dict[str, Any]:
    _ensure_file(final_output_path)

    probe = _ffprobe_json(final_output_path)
    audio_stream = _first_audio_stream(probe)
    fmt = probe.get("format") or {}

    analysis = analyze_sm_input(final_output_path)
    delivery_metrics = _collect_delivery_metrics(analysis)

    return {
        "status": "ok",
        "final_output_path": final_output_path,
        "probe": probe,
        "delivery_metrics": delivery_metrics,
        "duration_sec": _to_float(fmt.get("duration"), 0.0),
        "filesize_bytes": _to_int(fmt.get("size"), 0),
        "bitrate_bps": _to_int(fmt.get("bit_rate")),
        "format_name": fmt.get("format_name"),
        "codec_name": audio_stream.get("codec_name"),
        "sample_rate_hz": _to_int(audio_stream.get("sample_rate")),
        "channels": _to_int(audio_stream.get("channels")),
        "bits_per_sample": _to_int(audio_stream.get("bits_per_sample")),
    }


def _encode_audio(src_path: str, dst_path: str, fmt: str) -> None:
    fmt = _normalize_format(fmt)
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

    if fmt == "wav16":
        cmd = (
            f"ffmpeg -y -hide_banner -i {shlex.quote(src_path)} "
            f"-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(dst_path)}"
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


def build_post_render_verdict(inspect_report: Dict[str, Any]) -> Dict[str, Any]:
    warnings: list[str] = []
    failures: list[str] = []

    duration_sec = _to_float(inspect_report.get("duration_sec"), 0.0) or 0.0
    filesize_bytes = _to_int(inspect_report.get("filesize_bytes"), 0) or 0

    metrics = inspect_report.get("delivery_metrics") or {}

    integrated_lufs = _to_float(metrics.get("integrated_lufs"))
    true_peak_dbtp = _to_float(metrics.get("true_peak_dbtp"))
    sample_peak_dbfs = _to_float(metrics.get("sample_peak_dbfs"))
    crest_db = _to_float(metrics.get("crest_db"))
    lra_ebu = _to_float(metrics.get("lra_ebu"))
    near_clip_ratio = _to_float(metrics.get("near_clip_ratio"))
    harshness_index = _to_float(metrics.get("harshness_index"))
    sibilance_index = _to_float(metrics.get("sibilance_index"))

    if filesize_bytes <= 0:
        failures.append("empty_output_file")

    if duration_sec <= 0.25:
        failures.append("invalid_or_too_short_duration")

    if true_peak_dbtp is None:
        warnings.append("true_peak_missing")
    else:
        if true_peak_dbtp > 0.30:
            failures.append("true_peak_above_safe_fail_zone")
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

    if failures:
        verdict = "fail"
    elif warnings:
        verdict = "warn"
    else:
        verdict = "ok"

    return {
        "status": "ok",
        "verdict": verdict,
        "warnings": warnings,
        "failures": failures,
    }


def build_post_render_manifest(
    inspect_report: Dict[str, Any],
    derivatives_report: Dict[str, Any],
    verdict_report: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "status": "ok",
        "verdict": verdict_report.get("verdict", "warn"),
        "warnings": list(verdict_report.get("warnings", []) or []),
        "failures": list(verdict_report.get("failures", []) or []),
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
) -> Dict[str, Any]:
    try:
        inspect_report = inspect_render_output(final_output_path)
        derivatives_report = build_post_render_derivatives(
            final_output_path=final_output_path,
            td=td,
            requested_format=requested_format,
        )
        verdict_report = build_post_render_verdict(inspect_report)
        manifest = build_post_render_manifest(
            inspect_report=inspect_report,
            derivatives_report=derivatives_report,
            verdict_report=verdict_report,
        )

        return {
            "status": "ok",
            "verdict": verdict_report.get("verdict"),
            "inspect_report": inspect_report,
            "derivatives_report": derivatives_report,
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
    return run_post_render_stage(
        final_output_path=final_output_path,
        td=td,
        requested_format=requested_format,
    )

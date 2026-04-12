# sm/metrics.py

import json
import os
import re
import shlex
import subprocess
import tempfile
from typing import Any, Dict, Optional

from analyze_mastering import run_analysis

from .contracts import AnalysisMetrics


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


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s or s.lower() in ("nan", "none", "null", "-inf", "inf"):
            return None
        return float(s)
    except Exception:
        return None


def _extract_re_float(pattern: str, text: str) -> Optional[float]:
    m = re.search(pattern, text, re.IGNORECASE)
    if not m:
        return None
    return _safe_float(m.group(1))


def _extract_last_json_block(text: str) -> Optional[Dict[str, Any]]:
    start = -1
    depth = 0
    last_obj = None

    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and start != -1:
                    chunk = text[start:i + 1]
                    try:
                        last_obj = json.loads(chunk)
                    except Exception:
                        pass
                    start = -1
    return last_obj


def _probe_volumedetect(in_path: str) -> Dict[str, Optional[float]]:
    cmd = f'ffmpeg -hide_banner -nostats -i {shlex.quote(in_path)} -af "volumedetect" -f null -'
    _, err = _run(cmd)
    mean_volume = _extract_re_float(r"mean_volume:\s*(-?\d+(?:\.\d+)?)\s*dB", err)
    max_volume = _extract_re_float(r"max_volume:\s*(-?\d+(?:\.\d+)?)\s*dB", err)
    return {
        "rms_dbfs": mean_volume,
        "sample_peak_dbfs": max_volume,
    }


def _probe_loudnorm_input_stats(in_path: str) -> Dict[str, Optional[float]]:
    ln = "loudnorm=I=-14:TP=-1.0:LRA=7:print_format=json"
    cmd = f'ffmpeg -hide_banner -nostats -i {shlex.quote(in_path)} -af "{ln}" -f null -'
    _, err = _run(cmd)
    stats = _extract_last_json_block(err) or {}
    return {
        "integrated_lufs": _safe_float(stats.get("input_i")),
        "true_peak_dbtp": _safe_float(stats.get("input_tp")),
        "lra_ebu": _safe_float(stats.get("input_lra")),
        "input_thresh": _safe_float(stats.get("input_thresh")),
        "target_offset": _safe_float(stats.get("target_offset")),
    }


def _collect_stage_metrics(in_path: str) -> Dict[str, Optional[float]]:
    vd = _probe_volumedetect(in_path)
    ln = _probe_loudnorm_input_stats(in_path)

    rms_dbfs = vd.get("rms_dbfs")
    sample_peak_dbfs = vd.get("sample_peak_dbfs")
    integrated_lufs = ln.get("integrated_lufs")
    true_peak_dbtp = ln.get("true_peak_dbtp")

    crest_db = None
    if sample_peak_dbfs is not None and rms_dbfs is not None:
        crest_db = sample_peak_dbfs - rms_dbfs

    plr_proxy_db = None
    if true_peak_dbtp is not None and integrated_lufs is not None:
        plr_proxy_db = true_peak_dbtp - integrated_lufs

    return {
        "integrated_lufs": integrated_lufs,
        "true_peak_dbtp": true_peak_dbtp,
        "rms_dbfs": rms_dbfs,
        "sample_peak_dbfs": sample_peak_dbfs,
        "crest_db": crest_db,
        "plr_proxy_db": plr_proxy_db,
        "lra_ebu": ln.get("lra_ebu"),
        "input_thresh": ln.get("input_thresh"),
        "target_offset": ln.get("target_offset"),
    }


def _extract_input_profile_from_analysis(input_path: str, td: str) -> Dict[str, Any]:
    """
    Берём rich metric set из текущего analyze_mastering.run_analysis.
    Сохраняем probe wav и читаем report['before'].
    """
    out_dir = os.path.join(td, "sm_metric_probe")
    os.makedirs(out_dir, exist_ok=True)

    probe_path = os.path.join(td, "probe.wav")
    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(input_path)} '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(probe_path)}'
    )
    _run(cmd)

    report, _suggestion = run_analysis(input_path, probe_path, out_dir)

    if isinstance(report, dict) and isinstance(report.get("before"), dict):
        return report["before"]

    return {}


def _get_metric(src: Dict[str, Any], key: str, fallback: Optional[float] = None) -> Optional[float]:
    if key not in src:
        return fallback
    v = _safe_float(src.get(key))
    return fallback if v is None else v


def collect_sm_metrics(input_path: str) -> AnalysisMetrics:
    """
    Финальный production metric set для V1.
    1. Тянем rich metrics из текущего analyze_mastering
    2. Добираем базовые loudness / crest / plr через ffmpeg как fallback
    """
    with tempfile.TemporaryDirectory() as td:
        try:
            profile = _extract_input_profile_from_analysis(input_path, td)
        except Exception:
            profile = {}

        try:
            stage = _collect_stage_metrics(input_path)
        except Exception:
            stage = {}

        return AnalysisMetrics(
            # Body / support
            body_150_400_db=_get_metric(profile, "body_150_400_db"),
            low_body_150_300_db=_get_metric(profile, "low_body_150_300_db"),
            lowmid_120_300_db=_get_metric(profile, "lowmid_120_300_db"),

            # Buildup / mud
            lowmid_buildup_200_400_db=_get_metric(profile, "lowmid_buildup_200_400_db"),
            mud_200_500_db=_get_metric(profile, "mud_200_500_db"),
            mud_to_body_db=_get_metric(profile, "mud_to_body_db"),
            lowmid_buildup_ratio_db=_get_metric(profile, "lowmid_buildup_ratio_db"),

            # Bass/body connection
            bass_to_body_db=_get_metric(profile, "bass_to_body_db"),
            low_foundation_ratio_db=_get_metric(profile, "low_foundation_ratio_db"),
            sub_to_body_db=_get_metric(profile, "sub_to_body_db"),
            low_foundation_50_100_db=_get_metric(profile, "low_foundation_50_100_db"),
            bass_60_120_db=_get_metric(profile, "bass_60_120_db"),

            # Mid / projection handoff
            mid_1k_2k_db=_get_metric(profile, "mid_1k_2k_db"),
            presence_2k_5k_db=_get_metric(profile, "presence_2k_5k_db"),
            presence_to_body_db=_get_metric(profile, "presence_to_body_db"),

            # Harsh / sibilance
            harsh_2p5k_6k_db=_get_metric(profile, "harsh_2p5k_6k_db"),
            harshness_index=_get_metric(profile, "harshness_index"),
            harsh_to_mid_db=_get_metric(profile, "harsh_to_mid_db"),
            sibilance_5k_9k_db=_get_metric(profile, "sibilance_5k_9k_db"),
            sibilance_index=_get_metric(profile, "sibilance_index"),

            # Air / top contour
            air_8k_12k_db=_get_metric(profile, "air_8k_12k_db"),
            air_8k_16k_db=_get_metric(profile, "air_8k_16k_db"),
            air16_to_body_db=_get_metric(profile, "air16_to_body_db"),
            air_ratio_db=_get_metric(profile, "air_ratio_db"),
            tilt_indicator_db=_get_metric(profile, "tilt_indicator_db"),

            # Dynamics / delivery
            crest_db=_get_metric(profile, "crest_db", stage.get("crest_db")),
            punch_proxy=_get_metric(profile, "punch_proxy"),
            plr_proxy_db=_get_metric(profile, "plr_proxy_db", stage.get("plr_proxy_db")),
            integrated_lufs=_get_metric(profile, "integrated_lufs", stage.get("integrated_lufs")),
            true_peak_dbtp=_get_metric(profile, "true_peak_dbtp", stage.get("true_peak_dbtp")),

            # Stress / context
            near_clip_ratio=_get_metric(profile, "near_clip_ratio"),
            limiter_stress_proxy=_get_metric(profile, "limiter_stress_proxy"),
            transient_index=_get_metric(profile, "transient_index"),
            momentary_to_integrated_gap_db=_get_metric(profile, "momentary_to_integrated_gap_db"),
            short_term_to_integrated_gap_db=_get_metric(profile, "short_term_to_integrated_gap_db"),

            # Useful extras
            rms_dbfs=_get_metric(profile, "rms_dbfs", stage.get("rms_dbfs")),
            sample_peak_dbfs=_get_metric(profile, "sample_peak_dbfs", stage.get("sample_peak_dbfs")),
            lra_ebu=_get_metric(profile, "lra_ebu", stage.get("lra_ebu")),
        )

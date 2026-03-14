# analyze_mastering.py
# -*- coding: utf-8 -*-

import os
import json
from typing import Dict, Tuple, Any, List

import numpy as np
import librosa
import pyloudnorm as pyln


# ---------- constants ----------

TARGET_SR = 48000
EPS = 1e-18
ABS_GATE_LUFS = -70.0
TRUE_PEAK_OVERSAMPLE = 4
TRIM_TOP_DB = 40.0


# ---------- helpers ----------

def _safe_db(x: float, floor: float = EPS) -> float:
    return float(20.0 * np.log10(max(float(x), floor)))


def _safe_db10(x: float, floor: float = EPS) -> float:
    return float(10.0 * np.log10(max(float(x), floor)))


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if np.isfinite(v):
            return v
        return float(default)
    except Exception:
        return float(default)


def _trim_stereo(y: np.ndarray, sr: int, top_db: float = TRIM_TOP_DB) -> np.ndarray:
    """
    y: (2, n) or (n,)
    Returns trimmed stereo (2, n)
    """
    if y.ndim == 1:
        y = np.vstack([y, y])

    mono_sum = np.mean(y, axis=0)
    energy = librosa.feature.rms(y=mono_sum, frame_length=2048, hop_length=512)[0]
    if energy.size == 0:
        return y

    thr = float(np.max(energy)) * (10.0 ** (-top_db / 20.0))
    frames = np.nonzero(energy > thr)[0]
    if frames.size == 0:
        return y[:, :0]

    start_frame = int(frames[0])
    end_frame = int(frames[-1])
    start_sample = max(0, start_frame * 512)
    end_sample = min(y.shape[1], int(end_frame * 512 + 2048))
    return y[:, start_sample:end_sample]


def _mono(y_stereo: np.ndarray) -> np.ndarray:
    return np.mean(y_stereo, axis=0)


def _rms_dbfs(y: np.ndarray) -> float:
    rms = float(np.sqrt(np.mean(np.square(y))))
    return _safe_db(rms, floor=1e-12)


def _crest_db(y_mono: np.ndarray) -> float:
    sample_peak = float(np.max(np.abs(y_mono)))
    rms = float(np.sqrt(np.mean(np.square(y_mono))))
    return float(_safe_db(sample_peak, 1e-12) - _safe_db(rms, 1e-12))


def _sample_peak_dbfs(y: np.ndarray) -> float:
    peak = float(np.max(np.abs(y)))
    return _safe_db(peak, floor=1e-12)


def _true_peak_dbtp(y: np.ndarray, sr: int, oversample: int = TRUE_PEAK_OVERSAMPLE) -> float:
    """
    Oversampled inter-sample true-peak approximation.
    Returns dBTP.
    """
    if y.ndim == 1:
        y = np.vstack([y, y])

    target_sr = sr * oversample
    peaks = []
    for ch in range(y.shape[0]):
        up = librosa.resample(y[ch], orig_sr=sr, target_sr=target_sr, res_type="soxr_hq")
        peaks.append(float(np.max(np.abs(up))))
    return _safe_db(max(peaks), floor=1e-12)


def _near_clip_ratio(y: np.ndarray, threshold_dbfs: float = -1.0) -> float:
    thr = 10.0 ** (threshold_dbfs / 20.0)
    total = y.size
    if total == 0:
        return 0.0
    return float(np.mean(np.abs(y) >= thr))


def _clip_ratio(y: np.ndarray, threshold_linear: float = 0.9999) -> float:
    total = y.size
    if total == 0:
        return 0.0
    return float(np.mean(np.abs(y) >= threshold_linear))


def _transient_index(y_mono: np.ndarray, sr: int) -> float:
    hop = 512
    n_fft = 2048
    S = np.abs(librosa.stft(y_mono, n_fft=n_fft, hop_length=hop, window="hann"))
    if S.size == 0 or S.shape[1] < 2:
        return 0.0

    diff = np.diff(S, axis=1)
    flux = float(np.mean(np.maximum(diff, 0.0)))
    energy = float(np.mean(S))
    if energy < 1e-12:
        return 0.0
    return float(flux / (energy + 1e-12))


def _loudness_series(
    y_stereo: np.ndarray,
    sr: int,
    window_sec: float,
    hop_sec: float,
) -> np.ndarray:
    """
    Windowed BS.1770-style loudness series using pyloudnorm per window.
    """
    if y_stereo.ndim == 1:
        y_stereo = np.vstack([y_stereo, y_stereo])

    win = max(1, int(round(window_sec * sr)))
    hop = max(1, int(round(hop_sec * sr)))
    n = y_stereo.shape[1]

    meter = pyln.Meter(sr)
    values: List[float] = []

    if n < win:
        seg = y_stereo.T
        try:
            values.append(float(meter.integrated_loudness(seg)))
        except Exception:
            values.append(float(ABS_GATE_LUFS))
        return np.array(values, dtype=np.float64)

    for start in range(0, n - win + 1, hop):
        seg = y_stereo[:, start:start + win]
        if float(np.max(np.abs(seg))) < 1e-8:
            values.append(float(ABS_GATE_LUFS))
            continue
        try:
            L = float(meter.integrated_loudness(seg.T))
        except Exception:
            L = float(ABS_GATE_LUFS)
        if not np.isfinite(L):
            L = float(ABS_GATE_LUFS)
        values.append(L)

    return np.array(values, dtype=np.float64)


def _ebu_lra_from_short_term(short_term_lufs: np.ndarray) -> float:
    """
    Approximate EBU Tech 3342 / R128 LRA:
    - use short-term loudness blocks
    - absolute gate -70 LUFS
    - relative gate = average of abs-gated blocks - 20 LU
    - LRA = p95 - p10 on doubly gated set
    """
    if short_term_lufs.size == 0:
        return 0.0

    st = short_term_lufs[np.isfinite(short_term_lufs)]
    if st.size == 0:
        return 0.0

    abs_gated = st[st >= ABS_GATE_LUFS]
    if abs_gated.size == 0:
        return 0.0

    rel_gate = float(np.mean(abs_gated) - 20.0)
    doubly_gated = abs_gated[abs_gated >= rel_gate]
    if doubly_gated.size < 2:
        return 0.0

    p10 = float(np.percentile(doubly_gated, 10))
    p95 = float(np.percentile(doubly_gated, 95))
    return float(max(0.0, p95 - p10))


def _integrated_lufs(y_stereo: np.ndarray, sr: int) -> float:
    meter = pyln.Meter(sr)
    return float(meter.integrated_loudness(y_stereo.T))


def _fft_psd(y: np.ndarray, sr: int, n_fft: int = 8192, hop: int = 2048) -> Tuple[np.ndarray, np.ndarray]:
    S = np.abs(librosa.stft(y, n_fft=n_fft, hop_length=hop, window="hann")) ** 2
    if S.size == 0:
        freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)
        return freqs, np.zeros_like(freqs, dtype=np.float64) + EPS
    freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)
    psd = np.mean(S, axis=1) + EPS
    return freqs, psd


def _band_power_db(freqs: np.ndarray, psd: np.ndarray, lo: float, hi: float) -> float:
    idx = np.where((freqs >= lo) & (freqs < hi))[0]
    if idx.size == 0:
        return _safe_db10(EPS)
    return _safe_db10(float(np.mean(psd[idx]) + EPS))


def _band_centers_31() -> np.ndarray:
    centers = [20.0]
    ratio = 2 ** (1 / 3)
    for _ in range(30):
        centers.append(centers[-1] * ratio)
    return np.array(centers, dtype=np.float64)


def _band_db_31(y_mono: np.ndarray, sr: int) -> Tuple[np.ndarray, np.ndarray]:
    freqs, psd = _fft_psd(y_mono, sr, n_fft=8192, hop=2048)
    centers = _band_centers_31()
    band_db = []

    for c in centers:
        lo = c / (2 ** (1 / 6))
        hi = c * (2 ** (1 / 6))
        lo = max(10.0, lo)
        hi = min(sr / 2.0, hi)
        band_db.append(_band_power_db(freqs, psd, lo, hi))

    return centers, np.array(band_db, dtype=np.float64)


def _compute_band_aggregates(y_mono: np.ndarray, sr: int) -> Dict[str, float]:
    freqs, psd = _fft_psd(y_mono, sr, n_fft=8192, hop=2048)

    bands = {
        "sub_20_60_db": _band_power_db(freqs, psd, 20.0, 60.0),
        "low_foundation_50_100_db": _band_power_db(freqs, psd, 50.0, 100.0),
        "bass_60_120_db": _band_power_db(freqs, psd, 60.0, 120.0),
        "lowmid_120_300_db": _band_power_db(freqs, psd, 120.0, 300.0),
        "low_body_150_300_db": _band_power_db(freqs, psd, 150.0, 300.0),
        "body_150_400_db": _band_power_db(freqs, psd, 150.0, 400.0),
        "lowmid_buildup_200_400_db": _band_power_db(freqs, psd, 200.0, 400.0),
        "mud_200_500_db": _band_power_db(freqs, psd, 200.0, 500.0),
        "presence_2k_5k_db": _band_power_db(freqs, psd, 2000.0, 5000.0),
        "harsh_2p5k_6k_db": _band_power_db(freqs, psd, 2500.0, 6000.0),
        "sibilance_5k_9k_db": _band_power_db(freqs, psd, 5000.0, 9000.0),
        "air_8k_12k_db": _band_power_db(freqs, psd, 8000.0, 12000.0),
        "air_8k_16k_db": _band_power_db(freqs, psd, 8000.0, 16000.0),
        "mid_1k_2k_db": _band_power_db(freqs, psd, 1000.0, 2000.0),
    }

    body = bands["body_150_400_db"]
    low_body = bands["low_body_150_300_db"]
    mid = bands["mid_1k_2k_db"]

    ratios = {
        "sub_to_body_db": bands["sub_20_60_db"] - body,
        "low_foundation_ratio_db": bands["low_foundation_50_100_db"] - low_body,
        "bass_to_body_db": bands["bass_60_120_db"] - body,
        "presence_to_body_db": bands["presence_2k_5k_db"] - body,
        "harshness_ratio_db": bands["harsh_2p5k_6k_db"] - body,
        "air_ratio_db": bands["air_8k_12k_db"] - low_body,
        "air16_to_body_db": bands["air_8k_16k_db"] - body,
        "lowmid_buildup_ratio_db": bands["lowmid_buildup_200_400_db"] - bands["presence_2k_5k_db"],
        "mud_to_body_db": bands["mud_200_500_db"] - body,
        "sibilance_to_mid_db": bands["sibilance_5k_9k_db"] - mid,
        "harsh_to_mid_db": bands["harsh_2p5k_6k_db"] - mid,
    }

    out = {}
    out.update({k: float(v) for k, v in bands.items()})
    out.update({k: float(v) for k, v in ratios.items()})
    return out


def _fft_lowpass(x: np.ndarray, sr: int, cutoff_hz: float) -> np.ndarray:
    if x.size == 0:
        return x.copy()
    X = np.fft.rfft(x)
    freqs = np.fft.rfftfreq(x.size, d=1.0 / sr)
    X[freqs > cutoff_hz] = 0.0
    return np.fft.irfft(X, n=x.size).astype(np.float64)


def _stereo_metrics(y_stereo: np.ndarray, sr: int) -> Dict[str, float]:
    if y_stereo.ndim == 1:
        y_stereo = np.vstack([y_stereo, y_stereo])

    L = y_stereo[0].astype(np.float64)
    R = y_stereo[1].astype(np.float64)
    M = 0.5 * (L + R)
    S = 0.5 * (L - R)

    def corr(a: np.ndarray, b: np.ndarray) -> float:
        if a.size == 0 or b.size == 0:
            return 1.0
        sa = float(np.std(a))
        sb = float(np.std(b))
        if sa < 1e-12 or sb < 1e-12:
            return 1.0
        c = float(np.corrcoef(a, b)[0, 1])
        if not np.isfinite(c):
            return 1.0
        return float(np.clip(c, -1.0, 1.0))

    full_corr = corr(L, R)

    m_energy = float(np.mean(M * M) + EPS)
    s_energy = float(np.mean(S * S) + EPS)
    side_mid_ratio_db = _safe_db10(s_energy / m_energy)

    L_low = _fft_lowpass(L, sr, 120.0)
    R_low = _fft_lowpass(R, sr, 120.0)
    M_low = 0.5 * (L_low + R_low)
    S_low = 0.5 * (L_low - R_low)

    low_corr = corr(L_low, R_low)
    low_m_energy = float(np.mean(M_low * M_low) + EPS)
    low_s_energy = float(np.mean(S_low * S_low) + EPS)
    low_side_mid_ratio_db = _safe_db10(low_s_energy / low_m_energy)

    # coherence-like proxy: 1 means mono-safe / coherent, 0 worse
    low_band_coherence = float(np.clip((low_corr + 1.0) / 2.0, 0.0, 1.0))

    low_mono_risk = 0.0
    if low_corr < 0.80:
        low_mono_risk += min(1.0, (0.80 - low_corr) / 0.80)
    if low_side_mid_ratio_db > -12.0:
        low_mono_risk += min(1.0, (low_side_mid_ratio_db + 12.0) / 12.0)
    low_mono_risk = float(np.clip(low_mono_risk / 2.0, 0.0, 1.0))

    width_proxy = float(np.clip(10.0 ** (side_mid_ratio_db / 20.0), 0.0, 4.0))

    return {
        "stereo_corr": float(full_corr),
        "side_mid_ratio_db": float(side_mid_ratio_db),
        "width_proxy": float(width_proxy),
        "low_band_corr": float(low_corr),
        "low_band_side_mid_ratio_db": float(low_side_mid_ratio_db),
        "low_band_coherence": float(low_band_coherence),
        "low_mono_risk": float(low_mono_risk),
    }


def _window_band_series_db(
    y_mono: np.ndarray,
    sr: int,
    lo_hz: float,
    hi_hz: float,
    window_sec: float,
    hop_sec: float,
) -> np.ndarray:
    win = max(1, int(round(window_sec * sr)))
    hop = max(1, int(round(hop_sec * sr)))
    n = y_mono.size
    values = []

    if n < win:
        freqs, psd = _fft_psd(y_mono, sr, n_fft=4096, hop=1024)
        values.append(_band_power_db(freqs, psd, lo_hz, hi_hz))
        return np.array(values, dtype=np.float64)

    for start in range(0, n - win + 1, hop):
        seg = y_mono[start:start + win]
        freqs, psd = _fft_psd(seg, sr, n_fft=4096, hop=1024)
        values.append(_band_power_db(freqs, psd, lo_hz, hi_hz))

    return np.array(values, dtype=np.float64)


def _risk_metrics(
    metrics: Dict[str, float],
    short_term_lufs: np.ndarray,
    momentary_lufs: np.ndarray,
    y_stereo: np.ndarray,
    sr: int,
) -> Dict[str, float]:
    mono = _mono(y_stereo)

    st_mean = float(np.mean(short_term_lufs)) if short_term_lufs.size else metrics["integrated_lufs"]
    st_max = float(np.max(short_term_lufs)) if short_term_lufs.size else metrics["integrated_lufs"]
    m_max = float(np.max(momentary_lufs)) if momentary_lufs.size else metrics["integrated_lufs"]

    plr_proxy = float(metrics["true_peak_dbtp"] - metrics["integrated_lufs"])
    short_term_gap = float(st_max - metrics["integrated_lufs"])
    momentary_gap = float(m_max - metrics["integrated_lufs"])
    tp_margin_db = float(-1.0 - metrics["true_peak_dbtp"])  # target safety example: -1 dBTP

    # Windowed harshness / sibilance peaks
    harsh_series = _window_band_series_db(mono, sr, 2500.0, 6000.0, window_sec=0.4, hop_sec=0.1)
    sib_series = _window_band_series_db(mono, sr, 5000.0, 9000.0, window_sec=0.4, hop_sec=0.1)
    body_series = _window_band_series_db(mono, sr, 150.0, 400.0, window_sec=0.4, hop_sec=0.1)
    mid_series = _window_band_series_db(mono, sr, 1000.0, 2000.0, window_sec=0.4, hop_sec=0.1)

    harsh_peak_ratio = 0.0
    sibilance_peak_ratio = 0.0
    if harsh_series.size and body_series.size:
        harsh_peak_ratio = float(np.max(harsh_series[:min(harsh_series.size, body_series.size)] - body_series[:min(harsh_series.size, body_series.size)]))
    if sib_series.size and mid_series.size:
        sibilance_peak_ratio = float(np.max(sib_series[:min(sib_series.size, mid_series.size)] - mid_series[:min(sib_series.size, mid_series.size)]))

    harshness_index = float(
        0.65 * metrics["harshness_ratio_db"] +
        0.35 * harsh_peak_ratio
    )

    sibilance_index = float(
        0.60 * metrics["sibilance_to_mid_db"] +
        0.40 * sibilance_peak_ratio
    )

    limiter_stress_proxy = float(
        0.30 * max(0.0, -plr_proxy + 10.0) +
        0.25 * max(0.0, short_term_gap - 3.0) +
        0.20 * max(0.0, momentary_gap - 4.5) +
        0.15 * max(0.0, metrics["true_peak_dbtp"] + 1.0) +
        0.10 * max(0.0, metrics["low_foundation_ratio_db"] - 2.0)
    )

    low_end_overload_proxy = float(
        0.55 * max(0.0, metrics["sub_to_body_db"] - 1.5) +
        0.45 * max(0.0, metrics["low_foundation_ratio_db"] - 1.5)
    )

    thinness_proxy = float(
        0.55 * max(0.0, -metrics["bass_to_body_db"] - 1.5) +
        0.45 * max(0.0, metrics["presence_to_body_db"] - 4.0)
    )

    punch_proxy = float(
        max(0.0, metrics["crest_db"]) *
        (1.0 + 0.15 * max(0.0, metrics["transient_index"]))
    )

    isp_risk = 1.0 if metrics["true_peak_dbtp"] > -1.0 else 0.0
    clip_risk = 1.0 if metrics["clip_ratio"] > 0.0 else 0.0
    near_clip_risk = 1.0 if metrics["near_clip_ratio"] > 5e-4 else 0.0

    return {
        "plr_proxy_db": float(plr_proxy),
        "short_term_to_integrated_gap_db": float(short_term_gap),
        "momentary_to_integrated_gap_db": float(momentary_gap),
        "tp_margin_to_minus1_dbtp_db": float(tp_margin_db),
        "harsh_peak_ratio_db": float(harsh_peak_ratio),
        "sibilance_peak_ratio_db": float(sibilance_peak_ratio),
        "harshness_index": float(harshness_index),
        "sibilance_index": float(sibilance_index),
        "limiter_stress_proxy": float(limiter_stress_proxy),
        "low_end_overload_proxy": float(low_end_overload_proxy),
        "thinness_proxy": float(thinness_proxy),
        "punch_proxy": float(punch_proxy),
        "isp_risk_flag": float(isp_risk),
        "clip_risk_flag": float(clip_risk),
        "near_clip_risk_flag": float(near_clip_risk),
    }


def _base_selector_inputs(metrics: Dict[str, float]) -> Dict[str, Any]:
    should_use_premaster = (
        metrics["true_peak_dbtp"] > -0.8 or
        metrics["clip_ratio"] > 0.0 or
        metrics["near_clip_ratio"] > 0.001 or
        metrics["harshness_index"] > 6.0 or
        metrics["low_end_overload_proxy"] > 2.0 or
        metrics["limiter_stress_proxy"] > 3.0
    )

    return {
        "input_lufs_i": float(metrics["integrated_lufs"]),
        "input_lra_ebu": float(metrics["lra_ebu"]),
        "input_true_peak_dbtp": float(metrics["true_peak_dbtp"]),
        "input_sample_peak_dbfs": float(metrics["sample_peak_dbfs"]),
        "input_harshness_index": float(metrics["harshness_index"]),
        "input_low_end_overload_proxy": float(metrics["low_end_overload_proxy"]),
        "input_near_clip_ratio": float(metrics["near_clip_ratio"]),
        "input_clip_ratio": float(metrics["clip_ratio"]),
        "suggest_base_mode": "premaster" if should_use_premaster else "original",
    }


def _branch_rule_inputs(metrics: Dict[str, float]) -> Dict[str, Any]:
    reveal_allowance = float(np.clip(
        1.0
        - 0.08 * max(0.0, metrics["harshness_index"] - 3.0)
        - 0.06 * max(0.0, metrics["sibilance_index"] - 3.0)
        - 0.08 * max(0.0, metrics["low_mono_risk"] * 5.0 - 1.0),
        0.0, 1.0
    ))

    low_support_allowance = float(np.clip(
        1.0
        - 0.10 * max(0.0, metrics["low_end_overload_proxy"] - 1.0)
        - 0.10 * max(0.0, metrics["limiter_stress_proxy"] - 2.0),
        0.0, 1.0
    ))

    polish_allowance = float(np.clip(
        1.0
        - 0.10 * max(0.0, -metrics["plr_proxy_db"] + 8.0)
        - 0.08 * max(0.0, metrics["limiter_stress_proxy"] - 2.0),
        0.0, 1.0
    ))

    limiter_mode = "safe" if (
        metrics["true_peak_dbtp"] > -1.0 or
        metrics["limiter_stress_proxy"] > 3.0 or
        metrics["low_end_overload_proxy"] > 2.0
    ) else "normal"

    return {
        "reveal_allowance": float(reveal_allowance),
        "low_support_allowance": float(low_support_allowance),
        "polish_allowance": float(polish_allowance),
        "limiter_mode_hint": limiter_mode,
    }


def _heuristic_suggestion(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    lufs_delta = float(after["integrated_lufs"] - before["integrated_lufs"])
    air_ratio = float(after["air_ratio_db"])
    harsh_ratio = float(after["harshness_ratio_db"])
    low_foundation = float(after["low_foundation_ratio_db"])

    if lufs_delta > 1.2 and after["limiter_stress_proxy"] < 3.0:
        suggested_intensity = "high"
    elif lufs_delta > -0.4:
        suggested_intensity = "balanced"
    else:
        suggested_intensity = "low"

    tone_score = 0.55 * air_ratio + 0.25 * harsh_ratio - 0.20 * low_foundation
    if tone_score >= 2.0:
        suggested_tone = "bright"
    elif tone_score <= -2.0:
        suggested_tone = "warm"
    else:
        suggested_tone = "balanced"

    return {
        "notes": "Heuristic based on loudness change, air/harshness/body balance, and branch-safe risk proxies.",
        "suggested_intensity": suggested_intensity,
        "suggested_tone": suggested_tone,
        "tone_score": float(tone_score),
        "lufs_delta": float(lufs_delta),
    }


# ---------- core analysis ----------

def _analyze_one(path: str, target_sr: int = TARGET_SR) -> Dict[str, Any]:
    y, sr = librosa.load(path, sr=target_sr, mono=False)
    if y.ndim == 1:
        y = np.vstack([y, y])

    y = _trim_stereo(y, sr, top_db=TRIM_TOP_DB)
    if y.shape[1] == 0:
        raise RuntimeError("Audio is silent or empty after trimming silence.")

    mono = _mono(y)
    duration_sec = float(y.shape[1] / sr)

    integrated_lufs = _integrated_lufs(y, sr)

    short_term_lufs = _loudness_series(y, sr, window_sec=3.0, hop_sec=1.0)
    momentary_lufs = _loudness_series(y, sr, window_sec=0.4, hop_sec=0.1)

    lra_ebu = _ebu_lra_from_short_term(short_term_lufs)
    sample_peak_dbfs = _sample_peak_dbfs(y)
    true_peak_dbtp = _true_peak_dbtp(y, sr, oversample=TRUE_PEAK_OVERSAMPLE)
    rms_dbfs = _rms_dbfs(mono)
    crest_db = _crest_db(mono)
    transient_index = _transient_index(mono, sr)
    near_clip_ratio = _near_clip_ratio(y, threshold_dbfs=-1.0)
    clip_ratio = _clip_ratio(y, threshold_linear=0.9999)

    centers, band_db = _band_db_31(mono, sr)
    band_metrics = _compute_band_aggregates(mono, sr)
    stereo_metrics = _stereo_metrics(y, sr)

    metrics: Dict[str, float] = {
        "integrated_lufs": float(integrated_lufs),
        "short_term_lufs_mean": float(np.mean(short_term_lufs)) if short_term_lufs.size else float(integrated_lufs),
        "short_term_lufs_max": float(np.max(short_term_lufs)) if short_term_lufs.size else float(integrated_lufs),
        "momentary_lufs_mean": float(np.mean(momentary_lufs)) if momentary_lufs.size else float(integrated_lufs),
        "momentary_lufs_max": float(np.max(momentary_lufs)) if momentary_lufs.size else float(integrated_lufs),
        "lra_ebu": float(lra_ebu),
        "sample_peak_dbfs": float(sample_peak_dbfs),
        "true_peak_dbtp": float(true_peak_dbtp),
        "rms_dbfs": float(rms_dbfs),
        "crest_db": float(crest_db),
        "transient_index": float(transient_index),
        "near_clip_ratio": float(near_clip_ratio),
        "clip_ratio": float(clip_ratio),
    }

    metrics.update(band_metrics)
    metrics.update(stereo_metrics)

    risk_metrics = _risk_metrics(metrics, short_term_lufs, momentary_lufs, y, sr)
    metrics.update(risk_metrics)

    metrics["tilt_indicator_db"] = float(metrics["air_8k_12k_db"] - metrics["low_body_150_300_db"])

    base_selector_inputs = _base_selector_inputs(metrics)
    branch_rule_inputs = _branch_rule_inputs(metrics)

    return {
        "sr": int(sr),
        "duration_sec": float(duration_sec),

        # Standard loudness / dynamics
        "integrated_lufs": float(metrics["integrated_lufs"]),
        "short_term_lufs_mean": float(metrics["short_term_lufs_mean"]),
        "short_term_lufs_max": float(metrics["short_term_lufs_max"]),
        "momentary_lufs_mean": float(metrics["momentary_lufs_mean"]),
        "momentary_lufs_max": float(metrics["momentary_lufs_max"]),
        "lra_ebu": float(metrics["lra_ebu"]),
        "sample_peak_dbfs": float(metrics["sample_peak_dbfs"]),
        "true_peak_dbtp": float(metrics["true_peak_dbtp"]),
        "rms_dbfs": float(metrics["rms_dbfs"]),
        "crest_db": float(metrics["crest_db"]),
        "plr_proxy_db": float(metrics["plr_proxy_db"]),
        "transient_index": float(metrics["transient_index"]),

        # Safety
        "near_clip_ratio": float(metrics["near_clip_ratio"]),
        "clip_ratio": float(metrics["clip_ratio"]),
        "tp_margin_to_minus1_dbtp_db": float(metrics["tp_margin_to_minus1_dbtp_db"]),
        "isp_risk_flag": float(metrics["isp_risk_flag"]),
        "near_clip_risk_flag": float(metrics["near_clip_risk_flag"]),
        "clip_risk_flag": float(metrics["clip_risk_flag"]),

        # 31-band map
        "band_centers_hz": [float(x) for x in centers.tolist()],
        "band_db": [float(x) for x in band_db.tolist()],

        # Band aggregates
        "sub_20_60_db": float(metrics["sub_20_60_db"]),
        "low_foundation_50_100_db": float(metrics["low_foundation_50_100_db"]),
        "bass_60_120_db": float(metrics["bass_60_120_db"]),
        "lowmid_120_300_db": float(metrics["lowmid_120_300_db"]),
        "low_body_150_300_db": float(metrics["low_body_150_300_db"]),
        "body_150_400_db": float(metrics["body_150_400_db"]),
        "lowmid_buildup_200_400_db": float(metrics["lowmid_buildup_200_400_db"]),
        "mud_200_500_db": float(metrics["mud_200_500_db"]),
        "presence_2k_5k_db": float(metrics["presence_2k_5k_db"]),
        "harsh_2p5k_6k_db": float(metrics["harsh_2p5k_6k_db"]),
        "sibilance_5k_9k_db": float(metrics["sibilance_5k_9k_db"]),
        "air_8k_12k_db": float(metrics["air_8k_12k_db"]),
        "air_8k_16k_db": float(metrics["air_8k_16k_db"]),
        "mid_1k_2k_db": float(metrics["mid_1k_2k_db"]),

        # Ratios / proxies
        "sub_to_body_db": float(metrics["sub_to_body_db"]),
        "low_foundation_ratio_db": float(metrics["low_foundation_ratio_db"]),
        "bass_to_body_db": float(metrics["bass_to_body_db"]),
        "presence_to_body_db": float(metrics["presence_to_body_db"]),
        "harshness_ratio_db": float(metrics["harshness_ratio_db"]),
        "air_ratio_db": float(metrics["air_ratio_db"]),
        "air16_to_body_db": float(metrics["air16_to_body_db"]),
        "lowmid_buildup_ratio_db": float(metrics["lowmid_buildup_ratio_db"]),
        "mud_to_body_db": float(metrics["mud_to_body_db"]),
        "sibilance_to_mid_db": float(metrics["sibilance_to_mid_db"]),
        "harsh_to_mid_db": float(metrics["harsh_to_mid_db"]),
        "tilt_indicator_db": float(metrics["tilt_indicator_db"]),

        # Stereo
        "stereo_corr": float(metrics["stereo_corr"]),
        "side_mid_ratio_db": float(metrics["side_mid_ratio_db"]),
        "width_proxy": float(metrics["width_proxy"]),
        "low_band_corr": float(metrics["low_band_corr"]),
        "low_band_side_mid_ratio_db": float(metrics["low_band_side_mid_ratio_db"]),
        "low_band_coherence": float(metrics["low_band_coherence"]),
        "low_mono_risk": float(metrics["low_mono_risk"]),

        # Risk layer
        "short_term_to_integrated_gap_db": float(metrics["short_term_to_integrated_gap_db"]),
        "momentary_to_integrated_gap_db": float(metrics["momentary_to_integrated_gap_db"]),
        "harsh_peak_ratio_db": float(metrics["harsh_peak_ratio_db"]),
        "sibilance_peak_ratio_db": float(metrics["sibilance_peak_ratio_db"]),
        "harshness_index": float(metrics["harshness_index"]),
        "sibilance_index": float(metrics["sibilance_index"]),
        "limiter_stress_proxy": float(metrics["limiter_stress_proxy"]),
        "low_end_overload_proxy": float(metrics["low_end_overload_proxy"]),
        "thinness_proxy": float(metrics["thinness_proxy"]),
        "punch_proxy": float(metrics["punch_proxy"]),

        # Rule-ready outputs
        "base_selector_inputs": base_selector_inputs,
        "branch_rule_inputs": branch_rule_inputs,
    }


# ---------- diff ----------

def _diff_report(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    numeric_keys = [
        "integrated_lufs",
        "short_term_lufs_mean",
        "short_term_lufs_max",
        "momentary_lufs_mean",
        "momentary_lufs_max",
        "lra_ebu",
        "sample_peak_dbfs",
        "true_peak_dbtp",
        "rms_dbfs",
        "crest_db",
        "plr_proxy_db",
        "transient_index",
        "near_clip_ratio",
        "clip_ratio",
        "tp_margin_to_minus1_dbtp_db",
        "sub_20_60_db",
        "low_foundation_50_100_db",
        "bass_60_120_db",
        "lowmid_120_300_db",
        "low_body_150_300_db",
        "body_150_400_db",
        "lowmid_buildup_200_400_db",
        "mud_200_500_db",
        "presence_2k_5k_db",
        "harsh_2p5k_6k_db",
        "sibilance_5k_9k_db",
        "air_8k_12k_db",
        "air_8k_16k_db",
        "sub_to_body_db",
        "low_foundation_ratio_db",
        "bass_to_body_db",
        "presence_to_body_db",
        "harshness_ratio_db",
        "air_ratio_db",
        "air16_to_body_db",
        "lowmid_buildup_ratio_db",
        "mud_to_body_db",
        "sibilance_to_mid_db",
        "harsh_to_mid_db",
        "tilt_indicator_db",
        "stereo_corr",
        "side_mid_ratio_db",
        "width_proxy",
        "low_band_corr",
        "low_band_side_mid_ratio_db",
        "low_band_coherence",
        "low_mono_risk",
        "short_term_to_integrated_gap_db",
        "momentary_to_integrated_gap_db",
        "harsh_peak_ratio_db",
        "sibilance_peak_ratio_db",
        "harshness_index",
        "sibilance_index",
        "limiter_stress_proxy",
        "low_end_overload_proxy",
        "thinness_proxy",
        "punch_proxy",
    ]

    diff: Dict[str, Any] = {}
    for key in numeric_keys:
        if key in before and key in after:
            diff[f"{key}_delta"] = float(_safe_float(after[key]) - _safe_float(before[key]))

    return diff


# ---------- public API ----------

def run_analysis(before_path: str, after_path: str, out_dir: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Returns:
      report: { before: {...}, after: {...}, diff: {...} }
      suggestion: { notes, suggested_intensity, suggested_tone, ... }

    Writes JSON to out_dir/report.json
    """
    os.makedirs(out_dir, exist_ok=True)

    before = _analyze_one(before_path, target_sr=TARGET_SR)
    after = _analyze_one(after_path, target_sr=TARGET_SR)
    diff = _diff_report(before, after)

    report = {
        "before": before,
        "after": after,
        "diff": diff,
    }
    suggestion = _heuristic_suggestion(before, after)

    with open(os.path.join(out_dir, "report.json"), "w", encoding="utf-8") as f:
        json.dump(
            {
                "report": report,
                "preset_suggestion": suggestion,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    return report, suggestion

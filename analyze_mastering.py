# analyze_mastering.py
# -*- coding: utf-8 -*-

import os
import json
import math
from typing import Dict, Tuple, Any

import numpy as np
import librosa
import pyloudnorm as pyln


# ---------- helpers ----------

def _safe_db(x: float, floor: float = 1e-18) -> float:
    return float(20.0 * np.log10(max(float(x), floor)))

def _safe_db10(x: float, floor: float = 1e-18) -> float:
    return float(10.0 * np.log10(max(float(x), floor)))

def _trim_stereo(y: np.ndarray, sr: int, top_db: float = 40.0) -> np.ndarray:
    """
    y: (2, n) or (n,)
    Returns trimmed stereo (2, n)
    """
    if y.ndim == 1:
        y = np.vstack([y, y])

    mono_sum = y.sum(axis=0)
    # Determine non-silent region via RMS frames
    energy = librosa.feature.rms(y=mono_sum, frame_length=2048, hop_length=512)[0]
    if energy.size == 0:
        return y

    thr = np.max(energy) * (10 ** (-top_db / 20.0))
    frames = np.nonzero(energy > thr)[0]
    if frames.size == 0:
        return y[:, :0]  # empty

    start_frame = int(frames[0])
    end_frame = int(frames[-1])
    start_sample = start_frame * 512
    end_sample = min(y.shape[1], int(end_frame * 512 + 2048))
    return y[:, start_sample:end_sample]

def _true_peak_dbfs(y: np.ndarray) -> float:
    # y is (2,n) or (n,)
    peak = float(np.max(np.abs(y)))
    return _safe_db(peak, floor=1e-12)

def _rms_dbfs(y: np.ndarray) -> float:
    # y is mono (n,)
    rms = float(np.sqrt(np.mean(np.square(y))))
    return _safe_db(rms, floor=1e-12)

def _crest_db(y_mono: np.ndarray) -> float:
    tp = float(np.max(np.abs(y_mono)))
    rms = float(np.sqrt(np.mean(np.square(y_mono))))
    return float(_safe_db(tp, 1e-12) - _safe_db(rms, 1e-12))

def _compute_lra(y_stereo: np.ndarray, sr: int) -> float:
    """
    Approximate EBU LRA using 3s short-term loudness (p10..p95), gated for silence.
    """
    short_window = 3.0
    step = 1.0
    hop = int(step * sr)
    win = int(short_window * sr)
    n = y_stereo.shape[1]

    if n < win:
        return 0.0

    meter = pyln.Meter(sr)
    short_terms = []
    for start in range(0, n - win + 1, hop):
        seg = y_stereo[:, start:start + win]
        if float(np.max(np.abs(seg))) < 1e-6:
            continue
        L = float(meter.integrated_loudness(seg.T))
        short_terms.append(L)

    if not short_terms:
        return 0.0

    st = np.array(short_terms, dtype=np.float64)
    return float(np.percentile(st, 95) - np.percentile(st, 10))

def _integrated_lufs(y_stereo: np.ndarray, sr: int) -> float:
    meter = pyln.Meter(sr)
    return float(meter.integrated_loudness(y_stereo.T))

def _transient_index(y_mono: np.ndarray, sr: int) -> float:
    """
    Simple transient indicator: avg spectral flux / avg energy.
    Stable across genres, used only comparatively (before vs after).
    """
    hop = 512
    n_fft = 2048
    S = np.abs(librosa.stft(y_mono, n_fft=n_fft, hop_length=hop, window="hann"))
    if S.size == 0 or S.shape[1] < 2:
        return 0.0
    # Spectral flux
    diff = np.diff(S, axis=1)
    flux = np.mean(np.maximum(diff, 0.0))
    energy = float(np.mean(S))
    if energy < 1e-12:
        return 0.0
    return float(flux / (energy + 1e-12))

def _band_centers_31() -> np.ndarray:
    """
    31-band centers starting at 20 Hz doubling every 10 bands ~like 1/3 octave-ish (close enough).
    Matches your analyzer output length = 31.
    """
    centers = [20.0]
    ratio = 2 ** (1 / 3)  # ~1/3 octave
    for _ in range(30):
        centers.append(centers[-1] * ratio)
    return np.array(centers, dtype=np.float64)

def _band_db_31(y_mono: np.ndarray, sr: int) -> Tuple[np.ndarray, np.ndarray]:
    """
    Returns (centers_hz, band_db) where band_db is average power around each center using STFT PSD.
    """
    n_fft = 8192
    hop = 2048
    S = np.abs(librosa.stft(y_mono, n_fft=n_fft, hop_length=hop, window="hann")) ** 2
    if S.size == 0:
        centers = _band_centers_31()
        return centers, np.array([float("nan")] * len(centers), dtype=np.float64)

    freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)
    psd = np.mean(S, axis=1) + 1e-18

    centers = _band_centers_31()
    band_db = []
    for c in centers:
        # Band edges as geometric midpoints between centers
        # For first/last band clamp reasonably
        if c == centers[0]:
            lo = max(10.0, c / (2 ** (1 / 6)))
            hi = c * (2 ** (1 / 6))
        elif c == centers[-1]:
            lo = c / (2 ** (1 / 6))
            hi = min(sr / 2.0, c * (2 ** (1 / 6)))
        else:
            lo = c / (2 ** (1 / 6))
            hi = c * (2 ** (1 / 6))

        idx = np.where((freqs >= lo) & (freqs < hi))[0]
        if idx.size == 0:
            band_db.append(float("nan"))
        else:
            band_db.append(_safe_db10(float(np.mean(psd[idx])) + 1e-18))
    return centers, np.array(band_db, dtype=np.float64)

def _tilt_indicator_db(y_mono: np.ndarray, sr: int) -> float:
    """
    Tilt indicator used for heuristic: (8-12 kHz) - (150-300 Hz) in dB (power-based).
    """
    n_fft = 8192
    hop = 2048
    S = np.abs(librosa.stft(y_mono, n_fft=n_fft, hop_length=hop, window="hann")) ** 2
    if S.size == 0:
        return 0.0

    freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)
    psd = np.mean(S, axis=1) + 1e-18

    def band_power(lo: float, hi: float) -> float:
        idx = np.where((freqs >= lo) & (freqs < hi))[0]
        if idx.size == 0:
            return 1e-18
        return float(np.mean(psd[idx]) + 1e-18)

    hi = band_power(8000.0, 12000.0)
    lo = band_power(150.0, 300.0)
    return float(_safe_db10(hi) - _safe_db10(lo))


# ---------- core analysis ----------

def _analyze_one(path: str, target_sr: int = 48000) -> Dict[str, Any]:
    y, sr = librosa.load(path, sr=target_sr, mono=False)
    if y.ndim == 1:
        y = np.vstack([y, y])

    y = _trim_stereo(y, sr, top_db=40.0)
    if y.shape[1] == 0:
        raise RuntimeError("Audio is silent or empty after trimming silence.")

    L = y[0]
    R = y[1]
    mono = (L + R) * 0.5

    duration_sec = float(y.shape[1] / sr)

    # Metrics
    lufs_i = _integrated_lufs(y, sr)
    lra = _compute_lra(y, sr)
    tp_dbfs = _true_peak_dbfs(y)
    rms_dbfs = _rms_dbfs(mono)
    crest_db = _crest_db(mono)
    transient_index = _transient_index(mono, sr)

    centers, band_db = _band_db_31(mono, sr)

    return {
        "sr": int(sr),
        "duration_sec": float(duration_sec),
        "lufs_i": float(lufs_i),
        "lra": float(lra),
        "true_peak_dbfs": float(tp_dbfs),
        "rms_dbfs": float(rms_dbfs),
        "crest_db": float(crest_db),
        "transient_index": float(transient_index),
        "band_centers_hz": [float(x) for x in centers.tolist()],
        "band_db": [float(x) if np.isfinite(x) else float("nan") for x in band_db.tolist()],
    }

def _heuristic_suggestion(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    """
    Very simple heuristic like your analyzer output:
    - intensity suggestion mostly from loudness delta (did mastering push too hard or too soft)
    - tone from tilt indicator (8-12k vs 150-300)
    """
    # loudness delta: after - before (note: LUFS more negative = quieter)
    lufs_delta = float(after["lufs_i"] - before["lufs_i"])
    # Use AFTER signal for tilt indicator (what result "feels" like)
    # But compute from band_db is too rough; recalc indicator from bands:
    # We'll approximate with nearest bands:
    centers = np.array(after["band_centers_hz"], dtype=np.float64)
    bands = np.array(after["band_db"], dtype=np.float64)

    def avg_band(lo: float, hi: float) -> float:
        idx = np.where((centers >= lo) & (centers <= hi))[0]
        if idx.size == 0:
            return 0.0
        vals = bands[idx]
        vals = vals[np.isfinite(vals)]
        if vals.size == 0:
            return 0.0
        return float(np.mean(vals))

    hi_db = avg_band(8000.0, 12000.0)
    lo_db = avg_band(150.0, 300.0)
    tilt_indicator_db = float(hi_db - lo_db)

    # intensity: if after got much quieter (more negative LUFS), suggest "low" (gentler target)
    # if after got a bit louder, "balanced"; if much louder, "high"
    # NOTE: because LUFS is negative, "louder" means closer to 0, so delta positive => louder
    if lufs_delta > 1.0:
        suggested_intensity = "high"
    elif lufs_delta > -0.5:
        suggested_intensity = "balanced"
    else:
        suggested_intensity = "low"

    # tone: if tilt positive => bright; negative => warm
    if tilt_indicator_db >= 2.0:
        suggested_tone = "bright"
    elif tilt_indicator_db <= -2.0:
        suggested_tone = "warm"
    else:
        suggested_tone = "balanced"

    return {
        "notes": "Heuristic based on loudness change and spectral tilt (8–12 kHz vs 150–300 Hz).",
        "suggested_intensity": suggested_intensity,
        "suggested_tone": suggested_tone,
        "tilt_indicator_db": float(tilt_indicator_db),
    }


# ---------- public API ----------

def run_analysis(before_path: str, after_path: str, out_dir: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Returns:
      report: { before: {...}, after: {...}, diff: {...} }
      suggestion: { notes, suggested_intensity, suggested_tone, tilt_indicator_db }
    Also writes JSON to out_dir/report.json for debugging if desired.
    """
    os.makedirs(out_dir, exist_ok=True)

    before = _analyze_one(before_path, target_sr=48000)
    after = _analyze_one(after_path, target_sr=48000)

    diff = {
        "lufs_delta": float(after["lufs_i"] - before["lufs_i"]),
        "lra_delta": float(after["lra"] - before["lra"]),
        "true_peak_delta_db": float(after["true_peak_dbfs"] - before["true_peak_dbfs"]),
        "rms_delta_db": float(after["rms_dbfs"] - before["rms_dbfs"]),
        "crest_delta_db": float(after["crest_db"] - before["crest_db"]),
        "transient_index_delta": float(after["transient_index"] - before["transient_index"]),
    }

    report = {"before": before, "after": after, "diff": diff}
    suggestion = _heuristic_suggestion(before, after)

    # optional save
    try:
        with open(os.path.join(out_dir, "report.json"), "w", encoding="utf-8") as f:
            json.dump({"report": report, "preset_suggestion": suggestion}, f, ensure_ascii=False)
    except Exception:
        pass

    return report, suggestion

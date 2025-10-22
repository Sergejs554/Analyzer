#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Analyze mastering: BEFORE vs AFTER
Outputs: report.json, bands_1_3_octave.csv, plots_spectra.png, plots_diff.png, preset_suggestion.json
CLI usage:
  python analyze_mastering.py --before before.mp3 --after after.mp3 --outdir report
"""

import os, argparse, json
import numpy as np
import librosa
import pyloudnorm as pyln
import pandas as pd
import matplotlib.pyplot as plt

def rms_dbfs(x):
    rms = np.sqrt(np.mean(np.square(x)) + 1e-12)
    return 20.0 * np.log10(rms)

def true_peak_dbfs(x, sr, oversample=4):
    target_sr = sr * oversample
    x_os = librosa.resample(x, orig_sr=sr, target_sr=target_sr, res_type="kaiser_best")
    peak = np.max(np.abs(x_os)) + 1e-12
    return 20.0 * np.log10(peak)

def integrated_loudness_lufs(x, sr):
    meter = pyln.Meter(sr)  # EBU R128
    return meter.integrated_loudness(x)

def loudness_range_lra(x, sr):
    """
    LRA (EBU-approx): 95th - 10th percentile of short-term LUFS (3 s),
    gated относительно интегрированной громкости (I - 20 LU).
    """
    meter = pyln.Meter(sr)

    # 1) integrated loudness (для гейта)
    try:
        I = meter.integrated_loudness(x)
    except Exception:
        I = -23.0  # дефолт, если что-то пойдёт не так

    # 2) короткие окна 3s, шаг 1s
    win = int(3.0 * sr)
    hop = int(1.0 * sr)
    sts = []
    i = 0
    while i + win <= len(x):
        seg = x[i:i+win]
        try:
            # используем тот же интегрированный как proxy short-term
            sts.append(meter.integrated_loudness(seg))
        except Exception:
            pass
        i += hop

    if not sts:
        return 0.0

    sts = np.array(sts, dtype=float)

    # 3) относительный гейт: > (I - 20 LU)
    gated = sts[sts > (I - 20.0)]
    if gated.size < 3:
        gated = sts  # если слишком мало, считаем по всем

    p10 = float(np.percentile(gated, 10))
    p95 = float(np.percentile(gated, 95))
    LRA = max(0.0, p95 - p10)
    return LRA
def one_third_octave_centers(sr):
    centers, f = [], 20.0
    step = 2 ** (1/3)
    while f < sr/2.0:
        centers.append(f); f *= step
    return np.array(centers)

def band_limits(center):
    k = 2 ** (1/6)
    return center / k, center * k

def avg_band_levels_db(x, sr, centers):
    S = np.abs(librosa.stft(x, n_fft=8192, hop_length=2048, window="hann")) ** 2
    freqs = librosa.fft_frequencies(sr=sr, n_fft=8192)
    psd = np.mean(S, axis=1) + 1e-18
    band_db = []
    for c in centers:
        lo, hi = band_limits(c)
        idx = np.where((freqs >= lo) & (freqs < hi))[0]
        band_db.append(np.nan if len(idx)==0 else 10.0*np.log10(np.mean(psd[idx])))
    return np.array(band_db)

def transient_index(x, sr):
    onset_env = librosa.onset.onset_strength(y=x, sr=sr)
    return 0.0 if onset_env.size==0 else float(np.percentile(onset_env, 95))

def analyze_file(path, sr_target=48000):
    y, sr = librosa.load(path, sr=sr_target, mono=True)
    y, _ = librosa.effects.trim(y, top_db=40)
    m = {}
    m["sr"] = sr
    m["duration_sec"] = float(len(y)/sr)
    m["lufs_i"] = float(integrated_loudness_lufs(y, sr))
    m["lra"] = float(loudness_range_lra(y, sr))
    m["rms_dbfs"] = float(rms_dbfs(y))
    m["true_peak_dbfs"] = float(true_peak_dbfs(y, sr))
    m["crest_db"] = float(m["true_peak_dbfs"] - m["rms_dbfs"])
    m["transient_index"] = transient_index(y, sr)
    centers = one_third_octave_centers(sr)
    band_db = avg_band_levels_db(y, sr, centers)
    m["band_centers_hz"] = centers.tolist()
    m["band_db"] = band_db.tolist()
    return m

def plot_spectra(centers, before_db, after_db, out_png_base):
    plt.figure(figsize=(10,5))
    plt.semilogx(centers, before_db, label="Before")
    plt.semilogx(centers, after_db, label="After")
    plt.grid(True, which="both", ls=":")
    plt.xlabel("Frequency (Hz)"); plt.ylabel("Level (dB)")
    plt.title("1/3-Octave Average Spectrum"); plt.legend(); plt.tight_layout()
    plt.savefig(f"{out_png_base}_spectra.png", dpi=150); plt.close()

    plt.figure(figsize=(10,5))
    diff = after_db - before_db
    plt.semilogx(centers, diff)
    plt.grid(True, which="both", ls=":")
    plt.xlabel("Frequency (Hz)"); plt.ylabel("After - Before (dB)")
    plt.title("Spectral Difference (After - Before)")
    plt.axhline(0); plt.tight_layout()
    plt.savefig(f"{out_png_base}_diff.png", dpi=150); plt.close()

def run_analysis(before_path, after_path, outdir):
    os.makedirs(outdir, exist_ok=True)
    m_before = analyze_file(before_path)
    m_after  = analyze_file(after_path)

    report = {
        "before": m_before, "after": m_after,
        "diff": {
            "lufs_delta": float(m_after["lufs_i"] - m_before["lufs_i"]),
            "lra_delta": float(m_after["lra"] - m_before["lra"]),
            "rms_delta_db": float(m_after["rms_dbfs"] - m_before["rms_dbfs"]),
            "true_peak_delta_db": float(m_after["true_peak_dbfs"] - m_before["true_peak_dbfs"]),
            "crest_delta_db": float(m_after["crest_db"] - m_before["crest_db"]),
            "transient_index_delta": float(m_after["transient_index"] - m_before["transient_index"]),
        }
    }

    centers = np.array(m_before["band_centers_hz"])
    b_db = np.array(m_before["band_db"]); a_db = np.array(m_after["band_db"])
    diff_db = a_db - b_db
    df = pd.DataFrame({"center_hz": centers, "before_db": b_db, "after_db": a_db, "diff_db": diff_db})
    df.to_csv(os.path.join(outdir, "bands_1_3_octave.csv"), index=False)

    with open(os.path.join(outdir, "report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    plot_spectra(centers, b_db, a_db, os.path.join(outdir, "plots"))

    lufs_delta = report["diff"]["lufs_delta"]
    hi_mask = centers >= 8000; low_mask = (centers >= 150) & (centers <= 300)
    hi_tilt = float(np.nanmean(diff_db[hi_mask])) if np.any(hi_mask) else 0.0
    low_tilt = float(np.nanmean(diff_db[low_mask])) if np.any(low_mask) else 0.0
    tilt = hi_tilt - low_tilt

    if lufs_delta < -1.0: intensity = "low"
    elif -1.0 <= lufs_delta <= 1.0: intensity = "balanced"
    else: intensity = "high"

    if tilt > 0.75: tone = "bright"
    elif tilt < -0.75: tone = "warm"
    else: tone = "balanced"

    suggestion = {"suggested_intensity": intensity, "suggested_tone": tone,
                  "tilt_indicator_db": float(tilt),
                  "notes": "Heuristic based on loudness change and spectral tilt (8–12 kHz vs 150–300 Hz)."}
    with open(os.path.join(outdir, "preset_suggestion.json"), "w", encoding="utf-8") as f:
        json.dump(suggestion, f, indent=2)

    return report, suggestion

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--before", required=True)
    ap.add_argument("--after", required=True)
    ap.add_argument("--outdir", default="mastering_report")
    args = ap.parse_args()
    report, suggestion = run_analysis(args.before, args.after, args.outdir)
    print("=== SUMMARY ===")
    print(f"LUFS Δ: {report['diff']['lufs_delta']:.2f} | LRA Δ: {report['diff']['lra_delta']:.2f} | RMS Δ (dB): {report['diff']['rms_delta_db']:.2f}")
    print(f"TP Δ (dB): {report['diff']['true_peak_delta_db']:.2f} | Crest Δ: {report['diff']['crest_delta_db']:.2f} | Transients Δ: {report['diff']['transient_index_delta']:.2f}")
    print(f"Suggested Preset → Intensity: {suggestion['suggested_intensity']} | Tone: {suggestion['suggested_tone']} (tilt={suggestion['tilt_indicator_db']:.2f} dB)")

if __name__ == "__main__":
    main()

# auto_analysis.py
import numpy as np
import librosa
import pyloudnorm as pyln

# === изменено ===
# v2: секционный анализ (energy_curve -> rolling median -> hysteresis -> min section)
#      + подготовка данных для "≤10% плавного влияния" в DSP-цепочке

def _trim_stereo(y: np.ndarray, sr: int, top_db: float = 40.0) -> np.ndarray:
    """
    Trim leading/trailing silence based on summed stereo energy (robust for stereo).
    Returns trimmed stereo array shape (2, n).
    """
    if y.ndim == 1:
        y = np.vstack([y, y])

    mono_sum = y.sum(axis=0)
    energy = librosa.feature.rms(y=mono_sum, frame_length=2048, hop_length=512)[0]
    if energy.size > 0:
        thr = np.max(energy) * (10 ** (-top_db / 20))
        frames = np.nonzero(energy > thr)[0]
        if frames.size > 0:
            start_frame = int(frames[0])
            end_frame = int(frames[-1])
            start_sample = int(start_frame * 512)
            end_sample = min(y.shape[1], int(end_frame * 512 + 2048))
            y = y[:, start_sample:end_sample]

    if y.shape[1] == 0:
        raise RuntimeError("Audio is silent or empty after trimming silence.")

    return y

def _stereo_metrics(y: np.ndarray) -> dict:
    """Return stereo width ratio and narrow flag."""
    L = y[0]; R = y[1]
    if np.std(L) < 1e-6 or np.std(R) < 1e-6:
        corr = 1.0
    else:
        corr = float(np.corrcoef(L, R)[0, 1])

    mid = (L + R) / 2.0
    side = (L - R) / 2.0
    mid_energy = float(np.mean(mid ** 2))
    side_energy = float(np.mean(side ** 2))
    width_ratio = float(side_energy / (mid_energy + 1e-12))
    stereo_narrow = (corr > 0.9 and width_ratio < 0.1)
    return {"StereoWidth": round(width_ratio, 3), "StereoNarrow": bool(stereo_narrow), "StereoCorr": corr}

def _band_powers_db(mono: np.ndarray, sr: int) -> dict:
    """Return spectral tilt and sub-excess flags based on coarse PSD bands."""
    S = np.abs(librosa.stft(mono, n_fft=8192, hop_length=2048, window="hann")) ** 2
    freqs = librosa.fft_frequencies(sr=sr, n_fft=8192)
    psd = np.mean(S, axis=1) + 1e-18

    def band_power_db(lo, hi):
        idx = np.where((freqs >= lo) & (freqs < hi))[0]
        return float(10 * np.log10(np.mean(psd[idx]) + 1e-18)) if idx.size > 0 else -80.0

    low_band = band_power_db(20, 250)
    high_band = band_power_db(5000, 15000)
    tilt = high_band - low_band

    sub_band = band_power_db(20, 50)
    bass_band = band_power_db(50, 250)
    sub_excess = (sub_band - bass_band) > 3.0

    return {"Tilt_Detail": {"low_db": low_band, "high_db": high_band, "sub_db": sub_band, "bass_db": bass_band},
            "Tilt_dB": round(float(tilt), 2),
            "SubExcess": bool(sub_excess)}

def _integrated_lufs(y_stereo: np.ndarray, sr: int) -> float:
    meter = pyln.Meter(sr)
    return float(meter.integrated_loudness(y_stereo.T))

def _approx_lra(y_stereo: np.ndarray, sr: int) -> float:
    """
    Approximate LRA via short-term integrated loudness on sliding 3s window.
    (Good enough for control logic; we can refine later if needed.)
    """
    short_window = 3.0
    step = 1.0
    hop = int(step * sr)
    win = int(short_window * sr)
    n = y_stereo.shape[1]
    if n < win:
        return 0.0

    vals = []
    meter = pyln.Meter(sr)
    for start in range(0, n - win + 1, hop):
        seg = y_stereo[:, start:start + win]
        if np.max(np.abs(seg)) < 1e-6:
            continue
        vals.append(float(meter.integrated_loudness(seg.T)))

    if not vals:
        return 0.0

    vals = np.array(vals, dtype=float)
    return float(np.percentile(vals, 95) - np.percentile(vals, 10))

def _rms_db(mid: np.ndarray) -> float:
    rms = float(np.sqrt(np.mean(mid ** 2)))
    return float(20 * np.log10(rms + 1e-12))

def _true_peak_dbfs(y_stereo: np.ndarray) -> float:
    tp = float(np.max(np.abs(y_stereo)))
    return float(20 * np.log10(tp + 1e-12))

def _rolling_median(x: np.ndarray, win: int) -> np.ndarray:
    """
    Rolling median (O(n*win)) but win is small (<= ~25), OK for our use.
    """
    if win <= 1:
        return x.copy()
    n = x.size
    out = np.empty_like(x)
    half = win // 2
    for i in range(n):
        a = max(0, i - half)
        b = min(n, i + half + 1)
        out[i] = np.median(x[a:b])
    return out

def compute_energy_curve_db(mid: np.ndarray, sr: int, window_ms: int = 600, hop_ms: int = 100) -> tuple[np.ndarray, np.ndarray]:
    """
    Energy curve in dB (RMS over window), with hop.
    Returns (times_sec, curve_db).
    """
    win = max(1, int(sr * window_ms / 1000))
    hop = max(1, int(sr * hop_ms / 1000))

    # frame RMS
    rms = librosa.feature.rms(y=mid, frame_length=win, hop_length=hop, center=True)[0]
    curve_db = 20.0 * np.log10(rms + 1e-12)

    times = (np.arange(curve_db.size) * hop) / float(sr)
    return times.astype(float), curve_db.astype(float)

def detect_sections_from_curve(
    times: np.ndarray,
    curve_db: np.ndarray,
    median_win_sec: float = 2.0,
    hop_sec: float = 0.1,
    hysteresis_db: float = 1.5,
    min_section_sec: float = 4.0,
) -> list[dict]:
    """
    Segment the track into sections using rolling median baseline + hysteresis.
    Output: list of sections [{start, end, level, mean_db, peak_db}, ...]
    where level is 0..1 normalized (relative energy).
    """
    # rolling median smoothing
    median_win = max(1, int(median_win_sec / hop_sec))
    smooth = _rolling_median(curve_db, median_win)

    # normalize to 0..1 for "influence <=10%" later
    lo = float(np.percentile(smooth, 10))
    hi = float(np.percentile(smooth, 95))
    denom = max(1e-6, hi - lo)
    level = np.clip((smooth - lo) / denom, 0.0, 1.0)

    # hysteresis-based state changes on level
    # thresholds around current "state anchor" to avoid jitter
    sections = []
    cur_start = float(times[0])
    cur_anchor = float(level[0])

    def push_section(s0, s1, idx0, idx1, anchor):
        dur = s1 - s0
        if dur <= 0:
            return
        mean_db = float(np.mean(curve_db[idx0:idx1]))
        peak_db = float(np.max(curve_db[idx0:idx1]))
        sections.append({
            "start": float(s0),
            "end": float(s1),
            "level": float(anchor),
            "mean_db": mean_db,
            "peak_db": peak_db,
        })

    thr = float(hysteresis_db)
    # convert hysteresis in dB into approximate level hysteresis:
    # we map dB range -> level range via denom (hi-lo). clamp.
    h = float(np.clip(thr / max(1e-6, (hi - lo)), 0.02, 0.25))

    last_idx = 0
    for i in range(1, len(level)):
        # if deviates beyond hysteresis band -> new section
        if level[i] > cur_anchor + h or level[i] < cur_anchor - h:
            s0 = cur_start
            s1 = float(times[i])
            if (s1 - s0) >= min_section_sec:
                push_section(s0, s1, last_idx, i, cur_anchor)
                cur_start = s1
                last_idx = i
                cur_anchor = float(level[i])
            else:
                # too short: do not cut; slowly move anchor a bit
                cur_anchor = float(0.9 * cur_anchor + 0.1 * level[i])

    # tail
    push_section(cur_start, float(times[-1] + hop_sec), last_idx, len(level), cur_anchor)

    # merge tiny leftovers (second pass)
    merged = []
    for sec in sections:
        if not merged:
            merged.append(sec)
            continue
        prev = merged[-1]
        if (sec["end"] - sec["start"]) < min_section_sec:
            # merge into prev
            prev["end"] = sec["end"]
            prev["mean_db"] = float((prev["mean_db"] + sec["mean_db"]) / 2.0)
            prev["peak_db"] = max(prev["peak_db"], sec["peak_db"])
            prev["level"] = float((prev["level"] + sec["level"]) / 2.0)
        else:
            merged.append(sec)

    return merged

def build_section_influence_map(
    sections: list[dict],
    max_influence: float = 0.10,
    curve: str = "smoothstep",
) -> list[dict]:
    """
    Convert section 'level' (0..1) into a gentle multiplier/offset coefficient for DSP control.
    Influence is capped at ±max_influence.
    Output sections with "influence" in [-max_influence, +max_influence].

    NOTE: This does not apply DSP itself. It's a control signal for app.py/smart_auto.py later.
    """
    if not sections:
        return []

    levels = np.array([s["level"] for s in sections], dtype=float)
    # center around median to get +/- direction
    center = float(np.median(levels))
    x = np.clip(levels - center, -0.5, 0.5) * 2.0  # roughly [-1..1]

    def smoothstep(t):
        # t in [-1..1] -> smooth [-1..1]
        s = np.sign(t)
        a = np.abs(t)
        return s * (a * a * (3 - 2 * a))

    if curve == "smoothstep":
        x2 = smoothstep(x)
    else:
        x2 = x

    infl = np.clip(x2 * max_influence, -max_influence, max_influence)

    out = []
    for sec, inf in zip(sections, infl):
        out.append({**sec, "influence": float(inf)})
    return out

# === исходная функция сохранена + расширена ===
def analyze_file(path: str, target_sr: int = 48000) -> dict:
    """
    Global analysis:
    LUFS, RMS (dB), TruePeak (dBFS), LRA, Tilt (dB difference high vs low freq),
    SubExcess, StereoWidth, StereoNarrow
    """
    y, sr = librosa.load(path, sr=target_sr, mono=False)
    if y.ndim == 1:
        y = np.vstack([y, y])

    y = _trim_stereo(y, sr, top_db=40.0)

    L = y[0]; R = y[1]
    mid = (L + R) / 2.0
    mono_sum = y.sum(axis=0)

    stereo = _stereo_metrics(y)
    bands = _band_powers_db(mono_sum, sr)

    loudness = _integrated_lufs(y, sr)
    LRA = _approx_lra(y, sr)
    rms_db = _rms_db(mid)
    tp_dbfs = _true_peak_dbfs(y)

    return {
        "LUFS": float(loudness),
        "LRA": round(float(LRA), 2),
        "TruePeak_dBFS": round(float(tp_dbfs), 2),
        "RMS_dB": round(float(rms_db), 2),
        "Tilt_dB": bands["Tilt_dB"],
        "SubExcess": bool(bands["SubExcess"]),
        "StereoWidth": stereo["StereoWidth"],
        "StereoNarrow": bool(stereo["StereoNarrow"]),
    }

# === v2: новый публичный метод секционного анализа ===
def analyze_sections(
    path: str,
    target_sr: int = 48000,
    window_ms: int = 600,
    hop_ms: int = 100,
    median_win_sec: float = 2.0,
    hysteresis_db: float = 1.5,
    min_section_sec: float = 4.0,
    max_influence: float = 0.10,
) -> dict:
    """
    Returns:
    {
      "global": {... analyze_file ...},
      "energy_curve": {"hop_sec":..., "times": [...], "curve_db":[...], "smooth_db":[...]},
      "sections": [{start,end,level,mean_db,peak_db,influence}, ...]
    }
    """
    y, sr = librosa.load(path, sr=target_sr, mono=False)
    if y.ndim == 1:
        y = np.vstack([y, y])

    y = _trim_stereo(y, sr, top_db=40.0)
    mid = (y[0] + y[1]) / 2.0

    times, curve_db = compute_energy_curve_db(mid, sr, window_ms=window_ms, hop_ms=hop_ms)
    hop_sec = float(hop_ms / 1000.0)

    # smooth curve (for debug/telemetry)
    median_win = max(1, int(median_win_sec / hop_sec))
    smooth_db = _rolling_median(curve_db, median_win)

    sections = detect_sections_from_curve(
        times=times,
        curve_db=curve_db,
        median_win_sec=median_win_sec,
        hop_sec=hop_sec,
        hysteresis_db=hysteresis_db,
        min_section_sec=min_section_sec,
    )
    sections = build_section_influence_map(sections, max_influence=max_influence, curve="smoothstep")

    return {
        "global": analyze_file(path, target_sr=target_sr),
        "energy_curve": {
            "window_ms": int(window_ms),
            "hop_ms": int(hop_ms),
            "times": times.tolist(),
            "curve_db": curve_db.tolist(),
            "smooth_db": smooth_db.tolist(),
        },
        "sections": sections,
    }

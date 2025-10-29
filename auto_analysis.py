# auto_analysis.py

import numpy as np
import librosa, pyloudnorm as pyln

# === changed ===
def analyze_file(path: str, target_sr: int = 48000) -> dict:
    """
    Analyze an audio file and return metrics:
    LUFS, RMS (dB), TruePeak (dBFS), LRA, Tilt (dB difference high vs low freq), 
    SubExcess (bool for excessive sub-bass), StereoWidth (0 to 1), and a flag if width is low.
    """
    # Load audio in stereo (do not mix to mono yet)
    # librosa.load will return a numpy array of shape (n,) for mono or (2,n) for stereo
    y, sr = librosa.load(path, sr=target_sr, mono=False)
    if y.ndim == 1:
        # Convert mono to 2D array with identical channels for consistency
        y = np.vstack([y, y])
    # Trim silence from both channels (use combined amplitude)
    mono_sum = y.sum(axis=0)  # sum L+R to detect silent parts
    yt, _ = librosa.effects.trim(mono_sum, top_db=40)
    # Apply same trim indices to stereo signal
    # (librosa.effects.trim returns trimmed signal, but we want indices)
    # We can get indices by librosa.effects.trim on mono, but easier:
    # Actually, librosa.effects.trim already applied it and returned trimmed mono. 
    # We'll find non-silent indices manually for stereo:
    energy = librosa.feature.rms(y=mono_sum, frame_length=2048, hop_length=512)[0]
    # Determine trim by threshold (40 dB below max)
    if energy.size > 0:
        thr = np.max(energy) * (10**(-40/20))
        # find frames above threshold
        frames = np.nonzero(energy > thr)[0]
        if frames.size > 0:
            start_frame = frames[0]
            end_frame = frames[-1]
            start_sample = int(start_frame * 512)
            end_sample = min(y.shape[1], int(end_frame * 512 + 2048))
            y = y[:, start_sample:end_sample]
    # If no audio remains after trim, raise error
    if y.shape[1] == 0:
        raise RuntimeError("Audio is silent or empty after trimming silence.")

    # Calculate stereo width: correlation between L and R
    L = y[0]; R = y[1]
    if np.std(L) < 1e-6 or np.std(R) < 1e-6:
        # One channel is essentially silent, set correlation to 1 (mono-like)
        corr = 1.0
    else:
        corr = float(np.corrcoef(L, R)[0, 1])
    mid = (L + R) / 2.0
    side = (L - R) / 2.0
    mid_energy = np.mean(mid**2); side_energy = np.mean(side**2)
    # Width metric: ratio of side to mid energy (0 = purely mono, 1 = side equals mid)
    width_ratio = float(side_energy / (mid_energy + 1e-12))
    stereo_narrow = (corr > 0.9 and width_ratio < 0.1)  # flag if extremely narrow stereo

    # Compute loudness metrics
    meter = pyln.Meter(sr)
    loudness = meter.integrated_loudness(y.T)  # supply stereo signal (channels x samples transposed)
    # LRA (Loudness Range): use pyloudnorm if available or custom calculation
    # pyloudnorm Meter does not directly give LRA, so we approximate:
    short_window = 3.0  # 3 seconds window per EBU spec
    step = 1.0          # 1 sec step
    n_samples = y.shape[1]
    hop = int(step * sr)
    win = int(short_window * sr)
    if n_samples < win:
        LRA = 0.0
    else:
        # Compute gated short-term loudness for each 3s segment
        short_terms = []
        for start in range(0, n_samples - win + 1, hop):
            segment = y[:, start:start+win]
            # skip if segment is silent
            if np.max(np.abs(segment)) < 1e-6:
                continue
            L_segment = pyln.Meter(sr).integrated_loudness(segment.T)
            short_terms.append(L_segment)
        if not short_terms:
            LRA = 0.0
        else:
            short_terms = np.array(short_terms)
            # EBU R128 LRA: difference between 10th and 95th percentile of short-term loudness (excluding silent parts)
            low_percentile = np.percentile(short_terms, 10)
            high_percentile = np.percentile(short_terms, 95)
            LRA = float(high_percentile - low_percentile)
    # Calculate standard RMS and True Peak
    rms = float(np.sqrt(np.mean((mid)**2)))  # use mid (mono mix) for RMS
    rms_db = 20 * np.log10(rms + 1e-12)
    true_peak = float(np.max(np.abs(y)))  # max of either channel
    tp_dbfs = 20 * np.log10(true_peak + 1e-12)

    # Spectral analysis for tilt
    S = np.abs(librosa.stft(mono_sum, n_fft=8192, hop_length=2048, window="hann")) ** 2
    freqs = librosa.fft_frequencies(sr=sr, n_fft=8192)
    psd = np.mean(S, axis=1) + 1e-18
    def band_power_db(lo, hi):
        idx = np.where((freqs >= lo) & (freqs < hi))[0]
        return float(10 * np.log10(np.mean(psd[idx]) + 1e-18)) if idx.size > 0 else -80.0
    low_band = band_power_db(20, 250)    # Low frequencies (sub and bass)
    mid_band = band_power_db(250, 5000)  # Mid frequencies
    high_band = band_power_db(5000, 15000)  # High frequencies (treble)
    tilt = high_band - low_band  # difference in dB between high and low end
    sub_band = band_power_db(20, 50)    # Sub-bass region
    bass_band = band_power_db(50, 250)  # Bass region (just above sub)
    sub_excess = (sub_band - bass_band) > 3.0

    return {
        "LUFS": float(loudness),
        "LRA": round(LRA, 2),
        "TruePeak_dBFS": round(tp_dbfs, 2),
        "RMS_dB": round(rms_db, 2),
        "Tilt_dB": round(tilt, 2),
        "SubExcess": bool(sub_excess),
        "StereoWidth": round(width_ratio, 3),
        "StereoNarrow": bool(stereo_narrow)
    }

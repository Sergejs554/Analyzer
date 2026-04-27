from __future__ import annotations

import math
import os
import re
import tempfile
from typing import Any

import librosa
import numpy as np
import soundfile as sf
from scipy import signal
from scipy.ndimage import maximum_filter1d, uniform_filter1d


def _read(obj: Any, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _safe_name(x: str | None) -> str:
    x = (x or "x").strip().lower()
    x = re.sub(r"[^a-z0-9_]+", "_", x)
    x = re.sub(r"_+", "_", x).strip("_")
    return x or "x"


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _db_to_lin(x_db: float) -> float:
    return float(10.0 ** (float(x_db) / 20.0))


def _lin_to_db(x_lin: float) -> float:
    x_lin = max(float(x_lin), 1e-12)
    return 20.0 * math.log10(x_lin)


def _rms_dbfs(audio: np.ndarray) -> float:
    audio = np.asarray(audio, dtype=np.float32)
    if audio.size == 0:
        return -120.0
    rms = float(np.sqrt(np.mean(np.square(audio)) + 1e-12))
    return 20.0 * math.log10(max(rms, 1e-12))


def _ensure_2d_audio(audio: np.ndarray) -> np.ndarray:
    audio = np.asarray(audio, dtype=np.float32)
    if audio.ndim == 1:
        return audio[:, None]
    return audio.astype(np.float32, copy=False)


def _match_length(x: np.ndarray, target_len: int) -> np.ndarray:
    x = np.asarray(x)
    if len(x) == target_len:
        return x
    if len(x) > target_len:
        return x[:target_len]

    pad_shape = (target_len - len(x),) + x.shape[1:]
    pad = np.zeros(pad_shape, dtype=x.dtype)
    return np.concatenate([x, pad], axis=0)


def _write_wav(path: str, audio: np.ndarray, sr: int) -> None:
    sf.write(
        path,
        np.asarray(audio, dtype=np.float32),
        sr,
        format="WAV",
        subtype="PCM_24",
    )


def _load_audio(path: str) -> tuple[np.ndarray, int]:
    audio, sr = sf.read(path, always_2d=True)
    return _ensure_2d_audio(audio), int(sr)


def _make_tmp_wav(td: str, stage_name: str, stack_name: str, instance_name: str) -> str:
    fname = f"{_safe_name(stage_name)}__{_safe_name(stack_name)}__{_safe_name(instance_name)}.wav"
    return os.path.join(td, fname)


def _log_gaussian_band(freqs_hz: np.ndarray, center_hz: float, q: float) -> np.ndarray:
    center_hz = max(float(center_hz), 20.0)
    q = max(float(q), 0.35)

    safe_freqs = np.maximum(freqs_hz, 1.0)
    sigma_oct = 0.50 / q
    distance_oct = np.log2(safe_freqs / center_hz)
    weights = np.exp(-0.5 * (distance_oct / max(sigma_oct, 1e-6)) ** 2)

    if len(weights) > 0:
        weights[0] = 0.0
    return weights.astype(np.float32)


def _soft_band_weights(freqs_hz: np.ndarray, low_hz: float, high_hz: float) -> np.ndarray:
    low_hz = max(float(low_hz), 20.0)
    high_hz = max(float(high_hz), low_hz + 10.0)

    safe_freqs = np.maximum(freqs_hz, 1.0)
    log_f = np.log2(safe_freqs)
    log_low = math.log2(low_hz)
    log_high = math.log2(high_hz)

    slope = 0.08
    rise = 1.0 / (1.0 + np.exp(-(log_f - log_low) / slope))
    fall = 1.0 / (1.0 + np.exp((log_f - log_high) / slope))
    weights = rise * fall

    if len(weights) > 0:
        weights[0] = 0.0
    return weights.astype(np.float32)


def _high_shelf_weights(freqs_hz: np.ndarray, center_hz: float, softness_oct: float = 0.45) -> np.ndarray:
    center_hz = max(float(center_hz), 20.0)
    softness_oct = max(float(softness_oct), 0.10)

    safe_freqs = np.maximum(freqs_hz, 1.0)
    distance_oct = np.log2(safe_freqs / center_hz)
    weights = 1.0 / (1.0 + np.exp(-(distance_oct / softness_oct)))

    if len(weights) > 0:
        weights[0] = 0.0
    return weights.astype(np.float32)


def _upper_tilt_weights(freqs_hz: np.ndarray, pivot_hz: float, softness_oct: float = 0.35) -> np.ndarray:
    pivot_hz = max(float(pivot_hz), 100.0)
    softness_oct = max(float(softness_oct), 0.10)

    safe_freqs = np.maximum(freqs_hz, 1.0)
    distance_oct = np.log2(safe_freqs / pivot_hz)
    weights = 1.0 / (1.0 + np.exp(-(distance_oct / softness_oct)))

    if len(weights) > 0:
        weights[0] = 0.0
    return weights.astype(np.float32)


def _smooth_envelope_frames(
    x: np.ndarray,
    attack_ms: float,
    release_ms: float,
    hop_samples: int,
    sr: int,
) -> np.ndarray:
    x = np.asarray(x, dtype=np.float32)
    if x.size == 0:
        return x

    attack_ms = max(float(attack_ms), 1.0)
    release_ms = max(float(release_ms), attack_ms)

    attack_coeff = math.exp(-hop_samples / (sr * attack_ms * 0.001))
    release_coeff = math.exp(-hop_samples / (sr * release_ms * 0.001))

    out = np.zeros_like(x, dtype=np.float32)
    prev = float(x[0])

    for i, v in enumerate(x):
        v = float(v)
        coeff = attack_coeff if v > prev else release_coeff
        prev = coeff * prev + (1.0 - coeff) * v
        out[i] = prev

    return out


def _stft_params(sr: int) -> tuple[int, int]:
    n_fft = 2048 if sr >= 44100 else 1024
    hop = n_fft // 8
    return n_fft, hop


def _apply_framewise_spectral_gain(
    x: np.ndarray,
    sr: int,
    shape_weights: np.ndarray,
    gain_frames_db: np.ndarray,
) -> np.ndarray:
    x = np.asarray(x, dtype=np.float32)
    n_fft, hop = _stft_params(sr)

    Z = librosa.stft(
        x,
        n_fft=n_fft,
        hop_length=hop,
        win_length=n_fft,
        window="hann",
        center=True,
    )

    if Z.size == 0:
        return x.copy()

    frame_count = Z.shape[1]
    gain_frames_db = np.asarray(gain_frames_db, dtype=np.float32)

    if gain_frames_db.size == 0:
        gain_frames_db = np.zeros(frame_count, dtype=np.float32)
    elif len(gain_frames_db) < frame_count:
        pad = np.full(
            frame_count - len(gain_frames_db),
            float(gain_frames_db[-1]),
            dtype=np.float32,
        )
        gain_frames_db = np.concatenate([gain_frames_db, pad], axis=0)
    elif len(gain_frames_db) > frame_count:
        gain_frames_db = gain_frames_db[:frame_count]

    gain_matrix_db = shape_weights[:, None] * gain_frames_db[None, :]
    gain_matrix_lin = np.power(10.0, gain_matrix_db / 20.0).astype(np.complex64)

    Zp = Z * gain_matrix_lin
    y = librosa.istft(
        Zp,
        hop_length=hop,
        win_length=n_fft,
        window="hann",
        center=True,
        length=len(x),
    )

    return _match_length(np.asarray(y, dtype=np.float32), len(x))


def _build_band_activity_frames(
    detector_signal: np.ndarray,
    sr: int,
    center_hz: float,
    q: float,
    attack_ms: float,
    release_ms: float,
) -> np.ndarray:
    detector_signal = np.asarray(detector_signal, dtype=np.float32)
    n_fft, hop = _stft_params(sr)

    Z = librosa.stft(
        detector_signal,
        n_fft=n_fft,
        hop_length=hop,
        win_length=n_fft,
        window="hann",
        center=True,
    )
    freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)

    if Z.size == 0:
        return np.zeros(0, dtype=np.float32)

    det_weights = _log_gaussian_band(freqs, center_hz=center_hz, q=max(q * 0.9, 0.35))
    band_power = (np.abs(Z) ** 2 * det_weights[:, None]).sum(axis=0) / (det_weights.sum() + 1e-12)
    band_rms = np.sqrt(np.maximum(band_power, 1e-18)).astype(np.float32)

    detector_env = _smooth_envelope_frames(
        band_rms,
        attack_ms=attack_ms,
        release_ms=release_ms,
        hop_samples=hop,
        sr=sr,
    )

    if float(np.max(detector_env)) <= 1e-9:
        return np.zeros_like(detector_env, dtype=np.float32)

    low_thr = float(np.quantile(detector_env, 0.60))
    high_thr = float(np.quantile(detector_env, 0.95))

    if high_thr <= low_thr + 1e-9:
        return np.ones_like(detector_env, dtype=np.float32)

    return np.clip((detector_env - low_thr) / (high_thr - low_thr), 0.0, 1.0).astype(np.float32)


def _build_wideband_activity_frames(
    detector_signal: np.ndarray,
    sr: int,
    attack_ms: float,
    release_ms: float,
) -> np.ndarray:
    detector_signal = np.asarray(detector_signal, dtype=np.float32)
    frame_length = 2048 if sr >= 44100 else 1024
    hop_length = frame_length // 8

    rms = librosa.feature.rms(
        y=detector_signal,
        frame_length=frame_length,
        hop_length=hop_length,
        center=True,
    )[0].astype(np.float32)

    env = _smooth_envelope_frames(
        rms,
        attack_ms=attack_ms,
        release_ms=release_ms,
        hop_samples=hop_length,
        sr=sr,
    )

    if float(np.max(env)) <= 1e-9:
        return np.zeros_like(env, dtype=np.float32)

    low_thr = float(np.quantile(env, 0.55))
    high_thr = float(np.quantile(env, 0.95))

    if high_thr <= low_thr + 1e-9:
        return np.ones_like(env, dtype=np.float32)

    return np.clip((env - low_thr) / (high_thr - low_thr), 0.0, 1.0).astype(np.float32)


def _apply_fixed_bell_eq_audio(audio: np.ndarray, sr: int, center_hz: float, q: float, gain_db: float) -> np.ndarray:
    audio = _ensure_2d_audio(audio)
    n_fft, _ = _stft_params(sr)
    freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)
    shape_weights = _log_gaussian_band(freqs, center_hz=center_hz, q=q)

    out = []
    gain_frames_db = np.array([float(gain_db)], dtype=np.float32)

    for ch in range(audio.shape[1]):
        y = _apply_framewise_spectral_gain(
            x=audio[:, ch],
            sr=sr,
            shape_weights=shape_weights,
            gain_frames_db=gain_frames_db,
        )
        out.append(y)

    return np.stack(out, axis=1).astype(np.float32)


def _apply_fixed_high_shelf_audio(audio: np.ndarray, sr: int, center_hz: float, gain_db: float) -> np.ndarray:
    audio = _ensure_2d_audio(audio)
    n_fft, _ = _stft_params(sr)
    freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)
    shape_weights = _high_shelf_weights(freqs, center_hz=center_hz, softness_oct=0.45)

    out = []
    gain_frames_db = np.array([float(gain_db)], dtype=np.float32)

    for ch in range(audio.shape[1]):
        y = _apply_framewise_spectral_gain(
            x=audio[:, ch],
            sr=sr,
            shape_weights=shape_weights,
            gain_frames_db=gain_frames_db,
        )
        out.append(y)

    return np.stack(out, axis=1).astype(np.float32)


def _apply_dynamic_eq_audio(audio: np.ndarray, sr: int, op: Any) -> np.ndarray:
    audio = _ensure_2d_audio(audio)

    params = dict(_read(op, "params", {}) or {})
    center_hz = float(params.get("freq_hz", 1000.0))
    q = float(params.get("q", 1.0))
    gain_db = float(params.get("gain_db", 0.0))
    attack_ms = float(params.get("attack_ms", 10.0))
    release_ms = float(params.get("release_ms", 100.0))
    channel_mode = str(_read(op, "channel_mode", "stereo") or "stereo").lower()

    if audio.shape[1] == 1:
        detector = audio[:, 0]
        activity_frames = _build_band_activity_frames(
            detector_signal=detector,
            sr=sr,
            center_hz=center_hz,
            q=q,
            attack_ms=attack_ms,
            release_ms=release_ms,
        )
        n_fft, hop = _stft_params(sr)
        frame_count = librosa.stft(
            detector,
            n_fft=n_fft,
            hop_length=hop,
            win_length=n_fft,
            window="hann",
            center=True,
        ).shape[1]
        gain_frames_db = (activity_frames[:frame_count] * float(gain_db)).astype(np.float32)
        shape_weights = _log_gaussian_band(
            librosa.fft_frequencies(sr=sr, n_fft=n_fft),
            center_hz=center_hz,
            q=q,
        )
        y = _apply_framewise_spectral_gain(
            x=audio[:, 0],
            sr=sr,
            shape_weights=shape_weights,
            gain_frames_db=gain_frames_db,
        )
        return y[:, None]

    left = audio[:, 0]
    right = audio[:, 1]

    if channel_mode == "mid":
        mid = 0.5 * (left + right)
        side = 0.5 * (left - right)

        activity_frames = _build_band_activity_frames(
            detector_signal=mid,
            sr=sr,
            center_hz=center_hz,
            q=q,
            attack_ms=attack_ms,
            release_ms=release_ms,
        )
        n_fft, hop = _stft_params(sr)
        frame_count = librosa.stft(
            mid,
            n_fft=n_fft,
            hop_length=hop,
            win_length=n_fft,
            window="hann",
            center=True,
        ).shape[1]
        gain_frames_db = (activity_frames[:frame_count] * float(gain_db)).astype(np.float32)
        shape_weights = _log_gaussian_band(
            librosa.fft_frequencies(sr=sr, n_fft=n_fft),
            center_hz=center_hz,
            q=q,
        )

        mid_processed = _apply_framewise_spectral_gain(
            x=mid,
            sr=sr,
            shape_weights=shape_weights,
            gain_frames_db=gain_frames_db,
        )

        out_left = mid_processed + side
        out_right = mid_processed - side
        return np.stack([out_left, out_right], axis=1).astype(np.float32)

    detector = 0.5 * (left + right)
    activity_frames = _build_band_activity_frames(
        detector_signal=detector,
        sr=sr,
        center_hz=center_hz,
        q=q,
        attack_ms=attack_ms,
        release_ms=release_ms,
    )

    n_fft, hop = _stft_params(sr)
    frame_count = librosa.stft(
        detector,
        n_fft=n_fft,
        hop_length=hop,
        win_length=n_fft,
        window="hann",
        center=True,
    ).shape[1]

    gain_frames_db = (activity_frames[:frame_count] * float(gain_db)).astype(np.float32)
    shape_weights = _log_gaussian_band(
        librosa.fft_frequencies(sr=sr, n_fft=n_fft),
        center_hz=center_hz,
        q=q,
    )

    out_left = _apply_framewise_spectral_gain(
        x=left,
        sr=sr,
        shape_weights=shape_weights,
        gain_frames_db=gain_frames_db,
    )
    out_right = _apply_framewise_spectral_gain(
        x=right,
        sr=sr,
        shape_weights=shape_weights,
        gain_frames_db=gain_frames_db,
    )

    return np.stack([out_left, out_right], axis=1).astype(np.float32)


def _apply_dynamic_tilt_audio(audio: np.ndarray, sr: int, op: Any) -> np.ndarray:
    audio = _ensure_2d_audio(audio)
    params = dict(_read(op, "params", {}) or {})

    pivot_hz = float(params.get("pivot_hz", 1500.0))
    tilt_db = float(params.get("tilt_db", 0.0))
    attack_ms = float(params.get("attack_ms", 12.0))
    release_ms = float(params.get("release_ms", 150.0))
    channel_mode = str(_read(op, "channel_mode", "stereo") or "stereo").lower()

    if abs(tilt_db) <= 1e-9:
        return audio.copy()

    if audio.shape[1] == 1:
        detector = audio[:, 0]
    elif channel_mode == "mid":
        detector = 0.5 * (audio[:, 0] + audio[:, 1])
    else:
        detector = np.mean(audio, axis=1)

    activity_frames = _build_wideband_activity_frames(
        detector_signal=detector,
        sr=sr,
        attack_ms=attack_ms,
        release_ms=release_ms,
    )

    n_fft, hop = _stft_params(sr)
    freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)
    shape_weights = _upper_tilt_weights(freqs, pivot_hz=pivot_hz, softness_oct=0.35)

    frame_count = librosa.stft(
        detector,
        n_fft=n_fft,
        hop_length=hop,
        win_length=n_fft,
        window="hann",
        center=True,
    ).shape[1]

    activity_frames = _match_length(activity_frames, frame_count)
    gain_frames_db = (activity_frames * tilt_db).astype(np.float32)

    if audio.shape[1] == 1:
        y = _apply_framewise_spectral_gain(
            x=audio[:, 0],
            sr=sr,
            shape_weights=shape_weights,
            gain_frames_db=gain_frames_db,
        )
        return y[:, None]

    out = []
    for ch in range(audio.shape[1]):
        y = _apply_framewise_spectral_gain(
            x=audio[:, ch],
            sr=sr,
            shape_weights=shape_weights,
            gain_frames_db=gain_frames_db,
        )
        out.append(y)

    return np.stack(out, axis=1).astype(np.float32)


def _apply_band_component_audio(audio: np.ndarray, sr: int, low_hz: float, high_hz: float) -> np.ndarray:
    audio = _ensure_2d_audio(audio)
    n_fft, hop = _stft_params(sr)
    freqs = librosa.fft_frequencies(sr=sr, n_fft=n_fft)
    band_weights = _soft_band_weights(freqs, low_hz=low_hz, high_hz=high_hz)

    out = []

    for ch in range(audio.shape[1]):
        x = audio[:, ch]
        Z = librosa.stft(
            x,
            n_fft=n_fft,
            hop_length=hop,
            win_length=n_fft,
            window="hann",
            center=True,
        )
        if Z.size == 0:
            out.append(x.copy())
            continue

        Zp = Z * band_weights[:, None]
        y = librosa.istft(
            Zp,
            hop_length=hop,
            win_length=n_fft,
            window="hann",
            center=True,
            length=len(x),
        )
        out.append(_match_length(np.asarray(y, dtype=np.float32), len(x)))

    return np.stack(out, axis=1).astype(np.float32)


def _filter_band_mono(
    x: np.ndarray,
    sr: int,
    *,
    low_cut_hz: float | None = None,
    high_cut_hz: float | None = None,
) -> np.ndarray:
    y = np.asarray(x, dtype=np.float32)
    if y.size <= 8:
        return y.copy()

    nyq = max(float(sr) * 0.5, 1.0)

    try:
        if low_cut_hz is not None and float(low_cut_hz) > 20.0:
            low = _clamp(float(low_cut_hz) / nyq, 0.0005, 0.98)
            sos = signal.butter(2, low, btype="highpass", output="sos")
            y = signal.sosfilt(sos, y).astype(np.float32)

        if high_cut_hz is not None and float(high_cut_hz) > 20.0:
            high = _clamp(float(high_cut_hz) / nyq, 0.0005, 0.98)
            sos = signal.butter(2, high, btype="lowpass", output="sos")
            y = signal.sosfilt(sos, y).astype(np.float32)

        return np.nan_to_num(y, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)

    except Exception:
        return np.zeros_like(y, dtype=np.float32)


def _to_mid_side(audio: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    audio = _ensure_2d_audio(audio)

    if audio.shape[1] < 2:
        mono = audio[:, 0]
        return mono.copy(), np.zeros_like(mono)

    left = audio[:, 0]
    right = audio[:, 1]
    mid = (left + right) * 0.5
    side = (left - right) * 0.5
    return mid.astype(np.float32), side.astype(np.float32)


def _from_mid_side(mid: np.ndarray, side: np.ndarray) -> np.ndarray:
    left = mid + side
    right = mid - side
    return np.stack([left, right], axis=1).astype(np.float32)


def _build_parallel_compressor_gain(
    detector_signal: np.ndarray,
    sr: int,
    threshold_db: float,
    ratio: float,
    attack_ms: float,
    release_ms: float,
) -> np.ndarray:
    detector_signal = np.asarray(detector_signal, dtype=np.float32)

    frame_length = 1024 if sr >= 44100 else 512
    hop_length = frame_length // 4

    rms = librosa.feature.rms(
        y=detector_signal,
        frame_length=frame_length,
        hop_length=hop_length,
        center=True,
    )[0].astype(np.float32)

    rms_db = 20.0 * np.log10(np.maximum(rms, 1e-8))
    ratio = max(float(ratio), 1.0)

    over_db = np.maximum(rms_db - float(threshold_db), 0.0)
    target_reduction_db = over_db * (1.0 - 1.0 / ratio)

    reduction_db = _smooth_envelope_frames(
        target_reduction_db.astype(np.float32),
        attack_ms=attack_ms,
        release_ms=release_ms,
        hop_samples=hop_length,
        sr=sr,
    )

    frame_times = librosa.frames_to_time(
        np.arange(len(reduction_db)),
        sr=sr,
        hop_length=hop_length,
    )
    sample_times = np.arange(len(detector_signal), dtype=np.float32) / float(sr)

    if len(frame_times) == 0:
        return np.ones(len(detector_signal), dtype=np.float32)

    reduction_sample_db = np.interp(
        sample_times,
        frame_times,
        reduction_db,
        left=float(reduction_db[0]),
        right=float(reduction_db[-1]),
    ).astype(np.float32)

    return np.power(10.0, -reduction_sample_db / 20.0).astype(np.float32)


def _estimate_approx_true_peak_dbtp(audio: np.ndarray, sr: int, oversample: int = 2) -> float:
    audio = _ensure_2d_audio(audio)

    oversample = max(int(oversample), 1)
    if oversample == 1:
        peak = float(np.max(np.abs(audio))) if audio.size else 0.0
        return _lin_to_db(peak) if peak > 1e-12 else -120.0

    target_sr = int(sr * oversample)
    peak = 0.0

    for ch in range(audio.shape[1]):
        x = np.asarray(audio[:, ch], dtype=np.float32)
        if x.size == 0:
            continue
        y = librosa.resample(x, orig_sr=sr, target_sr=target_sr)
        ch_peak = float(np.max(np.abs(y))) if y.size else 0.0
        peak = max(peak, ch_peak)

    return _lin_to_db(peak) if peak > 1e-12 else -120.0


def _apply_output_trim_audio(audio: np.ndarray, gain_db: float) -> np.ndarray:
    audio = _ensure_2d_audio(audio)
    return (audio * _db_to_lin(gain_db)).astype(np.float32)


def _soft_ceiling_clip(audio: np.ndarray, ceiling_lin: float, knee_db: float = 0.90) -> np.ndarray:
    x = np.asarray(audio, dtype=np.float32)
    ceiling_lin = float(max(ceiling_lin, 1e-6))

    knee_db = float(max(knee_db, 0.20))
    knee_start = ceiling_lin / _db_to_lin(knee_db)
    knee_start = min(knee_start, ceiling_lin * 0.995)

    span = max(ceiling_lin - knee_start, 1e-6)

    ax = np.abs(x)
    sign = np.sign(x)

    out_abs = ax.copy()
    over = ax > knee_start

    if np.any(over):
        t = (ax[over] - knee_start) / span
        out_abs[over] = knee_start + span * np.tanh(t)

    out = sign * out_abs
    return np.clip(out, -ceiling_lin, ceiling_lin).astype(np.float32)


def _apply_true_peak_limiter_audio(
    audio: np.ndarray,
    sr: int,
    ceiling_db: float,
    threshold_db: float | None = None,
    attack_ms: float = 0.25,
    release_ms: float = 45.0,
    mix: float = 1.0,
    *,
    oversample: int = 2,
    safety_margin_db: float = 0.10,
    max_passes: int = 2,
) -> np.ndarray:
    dry = _ensure_2d_audio(audio)
    mix = float(np.clip(mix, 0.0, 1.0))

    ceiling_db = float(ceiling_db)
    ceiling_lin = _db_to_lin(ceiling_db)

    before_rms_db = _rms_dbfs(dry)

    target_sr = int(sr * max(int(oversample), 1))
    if target_sr != sr:
        os_channels = []
        for ch in range(dry.shape[1]):
            os_channels.append(
                librosa.resample(
                    dry[:, ch],
                    orig_sr=sr,
                    target_sr=target_sr,
                    res_type="soxr_hq",
                ).astype(np.float32)
            )
        min_len = min(len(x) for x in os_channels)
        wet_os = np.stack([x[:min_len] for x in os_channels], axis=1).astype(np.float32)
    else:
        wet_os = dry.copy()

    limited_os = _soft_ceiling_clip(wet_os, ceiling_lin, knee_db=0.90)

    if target_sr != sr:
        channels = []
        for ch in range(limited_os.shape[1]):
            channels.append(
                librosa.resample(
                    limited_os[:, ch],
                    orig_sr=target_sr,
                    target_sr=sr,
                    res_type="soxr_hq",
                ).astype(np.float32)
            )
        min_len = min(len(x) for x in channels)
        wet = np.stack([x[:min_len] for x in channels], axis=1).astype(np.float32)
        wet = _match_length(wet, len(dry))
    else:
        wet = limited_os

    wet = np.clip(wet, -ceiling_lin, ceiling_lin)

    after_rms_db = _rms_dbfs(wet)
    rms_loss_db = before_rms_db - after_rms_db

    if rms_loss_db > 0.35:
        makeup_db = _clamp(rms_loss_db - 0.20, 0.0, 0.90)

        driven_os = wet_os * _db_to_lin(makeup_db)
        limited_os = _soft_ceiling_clip(driven_os, ceiling_lin, knee_db=1.05)

        if target_sr != sr:
            channels = []
            for ch in range(limited_os.shape[1]):
                channels.append(
                    librosa.resample(
                        limited_os[:, ch],
                        orig_sr=target_sr,
                        target_sr=sr,
                        res_type="soxr_hq",
                    ).astype(np.float32)
                )
            min_len = min(len(x) for x in channels)
            wet = np.stack([x[:min_len] for x in channels], axis=1).astype(np.float32)
            wet = _match_length(wet, len(dry))
        else:
            wet = limited_os

        wet = np.clip(wet, -ceiling_lin, ceiling_lin)

    if mix < 0.999:
        wet = dry * (1.0 - mix) + wet * mix

    effective_ceiling_db = ceiling_db - abs(float(safety_margin_db))

    for _ in range(max_passes):
        current_tp_db = _estimate_approx_true_peak_dbtp(wet, sr, oversample=max(int(oversample), 1))
        needed_trim_db = effective_ceiling_db - current_tp_db
        if needed_trim_db >= -0.01:
            break
        wet *= _db_to_lin(needed_trim_db)

    return np.clip(wet, -1.0, 1.0).astype(np.float32)


def _execute_static_eq_file(input_path: str, output_path: str, op: Any) -> None:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    processed = _apply_fixed_bell_eq_audio(
        audio=audio,
        sr=sr,
        center_hz=float(params.get("freq_hz", 1000.0)),
        q=float(params.get("q", 1.0)),
        gain_db=float(params.get("gain_db", 0.0)),
    )
    _write_wav(output_path, processed, sr)


def _execute_high_shelf_file(input_path: str, output_path: str, op: Any) -> None:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    processed = _apply_fixed_high_shelf_audio(
        audio=audio,
        sr=sr,
        center_hz=float(params.get("freq_hz", 9000.0)),
        gain_db=float(params.get("gain_db", 0.0)),
    )
    _write_wav(output_path, processed, sr)


def _execute_dynamic_eq_file(input_path: str, output_path: str, op: Any) -> None:
    audio, sr = _load_audio(input_path)
    processed = _apply_dynamic_eq_audio(audio=audio, sr=sr, op=op)
    _write_wav(output_path, processed, sr)


def _execute_dynamic_tilt_file(input_path: str, output_path: str, op: Any) -> None:
    audio, sr = _load_audio(input_path)
    processed = _apply_dynamic_tilt_audio(audio=audio, sr=sr, op=op)
    _write_wav(output_path, processed, sr)


def _execute_parallel_eq_fill_file(input_path: str, output_path: str, op: Any) -> None:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    boosted = _apply_fixed_bell_eq_audio(
        audio=audio,
        sr=sr,
        center_hz=float(params.get("freq_hz", 170.0)),
        q=float(params.get("q", 0.9)),
        gain_db=float(params.get("gain_db", 0.0)),
    )

    mix = float(params.get("mix", 0.1))
    wet = (boosted - audio) * mix
    _write_wav(output_path, wet, sr)


def _execute_parallel_compressor_file(input_path: str, output_path: str, op: Any) -> None:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    threshold_db = float(params.get("threshold_db", -18.0))
    ratio = float(params.get("ratio", 1.5))
    attack_ms = float(params.get("attack_ms", 20.0))
    release_ms = float(params.get("release_ms", 120.0))
    mix = float(params.get("mix", 0.15))
    channel_mode = str(_read(op, "channel_mode", "stereo") or "stereo").lower()

    if audio.shape[1] == 1:
        detector = audio[:, 0]
    elif channel_mode == "mid":
        detector = 0.5 * (audio[:, 0] + audio[:, 1])
    else:
        detector = 0.5 * (audio[:, 0] + audio[:, 1])

    gain_lin = _build_parallel_compressor_gain(
        detector_signal=detector,
        sr=sr,
        threshold_db=threshold_db,
        ratio=ratio,
        attack_ms=attack_ms,
        release_ms=release_ms,
    )

    compressed = audio * gain_lin[:, None]
    wet = compressed * mix
    _write_wav(output_path, wet, sr)


def _execute_band_limited_saturation_file(input_path: str, output_path: str, op: Any) -> None:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    low_cut_hz = float(params.get("low_cut_hz", 1900.0))
    high_cut_hz = float(params.get("high_cut_hz", 5200.0))
    drive_db = float(params.get("drive_db", 1.0))
    mix = float(params.get("mix", 0.05))

    band = _apply_band_component_audio(
        audio=audio,
        sr=sr,
        low_hz=low_cut_hz,
        high_hz=high_cut_hz,
    )

    drive_lin = _db_to_lin(drive_db)
    saturated = np.tanh(band * drive_lin) / max(drive_lin, 1.0)

    wet = (saturated - band) * mix
    _write_wav(output_path, wet, sr)


def _execute_high_side_polish_file(input_path: str, output_path: str, op: Any) -> None:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    freq_hz = float(params.get("freq_hz", 9600.0) or 9600.0)
    mix = _clamp(float(params.get("mix", 0.06) or 0.06), 0.0, 0.22)
    side_gain_db = float(params.get("side_gain_db", 0.25) or 0.25)

    mid, side_ch = _to_mid_side(audio)
    high_side = _filter_band_mono(
        side_ch,
        sr,
        low_cut_hz=freq_hz,
        high_cut_hz=None,
    )

    side_gain = _db_to_lin(side_gain_db)
    effective = _clamp(
        (mix * 1.35) + (max(side_gain - 1.0, 0.0) * 0.85),
        0.0,
        0.18,
    )

    side_out = side_ch + high_side * effective
    processed = _from_mid_side(mid, side_out)

    _write_wav(output_path, np.clip(processed, -1.0, 1.0), sr)


def _execute_high_only_width_file(input_path: str, output_path: str, op: Any) -> None:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    low_cut_hz = float(params.get("low_cut_hz", 7000.0) or 7000.0)
    high_cut_hz = float(params.get("high_cut_hz", 16000.0) or 16000.0)
    width_amount = _clamp(float(params.get("width_amount", 0.06) or 0.06), 0.0, 0.20)
    mix = _clamp(float(params.get("mix", 0.05) or 0.05), 0.0, 0.22)

    mid, side_ch = _to_mid_side(audio)
    high_side_band = _filter_band_mono(
        side_ch,
        sr,
        low_cut_hz=low_cut_hz,
        high_cut_hz=high_cut_hz,
    )

    effective_width = _clamp(width_amount * (0.80 + mix * 4.0), 0.0, 0.24)
    side_out = side_ch + high_side_band * effective_width
    processed = _from_mid_side(mid, side_out)

    _write_wav(output_path, np.clip(processed, -1.0, 1.0), sr)


def _execute_output_trim_file(input_path: str, output_path: str, op: Any) -> None:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})
    gain_db = float(params.get("gain_db", 0.0))

    processed = _apply_output_trim_audio(audio=audio, gain_db=gain_db)
    _write_wav(output_path, processed, sr)


def _execute_true_peak_limiter_file(input_path: str, output_path: str, op: Any) -> None:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    processed = _apply_true_peak_limiter_audio(
        audio=audio,
        sr=sr,
        ceiling_db=float(params.get("gain_db", -1.0)),
        threshold_db=params.get("threshold_db"),
        attack_ms=float(params.get("attack_ms", 0.25)),
        release_ms=float(params.get("release_ms", 45.0)),
        mix=float(params.get("mix", 1.0)),
        oversample=2,
        safety_margin_db=0.10,
        max_passes=2,
    )
    _write_wav(output_path, processed, sr)


def _execute_op_file(
    input_path: str,
    td: str,
    stage_name: str,
    stack_name: str,
    op: Any,
    dry_run: bool = False,
) -> dict[str, Any]:
    backend_hint = str(_read(op, "backend_hint", "unknown") or "unknown").lower()
    op_kind = str(_read(op, "op_kind", "") or "").lower()
    instance_name = str(_read(op, "instance_name", "op") or "op")

    op_report = {
        "instance_name": _read(op, "instance_name"),
        "primitive_name": _read(op, "primitive_name"),
        "op_kind": _read(op, "op_kind"),
        "backend_hint": backend_hint,
        "executed": False,
        "pending_reason": None,
        "params": dict(_read(op, "params", {}) or {}),
        "output_path": input_path,
    }

    supported = {
        "static_eq",
        "broad_eq",
        "high_shelf",
        "dynamic_eq",
        "dynamic_tilt",
        "dynamic_eq_boost",
        "parallel_eq_fill",
        "parallel_eq_boost",
        "parallel_compressor",
        "band_limited_saturation",
        "band_limited_texture",
        "high_side_polish",
        "high_only_width",
        "output_trim",
        "ceiling_trim",
        "final_balance_guard",
        "true_peak_limiter",
    }

    if op_kind not in supported:
        op_report["pending_reason"] = "unsupported_op_kind"
        return op_report

    if dry_run:
        op_report["executed"] = True
        op_report["output_path"] = input_path
        return op_report

    output_path = _make_tmp_wav(td, stage_name, stack_name, instance_name)

    if op_kind in {"static_eq", "broad_eq"}:
        _execute_static_eq_file(input_path=input_path, output_path=output_path, op=op)
    elif op_kind == "high_shelf":
        _execute_high_shelf_file(input_path=input_path, output_path=output_path, op=op)
    elif op_kind in {"dynamic_eq", "dynamic_eq_boost"}:
        _execute_dynamic_eq_file(input_path=input_path, output_path=output_path, op=op)
    elif op_kind == "dynamic_tilt":
        _execute_dynamic_tilt_file(input_path=input_path, output_path=output_path, op=op)
    elif op_kind in {"parallel_eq_fill", "parallel_eq_boost"}:
        _execute_parallel_eq_fill_file(input_path=input_path, output_path=output_path, op=op)
    elif op_kind == "parallel_compressor":
        _execute_parallel_compressor_file(input_path=input_path, output_path=output_path, op=op)
    elif op_kind in {"band_limited_saturation", "band_limited_texture"}:
        _execute_band_limited_saturation_file(input_path=input_path, output_path=output_path, op=op)
    elif op_kind == "high_side_polish":
        _execute_high_side_polish_file(input_path=input_path, output_path=output_path, op=op)
    elif op_kind == "high_only_width":
        _execute_high_only_width_file(input_path=input_path, output_path=output_path, op=op)
    elif op_kind in {"output_trim", "ceiling_trim", "final_balance_guard"}:
        _execute_output_trim_file(input_path=input_path, output_path=output_path, op=op)
    elif op_kind == "true_peak_limiter":
        _execute_true_peak_limiter_file(input_path=input_path, output_path=output_path, op=op)
    else:
        op_report["pending_reason"] = "unsupported_op_kind"
        return op_report

    op_report["executed"] = True
    op_report["output_path"] = output_path
    return op_report


def _mix_audio_files(
    source_paths: list[str],
    output_path: str,
    gain_db: float = 0.0,
    weights: list[float] | None = None,
) -> str:
    if not source_paths:
        raise RuntimeError("No source paths for recombine.")

    first_audio, sr = _load_audio(source_paths[0])
    out = np.zeros_like(first_audio, dtype=np.float32)

    if weights is None:
        weights = [1.0] * len(source_paths)

    for src, w in zip(source_paths, weights):
        audio, src_sr = _load_audio(src)

        if src_sr != sr:
            raise RuntimeError("Sample rate mismatch inside recombine.")

        if audio.shape != out.shape:
            audio = _match_length(audio, len(out))
            if audio.shape[1] != out.shape[1]:
                raise RuntimeError("Channel mismatch inside recombine.")

        out += audio * float(w)

    out *= _db_to_lin(gain_db)
    _write_wav(output_path, out, sr)
    return output_path


def _make_delta_file(
    dry_path: str,
    processed_path: str,
    output_path: str,
) -> str:
    dry, sr = _load_audio(dry_path)
    processed, psr = _load_audio(processed_path)

    if psr != sr:
        raise RuntimeError("Sample rate mismatch inside delta.")

    processed = _match_length(processed, len(dry))

    if processed.shape[1] != dry.shape[1]:
        raise RuntimeError("Channel mismatch inside delta.")

    delta = processed - dry
    _write_wav(output_path, delta, sr)
    return output_path


def _op_outputs_wet_only(op_kind: str) -> bool:
    return str(op_kind).lower() in {
        "parallel_eq_fill",
        "parallel_eq_boost",
        "parallel_compressor",
        "band_limited_saturation",
        "band_limited_texture",
    }


def _op_is_finish_deess(op: Any) -> bool:
    primitive_name = str(_read(op, "primitive_name", "") or "").lower()
    op_kind = str(_read(op, "op_kind", "") or "").lower()
    return primitive_name == "local_desibilance_control" and op_kind == "dynamic_eq"


def _record_op_result(
    report: dict[str, Any],
    stack_report: dict[str, Any],
    stage_name: str,
    stack_name: str,
    op_result: dict[str, Any],
) -> bool:
    stack_report["ops"].append(
        {
            "instance_name": op_result["instance_name"],
            "primitive_name": op_result["primitive_name"],
            "op_kind": op_result["op_kind"],
            "backend_hint": op_result["backend_hint"],
            "executed": op_result["executed"],
            "pending_reason": op_result["pending_reason"],
            "params": op_result["params"],
        }
    )

    if op_result["executed"]:
        report["executed_op_count"] += 1
        return True

    report["pending_custom_op_count"] += 1
    report["unsupported_ops"].append(
        {
            "stage_name": stage_name,
            "stack_name": stack_name,
            "instance_name": op_result["instance_name"],
            "primitive_name": op_result["primitive_name"],
            "op_kind": op_result["op_kind"],
            "backend_hint": op_result["backend_hint"],
        }
    )
    return False


def _execute_serial_stack(
    *,
    ops: list[Any],
    input_path: str,
    td: str,
    stage_name: str,
    stack_name: str,
    report: dict[str, Any],
    stack_report: dict[str, Any],
    dry_run: bool,
) -> str:
    current_path = input_path

    for op in ops:
        op_result = _execute_op_file(
            input_path=current_path,
            td=td,
            stage_name=stage_name,
            stack_name=stack_name,
            op=op,
            dry_run=dry_run,
        )

        executed = _record_op_result(
            report=report,
            stack_report=stack_report,
            stage_name=stage_name,
            stack_name=stack_name,
            op_result=op_result,
        )

        if executed:
            current_path = op_result["output_path"]

    return current_path


def _execute_parallel_wet_stack(
    *,
    ops: list[Any],
    stack_input_path: str,
    td: str,
    stage_name: str,
    stack_name: str,
    report: dict[str, Any],
    stack_report: dict[str, Any],
    dry_run: bool,
    output_suffix: str,
) -> str | None:
    wet_paths: list[str] = []

    for op in ops:
        op_kind = str(_read(op, "op_kind", "") or "").lower()

        op_result = _execute_op_file(
            input_path=stack_input_path,
            td=td,
            stage_name=stage_name,
            stack_name=stack_name,
            op=op,
            dry_run=dry_run,
        )

        executed = _record_op_result(
            report=report,
            stack_report=stack_report,
            stage_name=stage_name,
            stack_name=stack_name,
            op_result=op_result,
        )

        if not executed:
            continue

        if dry_run:
            wet_paths.append(stack_input_path)
            continue

        if _op_outputs_wet_only(op_kind):
            wet_paths.append(op_result["output_path"])
        else:
            delta_path = _make_tmp_wav(
                td,
                stage_name,
                stack_name,
                f"{op_result['instance_name']}__delta",
            )
            wet_paths.append(
                _make_delta_file(
                    dry_path=stack_input_path,
                    processed_path=op_result["output_path"],
                    output_path=delta_path,
                )
            )

    if not wet_paths:
        return None

    if dry_run:
        return wet_paths[0]

    out_path = _make_tmp_wav(
        td,
        stage_name,
        stack_name,
        f"{stack_name}__{output_suffix}",
    )

    _mix_audio_files(
        source_paths=wet_paths,
        output_path=out_path,
        gain_db=0.0,
        weights=[1.0] * len(wet_paths),
    )

    return out_path


def _execute_finish_micro_stack(
    *,
    ops: list[Any],
    stack_input_path: str,
    td: str,
    stage_name: str,
    stack_name: str,
    report: dict[str, Any],
    stack_report: dict[str, Any],
    dry_run: bool,
) -> str | None:
    wet_ops: list[Any] = []
    deess_ops: list[Any] = []

    for op in ops:
        if _op_is_finish_deess(op):
            deess_ops.append(op)
        else:
            wet_ops.append(op)

    spark_bus_path = _execute_parallel_wet_stack(
        ops=wet_ops,
        stack_input_path=stack_input_path,
        td=td,
        stage_name=stage_name,
        stack_name=stack_name,
        report=report,
        stack_report=stack_report,
        dry_run=dry_run,
        output_suffix="spark_bus",
    )

    if spark_bus_path is None:
        return None

    current_path = spark_bus_path

    for op in deess_ops:
        op_result = _execute_op_file(
            input_path=current_path,
            td=td,
            stage_name=stage_name,
            stack_name=stack_name,
            op=op,
            dry_run=dry_run,
        )

        executed = _record_op_result(
            report=report,
            stack_report=stack_report,
            stage_name=stage_name,
            stack_name=stack_name,
            op_result=op_result,
        )

        if executed:
            current_path = op_result["output_path"]

    return current_path


def _execute_recombine(
    rec: Any,
    node_paths: dict[str, str | None],
    td: str,
    stage_name: str,
    dry_run: bool = False,
) -> tuple[dict[str, Any], str | None]:
    rec_name = str(_read(rec, "recombine_name", "recombine") or "recombine")
    kind = str(_read(rec, "render_recombine_kind", "passthrough_or_sum") or "passthrough_or_sum")
    source_nodes = list(_read(rec, "source_nodes", []) or [])
    target_node = _read(rec, "target_node")
    blend = float(_read(rec, "blend", 1.0) or 1.0)
    gain_db = float(_read(rec, "gain_db", 0.0) or 0.0)

    rec_report = {
        "recombine_name": _read(rec, "recombine_name"),
        "render_recombine_kind": kind,
        "source_nodes": source_nodes,
        "target_node": target_node,
        "blend": blend,
        "gain_db": gain_db,
    }

    source_paths = [node_paths.get(node) for node in source_nodes]
    if not source_paths or any(p is None for p in source_paths):
        return rec_report, None

    if dry_run:
        return rec_report, source_paths[0]

    out_path = os.path.join(td, f"{_safe_name(stage_name)}__{_safe_name(rec_name)}.wav")

    if kind == "assist_blend_sum" and len(source_paths) == 2:
        target_path = _mix_audio_files(
            source_paths=source_paths,
            output_path=out_path,
            gain_db=gain_db,
            weights=[1.0, blend],
        )
        return rec_report, target_path

    if kind == "finish_blend_sum" and len(source_paths) == 2:
        target_path = _mix_audio_files(
            source_paths=source_paths,
            output_path=out_path,
            gain_db=gain_db,
            weights=[1.0, blend],
        )
        return rec_report, target_path

    if kind == "guarded_parallel_sum":
        if len(source_paths) == 2 and len(source_nodes) == 2 and source_nodes[1].endswith("_bus"):
            target_path = _mix_audio_files(
                source_paths=source_paths,
                output_path=out_path,
                gain_db=0.0,
                weights=[1.0, _db_to_lin(gain_db) * blend],
            )
            return rec_report, target_path

        target_path = _mix_audio_files(
            source_paths=source_paths,
            output_path=out_path,
            gain_db=gain_db,
            weights=[1.0] * len(source_paths),
        )
        return rec_report, target_path

    if kind == "passthrough_or_sum":
        target_path = _mix_audio_files(
            source_paths=source_paths,
            output_path=out_path,
            gain_db=gain_db,
            weights=[1.0] * len(source_paths),
        )
        return rec_report, target_path

    target_path = _mix_audio_files(
        source_paths=source_paths,
        output_path=out_path,
        gain_db=gain_db,
        weights=[1.0] * len(source_paths),
    )
    return rec_report, target_path


def _execute_plan(render_plan: Any, input_path: str | None = None, dry_run: bool = False) -> dict:
    node_order = list(_read(render_plan, "node_order", []) or [])
    stages = list(_read(render_plan, "stages", []) or [])
    prepared_input_node = _read(render_plan, "prepared_input_node", "prepared_input")
    final_output_node = _read(render_plan, "final_output_node", "final_output")

    td = os.path.dirname(input_path) if input_path else tempfile.mkdtemp(prefix="sm_executor_")
    td = td or tempfile.mkdtemp(prefix="sm_executor_")

    node_paths: dict[str, str | None] = {name: None for name in node_order}
    if input_path:
        node_paths[prepared_input_node] = input_path

    report: dict[str, Any] = {
        "status": "ok",
        "plan_name": _read(render_plan, "plan_name", "sm_render_plan_v1"),
        "prepared_input_node": prepared_input_node,
        "final_output_node": final_output_node,
        "node_order": node_order,
        "node_paths": node_paths,
        "stage_reports": [],
        "unsupported_ops": [],
        "executed_op_count": 0,
        "pending_custom_op_count": 0,
        "safety_notes": list(_read(render_plan, "safety_notes", []) or []),
        "notes": list(_read(render_plan, "notes", []) or [])
        + [
            "executor_active_v3",
            "parallel_stack_semantics_fixed",
            "parallel_ops_run_from_same_tap_point",
            "parallel_outputs_are_wet_layers",
            "finish_spark_is_wet_micro_bus",
            "finish_deess_runs_on_spark_bus",
            "native_finish_width_ops_supported",
            "delivery_loudness_preserve_peak_safety_enabled",
            "recombine_blend_weights_fixed",
        ],
    }

    current_path = input_path

    for stage in stages:
        stage_name = _read(stage, "stage_name", "unknown_stage")
        input_node = _read(stage, "input_node")
        output_node = _read(stage, "output_node")
        stacks = list(_read(stage, "stacks", []) or [])
        recombine = list(_read(stage, "recombine", []) or [])

        stage_input_path = node_paths.get(input_node) or current_path
        stage_current_path = stage_input_path

        stage_report: dict[str, Any] = {
            "stage_name": stage_name,
            "stage_kind": _read(stage, "stage_kind"),
            "input_node": input_node,
            "output_node": output_node,
            "requires_custom_dsp": bool(_read(stage, "requires_custom_dsp", False)),
            "active_clamps": list(_read(stage, "active_clamps", []) or []),
            "safety_tags": list(_read(stage, "safety_tags", []) or []),
            "stack_reports": [],
            "recombine_reports": [],
            "resolved_input_path": stage_input_path,
            "resolved_output_path": None,
        }

        for stack in stacks:
            stack_name = _read(stack, "stack_name", "unknown_stack")
            render_mode = str(_read(stack, "render_mode", "serial_inplace") or "serial_inplace")
            tap_point = _read(stack, "tap_point")
            output_node_for_stack = _read(stack, "output_node")
            ops = list(_read(stack, "ops", []) or [])

            stack_input_path = node_paths.get(tap_point) or stage_current_path or stage_input_path

            stack_report: dict[str, Any] = {
                "stack_name": stack_name,
                "role": _read(stack, "role"),
                "render_mode": render_mode,
                "requires_custom_dsp": bool(_read(stack, "requires_custom_dsp", False)),
                "ops": [],
            }

            if not stack_input_path:
                raise RuntimeError(
                    f"Missing input path for stack {stack_name} in stage {stage_name}"
                )

            if render_mode in {"serial_inplace", "delivery_serial"}:
                stack_output_path = _execute_serial_stack(
                    ops=ops,
                    input_path=stack_input_path,
                    td=td,
                    stage_name=stage_name,
                    stack_name=stack_name,
                    report=report,
                    stack_report=stack_report,
                    dry_run=dry_run,
                )

                if output_node_for_stack:
                    node_paths[output_node_for_stack] = stack_output_path

                stage_current_path = stack_output_path

            elif render_mode in {"parallel_return", "parallel_assist_return"}:
                stack_output_path = _execute_parallel_wet_stack(
                    ops=ops,
                    stack_input_path=stack_input_path,
                    td=td,
                    stage_name=stage_name,
                    stack_name=stack_name,
                    report=report,
                    stack_report=stack_report,
                    dry_run=dry_run,
                    output_suffix="wet_sum",
                )

                if output_node_for_stack and stack_output_path:
                    node_paths[output_node_for_stack] = stack_output_path

            elif render_mode == "finish_micro_return":
                stack_output_path = _execute_finish_micro_stack(
                    ops=ops,
                    stack_input_path=stack_input_path,
                    td=td,
                    stage_name=stage_name,
                    stack_name=stack_name,
                    report=report,
                    stack_report=stack_report,
                    dry_run=dry_run,
                )

                if output_node_for_stack and stack_output_path:
                    node_paths[output_node_for_stack] = stack_output_path

            else:
                stack_output_path = _execute_serial_stack(
                    ops=ops,
                    input_path=stack_input_path,
                    td=td,
                    stage_name=stage_name,
                    stack_name=stack_name,
                    report=report,
                    stack_report=stack_report,
                    dry_run=dry_run,
                )

                if output_node_for_stack:
                    node_paths[output_node_for_stack] = stack_output_path

                stage_current_path = stack_output_path

            stage_report["stack_reports"].append(stack_report)

        if recombine:
            last_target_path = None

            for rec in recombine:
                rec_report, target_path = _execute_recombine(
                    rec=rec,
                    node_paths=node_paths,
                    td=td,
                    stage_name=stage_name,
                    dry_run=dry_run,
                )
                stage_report["recombine_reports"].append(rec_report)

                target_node = rec_report["target_node"]
                if target_node and target_path:
                    node_paths[target_node] = target_path
                    last_target_path = target_path

            if output_node and node_paths.get(output_node) is None:
                node_paths[output_node] = last_target_path or stage_current_path

        else:
            if output_node and node_paths.get(output_node) is None:
                node_paths[output_node] = stage_current_path

        stage_report["resolved_output_path"] = node_paths.get(output_node)
        report["stage_reports"].append(stage_report)
        current_path = node_paths.get(output_node) or current_path

    if report["pending_custom_op_count"] > 0:
        report["status"] = "partial_custom_dsp_pending"

    report["final_output_path"] = node_paths.get(final_output_node)
    return report


def build_render_execution_report(render_plan: Any, input_path: str | None = None) -> dict:
    return _execute_plan(
        render_plan=render_plan,
        input_path=input_path,
        dry_run=True,
    )


def execute_dsp_render_plan(render_plan: Any, input_path: str | None = None) -> dict:
    return _execute_plan(
        render_plan=render_plan,
        input_path=input_path,
        dry_run=False,
    )

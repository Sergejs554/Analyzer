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
from scipy.ndimage import maximum_filter1d


# ============================================================
# SM EXECUTOR CONTRACT
# ============================================================
# Executor не принимает музыкальные решения.
# Он только физически исполняет render_plan.
#
# Музыкальное мышление живет выше:
# analysis -> selector -> router -> clamps -> role_specs -> primitive_instances -> graph.
#
# Исполнительные законы:
# 1. Internal pipeline = float WAV.
# 2. No hidden normalization.
# 3. Parallel stacks return wet/delta only.
# 4. Bridge/support compression is band-limited, not fullband duplicate.
# 5. Delivery is terminal only.
# 6. Delivery does NOT buy loudness.
# 7. Positive delivery gain is blocked.
# 8. Limiter catches only real oversampled peak excess above target ceiling.
# 9. Limiter does NOT smooth/pre-catch material below target ceiling, except local future lookahead for real overs.
# 10. No RMS makeup inside limiter.
# 11. No global final safety trim as a normal tool.
# 12. If delivery goes over budget, executor reports upstream retry need.
# ============================================================

INTERNAL_WAV_SUBTYPE = "FLOAT"

NO_STAGE_NORMALIZATION = True
PARALLEL_STACKS_ARE_WET_ONLY = True

DELIVERY_IS_TERMINAL_ONLY = True
DELIVERY_BLOCKS_POSITIVE_GAIN = True
DELIVERY_LIMITER_TOUCHES_ONLY_REAL_OVERS = True
DELIVERY_NO_GLOBAL_SAFETY_TRIM = True

LIMITER_OVERSAMPLE = 4
LIMITER_NO_PRE_CEILING_SMOOTHING = True
LIMITER_NO_RMS_MAKEUP = True
LIMITER_NO_CREATIVE_COMPRESSION = True


# ============================================================
# BASIC HELPERS
# ============================================================

def _read(obj: Any, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on", "allow", "allowed"}


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
    return _lin_to_db(rms)


def _sample_peak_dbfs(audio: np.ndarray) -> float:
    audio = np.asarray(audio, dtype=np.float32)
    if audio.size == 0:
        return -120.0
    peak = float(np.max(np.abs(audio)))
    return _lin_to_db(peak) if peak > 1e-12 else -120.0


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
    os.makedirs(os.path.dirname(path), exist_ok=True)

    sf.write(
        path,
        np.asarray(audio, dtype=np.float32),
        sr,
        format="WAV",
        subtype=INTERNAL_WAV_SUBTYPE,
    )


def _load_audio(path: str) -> tuple[np.ndarray, int]:
    audio, sr = sf.read(path, always_2d=True)
    return _ensure_2d_audio(audio), int(sr)


def _audio_file_stats(path: str | None) -> dict[str, Any]:
    if not path or not os.path.isfile(path):
        return {
            "status": "missing",
            "sample_peak_dbfs": None,
            "rms_dbfs": None,
            "duration_sec": None,
            "channels": None,
            "sample_rate_hz": None,
        }

    try:
        audio, sr = _load_audio(path)
        return {
            "status": "ok",
            "sample_peak_dbfs": round(_sample_peak_dbfs(audio), 4),
            "rms_dbfs": round(_rms_dbfs(audio), 4),
            "duration_sec": round(len(audio) / float(sr), 4) if sr else 0.0,
            "channels": int(audio.shape[1]) if audio.ndim == 2 else 1,
            "sample_rate_hz": int(sr),
        }
    except Exception as exc:
        return {
            "status": "error",
            "error": str(exc)[:500],
            "sample_peak_dbfs": None,
            "rms_dbfs": None,
            "duration_sec": None,
            "channels": None,
            "sample_rate_hz": None,
        }


def _stats_delta(pre: dict[str, Any], post: dict[str, Any]) -> dict[str, Any]:
    try:
        pre_peak = pre.get("sample_peak_dbfs")
        post_peak = post.get("sample_peak_dbfs")
        pre_rms = pre.get("rms_dbfs")
        post_rms = post.get("rms_dbfs")

        return {
            "delta_peak_db": round(float(post_peak) - float(pre_peak), 4)
            if pre_peak is not None and post_peak is not None
            else None,
            "delta_rms_db": round(float(post_rms) - float(pre_rms), 4)
            if pre_rms is not None and post_rms is not None
            else None,
        }
    except Exception:
        return {
            "delta_peak_db": None,
            "delta_rms_db": None,
        }


def _make_tmp_wav(td: str, stage_name: str, stack_name: str, instance_name: str) -> str:
    fname = f"{_safe_name(stage_name)}__{_safe_name(stack_name)}__{_safe_name(instance_name)}.wav"
    return os.path.join(td, fname)


def _is_delivery_context(stage_name: str | None, op: Any) -> bool:
    stage_safe = _safe_name(stage_name)
    primitive_name = str(_read(op, "primitive_name", "") or "").lower()
    op_kind = str(_read(op, "op_kind", "") or "").lower()

    return (
        stage_safe.startswith("delivery")
        or primitive_name in {"output_gain_trim", "true_peak_limiter"}
        or op_kind in {"true_peak_limiter"}
    )


# ============================================================
# SPECTRAL HELPERS
# ============================================================

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


def _smooth_limiter_gain_samples(
    target_gain: np.ndarray,
    attack_ms: float,
    release_ms: float,
    sr: int,
) -> np.ndarray:
    target_gain = np.asarray(target_gain, dtype=np.float32)

    if target_gain.size == 0:
        return target_gain

    attack_ms = max(float(attack_ms), 0.05)
    release_ms = max(float(release_ms), attack_ms)

    attack_coeff = math.exp(-1.0 / (sr * attack_ms * 0.001))
    release_coeff = math.exp(-1.0 / (sr * release_ms * 0.001))

    out = np.zeros_like(target_gain, dtype=np.float32)
    prev = float(target_gain[0])

    for i, g in enumerate(target_gain):
        g = float(g)

        coeff = attack_coeff if g < prev else release_coeff
        prev = coeff * prev + (1.0 - coeff) * g
        out[i] = prev

    return np.clip(out, 0.0, 1.0).astype(np.float32)


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

    det_weights = _log_gaussian_band(
        freqs,
        center_hz=center_hz,
        q=max(q * 0.9, 0.35),
    )

    band_power = (np.abs(Z) ** 2 * det_weights[:, None]).sum(axis=0) / (
        det_weights.sum() + 1e-12
    )

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


# ============================================================
# AUDIO OPS
# ============================================================

def _apply_fixed_bell_eq_audio(
    audio: np.ndarray,
    sr: int,
    center_hz: float,
    q: float,
    gain_db: float,
) -> np.ndarray:
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


def _apply_fixed_high_shelf_audio(
    audio: np.ndarray,
    sr: int,
    center_hz: float,
    gain_db: float,
) -> np.ndarray:
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

        gain_frames_db = (activity_frames[:frame_count] * gain_db).astype(np.float32)

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

        return y[:, None].astype(np.float32)

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

        gain_frames_db = (activity_frames[:frame_count] * gain_db).astype(np.float32)

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

    gain_frames_db = (activity_frames[:frame_count] * gain_db).astype(np.float32)

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

        return y[:, None].astype(np.float32)

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


def _apply_band_component_audio(
    audio: np.ndarray,
    sr: int,
    low_hz: float,
    high_hz: float,
) -> np.ndarray:
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


# ============================================================
# COMPRESSOR / TRUE PEAK HELPERS
# ============================================================

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


def _estimate_approx_true_peak_dbtp(audio: np.ndarray, sr: int, oversample: int = LIMITER_OVERSAMPLE) -> float:
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

        y = librosa.resample(
            x,
            orig_sr=sr,
            target_sr=target_sr,
            res_type="soxr_hq",
        )

        ch_peak = float(np.max(np.abs(y))) if y.size else 0.0
        peak = max(peak, ch_peak)

    return _lin_to_db(peak) if peak > 1e-12 else -120.0


def _apply_output_trim_audio(audio: np.ndarray, gain_db: float) -> np.ndarray:
    audio = _ensure_2d_audio(audio)
    return (audio * _db_to_lin(gain_db)).astype(np.float32)


def _oversample_audio(audio: np.ndarray, sr: int, oversample: int) -> tuple[np.ndarray, int]:
    audio = _ensure_2d_audio(audio)

    oversample = max(int(oversample), 1)

    if oversample == 1:
        return audio.copy(), int(sr)

    target_sr = int(sr * oversample)

    channels = []

    for ch in range(audio.shape[1]):
        channels.append(
            librosa.resample(
                audio[:, ch],
                orig_sr=sr,
                target_sr=target_sr,
                res_type="soxr_hq",
            ).astype(np.float32)
        )

    min_len = min(len(x) for x in channels)
    out = np.stack([x[:min_len] for x in channels], axis=1).astype(np.float32)

    return out, target_sr


def _downsample_audio(
    audio_os: np.ndarray,
    target_sr: int,
    original_sr: int,
    original_len: int,
) -> np.ndarray:
    audio_os = _ensure_2d_audio(audio_os)

    if target_sr == original_sr:
        return _match_length(audio_os, original_len).astype(np.float32)

    channels = []

    for ch in range(audio_os.shape[1]):
        channels.append(
            librosa.resample(
                audio_os[:, ch],
                orig_sr=target_sr,
                target_sr=original_sr,
                res_type="soxr_hq",
            ).astype(np.float32)
        )

    min_len = min(len(x) for x in channels)
    out = np.stack([x[:min_len] for x in channels], axis=1).astype(np.float32)

    return _match_length(out, original_len).astype(np.float32)


def _future_peak_detector(peak: np.ndarray, lookahead_samples: int) -> np.ndarray:
    peak = np.asarray(peak, dtype=np.float32)
    lookahead_samples = max(int(lookahead_samples), 1)

    if peak.size == 0 or lookahead_samples <= 1:
        return peak.copy()

    origin = -(lookahead_samples // 2)

    return maximum_filter1d(
        peak,
        size=lookahead_samples,
        mode="constant",
        cval=0.0,
        origin=origin,
    ).astype(np.float32)


def _build_transparent_limiter_gain(
    audio_os: np.ndarray,
    sr_os: int,
    target_ceiling_db: float,
    threshold_db: float,
    attack_ms: float,
    release_ms: float,
    lookahead_ms: float = 1.25,
) -> tuple[np.ndarray, dict[str, Any]]:
    audio_os = _ensure_2d_audio(audio_os)

    target_ceiling_db = float(target_ceiling_db)
    threshold_db = float(threshold_db)

    target_ceiling_lin = _db_to_lin(target_ceiling_db)

    peak = np.max(np.abs(audio_os), axis=1).astype(np.float32)

    lookahead_samples = max(1, int(sr_os * lookahead_ms * 0.001))
    peak_detector = _future_peak_detector(peak, lookahead_samples=lookahead_samples)

    target_gain = np.ones_like(peak_detector, dtype=np.float32)

    over_zone = peak_detector > target_ceiling_lin

    if np.any(over_zone):
        hard_gain = target_ceiling_lin / np.maximum(peak_detector[over_zone], 1e-12)
        target_gain[over_zone] = np.minimum(target_gain[over_zone], hard_gain)

    target_gain = np.clip(target_gain, 0.0, 1.0).astype(np.float32)

    gain_env = _smooth_limiter_gain_samples(
        target_gain,
        attack_ms=attack_ms,
        release_ms=release_ms,
        sr=sr_os,
    )

    min_gain = float(np.min(gain_env)) if gain_env.size else 1.0
    max_reduction_db = -_lin_to_db(min_gain) if min_gain < 1.0 else 0.0
    active_ratio = float(np.mean(gain_env < 0.999)) if gain_env.size else 0.0
    real_over_ratio = float(np.mean(over_zone)) if peak_detector.size else 0.0

    debug = {
        "target_ceiling_db": round(target_ceiling_db, 4),
        "threshold_db": round(threshold_db, 4),
        "threshold_is_metadata_only": True,
        "lookahead_ms": round(float(lookahead_ms), 4),
        "lookahead_samples": int(lookahead_samples),
        "max_gain_reduction_db": round(max_reduction_db, 4),
        "active_gain_reduction_ratio": round(active_ratio, 6),
        "real_over_ceiling_ratio": round(real_over_ratio, 6),
        "peak_detector_dbfs": round(
            _lin_to_db(float(np.max(peak_detector))) if peak_detector.size else -120.0,
            4,
        ),
        "touches_only_real_overs_above_target_ceiling": True,
        "pre_ceiling_smoothing": False,
        "future_peak_detector": True,
    }

    return gain_env.astype(np.float32), debug


def _evaluate_delivery_damage_budget(debug: dict[str, Any]) -> dict[str, Any]:
    max_gr = abs(float(debug.get("max_gain_reduction_db", 0.0) or 0.0))
    active_ratio = abs(float(debug.get("active_gain_reduction_ratio", 0.0) or 0.0))
    rms_delta = float(debug.get("rms_delta_db", 0.0) or 0.0)
    rms_loss_abs = abs(min(rms_delta, 0.0))

    after_tp = debug.get("after_true_peak_dbtp")
    target_ceiling = debug.get("target_ceiling_db")

    after_tp_f = float(after_tp) if after_tp is not None else None
    target_f = float(target_ceiling) if target_ceiling is not None else None

    warnings: list[str] = []
    failures: list[str] = []

    if max_gr > 1.20:
        failures.append("delivery_max_gain_reduction_over_budget")
    elif max_gr > 0.60:
        warnings.append("delivery_max_gain_reduction_warning")

    if active_ratio > 0.020:
        failures.append("delivery_active_ratio_over_budget")
    elif active_ratio > 0.007:
        warnings.append("delivery_active_ratio_warning")

    if rms_loss_abs > 0.20:
        failures.append("delivery_rms_loss_over_budget")
    elif rms_loss_abs > 0.08:
        warnings.append("delivery_rms_loss_warning")

    if after_tp_f is not None and target_f is not None:
        if after_tp_f > target_f + 0.06:
            failures.append("delivery_residual_true_peak_above_target_after_local_catch")
        elif after_tp_f > target_f + 0.02:
            warnings.append("delivery_residual_true_peak_close_to_target")

    delivery_over_budget = bool(failures)

    retry_hints: list[str] = []

    if delivery_over_budget:
        retry_hints.extend(
            [
                "do_not_make_limiter_stronger",
                "do_not_add_delivery_makeup",
                "reduce_upstream_peak_pressure",
                "consider_lower_projection_assist_blend",
                "consider_lower_spark_blend",
                "consider_lower_support_recombine_gain",
                "consider_peak_aware_rebuild_before_delivery",
                "rerender_musical_graph_before_delivery",
            ]
        )

    return {
        "delivery_damage_budget": {
            "max_gain_reduction_db": round(max_gr, 4),
            "active_gain_reduction_ratio": round(active_ratio, 6),
            "rms_loss_abs_db": round(rms_loss_abs, 4),
            "warnings": warnings,
            "failures": failures,
            "delivery_over_budget": delivery_over_budget,
            "should_retry_upstream": delivery_over_budget,
            "retry_hints": retry_hints,
        },
        "delivery_over_budget": delivery_over_budget,
        "should_retry_upstream": delivery_over_budget,
        "delivery_budget_warnings": warnings,
        "delivery_budget_failures": failures,
        "delivery_retry_hints": retry_hints,
    }


def _apply_one_true_peak_catch_pass(
    audio: np.ndarray,
    sr: int,
    target_ceiling_db: float,
    threshold_db: float,
    attack_ms: float,
    release_ms: float,
    lookahead_ms: float,
    oversample: int,
) -> tuple[np.ndarray, dict[str, Any]]:
    dry = _ensure_2d_audio(audio)

    audio_os, sr_os = _oversample_audio(
        dry,
        sr,
        oversample=max(int(oversample), 1),
    )

    gain_env, limiter_debug = _build_transparent_limiter_gain(
        audio_os=audio_os,
        sr_os=sr_os,
        target_ceiling_db=target_ceiling_db,
        threshold_db=threshold_db,
        attack_ms=attack_ms,
        release_ms=release_ms,
        lookahead_ms=lookahead_ms,
    )

    limited_os = audio_os * gain_env[:, None]

    wet = _downsample_audio(
        audio_os=limited_os,
        target_sr=sr_os,
        original_sr=sr,
        original_len=len(dry),
    )

    return np.nan_to_num(wet, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32), limiter_debug


def _apply_true_peak_limiter_audio(
    audio: np.ndarray,
    sr: int,
    ceiling_db: float,
    threshold_db: float | None = None,
    attack_ms: float = 0.20,
    release_ms: float = 48.0,
    mix: float = 1.0,
    *,
    oversample: int = LIMITER_OVERSAMPLE,
    safety_margin_db: float = 0.10,
    max_passes: int = 4,
) -> tuple[np.ndarray, dict[str, Any]]:
    dry = _ensure_2d_audio(audio)

    requested_mix = float(np.clip(mix, 0.0, 1.0))
    applied_mix = 1.0

    ceiling_db = float(ceiling_db)
    target_ceiling_db = ceiling_db - abs(float(safety_margin_db))

    if threshold_db is None:
        threshold_db = ceiling_db - 0.45

    threshold_db = float(threshold_db)

    before_rms_db = _rms_dbfs(dry)
    before_peak_db = _sample_peak_dbfs(dry)
    before_tp_db = _estimate_approx_true_peak_dbtp(
        dry,
        sr,
        oversample=max(int(oversample), 1),
    )

    current = dry.copy()
    pass_reports: list[dict[str, Any]] = []

    for pass_index in range(max(1, int(max_passes))):
        pass_before_tp = _estimate_approx_true_peak_dbtp(
            current,
            sr,
            oversample=max(int(oversample), 1),
        )

        if pass_before_tp <= target_ceiling_db + 0.005:
            break

        processed, pass_debug = _apply_one_true_peak_catch_pass(
            audio=current,
            sr=sr,
            target_ceiling_db=target_ceiling_db,
            threshold_db=threshold_db,
            attack_ms=attack_ms,
            release_ms=release_ms,
            lookahead_ms=1.25,
            oversample=max(int(oversample), 1),
        )

        pass_after_tp = _estimate_approx_true_peak_dbtp(
            processed,
            sr,
            oversample=max(int(oversample), 1),
        )

        pass_debug.update(
            {
                "pass_index": int(pass_index),
                "pass_before_true_peak_dbtp": round(pass_before_tp, 4),
                "pass_after_true_peak_dbtp": round(pass_after_tp, 4),
                "pass_true_peak_delta_db": round(pass_after_tp - pass_before_tp, 4),
            }
        )

        pass_reports.append(pass_debug)
        current = processed

        if pass_after_tp <= target_ceiling_db + 0.005:
            break

        if abs(pass_after_tp - pass_before_tp) < 0.003:
            break

    wet = current

    if requested_mix < 0.999:
        # Terminal protection cannot be safely parallel-mixed.
        # We force 100% wet and report it.
        applied_mix = 1.0

    after_rms_db = _rms_dbfs(wet)
    after_peak_db = _sample_peak_dbfs(wet)
    after_tp_db = _estimate_approx_true_peak_dbtp(
        wet,
        sr,
        oversample=max(int(oversample), 1),
    )

    max_gr = 0.0
    active_ratio = 0.0
    real_over_ratio = 0.0

    for p in pass_reports:
        max_gr = max(max_gr, abs(float(p.get("max_gain_reduction_db", 0.0) or 0.0)))
        active_ratio = max(active_ratio, abs(float(p.get("active_gain_reduction_ratio", 0.0) or 0.0)))
        real_over_ratio = max(real_over_ratio, abs(float(p.get("real_over_ceiling_ratio", 0.0) or 0.0)))

    debug = {
        "limiter_mode": "transparent_true_peak_catch_only",
        "no_rms_makeup": True,
        "no_creative_compression": True,
        "no_pre_ceiling_smoothing": True,
        "no_global_final_safety_trim": True,
        "touches_only_real_overs_above_target_ceiling": True,
        "future_peak_detector": True,
        "ceiling_db": round(ceiling_db, 4),
        "target_ceiling_db": round(target_ceiling_db, 4),
        "safety_margin_db": round(abs(float(safety_margin_db)), 4),
        "threshold_db": round(threshold_db, 4),
        "threshold_is_metadata_only": True,
        "oversample": int(oversample),
        "requested_mix": round(requested_mix, 4),
        "applied_mix": round(applied_mix, 4),
        "mix_forced_to_full_protection": requested_mix < 0.999,
        "before_sample_peak_dbfs": round(before_peak_db, 4),
        "after_sample_peak_dbfs": round(after_peak_db, 4),
        "before_true_peak_dbtp": round(before_tp_db, 4),
        "after_true_peak_dbtp": round(after_tp_db, 4),
        "before_rms_dbfs": round(before_rms_db, 4),
        "after_rms_dbfs": round(after_rms_db, 4),
        "rms_delta_db": round(after_rms_db - before_rms_db, 4),
        "max_gain_reduction_db": round(max_gr, 4),
        "active_gain_reduction_ratio": round(active_ratio, 6),
        "real_over_ceiling_ratio": round(real_over_ratio, 6),
        "passes_executed": len(pass_reports),
        "pass_reports": pass_reports,
    }

    debug.update(_evaluate_delivery_damage_budget(debug))

    return np.nan_to_num(wet, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32), debug


# ============================================================
# FILE EXECUTORS
# ============================================================

def _execute_static_eq_file(input_path: str, output_path: str, op: Any) -> dict[str, Any]:
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
    return {}


def _execute_high_shelf_file(input_path: str, output_path: str, op: Any) -> dict[str, Any]:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    processed = _apply_fixed_high_shelf_audio(
        audio=audio,
        sr=sr,
        center_hz=float(params.get("freq_hz", 9000.0)),
        gain_db=float(params.get("gain_db", 0.0)),
    )

    _write_wav(output_path, processed, sr)
    return {}


def _execute_dynamic_eq_file(input_path: str, output_path: str, op: Any) -> dict[str, Any]:
    audio, sr = _load_audio(input_path)
    processed = _apply_dynamic_eq_audio(audio=audio, sr=sr, op=op)

    _write_wav(output_path, processed, sr)
    return {}


def _execute_dynamic_tilt_file(input_path: str, output_path: str, op: Any) -> dict[str, Any]:
    audio, sr = _load_audio(input_path)
    processed = _apply_dynamic_tilt_audio(audio=audio, sr=sr, op=op)

    _write_wav(output_path, processed, sr)
    return {}


def _execute_parallel_eq_fill_file(input_path: str, output_path: str, op: Any) -> dict[str, Any]:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    boosted = _apply_fixed_bell_eq_audio(
        audio=audio,
        sr=sr,
        center_hz=float(params.get("freq_hz", 170.0)),
        q=float(params.get("q", 0.9)),
        gain_db=float(params.get("gain_db", 0.0)),
    )

    mix = _clamp(float(params.get("mix", 0.1)), 0.0, 0.40)

    wet = (boosted - audio) * mix

    _write_wav(output_path, wet, sr)

    return {
        "parallel_contract": "wet_delta_only",
        "mix": round(mix, 6),
    }


def _execute_parallel_compressor_file(input_path: str, output_path: str, op: Any) -> dict[str, Any]:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    threshold_db = float(params.get("threshold_db", -18.0))
    ratio = float(params.get("ratio", 1.5))
    attack_ms = float(params.get("attack_ms", 28.0))
    release_ms = float(params.get("release_ms", 150.0))
    mix = _clamp(float(params.get("mix", 0.15)), 0.0, 0.30)
    channel_mode = str(_read(op, "channel_mode", "stereo") or "stereo").lower()

    primitive_name = str(_read(op, "primitive_name", "") or "").lower()

    if primitive_name == "transient_safe_support_compression":
        low_hz = float(params.get("low_cut_hz", 85.0) or 85.0)
        high_hz = float(params.get("high_cut_hz", 340.0) or 340.0)
    else:
        low_hz = float(params.get("low_cut_hz", 110.0) or 110.0)
        high_hz = float(params.get("high_cut_hz", 420.0) or 420.0)

    band = _apply_band_component_audio(
        audio=audio,
        sr=sr,
        low_hz=low_hz,
        high_hz=high_hz,
    )

    if band.shape[1] == 1:
        detector = band[:, 0]
    elif channel_mode == "mid":
        detector = 0.5 * (band[:, 0] + band[:, 1])
    else:
        detector = 0.5 * (band[:, 0] + band[:, 1])

    gain_lin = _build_parallel_compressor_gain(
        detector_signal=detector,
        sr=sr,
        threshold_db=threshold_db,
        ratio=ratio,
        attack_ms=attack_ms,
        release_ms=release_ms,
    )

    compressed_band = band * gain_lin[:, None]

    wet = compressed_band * mix

    _write_wav(output_path, wet, sr)

    mean_gain = float(np.mean(gain_lin)) if gain_lin.size else 1.0
    min_gain = float(np.min(gain_lin)) if gain_lin.size else 1.0

    return {
        "parallel_contract": "band_limited_wet_support_only",
        "band_low_hz": round(low_hz, 4),
        "band_high_hz": round(high_hz, 4),
        "mix": round(mix, 6),
        "avg_gain_reduction_db": round(
            -_lin_to_db(mean_gain) if mean_gain < 1.0 else 0.0,
            4,
        ),
        "max_gain_reduction_db": round(
            -_lin_to_db(min_gain) if min_gain < 1.0 else 0.0,
            4,
        ),
    }


def _execute_band_limited_saturation_file(input_path: str, output_path: str, op: Any) -> dict[str, Any]:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    low_cut_hz = float(params.get("low_cut_hz", 1900.0))
    high_cut_hz = float(params.get("high_cut_hz", 5200.0))
    drive_db = float(params.get("drive_db", 1.0))
    mix = _clamp(float(params.get("mix", 0.05)), 0.0, 0.30)

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

    return {
        "parallel_contract": "wet_harmonic_delta_only",
        "low_cut_hz": round(low_cut_hz, 4),
        "high_cut_hz": round(high_cut_hz, 4),
        "drive_db": round(drive_db, 4),
        "mix": round(mix, 6),
    }


def _execute_high_side_polish_file(input_path: str, output_path: str, op: Any) -> dict[str, Any]:
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

    _write_wav(output_path, processed, sr)

    return {
        "width_contract": "high_side_only_center_preserved",
        "freq_hz": round(freq_hz, 4),
        "effective": round(effective, 6),
    }


def _execute_high_only_width_file(input_path: str, output_path: str, op: Any) -> dict[str, Any]:
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

    _write_wav(output_path, processed, sr)

    return {
        "width_contract": "high_only_width_center_preserved",
        "low_cut_hz": round(low_cut_hz, 4),
        "high_cut_hz": round(high_cut_hz, 4),
        "effective_width": round(effective_width, 6),
    }


# ============================================================
# DELIVERY EXECUTION
# ============================================================

def _resolve_delivery_output_gain(
    op: Any,
    params: dict[str, Any],
    *,
    stage_name: str | None,
) -> tuple[float, dict[str, Any]]:
    requested_gain_db = float(params.get("gain_db", 0.0))
    is_delivery = _is_delivery_context(stage_name, op)

    if not is_delivery:
        applied_gain_db = _clamp(requested_gain_db, -12.0, 12.0)

        return applied_gain_db, {
            "gain_contract": "non_delivery_gain_operation",
            "requested_gain_db": round(requested_gain_db, 4),
            "applied_gain_db": round(applied_gain_db, 4),
            "delivery_context": False,
        }

    applied_gain_db = requested_gain_db
    positive_gain_blocked = False

    if DELIVERY_BLOCKS_POSITIVE_GAIN and requested_gain_db > 0.0:
        applied_gain_db = 0.0
        positive_gain_blocked = True

    applied_gain_db = _clamp(applied_gain_db, -3.0, 0.0)

    retry_hints: list[str] = []

    if positive_gain_blocked:
        retry_hints.extend(
            [
                "delivery_requested_positive_gain",
                "delivery_positive_gain_blocked",
                "do_not_buy_loudness_in_delivery",
                "if_loudness_needed_adjust_musical_graph_or_accept_peak_blocked_master",
            ]
        )

    debug = {
        "delivery_contract": "gain_stage_safety_only_no_loudness_purchase",
        "delivery_context": True,
        "requested_gain_db": round(requested_gain_db, 4),
        "applied_gain_db": round(applied_gain_db, 4),
        "positive_gain_blocked": positive_gain_blocked,
        "delivery_blocks_positive_gain": DELIVERY_BLOCKS_POSITIVE_GAIN,
        "upstream_loudness_purchase_blocked": positive_gain_blocked,
        "should_retry_upstream": False,
        "retry_hints": retry_hints,
    }

    return applied_gain_db, debug


def _execute_output_trim_file(
    input_path: str,
    output_path: str,
    op: Any,
    *,
    stage_name: str | None,
) -> dict[str, Any]:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    gain_db, debug = _resolve_delivery_output_gain(
        op,
        params,
        stage_name=stage_name,
    )

    processed = _apply_output_trim_audio(audio=audio, gain_db=gain_db)

    _write_wav(output_path, processed, sr)

    return debug


def _execute_true_peak_limiter_file(input_path: str, output_path: str, op: Any) -> dict[str, Any]:
    audio, sr = _load_audio(input_path)
    params = dict(_read(op, "params", {}) or {})

    ceiling_db = float(params.get("gain_db", -1.05))

    threshold_raw = params.get("threshold_db", None)
    threshold_db = float(threshold_raw) if threshold_raw is not None else ceiling_db - 0.45

    processed, limiter_debug = _apply_true_peak_limiter_audio(
        audio=audio,
        sr=sr,
        ceiling_db=ceiling_db,
        threshold_db=threshold_db,
        attack_ms=float(params.get("attack_ms", 0.20)),
        release_ms=float(params.get("release_ms", 48.0)),
        mix=float(params.get("mix", 1.0)),
        oversample=LIMITER_OVERSAMPLE,
        safety_margin_db=0.10,
        max_passes=4,
    )

    _write_wav(output_path, processed, sr)

    return {
        "delivery_contract": "terminal_true_peak_protection_only",
        **limiter_debug,
    }


# ============================================================
# OP ROUTING
# ============================================================

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
        "debug": {
            "executor_contract": "transparent_sm_render_engine",
            "pre": None,
            "post": None,
            "delta": None,
            "op_extra": {},
        },
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
        op_report["debug"]["dry_run"] = True
        return op_report

    pre_stats = _audio_file_stats(input_path)
    output_path = _make_tmp_wav(td, stage_name, stack_name, instance_name)

    op_extra: dict[str, Any] = {}

    if op_kind in {"static_eq", "broad_eq"}:
        op_extra = _execute_static_eq_file(
            input_path=input_path,
            output_path=output_path,
            op=op,
        )

    elif op_kind == "high_shelf":
        op_extra = _execute_high_shelf_file(
            input_path=input_path,
            output_path=output_path,
            op=op,
        )

    elif op_kind in {"dynamic_eq", "dynamic_eq_boost"}:
        op_extra = _execute_dynamic_eq_file(
            input_path=input_path,
            output_path=output_path,
            op=op,
        )

    elif op_kind == "dynamic_tilt":
        op_extra = _execute_dynamic_tilt_file(
            input_path=input_path,
            output_path=output_path,
            op=op,
        )

    elif op_kind in {"parallel_eq_fill", "parallel_eq_boost"}:
        op_extra = _execute_parallel_eq_fill_file(
            input_path=input_path,
            output_path=output_path,
            op=op,
        )

    elif op_kind == "parallel_compressor":
        op_extra = _execute_parallel_compressor_file(
            input_path=input_path,
            output_path=output_path,
            op=op,
        )

    elif op_kind in {"band_limited_saturation", "band_limited_texture"}:
        op_extra = _execute_band_limited_saturation_file(
            input_path=input_path,
            output_path=output_path,
            op=op,
        )

    elif op_kind == "high_side_polish":
        op_extra = _execute_high_side_polish_file(
            input_path=input_path,
            output_path=output_path,
            op=op,
        )

    elif op_kind == "high_only_width":
        op_extra = _execute_high_only_width_file(
            input_path=input_path,
            output_path=output_path,
            op=op,
        )

    elif op_kind in {"output_trim", "ceiling_trim", "final_balance_guard"}:
        op_extra = _execute_output_trim_file(
            input_path=input_path,
            output_path=output_path,
            op=op,
            stage_name=stage_name,
        )

    elif op_kind == "true_peak_limiter":
        op_extra = _execute_true_peak_limiter_file(
            input_path=input_path,
            output_path=output_path,
            op=op,
        )

    else:
        op_report["pending_reason"] = "unsupported_op_kind"
        return op_report

    post_stats = _audio_file_stats(output_path)

    op_report["executed"] = True
    op_report["output_path"] = output_path
    op_report["debug"]["pre"] = pre_stats
    op_report["debug"]["post"] = post_stats
    op_report["debug"]["delta"] = _stats_delta(pre_stats, post_stats)
    op_report["debug"]["op_extra"] = op_extra or {}

    return op_report


# ============================================================
# MIX / DELTA / STACK EXECUTION
# ============================================================

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


def _append_unique(items: list[str], value: str) -> None:
    if value not in items:
        items.append(value)


def _record_op_result(
    report: dict[str, Any],
    stack_report: dict[str, Any],
    stage_name: str,
    stack_name: str,
    op_result: dict[str, Any],
) -> bool:
    op_extra = {}

    if isinstance(op_result.get("debug"), dict):
        op_extra = op_result["debug"].get("op_extra", {}) or {}

    stack_report["ops"].append(
        {
            "instance_name": op_result["instance_name"],
            "primitive_name": op_result["primitive_name"],
            "op_kind": op_result["op_kind"],
            "backend_hint": op_result["backend_hint"],
            "executed": op_result["executed"],
            "pending_reason": op_result["pending_reason"],
            "params": op_result["params"],
            "debug": op_result.get("debug", {}),
        }
    )

    if op_result["executed"]:
        report["executed_op_count"] += 1

        if bool(op_extra.get("delivery_over_budget", False)):
            report["delivery_over_budget"] = True

        if bool(op_extra.get("should_retry_upstream", False)):
            report["should_retry_upstream"] = True

        if bool(op_extra.get("upstream_loudness_purchase_blocked", False)):
            report["delivery_positive_gain_blocked"] = True

        for hint in list(op_extra.get("retry_hints", []) or []):
            _append_unique(report["retry_hints"], str(hint))

        for hint in list(op_extra.get("delivery_retry_hints", []) or []):
            _append_unique(report["retry_hints"], str(hint))

        for warning in list(op_extra.get("delivery_budget_warnings", []) or []):
            _append_unique(report["delivery_budget_warnings"], str(warning))

        for failure in list(op_extra.get("delivery_budget_failures", []) or []):
            _append_unique(report["delivery_budget_failures"], str(failure))

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
        "debug": {
            "recombine_contract": "no_hidden_normalization",
            "source_paths": [],
            "output_stats": None,
        },
    }

    source_paths = [node_paths.get(node) for node in source_nodes]
    rec_report["debug"]["source_paths"] = source_paths

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

        rec_report["debug"]["output_stats"] = _audio_file_stats(target_path)
        return rec_report, target_path

    if kind == "finish_blend_sum" and len(source_paths) == 2:
        target_path = _mix_audio_files(
            source_paths=source_paths,
            output_path=out_path,
            gain_db=gain_db,
            weights=[1.0, blend],
        )

        rec_report["debug"]["output_stats"] = _audio_file_stats(target_path)
        return rec_report, target_path

    if kind == "guarded_parallel_sum":
        if len(source_paths) == 2 and len(source_nodes) == 2 and source_nodes[1].endswith("_bus"):
            target_path = _mix_audio_files(
                source_paths=source_paths,
                output_path=out_path,
                gain_db=0.0,
                weights=[1.0, _db_to_lin(gain_db) * blend],
            )

            rec_report["debug"]["output_stats"] = _audio_file_stats(target_path)
            return rec_report, target_path

        target_path = _mix_audio_files(
            source_paths=source_paths,
            output_path=out_path,
            gain_db=gain_db,
            weights=[1.0] * len(source_paths),
        )

        rec_report["debug"]["output_stats"] = _audio_file_stats(target_path)
        return rec_report, target_path

    if kind == "passthrough_or_sum":
        target_path = _mix_audio_files(
            source_paths=source_paths,
            output_path=out_path,
            gain_db=gain_db,
            weights=[1.0] * len(source_paths),
        )

        rec_report["debug"]["output_stats"] = _audio_file_stats(target_path)
        return rec_report, target_path

    target_path = _mix_audio_files(
        source_paths=source_paths,
        output_path=out_path,
        gain_db=gain_db,
        weights=[1.0] * len(source_paths),
    )

    rec_report["debug"]["output_stats"] = _audio_file_stats(target_path)
    return rec_report, target_path


# ============================================================
# PLAN EXECUTION
# ============================================================

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

        "delivery_positive_gain_blocked": False,
        "delivery_over_budget": False,
        "should_retry_upstream": False,
        "retry_hints": [],
        "delivery_budget_warnings": [],
        "delivery_budget_failures": [],

        "safety_notes": list(_read(render_plan, "safety_notes", []) or []),

        "executor_contract": {
            "internal_wav_subtype": INTERNAL_WAV_SUBTYPE,
            "no_stage_normalization": NO_STAGE_NORMALIZATION,
            "parallel_stacks_are_wet_only": PARALLEL_STACKS_ARE_WET_ONLY,
            "delivery_is_terminal_only": DELIVERY_IS_TERMINAL_ONLY,
            "delivery_blocks_positive_gain": DELIVERY_BLOCKS_POSITIVE_GAIN,
            "delivery_limiter_touches_only_real_overs": DELIVERY_LIMITER_TOUCHES_ONLY_REAL_OVERS,
            "delivery_no_global_safety_trim": DELIVERY_NO_GLOBAL_SAFETY_TRIM,
            "limiter_oversample": LIMITER_OVERSAMPLE,
            "limiter_no_pre_ceiling_smoothing": LIMITER_NO_PRE_CEILING_SMOOTHING,
            "limiter_no_rms_makeup": LIMITER_NO_RMS_MAKEUP,
            "limiter_no_creative_compression": LIMITER_NO_CREATIVE_COMPRESSION,
        },

        "notes": list(_read(render_plan, "notes", []) or [])
        + [
            "executor_active_v7",
            "transparent_sm_render_engine",
            "internal_float_pipeline_enabled",
            "no_hidden_stage_normalization",
            "parallel_stack_semantics_fixed",
            "parallel_ops_run_from_same_tap_point",
            "parallel_outputs_are_wet_layers",
            "bridge_compressor_band_limited_support_only",
            "finish_spark_is_wet_micro_bus",
            "finish_deess_runs_on_spark_bus_only",
            "native_finish_width_ops_supported",
            "delivery_terminal_only",
            "delivery_positive_gain_blocked",
            "delivery_cannot_buy_loudness",
            "delivery_limiter_real_overs_only",
            "delivery_does_not_touch_below_target_ceiling",
            "delivery_no_global_safety_trim",
            "delivery_damage_budget_enabled",
            "delivery_reports_upstream_retry_need_without_retrying",
            "delivery_no_rms_makeup",
            "limiter_transparent_true_peak_catch_only",
            "limiter_oversample_4x",
            "future_peak_detector_enabled",
            "recombine_weights_no_hidden_normalization",
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
            "debug": {
                "input_stats": _audio_file_stats(stage_input_path) if stage_input_path else None,
                "output_stats": None,
            },
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
                "debug": {
                    "tap_point": tap_point,
                    "input_path": stack_input_path,
                    "input_stats": _audio_file_stats(stack_input_path) if stack_input_path else None,
                    "output_path": None,
                    "output_stats": None,
                },
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

            stack_report["debug"]["output_path"] = stack_output_path
            stack_report["debug"]["output_stats"] = _audio_file_stats(stack_output_path) if stack_output_path else None

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
        stage_report["debug"]["output_stats"] = _audio_file_stats(stage_report["resolved_output_path"])

        report["stage_reports"].append(stage_report)

        current_path = node_paths.get(output_node) or current_path

    if report["pending_custom_op_count"] > 0:
        report["status"] = "partial_custom_dsp_pending"

    report["final_output_path"] = node_paths.get(final_output_node)
    report["final_output_stats"] = _audio_file_stats(report["final_output_path"])

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

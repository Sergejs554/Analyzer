# smart_auto.py

import numpy as np

# === изменено ===
# Smart Auto всегда работает по одному инженерному сценарию.
# Пользователь влияет только на intensity/tone (смещения поверх базового расчёта).

_INTENSITY_DELTA_I = {
    "low": -1.0,        # тише, больше динамики
    "balanced": 0.0,
    "high": +1.0        # громче/плотнее
}

_TONE_EQ_BIAS = {
    # (delta_low_shelf_db, delta_high_shelf_db)
    "warm":   (+0.8, -0.8),
    "balanced": (0.0, 0.0),
    "bright": (-0.8, +0.8),
}

def _clamp(x, lo, hi):
    return float(np.clip(x, lo, hi))

def decide_smart_params(analysis: dict, intensity: str = "balanced", tone_mode: str = "balanced") -> dict:
    """
    Smart Auto параметры + управляемые смещения от пользователя.
    Возвращает:
      loudnorm: {I, TP, LRA}
      tone: {low_shelf, high_shelf, hpf}
      comp: {ratio, threshold_db, attack, release}
      stereo_widen: bool
    """
    I = float(analysis["LUFS"])
    LRA = float(analysis["LRA"])
    tp = float(analysis["TruePeak_dBFS"])
    tilt = float(np.clip(float(analysis["Tilt_dB"]), -20.0, 20.0))
    sub_excess = bool(analysis["SubExcess"])
    stereo_narrow = bool(analysis.get("StereoNarrow", False))
    rms_db = float(analysis.get("RMS_dB", -20.0))

    # --- BASE TARGETS (инженерная база) ---
    # Loudness target
    target_I = float(np.interp(I, [-30, -22, -16, -12, -10], [-18, -16, -14.5, -13, -12]))
    target_I = _clamp(target_I, -16.5, -11.5)

    # LRA target
    if LRA >= 20:
        target_LRA = 8.0
    elif LRA >= 10:
        target_LRA = 6.0
    else:
        target_LRA = 5.0
    target_LRA = _clamp(target_LRA, 4.0, 10.0)

    # TP target
    target_TP = -1.0 if tp >= -0.1 else -0.5
    target_TP = _clamp(target_TP, -3.0, 0.0)

    # Tone shelves from tilt (base)
    high_shelf_gain = float(np.interp(tilt, [-20, 0, 20], [+3.0, 0.0, -3.0]))
    low_shelf_gain  = float(np.interp(tilt, [-20, 0, 20], [-2.5, 0.0, +2.0]))

    # Compression base (адаптивно от RMS/LRA)
    if LRA >= 15 or rms_db < -24:
        comp_ratio = 1.3; thr = rms_db + 6
    elif LRA >= 8 and rms_db < -20:
        comp_ratio = 1.5; thr = rms_db + 4
    elif rms_db < -16:
        comp_ratio = 1.8; thr = rms_db + 2
    else:
        comp_ratio = 2.0; thr = rms_db + 0

    attack = 20
    release = 150

    # --- USER BIASES (только intensity/tone) ---
    inten_key = (intensity or "balanced").lower()
    if inten_key not in _INTENSITY_DELTA_I:
        inten_key = "balanced"
    delta_I = _INTENSITY_DELTA_I[inten_key]
    target_I = _clamp(target_I + delta_I, -16.5, -11.0)  # чуть расширил верх, но безопасно

    # intensity также слегка двигает компрессию
    if inten_key == "low":
        comp_ratio = max(1.2, comp_ratio - 0.15)
        thr = thr + 0.5
    elif inten_key == "high":
        comp_ratio = min(2.4, comp_ratio + 0.15)
        thr = thr - 0.5

    tone_key = (tone_mode or "balanced").lower()
    if tone_key not in _TONE_EQ_BIAS:
        tone_key = "balanced"
    d_lo, d_hi = _TONE_EQ_BIAS[tone_key]

    low_shelf_gain = _clamp(low_shelf_gain + d_lo, -3.5, +3.5)
    high_shelf_gain = _clamp(high_shelf_gain + d_hi, -3.5, +3.5)

    # --- OUTPUT STRUCT ---
    tone = {
        "low_shelf":  {"g": round(low_shelf_gain, 2),  "f": 250,  "width": 1.0},
        "high_shelf": {"g": round(high_shelf_gain, 2), "f": 8000, "width": 0.8},
        "hpf": bool(sub_excess)
    }

    comp = {
        "ratio": round(float(comp_ratio), 2),
        "threshold_db": round(float(thr), 1),
        "attack": int(attack),
        "release": int(release)
    }

    stereo_widen = bool(stereo_narrow)

    return {
        "loudnorm": {"I": round(float(target_I), 2), "TP": float(target_TP), "LRA": float(target_LRA)},
        "tone": tone,
        "comp": comp,
        "stereo_widen": stereo_widen
    }

def build_smart_chain(params: dict) -> str:
    """
    Build the FFmpeg audio filter chain string from Smart Auto params.
    """
    tone = params["tone"]
    comp = params["comp"]
    ln = params["loudnorm"]

    filters = []

    if tone.get("hpf"):
        filters.append("highpass=f=30:width=0.7")

    lf = tone.get("low_shelf")
    if lf:
        filters.append(f"bass=g={lf['g']}:f={lf['f']}:w={lf['width']}")

    hf = tone.get("high_shelf")
    if hf:
        filters.append(f"treble=g={hf['g']}:f={hf['f']}:w={hf['width']}")

    filters.append(
        f"acompressor=ratio={comp['ratio']}:threshold={comp['threshold_db']}dB:"
        f"attack={comp['attack']}:release={comp['release']}"
    )

    if params.get("stereo_widen"):
        filters.append("stereowiden=delay=10:drymix=0.9:crossfeed=0.4:feedback=0.4")

    filters.append(f"loudnorm=I={ln['I']}:TP={ln['TP']}:LRA={ln['LRA']}:print_format=summary")

    return ",".join(filters)
# === конец изменения ===

# smart_auto.py
import numpy as np

# Smart Auto всегда работает по одному инженерному сценарию.
# Пользователь влияет только на intensity/tone (смещения поверх базового расчёта).
# v2: добавлено секционное влияние (≤10%) — готовим данные для app.py (секционный DSP)

_INTENSITY_DELTA_I = {
    "low": -1.0,        # тише, больше динамики
    "balanced": 0.0,
    "high": +1.0        # громче/плотнее
}

_TONE_EQ_BIAS = {
    # (delta_low_shelf_db, delta_high_shelf_db)
    "warm":     (+0.8, -0.8),
    "balanced": (0.0, 0.0),
    "bright":   (-0.8, +0.8),
}

def _clamp(x, lo, hi):
    return float(np.clip(x, lo, hi))

def _deepcopy_params(p: dict) -> dict:
    # быстрый deepcopy без импорта copy (структура маленькая)
    return {
        "loudnorm": dict(p["loudnorm"]),
        "tone": {
            "low_shelf": dict(p["tone"]["low_shelf"]) if p["tone"].get("low_shelf") else None,
            "high_shelf": dict(p["tone"]["high_shelf"]) if p["tone"].get("high_shelf") else None,
            "hpf": bool(p["tone"].get("hpf", False)),
        },
        "comp": dict(p["comp"]),
        "stereo_widen": bool(p.get("stereo_widen", False)),
    }

# --- секционное влияние ≤10% ---
# influence ∈ [-0.10..+0.10] приходит из auto_analysis.analyze_sections()
# Наша задача: очень мягко подвинуть targets/компрессию/эквализацию
# чтобы было "чуть плотнее в припеве, чуть мягче в куплете" без слышимых ступенек.

def apply_section_influence(base_params: dict, influence: float) -> dict:
    """
    Возвращает секционные параметры на основе base_params + influence (<=10%).
    ВНИМАНИЕ: здесь только математика параметров. Плавность/кроссфейд — в app.py.
    """
    p = _deepcopy_params(base_params)
    inf = float(np.clip(influence, -0.10, 0.10))

    # 1) loudnorm target I — лёгкий сдвиг (в пределах ~±0.7 LUFS)
    #    (это именно target, а не гейн прямо сейчас)
    p["loudnorm"]["I"] = _clamp(p["loudnorm"]["I"] + (inf * 7.0), -16.5, -11.0)

    # 2) compressor — чуть плотнее на +inf и чуть мягче на -inf
    #    ratio: +/- 0.15 макс; threshold: +/- 0.8 dB
    p["comp"]["ratio"] = round(_clamp(p["comp"]["ratio"] + (inf * 1.5), 1.20, 2.60), 2)
    p["comp"]["threshold_db"] = round(float(p["comp"]["threshold_db"] - (inf * 8.0)), 1)

    # 3) shelves — микро-движение, чтобы "воздух/плотность" на секциях ощущалась
    #    low shelf: +/- 0.35 dB, high shelf: +/- 0.35 dB
    lf = p["tone"].get("low_shelf")
    hf = p["tone"].get("high_shelf")
    if lf:
        lf["g"] = round(_clamp(float(lf["g"]) + (inf * 3.5), -3.5, +3.5), 2)
    if hf:
        hf["g"] = round(_clamp(float(hf["g"]) + (inf * 3.5), -3.5, +3.5), 2)

    # HPF и widen — не секционируем (это “глобальные” решения)
    return p

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
    target_I = float(np.interp(I, [-30, -22, -16, -12, -10], [-18, -16, -14.5, -13, -12]))
    target_I = _clamp(target_I, -16.5, -11.5)

    if LRA >= 20:
        target_LRA = 8.0
    elif LRA >= 10:
        target_LRA = 6.0
    else:
        target_LRA = 5.0
    target_LRA = _clamp(target_LRA, 4.0, 10.0)

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
    target_I = _clamp(target_I + delta_I, -16.5, -11.0)

    # intensity также слегка двигает компрессию
    if inten_key == "low":
        comp_ratio = max(1.2, comp_ratio - 0.15)
        thr = thr + 0.5
    elif inten_key == "high":
        comp_ratio = min(2.6, comp_ratio + 0.15)
        thr = thr - 0.5

    tone_key = (tone_mode or "balanced").lower()
    if tone_key not in _TONE_EQ_BIAS:
        tone_key = "balanced"
    d_lo, d_hi = _TONE_EQ_BIAS[tone_key]

    low_shelf_gain = _clamp(low_shelf_gain + d_lo, -3.5, +3.5)
    high_shelf_gain = _clamp(high_shelf_gain + d_hi, -3.5, +3.5)

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

def decide_smart_params_with_sections(
    global_analysis: dict,
    sections: list,
    intensity: str = "balanced",
    tone_mode: str = "balanced",
) -> dict:
    """
    Вход:
      global_analysis: auto_analysis.analyze_sections(... )["global"]
      sections: auto_analysis.analyze_sections(... )["sections"] (в каждом есть start/end/influence)
      intensity/tone_mode: от пользователя

    Выход:
      {
        "base_params": {...},
        "sections": [
          {"start":..., "end":..., "influence":..., "params": {...}},
          ...
        ]
      }
    """
    base = decide_smart_params(global_analysis, intensity=intensity, tone_mode=tone_mode)

    out_sections = []
    for s in (sections or []):
        inf = float(s.get("influence", 0.0))
        sp = apply_section_influence(base, inf)
        out_sections.append({
            "start": float(s.get("start", 0.0)),
            "end": float(s.get("end", 0.0)),
            "influence": float(np.clip(inf, -0.10, 0.10)),
            "params": sp
        })

    return {"base_params": base, "sections": out_sections}

def build_smart_chain(params: dict) -> str:
    """
    Build the FFmpeg audio filter chain string from Smart Auto params.
    (Для секционного режима app.py будет вызывать это для каждой секции отдельно)
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

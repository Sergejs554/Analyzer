# smart_auto.py

import numpy as np
from presets import PRESETS  # Load preset values for reference intensities and tone

def decide_smart_params(analysis: dict) -> dict:
    """
    Decide mastering parameters (loudness targets, EQ gains, compression) based on analysis.
    Returns a dict with keys: 'loudnorm', 'tone', 'comp', 'stereo_widen'.
    """
    I = analysis["LUFS"]
    LRA = analysis["LRA"]
    tp = analysis["TruePeak_dBFS"]
    tilt = float(np.clip(analysis["Tilt_dB"], -20.0, 20.0))
    sub_excess = analysis["SubExcess"]
    stereo_narrow = analysis.get("StereoNarrow", False)

    # Target integrated loudness (I) based on input loudness and desired intensity:
    # We interpolate target I: quieter tracks get more boost, very loud tracks get less boost (to avoid over-compression)
    # Map input LUFS to output target: 
    # If input is -22 or below (very quiet) -> target ~ -16 LUFS;
    # If input is -16 (moderate) -> target ~ -14.5 LUFS;
    # If input is -12 (already loud) -> target ~ -13 LUFS;
    # If input is -10 or above (very loud/compressed) -> target ~ -12 LUFS.
    target_I = float(np.interp(I, [-30, -22, -16, -12, -10], [-18, -16, -14.5, -13, -12]))
    # Constrain target_I to a safe range
    target_I = float(np.clip(target_I, -16.5, -11.5))

    # Target Loudness Range (LRA):
    # Aim for an output LRA around 6 (moderate dynamics). If input LRA is very high, we allow more, if very low, we try not to reduce further.
    if LRA >= 20:
        target_LRA = 8.0  # very dynamic track, preserve more dynamics
    elif LRA >= 10:
        target_LRA = 6.0
    else:
        target_LRA = 5.0  # if track is already very compressed, keep LRA a bit lower to avoid boosting noise floor
    target_LRA = float(np.clip(target_LRA, 4.0, 10.0))

    # Target True Peak:
    # Ensure some headroom. If input true peak was 0dBFS or above, we set a slightly lower TP to avoid clipping.
    target_TP = -1.0 if tp >= -0.1 else -0.5  # -1.0 dBFS for safety if clipping risk, else -0.5 dBFS
    target_TP = float(np.clip(target_TP, -3.0, 0.0))

    # Tone (EQ shelves) based on tilt:
    # We apply a smooth shelving EQ: if tilt is very negative (bass heavy), boost highs / cut lows; if tilt is very positive (too bright), boost lows / cut highs.
    # Scale gains: we don't exceed ±3 dB shelf typically.
    high_shelf_gain = np.interp(tilt, [-20, 0, 20], [+3.0, 0.0, -3.0])
    low_shelf_gain  = np.interp(tilt, [-20, 0, 20], [-2.5, 0.0, +2.0])
    tone = {
        "low_shelf":  {"g": round(low_shelf_gain, 2),  "f": 250,  "width": 1.0},
        "high_shelf": {"g": round(high_shelf_gain, 2), "f": 8000, "width": 0.8}
    }
    # If sub bass is excessive, include a high-pass filter at 30 Hz
    if sub_excess:
        tone["hpf"] = True
    else:
        tone["hpf"] = False

    # Compression settings based on RMS level and LRA:
    # Use input RMS (average level) to set threshold and ratio adaptively.
    rms_db = analysis.get("RMS_dB", -20.0)
    if LRA >= 15 or rms_db < -24:
        comp_ratio = 1.3; thr = rms_db + 6
    elif LRA >= 8 and rms_db < -20:
        comp_ratio = 1.5; thr = rms_db + 4
    elif rms_db < -16:
        comp_ratio = 1.8; thr = rms_db + 2
    else:
        comp_ratio = 2.0; thr = rms_db + 0  # for loud tracks, gentle compression (we don't overdo ratio because already compressed)
    comp = {"ratio": round(comp_ratio, 2), "threshold_db": round(thr, 1), "attack": 20, "release": 150}

    # Stereo widen if track is very narrow:
    stereo_widen = False
    if stereo_narrow:
        stereo_widen = True

    return {
        "loudnorm": {"I": round(target_I, 2), "TP": target_TP, "LRA": target_LRA},
        "tone": tone,
        "comp": comp,
        "stereo_widen": stereo_widen
    }

def build_smart_chain(params: dict) -> str:
    """
    Build the FFmpeg audio filter chain string from the parameters determined by Smart Auto.
    This includes EQ (shelves + optional HPF), compressor, optional stereo widening, and loudnorm.
    """
    tone = params["tone"]
    comp = params["comp"]
    ln = params["loudnorm"]
    filters = []
    # High-pass filter if needed
    if tone.get("hpf"):
        filters.append("highpass=f=30:width=0.7")
    # Low shelf EQ
    if tone.get("low_shelf"):
        lf = tone["low_shelf"]
        filters.append(f"bass=g={lf['g']}:f={lf['f']}:w={lf['width']}")
    # High shelf EQ
    if tone.get("high_shelf"):
        hf = tone["high_shelf"]
        filters.append(f"treble=g={hf['g']}:f={hf['f']}:w={hf['width']}")
    # Compression
    filters.append(f"acompressor=ratio={comp['ratio']}:threshold={comp['threshold_db']}dB:attack={comp['attack']}:release={comp['release']}")
    # Stereo widen (if needed, add with moderate settings)
    if params.get("stereo_widen"):
        # Use FFmpeg stereowiden filter with mild settings to avoid artifacts
        filters.append("stereowiden=delay=10:drymix=0.9:crossfeed=0.4:feedback=0.4")
    # Loudness normalization (EBU R128)
    filters.append(f"loudnorm=I={ln['I']}:TP={ln['TP']}:LRA={ln['LRA']}:print_format=summary")
    # Join all parts with commas
    chain = ",".join(filters)
    return chain

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify, send_file
import os, tempfile, requests, re, subprocess, shlex, json

from analyze_mastering import run_analysis
from auto_analysis import analyze_sections
from smart_auto import decide_smart_params_with_sections, build_smart_chain

app = Flask(__name__)

# --- helpers ---

GDRIVE_RX = re.compile(r"(?:https?://)?(?:drive\.google\.com)/(?:file/d/|open\?id=|uc\?id=)([\w-]+)")

def is_gdrive(url: str) -> bool:
    return GDRIVE_RX.search(url or "") is not None

def gdrive_file_id(url: str):
    m = GDRIVE_RX.search(url or "")
    if not m:
        return None
    return m.group(1)

def gdrive_direct(url: str) -> str:
    fid = gdrive_file_id(url)
    if not fid:
        return url
    return f"https://drive.google.com/uc?export=download&id={fid}"

def guess_ext(url: str, content_type: str | None) -> str:
    u = (url or "").lower()
    if ".wav" in u: return ".wav"
    if ".mp3" in u: return ".mp3"
    if ".m4a" in u: return ".m4a"
    if ".flac" in u: return ".flac"
    if ".aiff" in u or ".aif" in u: return ".aiff"
    if content_type:
        ct = content_type.lower()
        if "audio/wav" in ct or "audio/x-wav" in ct: return ".wav"
        if "audio/mpeg" in ct: return ".mp3"
        if "audio/mp4" in ct or "audio/x-m4a" in ct: return ".m4a"
        if "audio/flac" in ct: return ".flac"
        if "audio/aiff" in ct or "audio/x-aiff" in ct: return ".aiff"
    return ".wav"

def download_file(url: str, out_path: str, timeout: int = 120) -> tuple[int, str, str]:
    sess = requests.Session()
    r = sess.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()

    ct = (r.headers.get("Content-Type") or "").lower()
    if "text/html" in ct and "drive.google.com" in (r.url or ""):
        confirm = None
        for k, v in r.cookies.items():
            if k.startswith("download_warning"):
                confirm = v
                break
        if confirm:
            fid = gdrive_file_id(url) or gdrive_file_id(r.url)
            if fid:
                url2 = f"https://drive.google.com/uc?export=download&id={fid}&confirm={confirm}"
                r = sess.get(url2, timeout=timeout, allow_redirects=True)
                r.raise_for_status()
                ct = (r.headers.get("Content-Type") or "").lower()

    if "text/html" in ct:
        raise RuntimeError(f"Downloaded HTML instead of audio. final_url={r.url}")

    with open(out_path, "wb") as f:
        f.write(r.content)

    return len(r.content), (r.url or url), (r.headers.get("Content-Type") or "")

def _dl_to_named(td: str, label: str, url: str) -> tuple[str, dict]:
    tmp = os.path.join(td, f"{label}.tmp")
    size, final, ctype = download_file(url, tmp)
    ext = guess_ext(final, ctype)
    path = os.path.join(td, f"{label}{ext}")
    os.replace(tmp, path)
    dbg = {
        f"{label}_bytes": size,
        f"{label}_final_url": final,
        f"{label}_file": os.path.basename(path),
        f"{label}_content_type": ctype,
    }
    return path, dbg

def _run(cmd: str):
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.decode("utf-8", errors="ignore")[:4000])
    return p.stdout.decode("utf-8", errors="ignore"), p.stderr.decode("utf-8", errors="ignore")

# ---------------------------
# MR MASTERING v3.1 — PARALLEL BASS BLEND
# ---------------------------

def _clamp(x, lo, hi):
    return float(max(lo, min(hi, x)))

# TEMP TEST: afftdn off by default
_ENABLE_AFFTDN = (os.getenv("ENABLE_AFFTDN", "0").strip() == "1")

# Pre-Clean
_PRE_CLEAN_CHAIN = "highpass=f=25:width=0.7" + (",afftdn=nf=-25" if _ENABLE_AFFTDN else "")

# LOW-MID "ПЛЕЧИ"
_LOWMID_ON = (os.getenv("LOWMID_ON", "1").strip() == "1")
_LOWMID_F = float(os.getenv("LOWMID_F", "200"))
_LOWMID_W = float(os.getenv("LOWMID_W", "0.7"))
_LOWMID_G = float(os.getenv("LOWMID_G", "0.6"))

def _lowmid_filter() -> str:
    if not _LOWMID_ON:
        return "anull"
    f = _clamp(float(_LOWMID_F), 20.0, 800.0)
    w = _clamp(float(_LOWMID_W), 0.2, 3.0)
    g = _clamp(float(_LOWMID_G), -3.0, 3.0)
    return f"equalizer=f={f}:t=q:w={w}:g={g}"

# GLUE (legacy)
_GLUE_ON = (os.getenv("GLUE_ON", "0").strip() == "1")
_GLUE_RATIO = float(os.getenv("GLUE_RATIO", "1.6"))
_GLUE_THRESHOLD_DB = float(os.getenv("GLUE_THRESHOLD_DB", "-18"))
_GLUE_ATTACK_MS = float(os.getenv("GLUE_ATTACK_MS", "10"))
_GLUE_RELEASE_MS = float(os.getenv("GLUE_RELEASE_MS", "120"))
_GLUE_KNEE_DB = float(os.getenv("GLUE_KNEE_DB", "2"))
_GLUE_MAKEUP_DB = float(os.getenv("GLUE_MAKEUP_DB", "0"))
_GLUE_MIX = float(os.getenv("GLUE_MIX", "1.0"))

def _glue_filter() -> str:
    if not _GLUE_ON:
        return "anull"
    ratio = _clamp(float(_GLUE_RATIO), 1.0, 10.0)
    thr = _clamp(float(_GLUE_THRESHOLD_DB), -60.0, 0.0)
    att = _clamp(float(_GLUE_ATTACK_MS), 0.1, 200.0)
    rel = _clamp(float(_GLUE_RELEASE_MS), 5.0, 2000.0)
    knee = _clamp(float(_GLUE_KNEE_DB), 0.0, 12.0)
    makeup = _clamp(float(_GLUE_MAKEUP_DB), -6.0, 12.0)
    mix = _clamp(float(_GLUE_MIX), 0.0, 1.0)
    return (
        f"acompressor=threshold={thr}dB:ratio={ratio}:attack={att}:release={rel}:"
        f"knee={knee}dB:makeup={makeup}dB:mix={mix}"
    )

# KICKSAFE GLUE
_KICKSAFE_GLUE_ON = (os.getenv("KICKSAFE_GLUE_ON", "0").strip() == "1")
_KICKSAFE_XOVER_HZ = float(os.getenv("KICKSAFE_XOVER_HZ", "140"))
_KICKSAFE_RATIO = float(os.getenv("KICKSAFE_RATIO", "1.6"))
_KICKSAFE_THRESHOLD_DB = float(os.getenv("KICKSAFE_THRESHOLD_DB", "-20"))
_KICKSAFE_ATTACK_MS = float(os.getenv("KICKSAFE_ATTACK_MS", "10"))
_KICKSAFE_RELEASE_MS = float(os.getenv("KICKSAFE_RELEASE_MS", "160"))
_KICKSAFE_KNEE_DB = float(os.getenv("KICKSAFE_KNEE_DB", "2"))
_KICKSAFE_MAKEUP_DB = float(os.getenv("KICKSAFE_MAKEUP_DB", "0"))
_KICKSAFE_MIX = float(os.getenv("KICKSAFE_MIX", "0.25"))

def _kicksafe_glue_enabled() -> bool:
    return bool(_KICKSAFE_GLUE_ON)

def _kicksafe_glue_fc() -> str:
    xover = _clamp(float(_KICKSAFE_XOVER_HZ), 80.0, 240.0)
    ratio = _clamp(float(_KICKSAFE_RATIO), 1.0, 10.0)
    thr = _clamp(float(_KICKSAFE_THRESHOLD_DB), -60.0, 0.0)
    att = _clamp(float(_KICKSAFE_ATTACK_MS), 0.1, 200.0)
    rel = _clamp(float(_KICKSAFE_RELEASE_MS), 5.0, 2000.0)
    knee = _clamp(float(_KICKSAFE_KNEE_DB), 0.0, 12.0)
    makeup = _clamp(float(_KICKSAFE_MAKEUP_DB), -6.0, 12.0)
    mix = _clamp(float(_KICKSAFE_MIX), 0.0, 1.0)

    comp = (
        f"acompressor=threshold={thr}dB:ratio={ratio}:attack={att}:release={rel}:"
        f"knee={knee}dB:makeup={makeup}dB:mix={mix}"
    )

    return (
        f"[0:a]asplit=2[aL][aH];"
        f"[aL]lowpass=f={xover}:width=0.707[aLow];"
        f"[aH]highpass=f={xover}:width=0.707,{comp}[aHigh];"
        f"[aLow][aHigh]amix=inputs=2:normalize=0[aout]"
    )

# TRANSIENT
_TRANSIENT_ON = (os.getenv("TRANSIENT_ON", "0").strip() == "1")
_TRANSIENT_RATIO = float(os.getenv("TRANSIENT_RATIO", "1.8"))
_TRANSIENT_THRESHOLD_DB = float(os.getenv("TRANSIENT_THRESHOLD_DB", "-22"))
_TRANSIENT_ATTACK_MS = float(os.getenv("TRANSIENT_ATTACK_MS", "25"))
_TRANSIENT_RELEASE_MS = float(os.getenv("TRANSIENT_RELEASE_MS", "90"))
_TRANSIENT_KNEE_DB = float(os.getenv("TRANSIENT_KNEE_DB", "2"))
_TRANSIENT_MAKEUP_DB = float(os.getenv("TRANSIENT_MAKEUP_DB", "0"))
_TRANSIENT_MIX = float(os.getenv("TRANSIENT_MIX", "0.25"))

def _transient_filter() -> str:
    if not _TRANSIENT_ON:
        return "anull"
    ratio = _clamp(float(_TRANSIENT_RATIO), 1.0, 10.0)
    thr = _clamp(float(_TRANSIENT_THRESHOLD_DB), -60.0, 0.0)
    att = _clamp(float(_TRANSIENT_ATTACK_MS), 0.1, 200.0)
    rel = _clamp(float(_TRANSIENT_RELEASE_MS), 5.0, 2000.0)
    knee = _clamp(float(_TRANSIENT_KNEE_DB), 0.0, 12.0)
    makeup = _clamp(float(_TRANSIENT_MAKEUP_DB), -6.0, 12.0)
    mix = _clamp(float(_TRANSIENT_MIX), 0.0, 1.0)
    return (
        f"acompressor=threshold={thr}dB:ratio={ratio}:attack={att}:release={rel}:"
        f"knee={knee}dB:makeup={makeup}dB:mix={mix}"
    )

# MICRO HARMONICS
_HARM_ON = (os.getenv("HARM_ON", "0").strip() == "1")
_HARM_HP_HZ = float(os.getenv("HARM_HP_HZ", "140"))
_HARM_LP_HZ = float(os.getenv("HARM_LP_HZ", "12000"))
_HARM_DRIVE_DB = float(os.getenv("HARM_DRIVE_DB", "6"))
_HARM_MIX = float(os.getenv("HARM_MIX", "0.08"))

def _harm_enabled() -> bool:
    return bool(_HARM_ON)

def _harm_fc() -> str:
    hp = _clamp(float(_HARM_HP_HZ), 60.0, 300.0)
    lp = _clamp(float(_HARM_LP_HZ), 6000.0, 18000.0)
    drive_db = _clamp(float(_HARM_DRIVE_DB), 0.0, 18.0)
    mix = _clamp(float(_HARM_MIX), 0.0, 0.35)

    return (
        f"[0:a]asplit=2[dry][h];"
        f"[dry]volume=1[d0];"
        f"[h]"
        f"highpass=f={hp}:width=0.707,"
        f"lowpass=f={lp}:width=0.707,"
        f"volume={drive_db}dB,"
        f"asoftclip,"
        f"volume={mix}[h1];"
        f"[d0][h1]amix=inputs=2:normalize=0[aout]"
    )

# SUB DENSITY
_SUBDEN_ON = (os.getenv("SUBDEN_ON", "0").strip() == "1")
_SUBDEN_LO_HZ = float(os.getenv("SUBDEN_LO_HZ", "55"))
_SUBDEN_HI_HZ = float(os.getenv("SUBDEN_HI_HZ", "160"))
_SUBDEN_DRIVE_DB = float(os.getenv("SUBDEN_DRIVE_DB", "10"))
_SUBDEN_MIX = float(os.getenv("SUBDEN_MIX", "0.06"))

def _subden_enabled() -> bool:
    return bool(_SUBDEN_ON)

def _subden_fc() -> str:
    lo = _clamp(float(_SUBDEN_LO_HZ), 35.0, 90.0)
    hi = _clamp(float(_SUBDEN_HI_HZ), 110.0, 260.0)
    if hi <= lo + 10:
        hi = lo + 10

    drive_db = _clamp(float(_SUBDEN_DRIVE_DB), 0.0, 18.0)
    mix = _clamp(float(_SUBDEN_MIX), 0.0, 0.20)

    return (
        f"[0:a]asplit=2[dry][s];"
        f"[dry]volume=1[d0];"
        f"[s]"
        f"highpass=f={lo}:width=0.707,"
        f"lowpass=f={hi}:width=0.707,"
        f"volume={drive_db}dB,"
        f"asoftclip,"
        f"volume={mix}[s1];"
        f"[d0][s1]amix=inputs=2:normalize=0[aout]"
    )

# PARALLEL BASS BLEND BUS
_BASS_BLEND_ON = (os.getenv("BASS_BLEND_ON", "1").strip() == "1")
_BASS_BLEND_LO_HZ = float(os.getenv("BASS_BLEND_LO_HZ", "32"))
_BASS_BLEND_HI_HZ = float(os.getenv("BASS_BLEND_HI_HZ", "125"))
_BASS_BLEND_MIX = float(os.getenv("BASS_BLEND_MIX", "0.22"))
_BASS_BLEND_GAIN_DB = float(os.getenv("BASS_BLEND_GAIN_DB", "0.0"))

def _bass_blend_enabled() -> bool:
    return bool(_BASS_BLEND_ON)

def _render_bass_bus(in_path: str, out_path: str):
    lo = _clamp(float(_BASS_BLEND_LO_HZ), 20.0, 70.0)
    hi = _clamp(float(_BASS_BLEND_HI_HZ), 80.0, 220.0)
    if hi <= lo + 10:
        hi = lo + 10

    gain_db = _clamp(float(_BASS_BLEND_GAIN_DB), -3.0, 3.0)

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-af "{_PRE_CLEAN_CHAIN},highpass=f={lo}:width=0.707,lowpass=f={hi}:width=0.707,volume={gain_db}dB" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
    )
    _run(cmd)

def _apply_bass_blend_if_needed(processed_src: str, bass_src: str, out_path: str):
    if not _bass_blend_enabled():
        cmd = (
            f'ffmpeg -y -hide_banner -i {shlex.quote(processed_src)} '
            f'-c:a pcm_s16le -ar 48000 -ac 2 {shlex.quote(out_path)}'
        )
        _run(cmd)
        return

    mix = _clamp(float(_BASS_BLEND_MIX), 0.0, 0.60)

    fc = (
        f"[0:a]volume=1[p0];"
        f"[1:a]volume={mix}[b0];"
        f"[p0][b0]amix=inputs=2:normalize=0[aout]"
    )
    cmd = (
        f'ffmpeg -y -hide_banner '
        f'-i {shlex.quote(processed_src)} '
        f'-i {shlex.quote(bass_src)} '
        f'-filter_complex "{fc}" -map "[aout]" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
    )
    _run(cmd)

# AIR BUS
_AIR_AMOUNT = 0.16
_AIR_SHELF_F = 9000
_AIR_SHELF_G = 2.6
_AIR_WIDEN = 0.12

_RAMP_MIN = 0.08
_RAMP_MAX = 0.80

def _stereowiden_filter() -> str:
    d = int(round(_clamp(_AIR_WIDEN, 0.0, 1.0) * 100.0))
    d = max(1, min(100, d))
    return f"stereowiden=delay={d}"

def _pick_ramp(prev_len: float, next_len: float) -> float:
    r = min(_RAMP_MAX, 0.25 * prev_len, 0.25 * next_len)
    return _clamp(r, _RAMP_MIN, _RAMP_MAX)

def _build_mask_expr_from_sections(sections: list[dict]) -> str:
    if not sections:
        return "0.5"

    secs = sorted(sections, key=lambda s: float(s.get("start", 0.0)))
    starts = [float(s.get("start", 0.0)) for s in secs]
    ends = [float(s.get("end", 0.0)) for s in secs]
    w = [_clamp(float(s.get("level", 0.5)), 0.0, 1.0) for s in secs]

    expr = f"{w[-1]:.6f}"
    for i in range(len(w) - 2, -1, -1):
        b = max(starts[i+1], ends[i])
        prev_len = max(0.01, ends[i] - starts[i])
        next_len = max(0.01, ends[i+1] - starts[i+1])
        r = _pick_ramp(prev_len, next_len)

        left = b - r
        right = b + r
        wi = w[i]
        wj = w[i+1]

        expr = (
            f"if(lt(t,{left:.6f}),{wi:.6f},"
            f"if(lt(t,{right:.6f}),"
            f"({wi:.6f}+({wj:.6f}-{wi:.6f})*(t-{left:.6f})/{(2*r):.6f}),"
            f"({expr})"
            f"))"
        )
    return expr

def _strip_loudnorm(chain: str) -> tuple[str, str]:
    if "loudnorm=" not in chain:
        return chain, ""
    pre, ln = chain.rsplit("loudnorm=", 1)
    pre = pre.rstrip(",")
    ln = "loudnorm=" + ln
    return pre, ln

def _force_print_format_json(loudnorm_part: str) -> str:
    if "loudnorm=" not in loudnorm_part:
        return loudnorm_part
    if "print_format=" in loudnorm_part:
        return re.sub(r"(print_format=)(\w+)", r"\1json", loudnorm_part, count=1)
    return loudnorm_part + ":print_format=json"

def _extract_last_json_block(text: str):
    start = -1
    depth = 0
    last_obj = None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and start != -1:
                    chunk = text[start:i+1]
                    try:
                        last_obj = json.loads(chunk)
                    except Exception:
                        pass
                    start = -1
    return last_obj

def _build_loudnorm_two_pass(in_path: str, ln: dict, out_args: str, out_path: str):
    target_I = float(ln["I"])
    target_TP = float(ln["TP"])
    target_LRA = float(ln["LRA"])

    base_ln = f"loudnorm=I={target_I}:TP={target_TP}:LRA={target_LRA}:print_format=summary"
    pass1_ln = _force_print_format_json(base_ln)

    pass1_cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -af "{pass1_ln}" -f null -'
    _, err1 = _run(pass1_cmd)
    stats = _extract_last_json_block(err1)

    if stats:
        measured_args = (
            f"I={target_I}:TP={target_TP}:LRA={target_LRA}:"
            f"measured_I={stats.get('input_i', '-14')}:"
            f"measured_LRA={stats.get('input_lra', '7')}:"
            f"measured_TP={stats.get('input_tp', '-2')}:"
            f"measured_thresh={stats.get('input_thresh', '-24')}:"
            f"offset={stats.get('target_offset', '0')}:print_format=summary"
        )
        pass2_ln = "loudnorm=" + measured_args
    else:
        pass2_ln = base_ln

    pass2_cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -af "{pass2_ln}" {out_args} {shlex.quote(out_path)}'
    _run(pass2_cmd)

def _out_args(fmt: str) -> tuple[str, str, str]:
    fmt = (fmt or "wav16").lower()
    if fmt == "wav24":
        return "-ar 48000 -ac 2 -c:a pcm_s24le", "mastered_uhd.wav", "audio/wav"
    if fmt == "flac":
        return "-ar 48000 -ac 2 -c:a flac", "mastered.flac", "audio/flac"
    if fmt in ("mp3_320", "mp3"):
        return "-ar 48000 -ac 2 -c:a libmp3lame -b:a 320k", "mastered_320.mp3", "audio/mpeg"
    if fmt in ("aiff", "aif"):
        return "-ar 48000 -ac 2 -f aiff -c:a pcm_s16be", "mastered.aiff", "audio/aiff"
    return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav", "audio/wav"

def _normalize_tone(x: str) -> str:
    x = (x or "balanced").lower().strip()
    return x if x in ("warm", "balanced", "bright") else "balanced"

def _normalize_intensity(x: str) -> str:
    x = (x or "balanced").lower().strip()
    if x in ("low", "soft"): return "low"
    if x in ("high", "hard"): return "high"
    if x in ("normal", "balanced", "mid", "medium"): return "balanced"
    return "balanced"

def _normalize_format(x: str) -> str:
    x = (x or "wav16").lower().strip()
    if x in ("wav", "wav16"): return "wav16"
    if x in ("wav24",): return "wav24"
    if x in ("flac",): return "flac"
    if x in ("mp3", "mp3_320"): return "mp3_320"
    if x in ("aiff", "aif"): return "aiff"
    return "wav16"

# ---------------------------
# OUR BANDLAB v1 — REVEAL / AIR / MID OPENING
# ---------------------------
_BL_MID_ON = (os.getenv("BL_MID_ON", "1").strip() == "1")
_BL_MID_F = float(os.getenv("BL_MID_F", "1100"))
_BL_MID_G = float(os.getenv("BL_MID_G", "1.2"))
_BL_MID_W = float(os.getenv("BL_MID_W", "0.9"))

_BL_PRES_ON = (os.getenv("BL_PRES_ON", "1").strip() == "1")
_BL_PRES_F = float(os.getenv("BL_PRES_F", "2200"))
_BL_PRES_G = float(os.getenv("BL_PRES_G", "0.5"))
_BL_PRES_W = float(os.getenv("BL_PRES_W", "1.0"))

_BL_AIR_ON = (os.getenv("BL_AIR_ON", "1").strip() == "1")
_BL_AIR_SHELF_F = float(os.getenv("BL_AIR_SHELF_F", "7800"))
_BL_AIR_SHELF_G = float(os.getenv("BL_AIR_SHELF_G", "1.6"))
_BL_AIR_MIX = float(os.getenv("BL_AIR_MIX", "0.12"))

_BL_WIDTH_ON = (os.getenv("BL_WIDTH_ON", "1").strip() == "1")
_BL_WIDTH_HP_HZ = float(os.getenv("BL_WIDTH_HP_HZ", "4200"))
_BL_WIDTH_DELAY = int(float(os.getenv("BL_WIDTH_DELAY", "16")))
_BL_WIDTH_MIX = float(os.getenv("BL_WIDTH_MIX", "0.10"))

def _render_bandlab_like(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    intensity_scale = {
        "low": 0.85,
        "balanced": 1.00,
        "high": 1.15,
    }[intensity]

    tone_air_mul = {
        "warm": 0.90,
        "balanced": 1.00,
        "bright": 1.15,
    }[tone]

    tone_mid_mul = {
        "warm": 1.08,
        "balanced": 1.00,
        "bright": 0.95,
    }[tone]

    mid_f = _clamp(float(_BL_MID_F), 500.0, 2200.0)
    mid_g = _clamp(float(_BL_MID_G) * tone_mid_mul, -1.0, 3.0)
    mid_w = _clamp(float(_BL_MID_W), 0.2, 3.0)

    pres_f = _clamp(float(_BL_PRES_F), 1200.0, 4500.0)
    pres_g = _clamp(float(_BL_PRES_G), -1.0, 2.0)
    pres_w = _clamp(float(_BL_PRES_W), 0.2, 3.0)

    air_f = _clamp(float(_BL_AIR_SHELF_F), 5000.0, 14000.0)
    air_g = _clamp(float(_BL_AIR_SHELF_G) * tone_air_mul, 0.0, 4.0)
    air_mix = _clamp(float(_BL_AIR_MIX) * intensity_scale, 0.0, 0.25)

    width_hp = _clamp(float(_BL_WIDTH_HP_HZ), 2500.0, 10000.0)
    width_delay = int(max(1, min(100, int(_BL_WIDTH_DELAY))))
    width_mix = _clamp(float(_BL_WIDTH_MIX) * intensity_scale, 0.0, 0.20)

    tone_chain = []
    if _BL_MID_ON:
        tone_chain.append(f"equalizer=f={mid_f}:t=q:w={mid_w}:g={mid_g}")
    if _BL_PRES_ON:
        tone_chain.append(f"equalizer=f={pres_f}:t=q:w={pres_w}:g={pres_g}")
    if _BL_AIR_ON:
        tone_chain.append(f"highshelf=f={air_f}:g={air_g}")

    tone_fc = ",".join(tone_chain) if tone_chain else "anull"

    if _BL_WIDTH_ON and width_mix > 0.0:
        width_fc = (
            f"highpass=f={width_hp}:width=0.707,"
            f"stereowiden=delay={width_delay},"
            f"volume={width_mix}"
        )
    else:
        width_fc = "volume=0"

    fc = (
        f"[0:a]asplit=2[dry][fx];"
        f"[dry]volume=1[d0];"
        f"[fx]{tone_fc}[fx0];"
        f"[fx0]asplit=2[fxd][fxw];"
        f"[fxd]volume={air_mix}[fx1];"
        f"[fxw]{width_fc}[fx2];"
        f"[d0][fx1][fx2]amix=inputs=3:normalize=0[out]"
    )

    out_args, out_name, _mime = _out_args(fmt)
    out_name = f"bandlab_like_{out_name}"
    out_path = os.path.join(td, out_name)

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-filter_complex "{fc}" -map "[out]" '
        f'{out_args} {shlex.quote(out_path)}'
    )
    _run(cmd)

    return out_path, out_name

# ---------------------------
# OUR BAKUAGE v1 — LOW SUPPORT / BODY / DENSITY
# ---------------------------
_BK_LOW_ON = (os.getenv("BK_LOW_ON", "1").strip() == "1")
_BK_LOW_LO_HZ = float(os.getenv("BK_LOW_LO_HZ", "32"))
_BK_LOW_HI_HZ = float(os.getenv("BK_LOW_HI_HZ", "150"))
_BK_LOW_DRIVE_DB = float(os.getenv("BK_LOW_DRIVE_DB", "1.2"))
_BK_LOW_MIX = float(os.getenv("BK_LOW_MIX", "0.16"))

_BK_BODY_ON = (os.getenv("BK_BODY_ON", "1").strip() == "1")
_BK_BODY_F = float(os.getenv("BK_BODY_F", "220"))
_BK_BODY_G = float(os.getenv("BK_BODY_G", "0.9"))
_BK_BODY_W = float(os.getenv("BK_BODY_W", "0.9"))

_BK_MID_ON = (os.getenv("BK_MID_ON", "1").strip() == "1")
_BK_MID_F = float(os.getenv("BK_MID_F", "1150"))
_BK_MID_G = float(os.getenv("BK_MID_G", "1.2"))
_BK_MID_W = float(os.getenv("BK_MID_W", "0.95"))

_BK_PRES_ON = (os.getenv("BK_PRES_ON", "1").strip() == "1")
_BK_PRES_F = float(os.getenv("BK_PRES_F", "2200"))
_BK_PRES_G = float(os.getenv("BK_PRES_G", "0.6"))
_BK_PRES_W = float(os.getenv("BK_PRES_W", "1.0"))

_BK_SOFTTOP_ON = (os.getenv("BK_SOFTTOP_ON", "1").strip() == "1")
_BK_SOFTTOP_F = float(os.getenv("BK_SOFTTOP_F", "5200"))
_BK_SOFTTOP_G = float(os.getenv("BK_SOFTTOP_G", "-1.4"))

_BK_TONE_MIX = float(os.getenv("BK_TONE_MIX", "0.11"))

_BK_LIMITER_ON = (os.getenv("BK_LIMITER_ON", "1").strip() == "1")
_BK_LIMITER_CEILING_DB = float(os.getenv("BK_LIMITER_CEILING_DB", "-1.2"))

def _render_bakuage_like(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    intensity_scale = {
        "low": 0.90,
        "balanced": 1.00,
        "high": 1.12,
    }[intensity]

    tone_body_mul = {
        "warm": 1.10,
        "balanced": 1.00,
        "bright": 0.92,
    }[tone]

    tone_pres_mul = {
        "warm": 0.92,
        "balanced": 1.00,
        "bright": 1.08,
    }[tone]

    tone_softtop_mul = {
        "warm": 1.10,
        "balanced": 1.00,
        "bright": 0.75,
    }[tone]

    low_lo = _clamp(float(_BK_LOW_LO_HZ), 20.0, 70.0)
    low_hi = _clamp(float(_BK_LOW_HI_HZ), 80.0, 220.0)
    if low_hi <= low_lo + 10:
        low_hi = low_lo + 10
    low_drive = _clamp(float(_BK_LOW_DRIVE_DB), 0.0, 8.0)
    low_mix = _clamp(float(_BK_LOW_MIX) * intensity_scale, 0.0, 0.35)

    body_f = _clamp(float(_BK_BODY_F), 120.0, 400.0)
    body_g = _clamp(float(_BK_BODY_G) * tone_body_mul, -1.0, 3.0)
    body_w = _clamp(float(_BK_BODY_W), 0.2, 3.0)

    mid_f = _clamp(float(_BK_MID_F), 700.0, 1800.0)
    mid_g = _clamp(float(_BK_MID_G), -1.0, 3.0)
    mid_w = _clamp(float(_BK_MID_W), 0.2, 3.0)

    pres_f = _clamp(float(_BK_PRES_F), 1200.0, 4000.0)
    pres_g = _clamp(float(_BK_PRES_G) * tone_pres_mul, -1.0, 2.0)
    pres_w = _clamp(float(_BK_PRES_W), 0.2, 3.0)

    softtop_f = _clamp(float(_BK_SOFTTOP_F), 3500.0, 10000.0)
    softtop_g = _clamp(float(_BK_SOFTTOP_G) * tone_softtop_mul, -4.0, 0.0)

    tone_mix = _clamp(float(_BK_TONE_MIX) * intensity_scale, 0.0, 0.30)

    ceiling_db = _clamp(float(_BK_LIMITER_CEILING_DB), -3.0, -0.3)
    ceiling_lin = 10.0 ** (ceiling_db / 20.0)

    tone_chain = []
    if _BK_BODY_ON:
        tone_chain.append(f"equalizer=f={body_f}:t=q:w={body_w}:g={body_g}")
    if _BK_MID_ON:
        tone_chain.append(f"equalizer=f={mid_f}:t=q:w={mid_w}:g={mid_g}")
    if _BK_PRES_ON:
        tone_chain.append(f"equalizer=f={pres_f}:t=q:w={pres_w}:g={pres_g}")
    if _BK_SOFTTOP_ON:
        tone_chain.append(f"highshelf=f={softtop_f}:g={softtop_g}")

    tone_fc = ",".join(tone_chain) if tone_chain else "anull"

    fc_parts = [
        "[0:a]asplit=3[dry][low][tone]",
        "[dry]volume=1[d0]",
    ]

    if _BK_LOW_ON and low_mix > 0.0:
        fc_parts.append(
            f"[low]"
            f"highpass=f={low_lo}:width=0.707,"
            f"lowpass=f={low_hi}:width=0.707,"
            f"volume={low_drive}dB,"
            f"asoftclip,"
            f"volume={low_mix}[l1]"
        )
    else:
        fc_parts.append("[low]volume=0[l1]")

    fc_parts.append(f"[tone]{tone_fc},volume={tone_mix}[t1]")
    fc_parts.append("[d0][l1][t1]amix=inputs=3:normalize=0[m0]")

    if _BK_LIMITER_ON:
        fc_parts.append(f"[m0]alimiter=limit={ceiling_lin}:level=disabled[out]")
    else:
        fc_parts.append("[m0]anull[out]")

    fc = ";".join(fc_parts)

    out_args, out_name, _mime = _out_args(fmt)
    out_name = f"bakuage_like_{out_name}"
    out_path = os.path.join(td, out_name)

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-filter_complex "{fc}" -map "[out]" '
        f'{out_args} {shlex.quote(out_path)}'
    )
    _run(cmd)

    return out_path, out_name

# ---------------------------
# OUR ENHANCE v1 — POLISH / GLUE / FINISH
# ---------------------------
_ENH_AIR_ON = (os.getenv("ENH_AIR_ON", "1").strip() == "1")
_ENH_AIR_SHELF_F = float(os.getenv("ENH_AIR_SHELF_F", "9500"))
_ENH_AIR_SHELF_G = float(os.getenv("ENH_AIR_SHELF_G", "1.8"))
_ENH_AIR_MIX = float(os.getenv("ENH_AIR_MIX", "0.10"))

_ENH_WIDTH_ON = (os.getenv("ENH_WIDTH_ON", "1").strip() == "1")
_ENH_WIDTH_HP_HZ = float(os.getenv("ENH_WIDTH_HP_HZ", "3500"))
_ENH_WIDTH_DELAY = int(float(os.getenv("ENH_WIDTH_DELAY", "14")))
_ENH_WIDTH_MIX = float(os.getenv("ENH_WIDTH_MIX", "0.09"))

_ENH_GLOSS_ON = (os.getenv("ENH_GLOSS_ON", "1").strip() == "1")
_ENH_GLOSS_HP_HZ = float(os.getenv("ENH_GLOSS_HP_HZ", "7000"))
_ENH_GLOSS_LP_HZ = float(os.getenv("ENH_GLOSS_LP_HZ", "16000"))
_ENH_GLOSS_DRIVE_DB = float(os.getenv("ENH_GLOSS_DRIVE_DB", "4.0"))
_ENH_GLOSS_MIX = float(os.getenv("ENH_GLOSS_MIX", "0.035"))

_ENH_LIMITER_ON = (os.getenv("ENH_LIMITER_ON", "1").strip() == "1")
_ENH_LIMITER_CEILING_DB = float(os.getenv("ENH_LIMITER_CEILING_DB", "-1.2"))

def _render_enhance(in_path: str, fmt: str, td: str) -> tuple[str, str]:
    fmt = _normalize_format(fmt)

    air_f = _clamp(float(_ENH_AIR_SHELF_F), 5000.0, 16000.0)
    air_g = _clamp(float(_ENH_AIR_SHELF_G), 0.0, 4.0)
    air_mix = _clamp(float(_ENH_AIR_MIX), 0.0, 0.30)

    width_hp = _clamp(float(_ENH_WIDTH_HP_HZ), 1500.0, 10000.0)
    width_delay = int(max(1, min(100, int(_ENH_WIDTH_DELAY))))
    width_mix = _clamp(float(_ENH_WIDTH_MIX), 0.0, 0.25)

    gloss_hp = _clamp(float(_ENH_GLOSS_HP_HZ), 4000.0, 12000.0)
    gloss_lp = _clamp(float(_ENH_GLOSS_LP_HZ), max(gloss_hp + 1000.0, 7000.0), 19000.0)
    gloss_drive = _clamp(float(_ENH_GLOSS_DRIVE_DB), 0.0, 10.0)
    gloss_mix = _clamp(float(_ENH_GLOSS_MIX), 0.0, 0.12)

    ceiling_db = _clamp(float(_ENH_LIMITER_CEILING_DB), -3.0, -0.3)
    ceiling_lin = 10.0 ** (ceiling_db / 20.0)

    parts = ["[0:a]asplit=4[dry][air][wid][gls]"]
    parts.append("[dry]volume=1[d0]")

    if _ENH_AIR_ON and air_mix > 0.0:
        parts.append(f"[air]highshelf=f={air_f}:g={air_g},volume={air_mix}[a1]")
    else:
        parts.append("[air]volume=0[a1]")

    if _ENH_WIDTH_ON and width_mix > 0.0:
        parts.append(
            f"[wid]highpass=f={width_hp}:width=0.707,"
            f"stereowiden=delay={width_delay},"
            f"volume={width_mix}[w1]"
        )
    else:
        parts.append("[wid]volume=0[w1]")

    if _ENH_GLOSS_ON and gloss_mix > 0.0:
        parts.append(
            f"[gls]"
            f"highpass=f={gloss_hp}:width=0.707,"
            f"lowpass=f={gloss_lp}:width=0.707,"
            f"volume={gloss_drive}dB,"
            f"asoftclip,"
            f"volume={gloss_mix}[g1]"
        )
    else:
        parts.append("[gls]volume=0[g1]")

    parts.append("[d0][a1][w1][g1]amix=inputs=4:normalize=0[m0]")

    if _ENH_LIMITER_ON:
        parts.append(f"[m0]alimiter=limit={ceiling_lin}:level=disabled[out]")
    else:
        parts.append("[m0]anull[out]")

    fc = ";".join(parts)

    out_args, out_name, _mime = _out_args(fmt)
    out_name = f"enhance_{out_name}"
    out_path = os.path.join(td, out_name)

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-filter_complex "{fc}" -map "[out]" '
        f'{out_args} {shlex.quote(out_path)}'
    )
    _run(cmd)

    return out_path, out_name

# ---------------------------
# BLEND v1.2 — ORIGINAL BASE + LAYERS
# ---------------------------
_BLEND_BASE_GAIN = float(os.getenv("BLEND_BASE_GAIN", "1.0"))

_BLEND_LOW_LO_HZ = float(os.getenv("BLEND_LOW_LO_HZ", "25"))
_BLEND_LOW_HI_HZ = float(os.getenv("BLEND_LOW_HI_HZ", "125"))
_BLEND_LOW_GAIN = float(os.getenv("BLEND_LOW_GAIN", "0.13"))

_BLEND_REVEAL_LO_HZ = float(os.getenv("BLEND_REVEAL_LO_HZ", "500"))
_BLEND_REVEAL_HI_HZ = float(os.getenv("BLEND_REVEAL_HI_HZ", "7000"))
_BLEND_REVEAL_GAIN = float(os.getenv("BLEND_REVEAL_GAIN", "0.18"))

_BLEND_POLISH_GAIN = float(os.getenv("BLEND_POLISH_GAIN", "0.10"))

_BLEND_LIMITER_ON = (os.getenv("BLEND_LIMITER_ON", "1").strip() == "1")
_BLEND_LIMITER_CEILING_DB = float(os.getenv("BLEND_LIMITER_CEILING_DB", "-1.0"))

def _render_blend(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    bak_path, _ = _render_bakuage_like(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)
    bl_path, _ = _render_bandlab_like(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)
    enh_path, _ = _render_enhance(in_path, fmt="wav16", td=td)

    base_gain = _clamp(float(_BLEND_BASE_GAIN), 0.5, 1.5)
    low_lo = _clamp(float(_BLEND_LOW_LO_HZ), 20.0, 70.0)
    low_hi = _clamp(float(_BLEND_LOW_HI_HZ), 80.0, 300.0)
    if low_hi <= low_lo + 10:
        low_hi = low_lo + 10
    low_gain = _clamp(float(_BLEND_LOW_GAIN), 0.0, 0.35)

    reveal_lo = _clamp(float(_BLEND_REVEAL_LO_HZ), 250.0, 1200.0)
    reveal_hi = _clamp(float(_BLEND_REVEAL_HI_HZ), 4000.0, 12000.0)
    if reveal_hi <= reveal_lo + 100:
        reveal_hi = reveal_lo + 100
    reveal_gain = _clamp(float(_BLEND_REVEAL_GAIN), 0.0, 0.40)

    polish_gain = _clamp(float(_BLEND_POLISH_GAIN), 0.0, 0.30)

    ceiling_db = _clamp(float(_BLEND_LIMITER_CEILING_DB), -3.0, -0.3)
    ceiling_lin = 10.0 ** (ceiling_db / 20.0)

    fc_parts = [
        f"[0:a]volume={base_gain}[base]",
        f"[1:a]highpass=f={low_lo}:width=0.707,lowpass=f={low_hi}:width=0.707,volume={low_gain}[low]",
        f"[2:a]highpass=f={reveal_lo}:width=0.707,lowpass=f={reveal_hi}:width=0.707,volume={reveal_gain}[reveal]",
        f"[3:a]volume={polish_gain}[polish]",
        "[base][low][reveal][polish]amix=inputs=4:normalize=0[m0]",
    ]

    if _BLEND_LIMITER_ON:
        fc_parts.append(f"[m0]alimiter=limit={ceiling_lin}:level=disabled[out]")
    else:
        fc_parts.append("[m0]anull[out]")

    fc = ";".join(fc_parts)

    out_args, out_name, _mime = _out_args(fmt)
    out_name = f"blend_{out_name}"
    out_path = os.path.join(td, out_name)

    cmd = (
        f'ffmpeg -y -hide_banner '
        f'-i {shlex.quote(in_path)} '
        f'-i {shlex.quote(bak_path)} '
        f'-i {shlex.quote(bl_path)} '
        f'-i {shlex.quote(enh_path)} '
        f'-filter_complex "{fc}" -map "[out]" '
        f'{out_args} {shlex.quote(out_path)}'
    )
    _run(cmd)

    return out_path, out_name

def _render_base_no_loudnorm(in_path: str, chain_no_ln: str, out_path: str):
    lm = _lowmid_filter()

    glue = "anull"
    if _kicksafe_glue_enabled():
        glue = "anull"
    else:
        glue = _glue_filter()

    tr = _transient_filter()

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-af "{_PRE_CLEAN_CHAIN},{lm},{glue},{tr},{chain_no_ln}" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
    )
    _run(cmd)

def _apply_kicksafe_glue_if_needed(base_path: str, out_path: str):
    if not _kicksafe_glue_enabled():
        cmd = (
            f'ffmpeg -y -hide_banner -i {shlex.quote(base_path)} '
            f'-c:a pcm_s16le -ar 48000 -ac 2 {shlex.quote(out_path)}'
        )
        _run(cmd)
        return

    fc = _kicksafe_glue_fc()
    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(base_path)} '
        f'-filter_complex "{fc}" -map "[aout]" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
    )
    _run(cmd)

def _apply_harmonics_if_needed(in_path: str, out_path: str):
    if not _harm_enabled():
        cmd = (
            f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
            f'-c:a pcm_s16le -ar 48000 -ac 2 {shlex.quote(out_path)}'
        )
        _run(cmd)
        return

    fc = _harm_fc()
    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-filter_complex "{fc}" -map "[aout]" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
    )
    _run(cmd)

def _apply_subdensity_if_needed(in_path: str, out_path: str):
    if not _subden_enabled():
        cmd = (
            f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
            f'-c:a pcm_s16le -ar 48000 -ac 2 {shlex.quote(out_path)}'
        )
        _run(cmd)
        return

    fc = _subden_fc()
    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-filter_complex "{fc}" -map "[aout]" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
    )
    _run(cmd)

def _apply_air_bus(base_path: str, mask_expr: str, out_path: str):
    air_gain_expr = f"(({mask_expr})*{_AIR_AMOUNT:.6f})"
    fc = (
        f"[0:a]asplit=2[dry][air];"
        f"[dry]volume=1[d0];"
        f"[air]"
        f"highshelf=f={_AIR_SHELF_F}:g={_AIR_SHELF_G},"
        f"{_stereowiden_filter()},"
        f"volume='{air_gain_expr}':eval=frame[a1];"
        f"[d0][a1]amix=inputs=2:normalize=0[aout]"
    )
    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(base_path)} '
        f'-filter_complex "{fc}" -map "[aout]" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
    )
    _run(cmd)

def _render_master(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    sec = analyze_sections(in_path, target_sr=48000)
    global_a = sec["global"]
    sections = sec.get("sections") or []

    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    sp = decide_smart_params_with_sections(
        global_analysis=global_a,
        sections=sections,
        intensity=intensity,
        tone_mode=tone,
    )
    base_params = sp["base_params"]

    base_chain = build_smart_chain(base_params)
    base_no_ln, _ = _strip_loudnorm(base_chain)

    base_wav = os.path.join(td, "base.wav")
    glued_wav = os.path.join(td, "glued.wav")
    harm_wav = os.path.join(td, "harm.wav")
    subden_wav = os.path.join(td, "subden.wav")
    air_wav = os.path.join(td, "air.wav")
    bassbus_wav = os.path.join(td, "bassbus.wav")
    finalmix_wav = os.path.join(td, "finalmix.wav")

    _render_base_no_loudnorm(in_path, base_no_ln, base_wav)
    _apply_kicksafe_glue_if_needed(base_wav, glued_wav)
    _apply_harmonics_if_needed(glued_wav, harm_wav)
    _apply_subdensity_if_needed(harm_wav, subden_wav)

    mask_expr = _build_mask_expr_from_sections(sections)
    _apply_air_bus(subden_wav, mask_expr, air_wav)

    _render_bass_bus(in_path, bassbus_wav)
    _apply_bass_blend_if_needed(air_wav, bassbus_wav, finalmix_wav)

    out_args, out_name, _mime = _out_args(fmt)
    out_path = os.path.join(td, out_name)
    _build_loudnorm_two_pass(finalmix_wav, base_params["loudnorm"], out_args, out_path)

    return out_path, out_name

# --- routes ---

@app.get("/")
def root():
    return jsonify({
        "ok": True,
        "service": "analysis_mastering_api",
        "endpoints": [
            "/health",
            "/analyze",
            "/analyze_sections",
            "/compare_sections",
            "/master",
            "/bandlab",
            "/bakuage",
            "/enhance",
            "/blend",
        ]
    })

@app.get("/health")
def health():
    return jsonify({
        "ok": True,

        "ENABLE_AFFTDN": os.getenv("ENABLE_AFFTDN"),

        "LOWMID_ON": os.getenv("LOWMID_ON"),
        "LOWMID_F": os.getenv("LOWMID_F"),
        "LOWMID_W": os.getenv("LOWMID_W"),
        "LOWMID_G": os.getenv("LOWMID_G"),

        "GLUE_ON": os.getenv("GLUE_ON"),
        "GLUE_RATIO": os.getenv("GLUE_RATIO"),
        "GLUE_THRESHOLD_DB": os.getenv("GLUE_THRESHOLD_DB"),
        "GLUE_ATTACK_MS": os.getenv("GLUE_ATTACK_MS"),
        "GLUE_RELEASE_MS": os.getenv("GLUE_RELEASE_MS"),
        "GLUE_KNEE_DB": os.getenv("GLUE_KNEE_DB"),
        "GLUE_MAKEUP_DB": os.getenv("GLUE_MAKEUP_DB"),
        "GLUE_MIX": os.getenv("GLUE_MIX"),

        "KICKSAFE_GLUE_ON": os.getenv("KICKSAFE_GLUE_ON"),
        "KICKSAFE_XOVER_HZ": os.getenv("KICKSAFE_XOVER_HZ"),
        "KICKSAFE_RATIO": os.getenv("KICKSAFE_RATIO"),
        "KICKSAFE_THRESHOLD_DB": os.getenv("KICKSAFE_THRESHOLD_DB"),
        "KICKSAFE_ATTACK_MS": os.getenv("KICKSAFE_ATTACK_MS"),
        "KICKSAFE_RELEASE_MS": os.getenv("KICKSAFE_RELEASE_MS"),
        "KICKSAFE_KNEE_DB": os.getenv("KICKSAFE_KNEE_DB"),
        "KICKSAFE_MAKEUP_DB": os.getenv("KICKSAFE_MAKEUP_DB"),
        "KICKSAFE_MIX": os.getenv("KICKSAFE_MIX"),

        "TRANSIENT_ON": os.getenv("TRANSIENT_ON"),
        "TRANSIENT_RATIO": os.getenv("TRANSIENT_RATIO"),
        "TRANSIENT_THRESHOLD_DB": os.getenv("TRANSIENT_THRESHOLD_DB"),
        "TRANSIENT_ATTACK_MS": os.getenv("TRANSIENT_ATTACK_MS"),
        "TRANSIENT_RELEASE_MS": os.getenv("TRANSIENT_RELEASE_MS"),
        "TRANSIENT_KNEE_DB": os.getenv("TRANSIENT_KNEE_DB"),
        "TRANSIENT_MAKEUP_DB": os.getenv("TRANSIENT_MAKEUP_DB"),
        "TRANSIENT_MIX": os.getenv("TRANSIENT_MIX"),

        "HARM_ON": os.getenv("HARM_ON"),
        "HARM_HP_HZ": os.getenv("HARM_HP_HZ"),
        "HARM_LP_HZ": os.getenv("HARM_LP_HZ"),
        "HARM_DRIVE_DB": os.getenv("HARM_DRIVE_DB"),
        "HARM_MIX": os.getenv("HARM_MIX"),

        "SUBDEN_ON": os.getenv("SUBDEN_ON"),
        "SUBDEN_LO_HZ": os.getenv("SUBDEN_LO_HZ"),
        "SUBDEN_HI_HZ": os.getenv("SUBDEN_HI_HZ"),
        "SUBDEN_DRIVE_DB": os.getenv("SUBDEN_DRIVE_DB"),
        "SUBDEN_MIX": os.getenv("SUBDEN_MIX"),

        "BASS_BLEND_ON": os.getenv("BASS_BLEND_ON"),
        "BASS_BLEND_LO_HZ": os.getenv("BASS_BLEND_LO_HZ"),
        "BASS_BLEND_HI_HZ": os.getenv("BASS_BLEND_HI_HZ"),
        "BASS_BLEND_MIX": os.getenv("BASS_BLEND_MIX"),
        "BASS_BLEND_GAIN_DB": os.getenv("BASS_BLEND_GAIN_DB"),

        "BL_MID_ON": os.getenv("BL_MID_ON"),
        "BL_MID_F": os.getenv("BL_MID_F"),
        "BL_MID_G": os.getenv("BL_MID_G"),
        "BL_MID_W": os.getenv("BL_MID_W"),

        "BL_PRES_ON": os.getenv("BL_PRES_ON"),
        "BL_PRES_F": os.getenv("BL_PRES_F"),
        "BL_PRES_G": os.getenv("BL_PRES_G"),
        "BL_PRES_W": os.getenv("BL_PRES_W"),

        "BL_AIR_ON": os.getenv("BL_AIR_ON"),
        "BL_AIR_SHELF_F": os.getenv("BL_AIR_SHELF_F"),
        "BL_AIR_SHELF_G": os.getenv("BL_AIR_SHELF_G"),
        "BL_AIR_MIX": os.getenv("BL_AIR_MIX"),

        "BL_WIDTH_ON": os.getenv("BL_WIDTH_ON"),
        "BL_WIDTH_HP_HZ": os.getenv("BL_WIDTH_HP_HZ"),
        "BL_WIDTH_DELAY": os.getenv("BL_WIDTH_DELAY"),
        "BL_WIDTH_MIX": os.getenv("BL_WIDTH_MIX"),

        "BK_LOW_ON": os.getenv("BK_LOW_ON"),
        "BK_LOW_LO_HZ": os.getenv("BK_LOW_LO_HZ"),
        "BK_LOW_HI_HZ": os.getenv("BK_LOW_HI_HZ"),
        "BK_LOW_DRIVE_DB": os.getenv("BK_LOW_DRIVE_DB"),
        "BK_LOW_MIX": os.getenv("BK_LOW_MIX"),

        "BK_BODY_ON": os.getenv("BK_BODY_ON"),
        "BK_BODY_F": os.getenv("BK_BODY_F"),
        "BK_BODY_G": os.getenv("BK_BODY_G"),
        "BK_BODY_W": os.getenv("BK_BODY_W"),

        "BK_MID_ON": os.getenv("BK_MID_ON"),
        "BK_MID_F": os.getenv("BK_MID_F"),
        "BK_MID_G": os.getenv("BK_MID_G"),
        "BK_MID_W": os.getenv("BK_MID_W"),

        "BK_PRES_ON": os.getenv("BK_PRES_ON"),
        "BK_PRES_F": os.getenv("BK_PRES_F"),
        "BK_PRES_G": os.getenv("BK_PRES_G"),
        "BK_PRES_W": os.getenv("BK_PRES_W"),

        "BK_SOFTTOP_ON": os.getenv("BK_SOFTTOP_ON"),
        "BK_SOFTTOP_F": os.getenv("BK_SOFTTOP_F"),
        "BK_SOFTTOP_G": os.getenv("BK_SOFTTOP_G"),

        "BK_TONE_MIX": os.getenv("BK_TONE_MIX"),
        "BK_LIMITER_ON": os.getenv("BK_LIMITER_ON"),
        "BK_LIMITER_CEILING_DB": os.getenv("BK_LIMITER_CEILING_DB"),

        "ENH_AIR_ON": os.getenv("ENH_AIR_ON"),
        "ENH_AIR_SHELF_F": os.getenv("ENH_AIR_SHELF_F"),
        "ENH_AIR_SHELF_G": os.getenv("ENH_AIR_SHELF_G"),
        "ENH_AIR_MIX": os.getenv("ENH_AIR_MIX"),

        "ENH_WIDTH_ON": os.getenv("ENH_WIDTH_ON"),
        "ENH_WIDTH_HP_HZ": os.getenv("ENH_WIDTH_HP_HZ"),
        "ENH_WIDTH_DELAY": os.getenv("ENH_WIDTH_DELAY"),
        "ENH_WIDTH_MIX": os.getenv("ENH_WIDTH_MIX"),

        "ENH_GLOSS_ON": os.getenv("ENH_GLOSS_ON"),
        "ENH_GLOSS_HP_HZ": os.getenv("ENH_GLOSS_HP_HZ"),
        "ENH_GLOSS_LP_HZ": os.getenv("ENH_GLOSS_LP_HZ"),
        "ENH_GLOSS_DRIVE_DB": os.getenv("ENH_GLOSS_DRIVE_DB"),
        "ENH_GLOSS_MIX": os.getenv("ENH_GLOSS_MIX"),

        "ENH_LIMITER_ON": os.getenv("ENH_LIMITER_ON"),
        "ENH_LIMITER_CEILING_DB": os.getenv("ENH_LIMITER_CEILING_DB"),

        "BLEND_BASE_GAIN": os.getenv("BLEND_BASE_GAIN"),
        "BLEND_LOW_LO_HZ": os.getenv("BLEND_LOW_LO_HZ"),
        "BLEND_LOW_HI_HZ": os.getenv("BLEND_LOW_HI_HZ"),
        "BLEND_LOW_GAIN": os.getenv("BLEND_LOW_GAIN"),
        "BLEND_REVEAL_LO_HZ": os.getenv("BLEND_REVEAL_LO_HZ"),
        "BLEND_REVEAL_HI_HZ": os.getenv("BLEND_REVEAL_HI_HZ"),
        "BLEND_REVEAL_GAIN": os.getenv("BLEND_REVEAL_GAIN"),
        "BLEND_POLISH_GAIN": os.getenv("BLEND_POLISH_GAIN"),
        "BLEND_LIMITER_ON": os.getenv("BLEND_LIMITER_ON"),
        "BLEND_LIMITER_CEILING_DB": os.getenv("BLEND_LIMITER_CEILING_DB"),
    })

@app.get("/analyze")
def analyze():
    before = request.args.get("before")
    after = request.args.get("after")
    if not before or not after:
        return jsonify({"error": "provide ?before=<url>&after=<url>"}), 400

    if is_gdrive(before):
        before = gdrive_direct(before)
    if is_gdrive(after):
        after = gdrive_direct(after)

    try:
        with tempfile.TemporaryDirectory() as td:
            b_path, dbg_b = _dl_to_named(td, "before", before)
            a_path, dbg_a = _dl_to_named(td, "after", after)

            report, suggestion = run_analysis(b_path, a_path, os.path.join(td, "out"))
            debug = {}
            debug.update(dbg_b)
            debug.update(dbg_a)

            return jsonify({
                "report": report,
                "preset_suggestion": suggestion,
                "debug": debug
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/analyze_sections")
def analyze_sections_route():
    url = request.args.get("file")
    if not url:
        return jsonify({"error": "provide ?file=<url>"}), 400

    if is_gdrive(url):
        url = gdrive_direct(url)

    try:
        with tempfile.TemporaryDirectory() as td:
            f_path, dbg = _dl_to_named(td, "file", url)
            result = analyze_sections(f_path, target_sr=48000)
            return jsonify({"result": result, "debug": dbg})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/compare_sections")
def compare_sections_route():
    before = request.args.get("before")
    after = request.args.get("after")
    if not before or not after:
        return jsonify({"error": "provide ?before=<url>&after=<url>"}), 400

    if is_gdrive(before):
        before = gdrive_direct(before)
    if is_gdrive(after):
        after = gdrive_direct(after)

    try:
        with tempfile.TemporaryDirectory() as td:
            b_path, dbg_b = _dl_to_named(td, "before", before)
            a_path, dbg_a = _dl_to_named(td, "after", after)

            before_res = analyze_sections(b_path, target_sr=48000)
            after_res = analyze_sections(a_path, target_sr=48000)

            debug = {}
            debug.update(dbg_b)
            debug.update(dbg_a)

            return jsonify({
                "before": before_res,
                "after": after_res,
                "debug": debug
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/master")
def master_route():
    url = request.args.get("file")
    if not url:
        return jsonify({"error": "provide ?file=<url>"}), 400

    tone = _normalize_tone(request.args.get("tone") or "balanced")
    intensity = _normalize_intensity(request.args.get("intensity") or "balanced")
    fmt = _normalize_format(request.args.get("format") or "wav16")

    if is_gdrive(url):
        url = gdrive_direct(url)

    try:
        with tempfile.TemporaryDirectory() as td:
            in_path, _dbg = _dl_to_named(td, "file", url)
            out_path, out_name = _render_master(in_path, tone=tone, intensity=intensity, fmt=fmt, td=td)
            _out_args_str, _out_name2, mime = _out_args(fmt)

            return send_file(
                out_path,
                mimetype=mime,
                as_attachment=True,
                download_name=out_name
            )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/bandlab")
def bandlab_route():
    url = request.args.get("file")
    if not url:
        return jsonify({"error": "provide ?file=<url>"}), 400

    tone = _normalize_tone(request.args.get("tone") or "balanced")
    intensity = _normalize_intensity(request.args.get("intensity") or "balanced")
    fmt = _normalize_format(request.args.get("format") or "wav16")

    if is_gdrive(url):
        url = gdrive_direct(url)

    try:
        with tempfile.TemporaryDirectory() as td:
            in_path, _dbg = _dl_to_named(td, "file", url)
            out_path, out_name = _render_bandlab_like(in_path, tone=tone, intensity=intensity, fmt=fmt, td=td)
            _out_args_str, _out_name2, mime = _out_args(fmt)

            return send_file(
                out_path,
                mimetype=mime,
                as_attachment=True,
                download_name=out_name
            )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/bakuage")
def bakuage_route():
    url = request.args.get("file")
    if not url:
        return jsonify({"error": "provide ?file=<url>"}), 400

    tone = _normalize_tone(request.args.get("tone") or "balanced")
    intensity = _normalize_intensity(request.args.get("intensity") or "balanced")
    fmt = _normalize_format(request.args.get("format") or "wav16")

    if is_gdrive(url):
        url = gdrive_direct(url)

    try:
        with tempfile.TemporaryDirectory() as td:
            in_path, _dbg = _dl_to_named(td, "file", url)
            out_path, out_name = _render_bakuage_like(in_path, tone=tone, intensity=intensity, fmt=fmt, td=td)
            _out_args_str, _out_name2, mime = _out_args(fmt)

            return send_file(
                out_path,
                mimetype=mime,
                as_attachment=True,
                download_name=out_name
            )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/enhance")
def enhance_route():
    url = request.args.get("file")
    if not url:
        return jsonify({"error": "provide ?file=<url>"}), 400

    fmt = _normalize_format(request.args.get("format") or "wav16")

    if is_gdrive(url):
        url = gdrive_direct(url)

    try:
        with tempfile.TemporaryDirectory() as td:
            in_path, _dbg = _dl_to_named(td, "file", url)
            out_path, out_name = _render_enhance(in_path, fmt=fmt, td=td)
            _out_args_str, _out_name2, mime = _out_args(fmt)

            return send_file(
                out_path,
                mimetype=mime,
                as_attachment=True,
                download_name=out_name
            )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/blend")
def blend_route():
    url = request.args.get("file")
    if not url:
        return jsonify({"error": "provide ?file=<url>"}), 400

    tone = _normalize_tone(request.args.get("tone") or "balanced")
    intensity = _normalize_intensity(request.args.get("intensity") or "balanced")
    fmt = _normalize_format(request.args.get("format") or "wav16")

    if is_gdrive(url):
        url = gdrive_direct(url)

    try:
        with tempfile.TemporaryDirectory() as td:
            in_path, _dbg = _dl_to_named(td, "file", url)
            out_path, out_name = _render_blend(in_path, tone=tone, intensity=intensity, fmt=fmt, td=td)
            _out_args_str, _out_name2, mime = _out_args(fmt)

            return send_file(
                out_path,
                mimetype=mime,
                as_attachment=True,
                download_name=out_name
            )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

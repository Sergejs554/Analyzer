#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify, send_file
import os, tempfile, requests, re, subprocess, shlex, json

from analyze_mastering import run_analysis
from auto_analysis import analyze_sections  # секционный анализ
from smart_auto import (
    decide_smart_params,           # base params
    apply_section_influence,        # === изменено === (нужно для soft/hard рендера)
    build_smart_chain
)

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
# MR MASTERING v2 — CORE DSP
# ---------------------------

# Fixed Pre-Clean (НЕ зависит от tone/intensity, НЕ зависит от section mapping)
# Максимально безопасно: убираем инфраниз, лёгкий шумодав, без лимитера.
_PRE_CLEAN_CHAIN = "highpass=f=25:width=0.7,afftdn=nf=-25"

def _strip_loudnorm(chain: str) -> tuple[str, str]:
    """
    Split '... , loudnorm=...' into (pre_chain, loudnorm_part).
    If loudnorm not found: returns (chain, "")
    """
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
    """
    Two-pass loudnorm (FFmpeg) for final stage.
    Мы применяем loudnorm только ОДИН раз в конце (глобально), чтобы не было скачков.
    """
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
    """
    Returns (ffmpeg_out_args, filename, mimetype)
    """
    fmt = (fmt or "wav16").lower()
    if fmt == "wav24":
        return "-ar 48000 -ac 2 -c:a pcm_s24le", "mastered_uhd.wav", "audio/wav"
    if fmt == "flac":
        return "-ar 48000 -ac 2 -c:a flac", "mastered.flac", "audio/flac"
    if fmt in ("mp3_320", "mp3"):
        return "-ar 48000 -ac 2 -c:a libmp3lame -b:a 320k", "mastered_320.mp3", "audio/mpeg"
    if fmt == "aiff":
        # AIFF: PCM big-endian
        return "-ar 48000 -ac 2 -f aiff -c:a pcm_s16be", "mastered.aiff", "audio/aiff"
    # default wav16
    return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav", "audio/wav"

# === изменено ===
# Section-aware БЕЗ НАРЕЗКИ:
# 1) рендерим SOFT и HARD версии целиком (одна архитектура)
# 2) строим плавную маску mask(t) из секций (0..1)
# 3) миксуем: out = soft*(1-mask) + hard*mask
# 4) loudnorm 2-pass один раз в конце

_RAMP_MIN = 0.08
_RAMP_MAX = 0.80

def _clamp(x, lo, hi):
    return float(max(lo, min(hi, x)))

def _pick_ramp(prev_len: float, next_len: float) -> float:
    # рампа вокруг границы секций, чтобы не было слышимых точек
    r = min(_RAMP_MAX, 0.25 * prev_len, 0.25 * next_len)
    return _clamp(r, _RAMP_MIN, _RAMP_MAX)

def _build_mask_expr_from_sections(sections: list[dict]) -> tuple[str, float]:
    """
    Строим piecewise-linear mask(t) по секциям.
    mask в [0..1], плавные переходы на границах.

    Возвращает (expr, total_duration_est).
    """
    if not sections:
        return "0.5", 0.0

    secs = sorted(sections, key=lambda s: float(s.get("start", 0.0)))
    starts = [float(s.get("start", 0.0)) for s in secs]
    ends = [float(s.get("end", 0.0)) for s in secs]
    dur_est = float(max(ends) if ends else 0.0)

    # weight берем из "level" (0..1), это уже нормализовано в auto_analysis
    w = [ _clamp(float(s.get("level", 0.5)), 0.0, 1.0) for s in secs ]

    # Собираем выражение в обратную сторону (чтобы не городить суммирование/оверлап)
    # На каждой границе b делаем линейный переход w[i] -> w[i+1] на интервале [b-r, b+r]
    expr = f"{w[-1]:.6f}"
    for i in range(len(w) - 2, -1, -1):
        # граница между i и i+1
        b = max(starts[i+1], ends[i])  # безопасно
        prev_len = max(0.01, ends[i] - starts[i])
        next_len = max(0.01, ends[i+1] - starts[i+1])
        r = _pick_ramp(prev_len, next_len)

        left = b - r
        right = b + r
        wi = w[i]
        wj = w[i+1]

        # if(t < left) wi
        # else if(t < right) wi + (wj-wi)*(t-left)/(2r)
        # else -> expr (который уже описывает дальше)
        expr = (
            f"if(lt(t,{left:.6f}),{wi:.6f},"
            f"if(lt(t,{right:.6f}),"
            f"({wi:.6f}+({wj:.6f}-{wi:.6f})*(t-{left:.6f})/{(2*r):.6f}),"
            f"({expr})"
            f"))"
        )

    return expr, dur_est

def _render_variant(in_path: str, chain_no_ln: str, out_path: str):
    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-af "{_PRE_CLEAN_CHAIN},{chain_no_ln}" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
    )
    _run(cmd)

def _mix_soft_hard(soft_path: str, hard_path: str, mask_expr: str, out_path: str):
    # base = 1-mask
    base_expr = f"(1-({mask_expr}))"
    fc = (
        f"[0:a]volume='{base_expr}':eval=frame[a0];"
        f"[1:a]volume='{mask_expr}':eval=frame[a1];"
        f"[a0][a1]amix=inputs=2:normalize=0[aout]"
    )
    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(soft_path)} -i {shlex.quote(hard_path)} '
        f'-filter_complex "{fc}" -map "[aout]" -ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
    )
    _run(cmd)

def _normalize_intensity_alias(x: str) -> str:
    x = (x or "balanced").lower().strip()
    # поддерживаем старые и новые имена
    if x in ("soft", "low"): return "low"
    if x in ("normal", "balanced", "mid", "medium"): return "balanced"
    if x in ("hard", "high"): return "high"
    return "balanced"

def _normalize_tone(x: str) -> str:
    x = (x or "balanced").lower().strip()
    if x in ("warm", "balanced", "bright"):
        return x
    return "balanced"

def _normalize_format(x: str) -> str:
    x = (x or "wav16").lower().strip()
    if x in ("wav", "wav16"): return "wav16"
    if x in ("wav24",): return "wav24"
    if x in ("flac",): return "flac"
    if x in ("mp3", "mp3_320"): return "mp3_320"
    if x in ("aiff", "aif"): return "aiff"
    return "wav16"

def _render_master(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    """
    Pipeline (без склеек):
      1) analyze_sections -> берем sections(level)
      2) decide_smart_params(global, tone/intensity) -> base_params
      3) делаем soft_params = base + (-0.10), hard_params = base + (+0.10)
      4) рендерим soft.wav и hard.wav целиком (preclean + chain_no_loudnorm)
      5) строим mask(t) и морфим soft/hard
      6) loudnorm 2-pass один раз в конце (base loudnorm targets)
    """
    sec = analyze_sections(in_path, target_sr=48000)
    global_a = sec["global"]
    sections = sec.get("sections") or []

    tone = _normalize_tone(tone)
    intensity = _normalize_intensity_alias(intensity)
    fmt = _normalize_format(fmt)

    # base params (tone/intensity — пользовательские bias поверх базы; pre-clean/section mapping не трогаем)
    base_params = decide_smart_params(global_a, intensity=intensity, tone_mode=tone)

    # делаем две “крайние” версии для морфа
    soft_params = apply_section_influence(base_params, -0.10)
    hard_params = apply_section_influence(base_params, +0.10)

    # цепи без loudnorm (loudnorm только в конце)
    soft_chain = build_smart_chain(soft_params)
    hard_chain = build_smart_chain(hard_params)
    soft_no_ln, _ = _strip_loudnorm(soft_chain)
    hard_no_ln, _ = _strip_loudnorm(hard_chain)

    soft_wav = os.path.join(td, "soft.wav")
    hard_wav = os.path.join(td, "hard.wav")
    mixed_wav = os.path.join(td, "mixed.wav")

    _render_variant(in_path, soft_no_ln, soft_wav)
    _render_variant(in_path, hard_no_ln, hard_wav)

    mask_expr, _dur = _build

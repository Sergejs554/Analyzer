#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask, request, jsonify, send_file
import os
import tempfile
import requests
import re
import subprocess
import shlex
import json
import time

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
    if ".wav" in u:
        return ".wav"
    if ".mp3" in u:
        return ".mp3"
    if ".m4a" in u:
        return ".m4a"
    if ".flac" in u:
        return ".flac"
    if ".aiff" in u or ".aif" in u:
        return ".aiff"
    if content_type:
        ct = content_type.lower()
        if "audio/wav" in ct or "audio/x-wav" in ct:
            return ".wav"
        if "audio/mpeg" in ct:
            return ".mp3"
        if "audio/mp4" in ct or "audio/x-m4a" in ct:
            return ".m4a"
        if "audio/flac" in ct:
            return ".flac"
        if "audio/aiff" in ct or "audio/x-aiff" in ct:
            return ".aiff"
    return ".wav"


def _requests_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
    })
    return sess


def download_file(url: str, out_path: str, timeout: int = 180) -> tuple[int, str, str]:
    """
    Stream download into temp file.
    Safer for Railway than loading full file into RAM.
    """
    last_err = None

    for attempt in range(3):
        sess = _requests_session()
        try:
            r = sess.get(url, timeout=(20, timeout), allow_redirects=True, stream=True)
            r.raise_for_status()

            ct = (r.headers.get("Content-Type") or "").lower()
            final_url = (r.url or url)

            if "text/html" in ct and "drive.google.com" in final_url:
                confirm = None
                for k, v in r.cookies.items():
                    if k.startswith("download_warning"):
                        confirm = v
                        break
                if confirm:
                    fid = gdrive_file_id(url) or gdrive_file_id(final_url)
                    if fid:
                        r.close()
                        url2 = f"https://drive.google.com/uc?export=download&id={fid}&confirm={confirm}"
                        r = sess.get(url2, timeout=(20, timeout), allow_redirects=True, stream=True)
                        r.raise_for_status()
                        ct = (r.headers.get("Content-Type") or "").lower()
                        final_url = (r.url or url2)

            if "text/html" in ct:
                raise RuntimeError(f"Downloaded HTML instead of audio. final_url={final_url}")

            total = 0
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    total += len(chunk)
            r.close()

            if total <= 0:
                raise RuntimeError(f"Downloaded empty file. final_url={final_url}")

            return total, final_url, (r.headers.get("Content-Type") or "")

        except Exception as e:
            last_err = e
            try:
                if os.path.exists(out_path):
                    os.remove(out_path)
            except Exception:
                pass
            time.sleep(min(1 + attempt, 3))

    raise RuntimeError(f"download_file failed: {last_err}")


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


def _clamp(x, lo, hi):
    return float(max(lo, min(hi, x)))


def _db_to_lin(db: float) -> float:
    return 10.0 ** (float(db) / 20.0)


def _os_softclip_chain(
    drive_db: float,
    hp: float | None = None,
    lp: float | None = None,
    post_gain_db: float = 0.0,
) -> str:
    parts = []
    if hp is not None:
        parts.append(f"highpass=f={_clamp(hp, 20.0, 18000.0)}:width=0.707")
    if lp is not None:
        parts.append(f"lowpass=f={_clamp(lp, 40.0, 20000.0)}:width=0.707")
    parts.extend([
        f"volume={_clamp(drive_db, 0.0, 24.0)}dB",
        "aresample=192000",
        "asoftclip",
        "aresample=48000",
    ])
    if abs(post_gain_db) > 1e-9:
        parts.append(f"volume={post_gain_db}dB")
    return ",".join(parts)


# ---------------------------
# GLOBAL / BASE
# ---------------------------

_ENABLE_AFFTDN = (os.getenv("ENABLE_AFFTDN", "0").strip() == "1")
_PRE_CLEAN_CHAIN = "highpass=f=25:width=0.7" + (",afftdn=nf=-25" if _ENABLE_AFFTDN else "")

_BASE_LOWMID_ON = (os.getenv("BASE_LOWMID_ON", "1").strip() == "1")
_BASE_LOWMID_F = float(os.getenv("BASE_LOWMID_F", "220"))
_BASE_LOWMID_W = float(os.getenv("BASE_LOWMID_W", "0.9"))
_BASE_LOWMID_G = float(os.getenv("BASE_LOWMID_G", "-0.6"))

_GLUE_ON = (os.getenv("GLUE_ON", "0").strip() == "1")
_GLUE_RATIO = float(os.getenv("GLUE_RATIO", "1.4"))
_GLUE_THRESHOLD_DB = float(os.getenv("GLUE_THRESHOLD_DB", "-18"))
_GLUE_ATTACK_MS = float(os.getenv("GLUE_ATTACK_MS", "15"))
_GLUE_RELEASE_MS = float(os.getenv("GLUE_RELEASE_MS", "140"))
_GLUE_KNEE_DB = float(os.getenv("GLUE_KNEE_DB", "2"))
_GLUE_MAKEUP_DB = float(os.getenv("GLUE_MAKEUP_DB", "0"))
_GLUE_MIX = float(os.getenv("GLUE_MIX", "0.7"))

_TRANSIENT_ON = (os.getenv("TRANSIENT_ON", "0").strip() == "1")
_TRANSIENT_RATIO = float(os.getenv("TRANSIENT_RATIO", "1.4"))
_TRANSIENT_THRESHOLD_DB = float(os.getenv("TRANSIENT_THRESHOLD_DB", "-20"))
_TRANSIENT_ATTACK_MS = float(os.getenv("TRANSIENT_ATTACK_MS", "25"))
_TRANSIENT_RELEASE_MS = float(os.getenv("TRANSIENT_RELEASE_MS", "100"))
_TRANSIENT_KNEE_DB = float(os.getenv("TRANSIENT_KNEE_DB", "2"))
_TRANSIENT_MAKEUP_DB = float(os.getenv("TRANSIENT_MAKEUP_DB", "0"))
_TRANSIENT_MIX = float(os.getenv("TRANSIENT_MIX", "0.20"))


def _base_lowmid_filter() -> str:
    if not _BASE_LOWMID_ON:
        return "anull"
    f = _clamp(_BASE_LOWMID_F, 20.0, 800.0)
    w = _clamp(_BASE_LOWMID_W, 0.2, 3.0)
    g = _clamp(_BASE_LOWMID_G, -3.0, 3.0)
    return f"equalizer=f={f}:t=q:w={w}:g={g}"


def _glue_filter() -> str:
    if not _GLUE_ON:
        return "anull"
    ratio = _clamp(_GLUE_RATIO, 1.0, 10.0)
    thr = _clamp(_GLUE_THRESHOLD_DB, -60.0, 0.0)
    att = _clamp(_GLUE_ATTACK_MS, 0.1, 200.0)
    rel = _clamp(_GLUE_RELEASE_MS, 5.0, 2000.0)
    knee = _clamp(_GLUE_KNEE_DB, 0.0, 12.0)
    makeup = _clamp(_GLUE_MAKEUP_DB, -6.0, 12.0)
    mix = _clamp(_GLUE_MIX, 0.0, 1.0)
    return (
        f"acompressor=threshold={thr}dB:ratio={ratio}:attack={att}:release={rel}:"
        f"knee={knee}dB:makeup={makeup}dB:mix={mix}"
    )


def _transient_filter() -> str:
    if not _TRANSIENT_ON:
        return "anull"
    ratio = _clamp(_TRANSIENT_RATIO, 1.0, 10.0)
    thr = _clamp(_TRANSIENT_THRESHOLD_DB, -60.0, 0.0)
    att = _clamp(_TRANSIENT_ATTACK_MS, 0.1, 200.0)
    rel = _clamp(_TRANSIENT_RELEASE_MS, 5.0, 2000.0)
    knee = _clamp(_TRANSIENT_KNEE_DB, 0.0, 12.0)
    makeup = _clamp(_TRANSIENT_MAKEUP_DB, -6.0, 12.0)
    mix = _clamp(_TRANSIENT_MIX, 0.0, 1.0)
    return (
        f"acompressor=threshold={thr}dB:ratio={ratio}:attack={att}:release={rel}:"
        f"knee={knee}dB:makeup={makeup}dB:mix={mix}"
    )


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
                    chunk = text[start:i + 1]
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
    if x in ("low", "soft"):
        return "low"
    if x in ("high", "hard"):
        return "high"
    if x in ("normal", "balanced", "mid", "medium"):
        return "balanced"
    return "balanced"


def _normalize_format(x: str) -> str:
    x = (x or "wav16").lower().strip()
    if x in ("wav", "wav16"):
        return "wav16"
    if x in ("wav24",):
        return "wav24"
    if x in ("flac",):
        return "flac"
    if x in ("mp3", "mp3_320"):
        return "mp3_320"
    if x in ("aiff", "aif"):
        return "aiff"
    return "wav16"


def _render_base_no_loudnorm(in_path: str, chain_no_ln: str, out_path: str):
    lm = _base_lowmid_filter()
    glue = _glue_filter()
    tr = _transient_filter()

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-af "{_PRE_CLEAN_CHAIN},{lm},{glue},{tr},{chain_no_ln}" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
    )
    _run(cmd)


# ---------------------------
# LOW SUPPORT BRANCH
# donor only, pre-limiter
# ---------------------------

_LS_FOUND_ON = (os.getenv("LS_FOUND_ON", "1").strip() == "1")
_LS_FOUND_LO_HZ = float(os.getenv("LS_FOUND_LO_HZ", "32"))
_LS_FOUND_HI_HZ = float(os.getenv("LS_FOUND_HI_HZ", "120"))
_LS_FOUND_RATIO = float(os.getenv("LS_FOUND_RATIO", "1.5"))
_LS_FOUND_THRESHOLD_DB = float(os.getenv("LS_FOUND_THRESHOLD_DB", "-22"))
_LS_FOUND_ATTACK_MS = float(os.getenv("LS_FOUND_ATTACK_MS", "28"))
_LS_FOUND_RELEASE_MS = float(os.getenv("LS_FOUND_RELEASE_MS", "180"))
_LS_FOUND_MIX = float(os.getenv("LS_FOUND_MIX", "0.18"))

_LS_HARM_ON = (os.getenv("LS_HARM_ON", "1").strip() == "1")
_LS_HARM_HP_HZ = float(os.getenv("LS_HARM_HP_HZ", "48"))
_LS_HARM_LP_HZ = float(os.getenv("LS_HARM_LP_HZ", "165"))
_LS_HARM_DRIVE_DB = float(os.getenv("LS_HARM_DRIVE_DB", "6"))
_LS_HARM_MIX = float(os.getenv("LS_HARM_MIX", "0.08"))

_LS_BODY_ON = (os.getenv("LS_BODY_ON", "1").strip() == "1")
_LS_BODY_F = float(os.getenv("LS_BODY_F", "220"))
_LS_BODY_G = float(os.getenv("LS_BODY_G", "0.8"))
_LS_BODY_W = float(os.getenv("LS_BODY_W", "0.9"))
_LS_BODY_MIX = float(os.getenv("LS_BODY_MIX", "0.08"))

_LS_GUARD_ON = (os.getenv("LS_GUARD_ON", "1").strip() == "1")
_LS_GUARD_F = float(os.getenv("LS_GUARD_F", "280"))
_LS_GUARD_G = float(os.getenv("LS_GUARD_G", "-1.2"))
_LS_GUARD_W = float(os.getenv("LS_GUARD_W", "1.1"))

_LS_MONO_ON = (os.getenv("LS_MONO_ON", "1").strip() == "1")
_LS_OUT_TRIM_DB = float(os.getenv("LS_OUT_TRIM_DB", "-1.0"))


def _render_low_support_branch(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    intensity_scale = {
        "low": 0.88,
        "balanced": 1.00,
        "high": 1.12,
    }[intensity]

    tone_body_mul = {
        "warm": 1.10,
        "balanced": 1.00,
        "bright": 0.92,
    }[tone]

    found_lo = _clamp(_LS_FOUND_LO_HZ, 20.0, 70.0)
    found_hi = _clamp(_LS_FOUND_HI_HZ, 70.0, 220.0)
    if found_hi <= found_lo + 10:
        found_hi = found_lo + 10

    found_ratio = _clamp(_LS_FOUND_RATIO, 1.0, 4.0)
    found_thr = _clamp(_LS_FOUND_THRESHOLD_DB, -60.0, 0.0)
    found_att = _clamp(_LS_FOUND_ATTACK_MS, 1.0, 200.0)
    found_rel = _clamp(_LS_FOUND_RELEASE_MS, 20.0, 1200.0)
    found_mix = _clamp(_LS_FOUND_MIX * intensity_scale, 0.0, 0.40)

    harm_hp = _clamp(_LS_HARM_HP_HZ, 35.0, 100.0)
    harm_lp = _clamp(_LS_HARM_LP_HZ, 90.0, 260.0)
    if harm_lp <= harm_hp + 10:
        harm_lp = harm_hp + 10
    harm_drive = _clamp(_LS_HARM_DRIVE_DB, 0.0, 18.0)
    harm_mix = _clamp(_LS_HARM_MIX * intensity_scale, 0.0, 0.25)

    body_f = _clamp(_LS_BODY_F, 120.0, 380.0)
    body_g = _clamp(_LS_BODY_G * tone_body_mul, -1.0, 3.0)
    body_w = _clamp(_LS_BODY_W, 0.2, 3.0)
    body_mix = _clamp(_LS_BODY_MIX * intensity_scale, 0.0, 0.20)

    guard_f = _clamp(_LS_GUARD_F, 180.0, 450.0)
    guard_g = _clamp(_LS_GUARD_G, -6.0, 0.0)
    guard_w = _clamp(_LS_GUARD_W, 0.2, 3.0)

    out_trim_db = _clamp(_LS_OUT_TRIM_DB, -18.0, 6.0)

    parts = ["[0:a]asplit=3[fnd][harm][body]"]

    if _LS_FOUND_ON and found_mix > 0.0:
        parts.append(
            f"[fnd]"
            f"highpass=f={found_lo}:width=0.707,"
            f"lowpass=f={found_hi}:width=0.707,"
            f"acompressor=threshold={found_thr}dB:ratio={found_ratio}:attack={found_att}:release={found_rel}:knee=2dB:makeup=0dB:mix=1,"
            f"volume={found_mix}[f1]"
        )
    else:
        parts.append("[fnd]volume=0[f1]")

    if _LS_HARM_ON and harm_mix > 0.0:
        parts.append(
            f"[harm]"
            f"{_os_softclip_chain(drive_db=harm_drive, hp=harm_hp, lp=harm_lp)},"
            f"lowpass=f={max(harm_lp, 120.0)}:width=0.707,"
            f"volume={harm_mix}[h1]"
        )
    else:
        parts.append("[harm]volume=0[h1]")

    body_chain = [
        f"highpass=f={max(found_hi - 20.0, 80.0)}:width=0.707",
        f"lowpass=f={max(body_f + 130.0, 240.0)}:width=0.707",
        f"equalizer=f={body_f}:t=q:w={body_w}:g={body_g}",
    ]
    if _LS_GUARD_ON:
        body_chain.append(f"equalizer=f={guard_f}:t=q:w={guard_w}:g={guard_g}")
    body_chain.append(f"volume={body_mix}")
    if _LS_MONO_ON:
        body_chain.append("pan=stereo|c0=.5*c0+.5*c1|c1=.5*c0+.5*c1")

    if _LS_BODY_ON and body_mix > 0.0:
        parts.append(f"[body]{','.join(body_chain)}[b1]")
    else:
        parts.append("[body]volume=0[b1]")

    parts.append("[f1][h1][b1]amix=inputs=3:normalize=0[m0]")
    if abs(out_trim_db) > 1e-9:
        parts.append(f"[m0]volume={out_trim_db}dB[out]")
    else:
        parts.append("[m0]anull[out]")

    fc = ";".join(parts)

    out_args, out_name, _mime = _out_args(fmt)
    out_name = f"low_support_{out_name}"
    out_path = os.path.join(td, out_name)

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-filter_complex "{fc}" -map "[out]" '
        f'{out_args} {shlex.quote(out_path)}'
    )
    _run(cmd)
    return out_path, out_name


# ---------------------------
# REVEAL / PRESENCE / MID-AIR BRANCH
# donor only, pre-limiter
# ---------------------------

_RV_CORE_ON = (os.getenv("RV_CORE_ON", "1").strip() == "1")
_RV_LO_HZ = float(os.getenv("RV_LO_HZ", "550"))
_RV_HI_HZ = float(os.getenv("RV_HI_HZ", "7800"))

# === changed ===
_RV_MID_F = float(os.getenv("RV_MID_F", "1150"))
_RV_MID_G = float(os.getenv("RV_MID_G", "1.00"))
_RV_MID_W = float(os.getenv("RV_MID_W", "0.95"))

_RV_PRES_F = float(os.getenv("RV_PRES_F", "2100"))
_RV_PRES_G = float(os.getenv("RV_PRES_G", "0.48"))
_RV_PRES_W = float(os.getenv("RV_PRES_W", "0.95"))

_RV_CORE_MIX = float(os.getenv("RV_CORE_MIX", "0.135"))

_RV_EXCITE_ON = (os.getenv("RV_EXCITE_ON", "1").strip() == "1")
_RV_EXCITE_HP_HZ = float(os.getenv("RV_EXCITE_HP_HZ", "2200"))
_RV_EXCITE_LP_HZ = float(os.getenv("RV_EXCITE_LP_HZ", "8200"))
_RV_EXCITE_DRIVE_DB = float(os.getenv("RV_EXCITE_DRIVE_DB", "2.8"))
_RV_EXCITE_MIX = float(os.getenv("RV_EXCITE_MIX", "0.040"))

_RV_AIR_ON = (os.getenv("RV_AIR_ON", "1").strip() == "1")
_RV_AIR_F = float(os.getenv("RV_AIR_F", "9000"))
_RV_AIR_G = float(os.getenv("RV_AIR_G", "1.35"))
_RV_AIR_MIX = float(os.getenv("RV_AIR_MIX", "0.085"))

_RV_WIDTH_ON = (os.getenv("RV_WIDTH_ON", "1").strip() == "1")
_RV_WIDTH_HP_HZ = float(os.getenv("RV_WIDTH_HP_HZ", "5200"))
_RV_WIDTH_M = float(os.getenv("RV_WIDTH_M", "1.12"))
_RV_WIDTH_MIX = float(os.getenv("RV_WIDTH_MIX", "0.080"))

_RV_GUARD_ON = (os.getenv("RV_GUARD_ON", "1").strip() == "1")
_RV_GUARD_F = float(os.getenv("RV_GUARD_F", "3400"))
_RV_GUARD_G = float(os.getenv("RV_GUARD_G", "-0.75"))
_RV_GUARD_W = float(os.getenv("RV_GUARD_W", "1.3"))

_RV_SIB_F = float(os.getenv("RV_SIB_F", "7200"))
_RV_SIB_G = float(os.getenv("RV_SIB_G", "-0.65"))
_RV_SIB_W = float(os.getenv("RV_SIB_W", "1.5"))

_RV_OUT_TRIM_DB = float(os.getenv("RV_OUT_TRIM_DB", "-1.5"))


def _render_reveal_branch(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    intensity_scale = {
        "low": 0.86,
        "balanced": 1.00,
        "high": 1.10,
    }[intensity]

    tone_air_mul = {
        "warm": 0.88,
        "balanced": 1.00,
        "bright": 1.12,
    }[tone]

    tone_pres_mul = {
        "warm": 0.90,
        "balanced": 1.00,
        "bright": 1.08,
    }[tone]

    tone_mid_mul = {
        "warm": 1.04,
        "balanced": 1.00,
        "bright": 0.96,
    }[tone]

    lo_hz = _clamp(_RV_LO_HZ, 250.0, 2000.0)
    hi_hz = _clamp(_RV_HI_HZ, 3500.0, 14000.0)
    if hi_hz <= lo_hz + 300:
        hi_hz = lo_hz + 300

    mid_f = _clamp(_RV_MID_F, 700.0, 2200.0)
    mid_g = _clamp(_RV_MID_G * tone_mid_mul, -1.0, 2.5)
    mid_w = _clamp(_RV_MID_W, 0.2, 3.0)

    pres_f = _clamp(_RV_PRES_F, 1400.0, 4200.0)
    pres_g = _clamp(_RV_PRES_G * tone_pres_mul, -1.0, 2.0)
    pres_w = _clamp(_RV_PRES_W, 0.2, 3.0)

    core_mix = _clamp(_RV_CORE_MIX * intensity_scale, 0.0, 0.20)

    excite_hp = _clamp(_RV_EXCITE_HP_HZ, 1400.0, 6000.0)
    excite_lp = _clamp(_RV_EXCITE_LP_HZ, 5000.0, 14000.0)
    if excite_lp <= excite_hp + 1200:
        excite_lp = excite_hp + 1200
    excite_drive = _clamp(_RV_EXCITE_DRIVE_DB, 0.0, 8.0)
    excite_mix = _clamp(_RV_EXCITE_MIX * intensity_scale, 0.0, 0.10)

    air_f = _clamp(_RV_AIR_F, 6500.0, 16000.0)
    air_g = _clamp(_RV_AIR_G * tone_air_mul, 0.0, 3.0)
    air_mix = _clamp(_RV_AIR_MIX * intensity_scale, 0.0, 0.16)

    width_hp = _clamp(_RV_WIDTH_HP_HZ, 3500.0, 12000.0)
    width_m = _clamp(_RV_WIDTH_M, 1.0, 1.5)
    width_mix = _clamp(_RV_WIDTH_MIX * intensity_scale, 0.0, 0.12)

    guard_f = _clamp(_RV_GUARD_F, 2400.0, 6000.0)
    guard_g = _clamp(_RV_GUARD_G, -4.0, 0.0)
    guard_w = _clamp(_RV_GUARD_W, 0.2, 4.0)

    sib_f = _clamp(_RV_SIB_F, 5500.0, 12000.0)
    sib_g = _clamp(_RV_SIB_G, -4.0, 0.0)
    sib_w = _clamp(_RV_SIB_W, 0.2, 4.0)

    out_trim_db = _clamp(_RV_OUT_TRIM_DB, -18.0, 6.0)

    parts = ["[0:a]asplit=4[core][exc][air][wid]"]

    core_chain = [
        f"highpass=f={lo_hz}:width=0.707",
        f"lowpass=f={hi_hz}:width=0.707",
        f"equalizer=f={mid_f}:t=q:w={mid_w}:g={mid_g}",
        f"equalizer=f={pres_f}:t=q:w={pres_w}:g={pres_g}",
    ]
    if _RV_GUARD_ON:
        core_chain.append(f"equalizer=f={guard_f}:t=q:w={guard_w}:g={guard_g}")
        core_chain.append(f"equalizer=f={sib_f}:t=q:w={sib_w}:g={sib_g}")
    core_chain.append(f"volume={core_mix}")

    if _RV_CORE_ON and core_mix > 0.0:
        parts.append(f"[core]{','.join(core_chain)}[c1]")
    else:
        parts.append("[core]volume=0[c1]")

    if _RV_EXCITE_ON and excite_mix > 0.0:
        exc_chain = _os_softclip_chain(
            drive_db=excite_drive,
            hp=excite_hp,
            lp=excite_lp,
            post_gain_db=0.0,
        )
        if _RV_GUARD_ON:
            exc_chain = (
                exc_chain
                + f",equalizer=f={guard_f}:t=q:w={guard_w}:g={guard_g}"
                + f",equalizer=f={sib_f}:t=q:w={sib_w}:g={sib_g}"
            )
        exc_chain = exc_chain + f",volume={excite_mix}"
        parts.append(f"[exc]{exc_chain}[e1]")
    else:
        parts.append("[exc]volume=0[e1]")

    if _RV_AIR_ON and air_mix > 0.0:
        parts.append(
            f"[air]"
            f"highpass=f={max(air_f * 0.58, 5200.0)}:width=0.707,"
            f"highshelf=f={air_f}:g={air_g},"
            f"volume={air_mix}[a1]"
        )
    else:
        parts.append("[air]volume=0[a1]")

    if _RV_WIDTH_ON and width_mix > 0.0:
        parts.append(
            f"[wid]"
            f"highpass=f={width_hp}:width=0.707,"
            f"extrastereo=m={width_m},"
            f"highpass=f={width_hp}:width=0.707,"
            f"volume={width_mix}[w1]"
        )
    else:
        parts.append("[wid]volume=0[w1]")

    parts.append("[c1][e1][a1][w1]amix=inputs=4:normalize=0[m0]")
    if abs(out_trim_db) > 1e-9:
        parts.append(f"[m0]volume={out_trim_db}dB[out]")
    else:
        parts.append("[m0]anull[out]")

    fc = ";".join(parts)

    out_args, out_name, _mime = _out_args(fmt)
    out_name = f"reveal_{out_name}"
    out_path = os.path.join(td, out_name)

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-filter_complex "{fc}" -map "[out]" '
        f'{out_args} {shlex.quote(out_path)}'
    )
    _run(cmd)
    return out_path, out_name


# ---------------------------
# POLISH / ENHANCE BRANCH
# donor only, pre-limiter
# ---------------------------

_PL_GLUE_ON = (os.getenv("PL_GLUE_ON", "1").strip() == "1")
_PL_GLUE_RATIO = float(os.getenv("PL_GLUE_RATIO", "1.22"))
_PL_GLUE_THRESHOLD_DB = float(os.getenv("PL_GLUE_THRESHOLD_DB", "-19"))
_PL_GLUE_ATTACK_MS = float(os.getenv("PL_GLUE_ATTACK_MS", "12"))
_PL_GLUE_RELEASE_MS = float(os.getenv("PL_GLUE_RELEASE_MS", "140"))
_PL_GLUE_MIX = float(os.getenv("PL_GLUE_MIX", "0.10"))

_PL_AIR_ON = (os.getenv("PL_AIR_ON", "1").strip() == "1")
_PL_AIR_F = float(os.getenv("PL_AIR_F", "9500"))
_PL_AIR_G = float(os.getenv("PL_AIR_G", "1.4"))
_PL_AIR_MIX = float(os.getenv("PL_AIR_MIX", "0.06"))

_PL_GLOSS_ON = (os.getenv("PL_GLOSS_ON", "1").strip() == "1")
_PL_GLOSS_HP_HZ = float(os.getenv("PL_GLOSS_HP_HZ", "6500"))
_PL_GLOSS_LP_HZ = float(os.getenv("PL_GLOSS_LP_HZ", "15500"))
_PL_GLOSS_DRIVE_DB = float(os.getenv("PL_GLOSS_DRIVE_DB", "3.5"))
_PL_GLOSS_MIX = float(os.getenv("PL_GLOSS_MIX", "0.03"))

_PL_WIDTH_ON = (os.getenv("PL_WIDTH_ON", "1").strip() == "1")
_PL_WIDTH_HP_HZ = float(os.getenv("PL_WIDTH_HP_HZ", "4200"))
_PL_WIDTH_M = float(os.getenv("PL_WIDTH_M", "1.10"))
_PL_WIDTH_MIX = float(os.getenv("PL_WIDTH_MIX", "0.05"))

_PL_OUT_TRIM_DB = float(os.getenv("PL_OUT_TRIM_DB", "-1.5"))


def _render_polish_branch(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    intensity_scale = {
        "low": 0.85,
        "balanced": 1.00,
        "high": 1.10,
    }[intensity]

    tone_air_mul = {
        "warm": 0.88,
        "balanced": 1.00,
        "bright": 1.12,
    }[tone]

    glue_ratio = _clamp(_PL_GLUE_RATIO, 1.0, 2.0)
    glue_thr = _clamp(_PL_GLUE_THRESHOLD_DB, -60.0, 0.0)
    glue_att = _clamp(_PL_GLUE_ATTACK_MS, 1.0, 200.0)
    glue_rel = _clamp(_PL_GLUE_RELEASE_MS, 20.0, 2000.0)
    glue_mix = _clamp(_PL_GLUE_MIX * intensity_scale, 0.0, 0.30)

    air_f = _clamp(_PL_AIR_F, 6000.0, 16000.0)
    air_g = _clamp(_PL_AIR_G * tone_air_mul, 0.0, 4.0)
    air_mix = _clamp(_PL_AIR_MIX * intensity_scale, 0.0, 0.20)

    gloss_hp = _clamp(_PL_GLOSS_HP_HZ, 3500.0, 12000.0)
    gloss_lp = _clamp(_PL_GLOSS_LP_HZ, 7000.0, 19000.0)
    if gloss_lp <= gloss_hp + 1000:
        gloss_lp = gloss_hp + 1000
    gloss_drive = _clamp(_PL_GLOSS_DRIVE_DB, 0.0, 10.0)
    gloss_mix = _clamp(_PL_GLOSS_MIX * intensity_scale, 0.0, 0.10)

    width_hp = _clamp(_PL_WIDTH_HP_HZ, 2500.0, 12000.0)
    width_m = _clamp(_PL_WIDTH_M, 1.0, 1.8)
    width_mix = _clamp(_PL_WIDTH_MIX * intensity_scale, 0.0, 0.15)

    out_trim_db = _clamp(_PL_OUT_TRIM_DB, -18.0, 6.0)

    parts = ["[0:a]asplit=4[gl][air][gls][wid]"]

    if _PL_GLUE_ON and glue_mix > 0.0:
        parts.append(
            f"[gl]"
            f"acompressor=threshold={glue_thr}dB:ratio={glue_ratio}:attack={glue_att}:release={glue_rel}:knee=2dB:makeup=0dB:mix=1,"
            f"volume={glue_mix}[g0]"
        )
    else:
        parts.append("[gl]volume=0[g0]")

    if _PL_AIR_ON and air_mix > 0.0:
        parts.append(
            f"[air]"
            f"highpass=f={max(air_f * 0.55, 4500.0)}:width=0.707,"
            f"highshelf=f={air_f}:g={air_g},"
            f"volume={air_mix}[a0]"
        )
    else:
        parts.append("[air]volume=0[a0]")

    if _PL_GLOSS_ON and gloss_mix > 0.0:
        gloss_chain = _os_softclip_chain(
            drive_db=gloss_drive,
            hp=gloss_hp,
            lp=gloss_lp,
            post_gain_db=0.0,
        )
        parts.append(f"[gls]{gloss_chain},volume={gloss_mix}[s0]")
    else:
        parts.append("[gls]volume=0[s0]")

    if _PL_WIDTH_ON and width_mix > 0.0:
        parts.append(
            f"[wid]"
            f"highpass=f={width_hp}:width=0.707,"
            f"extrastereo=m={width_m},"
            f"highpass=f={width_hp}:width=0.707,"
            f"volume={width_mix}[w0]"
        )
    else:
        parts.append("[wid]volume=0[w0]")

    parts.append("[g0][a0][s0][w0]amix=inputs=4:normalize=0[m0]")
    if abs(out_trim_db) > 1e-9:
        parts.append(f"[m0]volume={out_trim_db}dB[out]")
    else:
        parts.append("[m0]anull[out]")

    fc = ";".join(parts)

    out_args, out_name, _mime = _out_args(fmt)
    out_name = f"polish_{out_name}"
    out_path = os.path.join(td, out_name)

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-filter_complex "{fc}" -map "[out]" '
        f'{out_args} {shlex.quote(out_path)}'
    )
    _run(cmd)
    return out_path, out_name


# wrappers to preserve internal naming
def _render_bandlab_like(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    return _render_reveal_branch(in_path, tone=tone, intensity=intensity, fmt=fmt, td=td)


def _render_bakuage_like(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    return _render_low_support_branch(in_path, tone=tone, intensity=intensity, fmt=fmt, td=td)


def _render_enhance(in_path: str, fmt: str, td: str, tone: str = "balanced", intensity: str = "balanced") -> tuple[str, str]:
    return _render_polish_branch(in_path, tone=tone, intensity=intensity, fmt=fmt, td=td)


# ---------------------------
# FINAL BLEND + POST
# ---------------------------

_BLEND_BASE_GAIN = float(os.getenv("BLEND_BASE_GAIN", "1.0"))
_BLEND_LOW_GAIN_DB = float(os.getenv("BLEND_LOW_GAIN_DB", "-13.0"))
_BLEND_REVEAL_GAIN_DB = float(os.getenv("BLEND_REVEAL_GAIN_DB", "-15.0"))
_BLEND_POLISH_GAIN_DB = float(os.getenv("BLEND_POLISH_GAIN_DB", "-18.0"))

_PREPOST_CLIP_ON = (os.getenv("PREPOST_CLIP_ON", "1").strip() == "1")
_PREPOST_CLIP_DRIVE_DB = float(os.getenv("PREPOST_CLIP_DRIVE_DB", "0.6"))
_PREPOST_CLIP_POST_GAIN_DB = float(os.getenv("PREPOST_CLIP_POST_GAIN_DB", "-0.4"))

_BLEND_POST_I = float(os.getenv("BLEND_POST_I", "-10.8"))
_BLEND_POST_TP = float(os.getenv("BLEND_POST_TP", "-1.0"))
_BLEND_POST_LRA = float(os.getenv("BLEND_POST_LRA", "7.0"))

_BANDLAB_PREVIEW_GAIN_DB = float(os.getenv("BANDLAB_PREVIEW_GAIN_DB", "0.0"))
_BAKUAGE_PREVIEW_GAIN_DB = float(os.getenv("BAKUAGE_PREVIEW_GAIN_DB", "0.0"))
_ENHANCE_PREVIEW_GAIN_DB = float(os.getenv("ENHANCE_PREVIEW_GAIN_DB", "0.0"))


def _render_final_blend(base_src: str, low_src: str, reveal_src: str, polish_src: str, out_path: str):
    base_gain = _clamp(_BLEND_BASE_GAIN, 0.5, 1.5)
    low_gain_db = _clamp(_BLEND_LOW_GAIN_DB, -36.0, 6.0)
    reveal_gain_db = _clamp(_BLEND_REVEAL_GAIN_DB, -36.0, 6.0)
    polish_gain_db = _clamp(_BLEND_POLISH_GAIN_DB, -36.0, 6.0)

    parts = [
        f"[0:a]volume={base_gain}[base]",
        f"[1:a]volume={low_gain_db}dB[low]",
        f"[2:a]volume={reveal_gain_db}dB[reveal]",
        f"[3:a]volume={polish_gain_db}dB[polish]",
        "[base][low][reveal][polish]amix=inputs=4:normalize=0[m0]",
    ]

    if _PREPOST_CLIP_ON:
        parts.append(
            f"[m0]{_os_softclip_chain(drive_db=_PREPOST_CLIP_DRIVE_DB, hp=None, lp=None, post_gain_db=_PREPOST_CLIP_POST_GAIN_DB)}[out]"
        )
    else:
        parts.append("[m0]anull[out]")

    fc = ";".join(parts)
    cmd = (
        f'ffmpeg -y -hide_banner '
        f'-i {shlex.quote(base_src)} '
        f'-i {shlex.quote(low_src)} '
        f'-i {shlex.quote(reveal_src)} '
        f'-i {shlex.quote(polish_src)} '
        f'-filter_complex "{fc}" -map "[out]" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
    )
    _run(cmd)


def _render_post_stage(in_path: str, fmt: str, td: str, loudnorm_params: dict | None = None) -> tuple[str, str]:
    fmt = _normalize_format(fmt)
    out_args, out_name, _mime = _out_args(fmt)
    out_path = os.path.join(td, out_name)

    if loudnorm_params is None:
        loudnorm_params = {
            "I": _BLEND_POST_I,
            "TP": _BLEND_POST_TP,
            "LRA": _BLEND_POST_LRA,
        }

    _build_loudnorm_two_pass(in_path, loudnorm_params, out_args, out_path)
    return out_path, out_name


def _render_single_branch_preview(
    in_path: str,
    tone: str,
    intensity: str,
    fmt: str,
    td: str,
    branch_kind: str,
) -> tuple[str, str]:
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    base_wav = os.path.join(td, f"{branch_kind}_base.wav")
    branch_wav = os.path.join(td, f"{branch_kind}_branch.wav")
    premix_wav = os.path.join(td, f"{branch_kind}_premix.wav")

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-af "{_PRE_CLEAN_CHAIN}" -ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(base_wav)}'
    )
    _run(cmd)

    if branch_kind == "bandlab":
        branch_wav, _ = _render_reveal_branch(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)
        preview_gain_db = _clamp(_BANDLAB_PREVIEW_GAIN_DB, -24.0, 18.0)
        preview_name = "bandlab_preview"
    elif branch_kind == "bakuage":
        branch_wav, _ = _render_low_support_branch(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)
        preview_gain_db = _clamp(_BAKUAGE_PREVIEW_GAIN_DB, -24.0, 18.0)
        preview_name = "bakuage_preview"
    elif branch_kind == "enhance":
        branch_wav, _ = _render_polish_branch(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)
        preview_gain_db = _clamp(_ENHANCE_PREVIEW_GAIN_DB, -24.0, 18.0)
        preview_name = "enhance_preview"
    else:
        raise RuntimeError(f"Unknown branch_kind: {branch_kind}")

    parts = [
        "[0:a]volume=1[base]",
        f"[1:a]volume={preview_gain_db}dB[br]",
        "[base][br]amix=inputs=2:normalize=0[m0]",
    ]

    if _PREPOST_CLIP_ON:
        parts.append(
            f"[m0]{_os_softclip_chain(drive_db=_PREPOST_CLIP_DRIVE_DB, hp=None, lp=None, post_gain_db=_PREPOST_CLIP_POST_GAIN_DB)}[out]"
        )
    else:
        parts.append("[m0]anull[out]")

    fc = ";".join(parts)

    cmd = (
        f'ffmpeg -y -hide_banner '
        f'-i {shlex.quote(base_wav)} '
        f'-i {shlex.quote(branch_wav)} '
        f'-filter_complex "{fc}" -map "[out]" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(premix_wav)}'
    )
    _run(cmd)

    out_path, out_name = _render_post_stage(premix_wav, fmt=fmt, td=td, loudnorm_params=None)
    final_name = f"{preview_name}_{out_name}"
    final_path = os.path.join(td, final_name)
    os.replace(out_path, final_path)
    return final_path, final_name


# ---------------------------
# MASTER / BLEND RENDERS
# ---------------------------

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
    low_wav = os.path.join(td, "low.wav")
    reveal_wav = os.path.join(td, "reveal.wav")
    polish_wav = os.path.join(td, "polish.wav")
    premix_wav = os.path.join(td, "premix.wav")

    _render_base_no_loudnorm(in_path, base_no_ln, base_wav)
    low_wav, _ = _render_low_support_branch(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)
    reveal_wav, _ = _render_reveal_branch(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)
    polish_wav, _ = _render_polish_branch(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)

    _render_final_blend(base_wav, low_wav, reveal_wav, polish_wav, premix_wav)
    out_path, out_name = _render_post_stage(premix_wav, fmt=fmt, td=td, loudnorm_params=base_params["loudnorm"])

    return out_path, out_name


def _render_blend(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    base_wav = os.path.join(td, "base_original.wav")
    low_wav = os.path.join(td, "low.wav")
    reveal_wav = os.path.join(td, "reveal.wav")
    polish_wav = os.path.join(td, "polish.wav")
    premix_wav = os.path.join(td, "premix.wav")

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-af "{_PRE_CLEAN_CHAIN}" -ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(base_wav)}'
    )
    _run(cmd)

    low_wav, _ = _render_low_support_branch(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)
    reveal_wav, _ = _render_reveal_branch(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)
    polish_wav, _ = _render_polish_branch(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)

    _render_final_blend(base_wav, low_wav, reveal_wav, polish_wav, premix_wav)
    out_path, out_name = _render_post_stage(premix_wav, fmt=fmt, td=td, loudnorm_params=None)

    blend_name = f"blend_{out_name}"
    blend_path = os.path.join(td, blend_name)
    os.replace(out_path, blend_path)
    return blend_path, blend_name


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
            "/bandlab_branch",
            "/bakuage_branch",
            "/enhance_branch",
        ]
    })


@app.get("/health")
def health():
    return jsonify({
        "ok": True,

        "ENABLE_AFFTDN": os.getenv("ENABLE_AFFTDN"),

        "BASE_LOWMID_ON": os.getenv("BASE_LOWMID_ON"),
        "BASE_LOWMID_F": os.getenv("BASE_LOWMID_F"),
        "BASE_LOWMID_W": os.getenv("BASE_LOWMID_W"),
        "BASE_LOWMID_G": os.getenv("BASE_LOWMID_G"),

        "GLUE_ON": os.getenv("GLUE_ON"),
        "GLUE_RATIO": os.getenv("GLUE_RATIO"),
        "GLUE_THRESHOLD_DB": os.getenv("GLUE_THRESHOLD_DB"),
        "GLUE_ATTACK_MS": os.getenv("GLUE_ATTACK_MS"),
        "GLUE_RELEASE_MS": os.getenv("GLUE_RELEASE_MS"),
        "GLUE_MIX": os.getenv("GLUE_MIX"),

        "TRANSIENT_ON": os.getenv("TRANSIENT_ON"),

        "LS_FOUND_ON": os.getenv("LS_FOUND_ON"),
        "LS_FOUND_LO_HZ": os.getenv("LS_FOUND_LO_HZ"),
        "LS_FOUND_HI_HZ": os.getenv("LS_FOUND_HI_HZ"),
        "LS_FOUND_MIX": os.getenv("LS_FOUND_MIX"),
        "LS_HARM_ON": os.getenv("LS_HARM_ON"),
        "LS_HARM_DRIVE_DB": os.getenv("LS_HARM_DRIVE_DB"),
        "LS_HARM_MIX": os.getenv("LS_HARM_MIX"),
        "LS_BODY_ON": os.getenv("LS_BODY_ON"),
        "LS_BODY_F": os.getenv("LS_BODY_F"),
        "LS_BODY_G": os.getenv("LS_BODY_G"),
        "LS_BODY_MIX": os.getenv("LS_BODY_MIX"),
        "LS_GUARD_ON": os.getenv("LS_GUARD_ON"),
        "LS_GUARD_F": os.getenv("LS_GUARD_F"),
        "LS_GUARD_G": os.getenv("LS_GUARD_G"),
        "LS_MONO_ON": os.getenv("LS_MONO_ON"),

        "RV_CORE_ON": os.getenv("RV_CORE_ON"),
        "RV_LO_HZ": os.getenv("RV_LO_HZ"),
        "RV_HI_HZ": os.getenv("RV_HI_HZ"),
        "RV_MID_F": os.getenv("RV_MID_F"),
        "RV_MID_G": os.getenv("RV_MID_G"),
        "RV_PRES_F": os.getenv("RV_PRES_F"),
        "RV_PRES_G": os.getenv("RV_PRES_G"),
        "RV_EXCITE_ON": os.getenv("RV_EXCITE_ON"),
        "RV_EXCITE_DRIVE_DB": os.getenv("RV_EXCITE_DRIVE_DB"),
        "RV_AIR_ON": os.getenv("RV_AIR_ON"),
        "RV_AIR_F": os.getenv("RV_AIR_F"),
        "RV_AIR_G": os.getenv("RV_AIR_G"),
        "RV_WIDTH_ON": os.getenv("RV_WIDTH_ON"),
        "RV_WIDTH_HP_HZ": os.getenv("RV_WIDTH_HP_HZ"),
        "RV_WIDTH_M": os.getenv("RV_WIDTH_M"),
        "RV_GUARD_ON": os.getenv("RV_GUARD_ON"),

        "PL_GLUE_ON": os.getenv("PL_GLUE_ON"),
        "PL_GLUE_RATIO": os.getenv("PL_GLUE_RATIO"),
        "PL_GLUE_THRESHOLD_DB": os.getenv("PL_GLUE_THRESHOLD_DB"),
        "PL_GLUE_MIX": os.getenv("PL_GLUE_MIX"),
        "PL_AIR_ON": os.getenv("PL_AIR_ON"),
        "PL_AIR_F": os.getenv("PL_AIR_F"),
        "PL_AIR_G": os.getenv("PL_AIR_G"),
        "PL_GLOSS_ON": os.getenv("PL_GLOSS_ON"),
        "PL_GLOSS_HP_HZ": os.getenv("PL_GLOSS_HP_HZ"),
        "PL_GLOSS_LP_HZ": os.getenv("PL_GLOSS_LP_HZ"),
        "PL_GLOSS_DRIVE_DB": os.getenv("PL_GLOSS_DRIVE_DB"),
        "PL_WIDTH_ON": os.getenv("PL_WIDTH_ON"),
        "PL_WIDTH_HP_HZ": os.getenv("PL_WIDTH_HP_HZ"),

        "BLEND_BASE_GAIN": os.getenv("BLEND_BASE_GAIN"),
        "BLEND_LOW_GAIN_DB": os.getenv("BLEND_LOW_GAIN_DB"),
        "BLEND_REVEAL_GAIN_DB": os.getenv("BLEND_REVEAL_GAIN_DB"),
        "BLEND_POLISH_GAIN_DB": os.getenv("BLEND_POLISH_GAIN_DB"),
        "PREPOST_CLIP_ON": os.getenv("PREPOST_CLIP_ON"),
        "PREPOST_CLIP_DRIVE_DB": os.getenv("PREPOST_CLIP_DRIVE_DB"),
        "PREPOST_CLIP_POST_GAIN_DB": os.getenv("PREPOST_CLIP_POST_GAIN_DB"),

        "BLEND_POST_I": os.getenv("BLEND_POST_I"),
        "BLEND_POST_TP": os.getenv("BLEND_POST_TP"),
        "BLEND_POST_LRA": os.getenv("BLEND_POST_LRA"),

        "BANDLAB_PREVIEW_GAIN_DB": os.getenv("BANDLAB_PREVIEW_GAIN_DB"),
        "BAKUAGE_PREVIEW_GAIN_DB": os.getenv("BAKUAGE_PREVIEW_GAIN_DB"),
        "ENHANCE_PREVIEW_GAIN_DB": os.getenv("ENHANCE_PREVIEW_GAIN_DB"),
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
            out_path, out_name = _render_single_branch_preview(
                in_path=in_path,
                tone=tone,
                intensity=intensity,
                fmt=fmt,
                td=td,
                branch_kind="bandlab",
            )
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
            out_path, out_name = _render_single_branch_preview(
                in_path=in_path,
                tone=tone,
                intensity=intensity,
                fmt=fmt,
                td=td,
                branch_kind="bakuage",
            )
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

    tone = _normalize_tone(request.args.get("tone") or "balanced")
    intensity = _normalize_intensity(request.args.get("intensity") or "balanced")
    fmt = _normalize_format(request.args.get("format") or "wav16")

    if is_gdrive(url):
        url = gdrive_direct(url)

    try:
        with tempfile.TemporaryDirectory() as td:
            in_path, _dbg = _dl_to_named(td, "file", url)
            out_path, out_name = _render_single_branch_preview(
                in_path=in_path,
                tone=tone,
                intensity=intensity,
                fmt=fmt,
                td=td,
                branch_kind="enhance",
            )
            _out_args_str, _out_name2, mime = _out_args(fmt)
            return send_file(
                out_path,
                mimetype=mime,
                as_attachment=True,
                download_name=out_name
            )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/bandlab_branch")
def bandlab_branch_route():
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
            out_path, out_name = _render_reveal_branch(in_path, tone=tone, intensity=intensity, fmt=fmt, td=td)
            _out_args_str, _out_name2, mime = _out_args(fmt)
            return send_file(
                out_path,
                mimetype=mime,
                as_attachment=True,
                download_name=out_name
            )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/bakuage_branch")
def bakuage_branch_route():
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
            out_path, out_name = _render_low_support_branch(in_path, tone=tone, intensity=intensity, fmt=fmt, td=td)
            _out_args_str, _out_name2, mime = _out_args(fmt)
            return send_file(
                out_path,
                mimetype=mime,
                as_attachment=True,
                download_name=out_name
            )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/enhance_branch")
def enhance_branch_route():
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
            out_path, out_name = _render_polish_branch(in_path, tone=tone, intensity=intensity, fmt=fmt, td=td)
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

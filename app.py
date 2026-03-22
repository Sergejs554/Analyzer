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


def _safe_float(v):
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s or s.lower() in ("nan", "none", "null", "-inf", "inf"):
            return None
        return float(s)
    except Exception:
        return None


def _extract_re_float(pattern: str, text: str):
    m = re.search(pattern, text, re.IGNORECASE)
    if not m:
        return None
    return _safe_float(m.group(1))


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


def _probe_duration_sec(in_path: str):
    cmd = (
        f'ffprobe -v error -show_entries format=duration '
        f'-of default=noprint_wrappers=1:nokey=1 {shlex.quote(in_path)}'
    )
    out, _ = _run(cmd)
    lines = [x.strip() for x in out.splitlines() if x.strip()]
    if not lines:
        return None
    return _safe_float(lines[-1])


def _probe_volumedetect(in_path: str) -> dict:
    cmd = f'ffmpeg -hide_banner -nostats -i {shlex.quote(in_path)} -af "volumedetect" -f null -'
    _, err = _run(cmd)
    mean_volume = _extract_re_float(r"mean_volume:\s*(-?\d+(?:\.\d+)?)\s*dB", err)
    max_volume = _extract_re_float(r"max_volume:\s*(-?\d+(?:\.\d+)?)\s*dB", err)
    return {
        "rms_dbfs": mean_volume,
        "sample_peak_dbfs": max_volume,
    }


def _probe_loudnorm_input_stats(in_path: str) -> dict:
    ln = "loudnorm=I=-14:TP=-1.0:LRA=7:print_format=json"
    cmd = f'ffmpeg -hide_banner -nostats -i {shlex.quote(in_path)} -af "{ln}" -f null -'
    _, err = _run(cmd)
    stats = _extract_last_json_block(err) or {}
    return {
        "integrated_lufs": _safe_float(stats.get("input_i")),
        "true_peak_dbtp": _safe_float(stats.get("input_tp")),
        "lra_ebu": _safe_float(stats.get("input_lra")),
        "input_thresh": _safe_float(stats.get("input_thresh")),
        "target_offset": _safe_float(stats.get("target_offset")),
    }


def _collect_stage_metrics(in_path: str) -> dict:
    vd = _probe_volumedetect(in_path)
    ln = _probe_loudnorm_input_stats(in_path)
    duration_sec = _probe_duration_sec(in_path)

    rms_dbfs = vd.get("rms_dbfs")
    sample_peak_dbfs = vd.get("sample_peak_dbfs")
    integrated_lufs = ln.get("integrated_lufs")
    true_peak_dbtp = ln.get("true_peak_dbtp")

    crest_db = None
    if sample_peak_dbfs is not None and rms_dbfs is not None:
        crest_db = sample_peak_dbfs - rms_dbfs

    plr_proxy_db = None
    if true_peak_dbtp is not None and integrated_lufs is not None:
        plr_proxy_db = true_peak_dbtp - integrated_lufs

    tp_margin_to_minus1_dbtp_db = None
    if true_peak_dbtp is not None:
        tp_margin_to_minus1_dbtp_db = -1.0 - true_peak_dbtp

    return {
        "duration_sec": duration_sec,
        "integrated_lufs": integrated_lufs,
        "true_peak_dbtp": true_peak_dbtp,
        "tp_margin_to_minus1_dbtp_db": tp_margin_to_minus1_dbtp_db,
        "rms_dbfs": rms_dbfs,
        "sample_peak_dbfs": sample_peak_dbfs,
        "crest_db": crest_db,
        "plr_proxy_db": plr_proxy_db,
        "lra_ebu": ln.get("lra_ebu"),
        "input_thresh": ln.get("input_thresh"),
        "target_offset": ln.get("target_offset"),
    }


def _metric_deltas(ref: dict, cur: dict) -> dict:
    keys = [
        "integrated_lufs",
        "true_peak_dbtp",
        "tp_margin_to_minus1_dbtp_db",
        "rms_dbfs",
        "sample_peak_dbfs",
        "crest_db",
        "plr_proxy_db",
        "lra_ebu",
        "input_thresh",
        "target_offset",
    ]
    out = {}
    for k in keys:
        a = ref.get(k)
        b = cur.get(k)
        if a is None or b is None:
            out[f"{k}_delta"] = None
        else:
            out[f"{k}_delta"] = b - a
    return out


# ---------------------------
# NORMALIZERS
# ---------------------------

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

_RV_MID_F = float(os.getenv("RV_MID_F", "1150"))
_RV_MID_G = float(os.getenv("RV_MID_G", "0.98"))
_RV_MID_W = float(os.getenv("RV_MID_W", "0.95"))

_RV_PRES_F = float(os.getenv("RV_PRES_F", "2100"))
_RV_PRES_G = float(os.getenv("RV_PRES_G", "0.41"))
_RV_PRES_W = float(os.getenv("RV_PRES_W", "0.95"))

_RV_CORE_MIX = float(os.getenv("RV_CORE_MIX", "0.124"))

_RV_EXCITE_ON = (os.getenv("RV_EXCITE_ON", "1").strip() == "1")
_RV_EXCITE_HP_HZ = float(os.getenv("RV_EXCITE_HP_HZ", "2200"))
_RV_EXCITE_LP_HZ = float(os.getenv("RV_EXCITE_LP_HZ", "8200"))
_RV_EXCITE_DRIVE_DB = float(os.getenv("RV_EXCITE_DRIVE_DB", "2.25"))
_RV_EXCITE_MIX = float(os.getenv("RV_EXCITE_MIX", "0.027"))

_RV_AIR_ON = (os.getenv("RV_AIR_ON", "1").strip() == "1")
_RV_AIR_F = float(os.getenv("RV_AIR_F", "9000"))
_RV_AIR_G = float(os.getenv("RV_AIR_G", "1.32"))
_RV_AIR_MIX = float(os.getenv("RV_AIR_MIX", "0.072"))

_RV_WIDTH_ON = (os.getenv("RV_WIDTH_ON", "1").strip() == "1")
_RV_WIDTH_HP_HZ = float(os.getenv("RV_WIDTH_HP_HZ", "5200"))
_RV_WIDTH_M = float(os.getenv("RV_WIDTH_M", "1.09"))
_RV_WIDTH_MIX = float(os.getenv("RV_WIDTH_MIX", "0.055"))

_RV_GUARD_ON = (os.getenv("RV_GUARD_ON", "1").strip() == "1")
_RV_GUARD_F = float(os.getenv("RV_GUARD_F", "3400"))
_RV_GUARD_G = float(os.getenv("RV_GUARD_G", "-1.10"))
_RV_GUARD_W = float(os.getenv("RV_GUARD_W", "1.3"))

_RV_SIB_F = float(os.getenv("RV_SIB_F", "7200"))
_RV_SIB_G = float(os.getenv("RV_SIB_G", "-1.05"))
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
# branch-only donor
# Mixea V2.5 = cleanup -> body -> center -> presence rebuild -> sheen -> air -> guard -> punch
# ---------------------------

# --- shared scaling ---
_MX_TONE_BODY_MUL = float(os.getenv("MX_TONE_BODY_MUL", "1.00"))
_MX_TONE_MID_MUL = float(os.getenv("MX_TONE_MID_MUL", "1.00"))
_MX_TONE_FINISH_MUL = float(os.getenv("MX_TONE_FINISH_MUL", "1.00"))

_MX_INTENSITY_CORE = float(os.getenv("MX_INTENSITY_CORE", "1.00"))
_MX_INTENSITY_FINISH = float(os.getenv("MX_INTENSITY_FINISH", "1.00"))
_MX_INTENSITY_DYNAMIC = float(os.getenv("MX_INTENSITY_DYNAMIC", "1.00"))

# --- structural cleanup ---
_MX_CLEAN_F1 = float(os.getenv("MX_CLEAN_F1", "280"))
_MX_CLEAN_G1 = float(os.getenv("MX_CLEAN_G1", "-0.95"))
_MX_CLEAN_W1 = float(os.getenv("MX_CLEAN_W1", "1.10"))

_MX_CLEAN_F2 = float(os.getenv("MX_CLEAN_F2", "430"))
_MX_CLEAN_G2 = float(os.getenv("MX_CLEAN_G2", "-0.45"))
_MX_CLEAN_W2 = float(os.getenv("MX_CLEAN_W2", "1.00"))

_MX_CLEAN_F3 = float(os.getenv("MX_CLEAN_F3", "670"))
_MX_CLEAN_G3 = float(os.getenv("MX_CLEAN_G3", "-0.18"))
_MX_CLEAN_W3 = float(os.getenv("MX_CLEAN_W3", "1.00"))

# --- body anchor ---
_MX_BODY_F1 = float(os.getenv("MX_BODY_F1", "205"))
_MX_BODY_G1 = float(os.getenv("MX_BODY_G1", "1.78"))
_MX_BODY_W1 = float(os.getenv("MX_BODY_W1", "1.00"))

_MX_BODY_F2 = float(os.getenv("MX_BODY_F2", "305"))
_MX_BODY_G2 = float(os.getenv("MX_BODY_G2", "0.62"))
_MX_BODY_W2 = float(os.getenv("MX_BODY_W2", "1.00"))

_MX_BODY_GUARD_F = float(os.getenv("MX_BODY_GUARD_F", "390"))
_MX_BODY_GUARD_G = float(os.getenv("MX_BODY_GUARD_G", "-0.30"))
_MX_BODY_GUARD_W = float(os.getenv("MX_BODY_GUARD_W", "1.15"))

# --- mid-anchor projection ---
_MX_PROJ_F1 = float(os.getenv("MX_PROJ_F1", "1280"))
_MX_PROJ_G1 = float(os.getenv("MX_PROJ_G1", "1.36"))
_MX_PROJ_W1 = float(os.getenv("MX_PROJ_W1", "0.95"))

_MX_PROJ_F2 = float(os.getenv("MX_PROJ_F2", "2050"))
_MX_PROJ_G2 = float(os.getenv("MX_PROJ_G2", "0.64"))
_MX_PROJ_W2 = float(os.getenv("MX_PROJ_W2", "1.00"))

_MX_PROJ_F3 = float(os.getenv("MX_PROJ_F3", "3500"))
_MX_PROJ_G3 = float(os.getenv("MX_PROJ_G3", "-0.22"))
_MX_PROJ_W3 = float(os.getenv("MX_PROJ_W3", "1.15"))

# ---------------------------
# NEW TOP MODULE V2.5
# ---------------------------

# --- shared top scaling ---
_TOP_TONE_SHEEN_MUL = float(os.getenv("TOP_TONE_SHEEN_MUL", "1.00"))
_TOP_TONE_AIR_MUL = float(os.getenv("TOP_TONE_AIR_MUL", "1.00"))

_TOP_INTENSITY_SHEEN_MUL = float(os.getenv("TOP_INTENSITY_SHEEN_MUL", "1.00"))
_TOP_INTENSITY_GUARD_MUL = float(os.getenv("TOP_INTENSITY_GUARD_MUL", "1.00"))

_TOP_GLOBAL_BUILD_MUL = float(os.getenv("TOP_GLOBAL_BUILD_MUL", "1.00"))
_TOP_GLOBAL_SAFE_MUL = float(os.getenv("TOP_GLOBAL_SAFE_MUL", "1.00"))

# --- Presence Cleanup Notch ---
_PRES_NOTCH_F = float(os.getenv("PRES_NOTCH_F", "3880"))
_PRES_NOTCH_G = float(os.getenv("PRES_NOTCH_G", "-0.34"))
_PRES_NOTCH_W = float(os.getenv("PRES_NOTCH_W", "0.92"))
_PRES_NOTCH_AMOUNT = float(os.getenv("PRES_NOTCH_AMOUNT", "1.00"))

# --- Lower-Presence Anchor ---
_PRES_ANCHOR_F = float(os.getenv("PRES_ANCHOR_F", "3260"))
_PRES_ANCHOR_G = float(os.getenv("PRES_ANCHOR_G", "1.66"))
_PRES_ANCHOR_W = float(os.getenv("PRES_ANCHOR_W", "0.92"))
_PRES_ANCHOR_TILT = float(os.getenv("PRES_ANCHOR_TILT", "-0.18"))
_PRES_ANCHOR_AMOUNT = float(os.getenv("PRES_ANCHOR_AMOUNT", "0.96"))

# --- Upper-Presence Bridge ---
_PRES_BRIDGE_F = float(os.getenv("PRES_BRIDGE_F", "4740"))
_PRES_BRIDGE_G = float(os.getenv("PRES_BRIDGE_G", "0.86"))
_PRES_BRIDGE_W = float(os.getenv("PRES_BRIDGE_W", "0.88"))
_PRES_BRIDGE_HP = float(os.getenv("PRES_BRIDGE_HP", "4520"))
_PRES_BRIDGE_LP = float(os.getenv("PRES_BRIDGE_LP", "5750"))
_PRES_BRIDGE_MIX = float(os.getenv("PRES_BRIDGE_MIX", "0.082"))
_PRES_BRIDGE_SOFTNESS = float(os.getenv("PRES_BRIDGE_SOFTNESS", "0.88"))
_PRES_BRIDGE_POST_TRIM = float(os.getenv("PRES_BRIDGE_POST_TRIM", "-0.22"))

# --- Presence Glue / Micro-Finish ---
_PRES_GLUE_F = float(os.getenv("PRES_GLUE_F", "4720"))
_PRES_GLUE_G = float(os.getenv("PRES_GLUE_G", "0.15"))
_PRES_GLUE_W = float(os.getenv("PRES_GLUE_W", "1.45"))
_PRES_GLUE_MIX = float(os.getenv("PRES_GLUE_MIX", "0.028"))
_PRES_GLUE_SOFTNESS = float(os.getenv("PRES_GLUE_SOFTNESS", "0.86"))

# --- Shared Presence Scalars ---
_PRES_TONE_MUL = float(os.getenv("PRES_TONE_MUL", "1.00"))
_PRES_INTENSITY_MUL = float(os.getenv("PRES_INTENSITY_MUL", "1.00"))
_PRES_BUILD_MUL = float(os.getenv("PRES_BUILD_MUL", "1.00"))
_PRES_SAFE_MUL = float(os.getenv("PRES_SAFE_MUL", "1.00"))

# --- Parallel Sum / Balance ---
_PRES_PARALLEL_MUL = float(os.getenv("PRES_PARALLEL_MUL", "0.84"))
_PRES_SUM_TRIM = float(os.getenv("PRES_SUM_TRIM", "-0.20"))
_PRES_BRIDGE_TO_GLUE_BAL = float(os.getenv("PRES_BRIDGE_TO_GLUE_BAL", "1.18"))
# --- Sheen Builder ---
_TOP_SHEEN_HP = float(os.getenv("TOP_SHEEN_HP", "6100"))
_TOP_SHEEN_LP = float(os.getenv("TOP_SHEEN_LP", "9400"))
_TOP_SHEEN_DRIVE = float(os.getenv("TOP_SHEEN_DRIVE", "1.15"))
_TOP_SHEEN_MIX = float(os.getenv("TOP_SHEEN_MIX", "0.145"))

_TOP_SHEEN_POST_F = float(os.getenv("TOP_SHEEN_POST_F", "7600"))
_TOP_SHEEN_POST_G = float(os.getenv("TOP_SHEEN_POST_G", "-0.18"))
_TOP_SHEEN_POST_W = float(os.getenv("TOP_SHEEN_POST_W", "1.15"))
_TOP_SHEEN_POLISH_GAIN = float(os.getenv("TOP_SHEEN_POLISH_GAIN", "0.70"))

# --- Air Completion ---
_TOP_AIR_F = float(os.getenv("TOP_AIR_F", "10800"))
_TOP_AIR_G = float(os.getenv("TOP_AIR_G", "0.95"))
_TOP_AIR_TILT = float(os.getenv("TOP_AIR_TILT", "0.00"))
_TOP_AIR_BLEND = float(os.getenv("TOP_AIR_BLEND", "1.00"))

# --- Safety Guard ---
_TOP_GUARD_LO_F = float(os.getenv("TOP_GUARD_LO_F", "4100"))
_TOP_GUARD_LO_G = float(os.getenv("TOP_GUARD_LO_G", "-0.32"))
_TOP_GUARD_LO_W = float(os.getenv("TOP_GUARD_LO_W", "1.20"))

_TOP_GUARD_HI_F = float(os.getenv("TOP_GUARD_HI_F", "7800"))
_TOP_GUARD_HI_G = float(os.getenv("TOP_GUARD_HI_G", "-0.22"))
_TOP_GUARD_HI_W = float(os.getenv("TOP_GUARD_HI_W", "1.35"))

_TOP_GUARD_GLOBAL_TRIM = float(os.getenv("TOP_GUARD_GLOBAL_TRIM", "1.00"))
_TOP_GUARD_SOFTNESS = float(os.getenv("TOP_GUARD_SOFTNESS", "1.00"))

# --- punch keeper / output ---
_MX_PUNCH_ON = (os.getenv("MX_PUNCH_ON", "1").strip() == "1")
_MX_PUNCH_THRESHOLD_DB = float(os.getenv("MX_PUNCH_THRESHOLD_DB", "-24"))
_MX_PUNCH_RATIO = float(os.getenv("MX_PUNCH_RATIO", "1.16"))
_MX_PUNCH_ATTACK_MS = float(os.getenv("MX_PUNCH_ATTACK_MS", "16"))
_MX_PUNCH_RELEASE_MS = float(os.getenv("MX_PUNCH_RELEASE_MS", "115"))
_MX_PUNCH_KNEE_DB = float(os.getenv("MX_PUNCH_KNEE_DB", "1.5"))
_MX_PUNCH_MAKEUP_DB = float(os.getenv("MX_PUNCH_MAKEUP_DB", "0.0"))

_MX_TRIM_DB = float(os.getenv("MX_TRIM_DB", "1.02"))


def _render_polish_branch(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    # --- shared tone scalers ---
    tone_body_mul = {
        "warm": 1.10,
        "balanced": 1.00,
        "bright": 0.93,
    }[tone] * _MX_TONE_BODY_MUL

    tone_mid_mul = {
        "warm": 0.96,
        "balanced": 1.00,
        "bright": 1.06,
    }[tone] * _MX_TONE_MID_MUL

    tone_finish_mul = {
        "warm": 0.90,
        "balanced": 1.00,
        "bright": 1.08,
    }[tone] * _MX_TONE_FINISH_MUL

    # --- shared intensity scalers ---
    intensity_core = {
        "low": 0.88,
        "balanced": 1.00,
        "high": 1.12,
    }[intensity] * _MX_INTENSITY_CORE

    intensity_finish = {
        "low": 0.82,
        "balanced": 1.00,
        "high": 1.12,
    }[intensity] * _MX_INTENSITY_FINISH

    intensity_dynamic = {
        "low": 0.92,
        "balanced": 1.00,
        "high": 1.08,
    }[intensity] * _MX_INTENSITY_DYNAMIC

    # ---------------------------
    # Section 1-3: serial tonal core
    # cleanup -> body -> center
    # ---------------------------

    clean_f1 = _clamp(_MX_CLEAN_F1, 220.0, 340.0)
    clean_g1 = _clamp(_MX_CLEAN_G1 * intensity_core, -2.5, -0.2)
    clean_w1 = _clamp(_MX_CLEAN_W1, 0.4, 2.5)

    clean_f2 = _clamp(_MX_CLEAN_F2, 360.0, 520.0)
    clean_g2 = _clamp(_MX_CLEAN_G2 * intensity_core, -1.5, 0.0)
    clean_w2 = _clamp(_MX_CLEAN_W2, 0.4, 2.5)

    clean_f3 = _clamp(_MX_CLEAN_F3, 560.0, 820.0)
    clean_g3 = _clamp(_MX_CLEAN_G3 * intensity_core, -1.2, 0.0)
    clean_w3 = _clamp(_MX_CLEAN_W3, 0.4, 2.5)

    body_f1 = _clamp(_MX_BODY_F1, 160.0, 250.0)
    body_g1 = _clamp(_MX_BODY_G1 * tone_body_mul * intensity_core, 0.0, 3.0)
    body_w1 = _clamp(_MX_BODY_W1, 0.4, 2.5)

    body_f2 = _clamp(_MX_BODY_F2, 240.0, 340.0)
    body_g2 = _clamp(_MX_BODY_G2 * tone_body_mul * intensity_core, 0.0, 2.0)
    body_w2 = _clamp(_MX_BODY_W2, 0.4, 2.5)

    body_guard_f = _clamp(_MX_BODY_GUARD_F, 280.0, 420.0)
    body_guard_g = _clamp(_MX_BODY_GUARD_G, -1.2, 0.0)
    body_guard_w = _clamp(_MX_BODY_GUARD_W, 0.4, 2.5)

    proj_f1 = _clamp(_MX_PROJ_F1, 1050.0, 1550.0)
    proj_g1 = _clamp(_MX_PROJ_G1 * tone_mid_mul * intensity_core, 0.0, 2.0)
    proj_w1 = _clamp(_MX_PROJ_W1, 0.4, 2.5)

    proj_f2 = _clamp(_MX_PROJ_F2, 1650.0, 2350.0)
    proj_g2 = _clamp(_MX_PROJ_G2 * tone_mid_mul * intensity_core, -0.2, 1.2)
    proj_w2 = _clamp(_MX_PROJ_W2, 0.4, 2.5)

    proj_f3 = _clamp(_MX_PROJ_F3, 3000.0, 4500.0)
    proj_g3 = _clamp(_MX_PROJ_G3 * tone_finish_mul, -1.5, 0.5)
    proj_w3 = _clamp(_MX_PROJ_W3, 0.4, 2.5)

    serial_parts = [
        f"equalizer=f={clean_f1}:t=q:w={clean_w1}:g={clean_g1}",
        f"equalizer=f={clean_f2}:t=q:w={clean_w2}:g={clean_g2}",
        f"equalizer=f={clean_f3}:t=q:w={clean_w3}:g={clean_g3}",
        f"equalizer=f={body_f1}:t=q:w={body_w1}:g={body_g1}",
        f"equalizer=f={body_f2}:t=q:w={body_w2}:g={body_g2}",
        f"equalizer=f={body_guard_f}:t=q:w={body_guard_w}:g={body_guard_g}",
        f"equalizer=f={proj_f1}:t=q:w={proj_w1}:g={proj_g1}",
        f"equalizer=f={proj_f2}:t=q:w={proj_w2}:g={proj_g2}",
        f"equalizer=f={proj_f3}:t=q:w={proj_w3}:g={proj_g3}",
    ]

    # ---------------------------
    # Section 4: top-module V2.5
    # ---------------------------

    # --- shared top scalers ---
    tone_sheen_mul = _clamp(_TOP_TONE_SHEEN_MUL, 0.85, 1.20)
    tone_air_mul = _clamp(_TOP_TONE_AIR_MUL, 0.80, 1.25)

    intensity_sheen_mul = _clamp(_TOP_INTENSITY_SHEEN_MUL, 0.85, 1.25)
    intensity_guard_mul = _clamp(_TOP_INTENSITY_GUARD_MUL, 0.85, 1.20)

    top_global_build_mul = _clamp(_TOP_GLOBAL_BUILD_MUL, 0.85, 1.15)
    top_global_safe_mul = _clamp(_TOP_GLOBAL_SAFE_MUL, 0.80, 1.20)

    tone_sheen_mode = {
        "warm": 0.92,
        "balanced": 1.00,
        "bright": 1.10,
    }[tone]

    tone_air_mode = {
        "warm": 0.90,
        "balanced": 1.00,
        "bright": 1.12,
    }[tone]

    top_intensity_mode = {
        "low": 0.90,
        "balanced": 1.00,
        "high": 1.12,
    }[intensity]

    # --- shared presence scalers ---
    pres_tone_mul = _clamp(_PRES_TONE_MUL, 0.85, 1.15)
    pres_intensity_mul = _clamp(_PRES_INTENSITY_MUL, 0.85, 1.20)
    pres_build_mul = _clamp(_PRES_BUILD_MUL, 0.80, 1.25)
    pres_safe_mul = _clamp(_PRES_SAFE_MUL, 0.75, 1.15)
    pres_parallel_mul = _clamp(_PRES_PARALLEL_MUL, 0.70, 1.25)
    pres_bridge_to_glue_bal = _clamp(_PRES_BRIDGE_TO_GLUE_BAL, 0.60, 1.60)
    pres_sum_trim = _clamp(_PRES_SUM_TRIM, -1.50, 0.50)

    pres_tone_mode = {
        "warm": 0.94,
        "balanced": 1.00,
        "bright": 1.08,
    }[tone]

    pres_intensity_mode = {
        "low": 0.90,
        "balanced": 1.00,
        "high": 1.12,
    }[intensity]

    pres_shared_build = pres_tone_mode * pres_tone_mul * pres_intensity_mode * pres_intensity_mul * pres_build_mul
    pres_shared_safe = pres_safe_mul

    # --- Presence Cleanup Notch ---
    pres_notch_f = _clamp(_PRES_NOTCH_F, 3300.0, 4400.0)
    pres_notch_g = _clamp(
        _PRES_NOTCH_G * _clamp(_PRES_NOTCH_AMOUNT, 0.70, 1.20) * pres_intensity_mode * pres_shared_safe,
        -1.20,
        -0.05,
    )
    pres_notch_w = _clamp(_PRES_NOTCH_W, 0.60, 1.60)

    # --- Lower-Presence Anchor ---
    pres_anchor_f = _clamp(_PRES_ANCHOR_F + (_clamp(_PRES_ANCHOR_TILT, -0.50, 0.50) * 180.0), 2900.0, 3600.0)
    pres_anchor_g = _clamp(
        _PRES_ANCHOR_G * _clamp(_PRES_ANCHOR_AMOUNT, 0.75, 1.30) * pres_shared_build,
        0.20,
        2.40,
    )
    pres_anchor_w = _clamp(_PRES_ANCHOR_W, 0.70, 1.80)

    # --- Upper-Presence Bridge ---
    pres_bridge_f = _clamp(_PRES_BRIDGE_F, 4400.0, 5400.0)
    pres_bridge_g = _clamp(
        _PRES_BRIDGE_G * pres_shared_build * pres_shared_safe,
        0.10,
        1.80,
    )
    pres_bridge_w = _clamp(_PRES_BRIDGE_W, 0.80, 2.00)
    pres_bridge_hp = _clamp(_PRES_BRIDGE_HP, 4000.0, 5000.0)
    pres_bridge_lp = _clamp(_PRES_BRIDGE_LP, 5200.0, 6800.0)
    if pres_bridge_lp <= pres_bridge_hp + 600.0:
        pres_bridge_lp = pres_bridge_hp + 600.0

    pres_bridge_softness = _clamp(_PRES_BRIDGE_SOFTNESS, 0.70, 1.30)
    pres_bridge_mix = _clamp(
        _PRES_BRIDGE_MIX * pres_parallel_mul * _clamp(pres_bridge_to_glue_bal, 0.60, 1.60),
        0.02,
        0.24,
    )
    pres_bridge_post_trim = _clamp(_PRES_BRIDGE_POST_TRIM, -1.00, 0.50)

    # --- Presence Glue / Micro-Finish ---
    pres_glue_f = _clamp(_PRES_GLUE_F, 4200.0, 5600.0)
    pres_glue_g = _clamp(
        _PRES_GLUE_G * (0.75 + 0.25 * pres_shared_build) * pres_shared_safe,
        0.02,
        0.70,
    )
    pres_glue_w = _clamp(_PRES_GLUE_W, 1.00, 2.60)
    pres_glue_softness = _clamp(_PRES_GLUE_SOFTNESS, 0.70, 1.40)
    pres_glue_mix = _clamp(
        _PRES_GLUE_MIX * pres_parallel_mul * (1.0 / _clamp(pres_bridge_to_glue_bal, 0.60, 1.60)),
        0.01,
        0.12,
    )

    # --- Sheen Builder ---
    top_sheen_hp = _clamp(_TOP_SHEEN_HP, 5500.0, 7500.0)
    top_sheen_lp = _clamp(_TOP_SHEEN_LP, 8500.0, 11000.0)
    if top_sheen_lp <= top_sheen_hp + 1200.0:
        top_sheen_lp = top_sheen_hp + 1200.0

    top_sheen_drive = _clamp(
        _TOP_SHEEN_DRIVE * tone_sheen_mode * tone_sheen_mul * top_intensity_mode * intensity_sheen_mul,
        0.0,
        3.0,
    )
    top_sheen_mix = _clamp(
        _TOP_SHEEN_MIX * top_intensity_mode * intensity_sheen_mul * top_global_build_mul,
        0.0,
        0.22,
    )
    top_sheen_post_f = _clamp(_TOP_SHEEN_POST_F, 5000.0, 9500.0)
    top_sheen_post_g = _clamp(_TOP_SHEEN_POST_G, -1.2, 0.8)
    top_sheen_post_w = _clamp(_TOP_SHEEN_POST_W, 0.7, 2.2)
    top_sheen_polish_gain = _clamp(
        _TOP_SHEEN_POLISH_GAIN * tone_sheen_mode * tone_sheen_mul * top_intensity_mode * intensity_sheen_mul,
        0.0,
        2.0,
    )

    # --- Air Completion ---
    top_air_f = _clamp(_TOP_AIR_F, 9500.0, 13500.0)
    top_air_g = _clamp(
        _TOP_AIR_G * tone_air_mode * tone_air_mul * _clamp(_TOP_AIR_BLEND, 0.50, 1.20),
        0.0,
        2.0,
    )
    top_air_tilt = _clamp(_TOP_AIR_TILT, 0.0, 1.0)

    # --- Safety Guard ---
    top_guard_lo_f = _clamp(_TOP_GUARD_LO_F, 3200.0, 5200.0)
    top_guard_lo_g = _clamp(
        _TOP_GUARD_LO_G
        * top_intensity_mode
        * intensity_guard_mul
        * top_global_safe_mul
        * _clamp(_TOP_GUARD_GLOBAL_TRIM, 0.50, 1.20),
        -1.8,
        0.0,
    )
    top_guard_lo_w = _clamp(
        _TOP_GUARD_LO_W * _clamp(_TOP_GUARD_SOFTNESS, 0.5, 1.5),
        0.7,
        2.4,
    )

    top_guard_hi_f = _clamp(_TOP_GUARD_HI_F, 6500.0, 9500.0)
    top_guard_hi_g = _clamp(
        _TOP_GUARD_HI_G
        * top_intensity_mode
        * intensity_guard_mul
        * top_global_safe_mul
        * _clamp(_TOP_GUARD_GLOBAL_TRIM, 0.50, 1.20),
        -1.8,
        0.0,
    )
    top_guard_hi_w = _clamp(
        _TOP_GUARD_HI_W * _clamp(_TOP_GUARD_SOFTNESS, 0.5, 1.5),
        0.8,
        2.6,
    )

    # ---------------------------
    # Section 5: punch keeper + output trim
    # ---------------------------

    punch_on = _MX_PUNCH_ON
    punch_thr = _clamp(_MX_PUNCH_THRESHOLD_DB / intensity_dynamic, -36.0, -12.0)
    punch_ratio = _clamp(_MX_PUNCH_RATIO * intensity_dynamic, 1.0, 1.5)
    punch_att = _clamp(_MX_PUNCH_ATTACK_MS / max(intensity_dynamic, 0.6), 4.0, 60.0)
    punch_rel = _clamp(_MX_PUNCH_RELEASE_MS * intensity_dynamic, 40.0, 260.0)
    punch_knee = _clamp(_MX_PUNCH_KNEE_DB, 0.0, 6.0)
    punch_makeup = _clamp(_MX_PUNCH_MAKEUP_DB, -1.0, 1.0)

    trim_db = _clamp(_MX_TRIM_DB, -6.0, 2.0)

    # build filter graph
    parts = []
    parts.append(f"[0:a]{','.join(serial_parts)}[mx_core]")

    # A. Presence Cleanup Notch -> Lower-Presence Anchor (serial)
    parts.append(
        f"[mx_core]"
        f"equalizer=f={pres_notch_f}:t=q:w={pres_notch_w}:g={pres_notch_g},"
        f"equalizer=f={pres_anchor_f}:t=q:w={pres_anchor_w}:g={pres_anchor_g}"
        f"[mx_pres_anchor]"
    )

    # B. Split after anchor: main + bridge + glue
    parts.append("[mx_pres_anchor]asplit=3[mx_pres_main][mx_pres_bridge_in][mx_pres_glue_in]")

    # C. Upper-Presence Bridge (parallel)
    parts.append(
        f"[mx_pres_bridge_in]"
        f"highpass=f={pres_bridge_hp}:width=0.707,"
        f"lowpass=f={pres_bridge_lp}:width=0.707,"
        f"equalizer=f={pres_bridge_f}:t=q:w={pres_bridge_w}:g={pres_bridge_g},"
        f"equalizer=f={pres_bridge_f}:t=q:w={_clamp(pres_bridge_w * pres_bridge_softness, 0.70, 2.40)}:g={_clamp(0.18 * pres_bridge_g, 0.02, 0.40)},"
        f"volume={pres_bridge_post_trim}dB,"
        f"volume={pres_bridge_mix}"
        f"[mx_pres_bridge]"
    )

    # D. Presence Glue / Micro-Finish (parallel)
    parts.append(
        f"[mx_pres_glue_in]"
        f"equalizer=f={pres_glue_f}:t=q:w={pres_glue_w}:g={pres_glue_g},"
        f"equalizer=f={pres_glue_f}:t=q:w={_clamp(pres_glue_w * pres_glue_softness, 0.90, 3.00)}:g={_clamp(0.12 * pres_glue_g, 0.01, 0.20)},"
        f"volume={pres_glue_mix}"
        f"[mx_pres_glue]"
    )

    # E. Presence Sum -> output to sheen stage
    if abs(pres_sum_trim) > 1e-9:
        parts.append(
            f"[mx_pres_main][mx_pres_bridge][mx_pres_glue]amix=inputs=3:normalize=0,"
            f"volume={pres_sum_trim}dB"
            f"[mx_presence_out]"
        )
    else:
        parts.append(
            "[mx_pres_main][mx_pres_bridge][mx_pres_glue]amix=inputs=3:normalize=0"
            "[mx_presence_out]"
        )

    # F. Split for sheen from new presence output
    parts.append("[mx_presence_out]asplit=2[mx_top_main][mx_top_sheen_in]")

    # G. Sheen Builder (parallel)
    parts.append(
        f"[mx_top_sheen_in]"
        f"highpass=f={top_sheen_hp}:width=0.707,"
        f"lowpass=f={top_sheen_lp}:width=0.707,"
        f"equalizer=f={(top_sheen_hp + top_sheen_lp) / 2.0}:t=q:w=1.10:g={top_sheen_polish_gain},"
        f"{_os_softclip_chain(drive_db=top_sheen_drive, hp=None, lp=None, post_gain_db=0.0)},"
        f"equalizer=f={top_sheen_post_f}:t=q:w={top_sheen_post_w}:g={top_sheen_post_g},"
        f"volume={top_sheen_mix}"
        f"[mx_top_sheen]"
    )

    # H. Sum
    parts.append("[mx_top_main][mx_top_sheen]amix=inputs=2:normalize=0[mx_top_built]")

    # I. Air Completion
    if top_air_tilt > 1e-9:
        parts.append(
            f"[mx_top_built]"
            f"highshelf=f={top_air_f}:g={top_air_g},"
            f"highshelf=f={max(9000.0, top_air_f - 1800.0)}:g={top_air_tilt}"
            f"[mx_top_air]"
        )
    else:
        parts.append(
            f"[mx_top_built]"
            f"highshelf=f={top_air_f}:g={top_air_g}"
            f"[mx_top_air]"
        )

    # J. Safety Guard
    parts.append(
        f"[mx_top_air]"
        f"equalizer=f={top_guard_lo_f}:t=q:w={top_guard_lo_w}:g={top_guard_lo_g},"
        f"equalizer=f={top_guard_hi_f}:t=q:w={top_guard_hi_w}:g={top_guard_hi_g}"
        f"[mx_after_top]"
    )

    # K. Punch keeper
    if punch_on:
        parts.append(
            f"[mx_after_top]"
            f"acompressor=threshold={punch_thr}dB:"
            f"ratio={punch_ratio}:"
            f"attack={punch_att}:"
            f"release={punch_rel}:"
            f"knee={punch_knee}dB:"
            f"makeup={punch_makeup}dB:"
            f"mix=1"
            f"[mx_after_punch]"
        )
    else:
        parts.append("[mx_after_top]anull[mx_after_punch]")

    # L. Output trim
    if abs(trim_db) > 1e-9:
        parts.append(f"[mx_after_punch]volume={trim_db}dB[out]")
    else:
        parts.append("[mx_after_punch]anull[out]")

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

_PREPOST_CLIP_ON = (os.getenv("PREPOST_CLIP_ON", "0").strip() == "1")
_PREPOST_CLIP_DRIVE_DB = float(os.getenv("PREPOST_CLIP_DRIVE_DB", "0.18"))
_PREPOST_CLIP_POST_GAIN_DB = float(os.getenv("PREPOST_CLIP_POST_GAIN_DB", "-0.08"))

_BLEND_POST_I = float(os.getenv("BLEND_POST_I", "-10.8"))
_BLEND_POST_TP = float(os.getenv("BLEND_POST_TP", "-1.0"))
_BLEND_POST_LRA = float(os.getenv("BLEND_POST_LRA", "7.0"))

_BANDLAB_PREVIEW_GAIN_DB = float(os.getenv("BANDLAB_PREVIEW_GAIN_DB", "0.0"))
_BAKUAGE_PREVIEW_GAIN_DB = float(os.getenv("BAKUAGE_PREVIEW_GAIN_DB", "0.0"))
_ENHANCE_PREVIEW_GAIN_DB = float(os.getenv("ENHANCE_PREVIEW_GAIN_DB", "0.0"))

_ART_BASE_GAIN = float(os.getenv("ART_BASE_GAIN", "1.0"))
_ART_REVEAL_GAIN_DB = float(os.getenv("ART_REVEAL_GAIN_DB", "-14.0"))
_ART_POLISH_GAIN_DB = float(os.getenv("ART_POLISH_GAIN_DB", "-16.0"))


def _render_guard_stage(in_path: str, out_path: str):
    if _PREPOST_CLIP_ON:
        cmd = (
            f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
            f'-af "{_os_softclip_chain(drive_db=_PREPOST_CLIP_DRIVE_DB, hp=None, lp=None, post_gain_db=_PREPOST_CLIP_POST_GAIN_DB)}" '
            f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
        )
    else:
        cmd = (
            f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
            f'-af "anull" -ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(out_path)}'
        )
    _run(cmd)


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


def _render_artistic_sum(base_src: str, reveal_src: str, polish_src: str, out_path: str):
    base_gain = _clamp(_ART_BASE_GAIN, 0.5, 1.5)
    reveal_gain_db = _clamp(_ART_REVEAL_GAIN_DB, -36.0, 6.0)
    polish_gain_db = _clamp(_ART_POLISH_GAIN_DB, -36.0, 6.0)

    fc = (
        f"[0:a]volume={base_gain}[base];"
        f"[1:a]volume={reveal_gain_db}dB[reveal];"
        f"[2:a]volume={polish_gain_db}dB[polish];"
        f"[base][reveal][polish]amix=inputs=3:normalize=0[out]"
    )

    cmd = (
        f'ffmpeg -y -hide_banner '
        f'-i {shlex.quote(base_src)} '
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
    bandlab_pre_wav = os.path.join(td, f"{branch_kind}_bandlab_pre.wav")
    guarded_wav = os.path.join(td, f"{branch_kind}_guarded.wav")

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

    fc = (
        f"[0:a]volume=1[base];"
        f"[1:a]volume={preview_gain_db}dB[br];"
        f"[base][br]amix=inputs=2:normalize=0[out]"
    )

    cmd = (
        f'ffmpeg -y -hide_banner '
        f'-i {shlex.quote(base_wav)} '
        f'-i {shlex.quote(branch_wav)} '
        f'-filter_complex "{fc}" -map "[out]" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(bandlab_pre_wav)}'
    )
    _run(cmd)

    _render_guard_stage(bandlab_pre_wav, guarded_wav)

    out_path, out_name = _render_post_stage(guarded_wav, fmt=fmt, td=td, loudnorm_params=None)
    final_name = f"{preview_name}_{out_name}"
    final_path = os.path.join(td, final_name)
    os.replace(out_path, final_path)
    return final_path, final_name


def _render_artistic_blend(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    base_wav = os.path.join(td, "art_base.wav")
    reveal_wav = os.path.join(td, "art_reveal.wav")
    polish_wav = os.path.join(td, "art_polish.wav")
    artistic_pre_wav = os.path.join(td, "artistic_pre.wav")
    artistic_guarded_wav = os.path.join(td, "artistic_guarded.wav")

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-af "{_PRE_CLEAN_CHAIN}" -ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(base_wav)}'
    )
    _run(cmd)

    reveal_wav, _ = _render_reveal_branch(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)
    polish_wav, _ = _render_polish_branch(in_path, tone=tone, intensity=intensity, fmt="wav16", td=td)

    _render_artistic_sum(base_wav, reveal_wav, polish_wav, artistic_pre_wav)
    _render_guard_stage(artistic_pre_wav, artistic_guarded_wav)

    out_path, out_name = _render_post_stage(artistic_guarded_wav, fmt=fmt, td=td, loudnorm_params=None)

    artistic_name = f"artistic_blend_{out_name}"
    artistic_path = os.path.join(td, artistic_name)
    os.replace(out_path, artistic_path)
    return artistic_path, artistic_name


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


# ---------------------------
# DIAGNOSTICS
# ---------------------------

def _render_bandlab_diagnostic_bundle(
    in_path: str,
    tone: str,
    intensity: str,
    fmt: str,
    td: str,
):
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    base_wav = os.path.join(td, "diag_bandlab_base.wav")
    reveal_wav = os.path.join(td, "diag_bandlab_reveal.wav")
    bandlab_pre_wav = os.path.join(td, "diag_bandlab_pre.wav")
    guarded_wav = os.path.join(td, "diag_bandlab_guarded.wav")

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-af "{_PRE_CLEAN_CHAIN}" -ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(base_wav)}'
    )
    _run(cmd)

    reveal_wav, reveal_name = _render_reveal_branch(
        in_path,
        tone=tone,
        intensity=intensity,
        fmt="wav16",
        td=td,
    )

    preview_gain_db = _clamp(_BANDLAB_PREVIEW_GAIN_DB, -24.0, 18.0)
    fc = (
        f"[0:a]volume=1[base];"
        f"[1:a]volume={preview_gain_db}dB[br];"
        f"[base][br]amix=inputs=2:normalize=0[out]"
    )

    cmd = (
        f'ffmpeg -y -hide_banner '
        f'-i {shlex.quote(base_wav)} '
        f'-i {shlex.quote(reveal_wav)} '
        f'-filter_complex "{fc}" -map "[out]" '
        f'-ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(bandlab_pre_wav)}'
    )
    _run(cmd)

    _render_guard_stage(bandlab_pre_wav, guarded_wav)

    final_path, final_name = _render_post_stage(guarded_wav, fmt=fmt, td=td, loudnorm_params=None)

    return {
        "before": in_path,
        "base_prepared": base_wav,
        "reveal_branch": reveal_wav,
        "bandlab_pre": bandlab_pre_wav,
        "bandlab_guarded": guarded_wav,
        "final_post": final_path,
        "reveal_branch_name": reveal_name,
        "final_name": final_name,
    }


def _collect_bandlab_diagnostic_report(stage_paths: dict) -> dict:
    order = ["before", "base_prepared", "reveal_branch", "bandlab_pre", "bandlab_guarded", "final_post"]

    stages = {}
    for key in order:
        path = stage_paths[key]
        stages[key] = {
            "file": os.path.basename(path),
            "metrics": _collect_stage_metrics(path),
        }

    input_metrics = stages["before"]["metrics"]

    for idx, key in enumerate(order):
        cur_metrics = stages[key]["metrics"]
        stages[key]["delta_vs_input"] = _metric_deltas(input_metrics, cur_metrics)
        if idx == 0:
            stages[key]["delta_vs_prev"] = None
        else:
            prev_metrics = stages[order[idx - 1]]["metrics"]
            stages[key]["delta_vs_prev"] = _metric_deltas(prev_metrics, cur_metrics)

    return stages


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
            "/artistic_blend",
            "/bandlab_branch",
            "/bakuage_branch",
            "/enhance_branch",
            "/bandlab_diagnostics",
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

        "PL_CLEAN_ON": os.getenv("PL_CLEAN_ON"),
        "PL_CLEAN_F1": os.getenv("PL_CLEAN_F1"),
        "PL_CLEAN_G1": os.getenv("PL_CLEAN_G1"),
        "PL_CLEAN_F2": os.getenv("PL_CLEAN_F2"),
        "PL_CLEAN_G2": os.getenv("PL_CLEAN_G2"),
        "PL_CLEAN_F3": os.getenv("PL_CLEAN_F3"),
        "PL_CLEAN_G3": os.getenv("PL_CLEAN_G3"),

        "PL_PROJ_ON": os.getenv("PL_PROJ_ON"),
        "PL_PROJ_F1": os.getenv("PL_PROJ_F1"),
        "PL_PROJ_G1": os.getenv("PL_PROJ_G1"),
        "PL_PROJ_F2": os.getenv("PL_PROJ_F2"),
        "PL_PROJ_G2": os.getenv("PL_PROJ_G2"),
        "PL_PROJ_F3": os.getenv("PL_PROJ_F3"),
        "PL_PROJ_G3": os.getenv("PL_PROJ_G3"),

        "PL_PUNCH_ON": os.getenv("PL_PUNCH_ON"),
        "PL_PUNCH_MODE": os.getenv("PL_PUNCH_MODE"),
        "PL_PUNCH_THRESHOLD_DB": os.getenv("PL_PUNCH_THRESHOLD_DB"),
        "PL_PUNCH_RATIO": os.getenv("PL_PUNCH_RATIO"),

        "PL_EDGE_ON": os.getenv("PL_EDGE_ON"),
        "PL_EDGE_HP_HZ": os.getenv("PL_EDGE_HP_HZ"),
        "PL_EDGE_LP_HZ": os.getenv("PL_EDGE_LP_HZ"),
        "PL_EDGE_DRIVE_DB": os.getenv("PL_EDGE_DRIVE_DB"),
        "PL_EDGE_MIX": os.getenv("PL_EDGE_MIX"),

        "PL_AIR_ON": os.getenv("PL_AIR_ON"),
        "PL_AIR_F": os.getenv("PL_AIR_F"),
        "PL_AIR_G": os.getenv("PL_AIR_G"),

        "PL_WIDTH_ON": os.getenv("PL_WIDTH_ON"),
        "PL_WIDTH_HP_HZ": os.getenv("PL_WIDTH_HP_HZ"),
        "PL_WIDTH_M": os.getenv("PL_WIDTH_M"),
        "PL_WIDTH_MIX": os.getenv("PL_WIDTH_MIX"),

        "PL_TRIM_DB": os.getenv("PL_TRIM_DB"),

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

        "ART_BASE_GAIN": os.getenv("ART_BASE_GAIN"),
        "ART_REVEAL_GAIN_DB": os.getenv("ART_REVEAL_GAIN_DB"),
        "ART_POLISH_GAIN_DB": os.getenv("ART_POLISH_GAIN_DB"),
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


@app.get("/bandlab_diagnostics")
def bandlab_diagnostics_route():
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
            in_path, dbg = _dl_to_named(td, "file", url)

            stage_paths = _render_bandlab_diagnostic_bundle(
                in_path=in_path,
                tone=tone,
                intensity=intensity,
                fmt=fmt,
                td=td,
            )
            stages = _collect_bandlab_diagnostic_report(stage_paths)

            return jsonify({
                "mode": "bandlab_diagnostics",
                "reveal_params": {
                    "RV_MID_G": _RV_MID_G,
                    "RV_PRES_G": _RV_PRES_G,
                    "RV_CORE_MIX": _RV_CORE_MIX,
                    "RV_EXCITE_DRIVE_DB": _RV_EXCITE_DRIVE_DB,
                    "RV_EXCITE_MIX": _RV_EXCITE_MIX,
                    "RV_AIR_G": _RV_AIR_G,
                    "RV_AIR_MIX": _RV_AIR_MIX,
                    "RV_WIDTH_M": _RV_WIDTH_M,
                    "RV_WIDTH_MIX": _RV_WIDTH_MIX,
                    "RV_GUARD_G": _RV_GUARD_G,
                    "RV_SIB_G": _RV_SIB_G,
                    "BANDLAB_PREVIEW_GAIN_DB": _BANDLAB_PREVIEW_GAIN_DB,
                    "PREPOST_CLIP_ON": _PREPOST_CLIP_ON,
                    "PREPOST_CLIP_DRIVE_DB": _PREPOST_CLIP_DRIVE_DB,
                    "PREPOST_CLIP_POST_GAIN_DB": _PREPOST_CLIP_POST_GAIN_DB,
                    "BLEND_POST_I": _BLEND_POST_I,
                    "BLEND_POST_TP": _BLEND_POST_TP,
                    "BLEND_POST_LRA": _BLEND_POST_LRA,
                },
                "stages": stages,
                "debug": dbg,
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
            out_path, out_name = _render_reveal_branch(
                in_path, tone=tone, intensity=intensity, fmt=fmt, td=td
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
            out_path, out_name = _render_low_support_branch(
                in_path, tone=tone, intensity=intensity, fmt=fmt, td=td
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
            out_path, out_name = _render_polish_branch(
                in_path, tone=tone, intensity=intensity, fmt=fmt, td=td
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


@app.get("/artistic_blend")
def artistic_blend_route():
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
            out_path, out_name = _render_artistic_blend(in_path, tone=tone, intensity=intensity, fmt=fmt, td=td)
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

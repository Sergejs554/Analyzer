#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify, send_file
import os, tempfile, requests, re, subprocess, shlex, json
from analyze_mastering import run_analysis
from auto_analysis import analyze_sections  # секционный анализ
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
    if content_type:
        ct = content_type.lower()
        if "audio/wav" in ct or "audio/x-wav" in ct: return ".wav"
        if "audio/mpeg" in ct: return ".mp3"
        if "audio/mp4" in ct or "audio/x-m4a" in ct: return ".m4a"
        if "audio/flac" in ct: return ".flac"
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

# === изменено ===
# Кроссфейд между секциями, чтобы убрать "штыки/провалы" на стыках
_XFADE_SEC = 0.25
_XFADE_MIN = 0.05

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
    # loudnorm=...:print_format=summary -> json
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
    We run loudnorm only (post section-EQ/comp), so it stays stable.
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
    if fmt == "mp3_320" or fmt == "mp3":
        return "-ar 48000 -ac 2 -c:a libmp3lame -b:a 320k", "mastered_320.mp3", "audio/mpeg"
    # default wav16
    return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav", "audio/wav"

# === изменено ===
def _xfade_duration(prev_start: float, prev_end: float, next_start: float, next_end: float) -> float:
    prev_len = max(0.0, prev_end - prev_start)
    next_len = max(0.0, next_end - next_start)
    d = min(_XFADE_SEC, prev_len * 0.45, next_len * 0.45)
    return float(max(_XFADE_MIN, d))

def _build_section_filter_complex(section_params: dict) -> str:
    """
    We apply ONLY pre-loudnorm part per section: pre_clean + (eq/comp/widen) WITHOUT loudnorm.
    Final loudnorm is global 2-pass after concatenation.

    === изменено ===
    Вместо concat используем acrossfade с overlap, чтобы убрать стыки.
    """
    base = section_params["base_params"]
    sections = section_params["sections"]

    # Build per-section chain WITHOUT loudnorm
    base_chain = build_smart_chain(base)
    pre_chain, _ = _strip_loudnorm(base_chain)

    # If we have no sections, just apply global pre_clean + pre_chain
    if not sections:
        return f"[0:a]{_PRE_CLEAN_CHAIN},{pre_chain}[aout]"

    # normalize section ordering just in case
    sections = sorted(sections, key=lambda s: float(s.get("start", 0.0)))

    seg_filters = []
    seg_labels = []

    n = len(sections)
    for i, s in enumerate(sections):
        sp = s["params"]
        ch = build_smart_chain(sp)
        ch_pre, _ = _strip_loudnorm(ch)

        s_start = max(0.0, float(s.get("start", 0.0)))
        s_end = max(s_start + 0.01, float(s.get("end", 0.0)))

        # overlap: extend each segment a bit into neighbors
        left = _XFADE_SEC if i > 0 else 0.0
        right = _XFADE_SEC if i < (n - 1) else 0.0

        start = max(0.0, s_start - left)
        end = max(start + 0.02, s_end + right)

        lbl = f"s{i}"

        seg_filters.append(
            f"[0:a]atrim=start={start}:end={end},asetpts=PTS-STARTPTS,{_PRE_CLEAN_CHAIN},{ch_pre}[{lbl}]"
        )
        seg_labels.append(lbl)

    # chain acrossfades
    if len(seg_labels) == 1:
        return ";".join(seg_filters + [f"[{seg_labels[0]}]anull[aout]"])

    xfade_filters = []
    cur = seg_labels[0]
    for i in range(1, len(seg_labels)):
        prev_sec = sections[i - 1]
        next_sec = sections[i]
        prev_start = max(0.0, float(prev_sec.get("start", 0.0)))
        prev_end = max(prev_start + 0.01, float(prev_sec.get("end", 0.0)))
        next_start = max(0.0, float(next_sec.get("start", 0.0)))
        next_end = max(next_start + 0.01, float(next_sec.get("end", 0.0)))

        d = _xfade_duration(prev_start, prev_end, next_start, next_end)

        out_lbl = "aout" if i == (len(seg_labels) - 1) else f"x{i}"
        xfade_filters.append(
            f"[{cur}][{seg_labels[i]}]acrossfade=d={d}:c1=tri:c2=tri[{out_lbl}]"
        )
        cur = out_lbl

    return ";".join(seg_filters + xfade_filters)

def _render_master(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    """
    Pipeline:
      analyze_sections -> smart params (base+sections) -> section processing -> acrossfade -> final loudnorm 2-pass -> output
    """
    # 1) Analyze (global + sections)
    sec = analyze_sections(in_path, target_sr=48000)
    global_a = sec["global"]
    sections = sec.get("sections") or []

    # 2) Decide params (base + section params) with user tone/intensity biases
    sp = decide_smart_params_with_sections(
        global_analysis=global_a,
        sections=sections,
        intensity=(intensity or "balanced"),
        tone_mode=(tone or "balanced"),
    )

    # 3) Render intermediate with section EQ/comp (NO loudnorm)
    interm = os.path.join(td, "intermediate.wav")
    fc = _build_section_filter_complex(sp)
    cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -filter_complex "{fc}" -map "[aout]" -ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(interm)}'
    _run(cmd)

    # 4) Final loudnorm 2-pass (global) with base targets
    out_args, out_name, _mime = _out_args(fmt)
    out_path = os.path.join(td, out_name)
    _build_loudnorm_two_pass(interm, sp["base_params"]["loudnorm"], out_args, out_path)

    return out_path, out_name

# --- routes ---

@app.get("/")
def root():
    return jsonify({
        "ok": True,
        "service": "analysis_mastering_api",
        "endpoints": ["/health", "/analyze", "/analyze_sections", "/compare_sections", "/master"]
    })

@app.get("/health")
def health():
    return jsonify({"ok": True})

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
    """
    MR MASTERING v2 — единственный endpoint для прод-мастеринга.

    Usage:
      /master?file=<url>&tone=warm|balanced|bright&intensity=low|balanced|high&format=wav16|wav24|flac|mp3_320

    Returns: mastered file as binary download.
    """
    url = request.args.get("file")
    if not url:
        return jsonify({"error": "provide ?file=<url>"}), 400

    tone = (request.args.get("tone") or "balanced").lower()
    intensity = (request.args.get("intensity") or "balanced").lower()
    fmt = (request.args.get("format") or "wav16").lower()

    if tone not in ("warm", "balanced", "bright"):
        tone = "balanced"
    if intensity not in ("low", "balanced", "high"):
        intensity = "balanced"
    if fmt not in ("wav16", "wav24", "flac", "mp3_320", "mp3"):
        fmt = "wav16"

    if is_gdrive(url):
        url = gdrive_direct(url)

    try:
        with tempfile.TemporaryDirectory() as td:
            in_path, dbg = _dl_to_named(td, "file", url)

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

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
# Кроссфейд делаем КОРОТКИЙ, чтобы не было "двойного слога" и смаза атак
_XFADE_SEC = 0.06
_XFADE_MIN = 0.02
# Снэп границ секций, чтобы убрать микро-щели/перехлёсты от округлений анализа
_SNAP_SEC = 0.20
# Если вдруг осталась большая щель, acrossfade выключаем (иначе будет странно)
_MAX_GAP_FOR_XFADE = 0.05

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
    if fmt == "mp3_320" or fmt == "mp3":
        return "-ar 48000 -ac 2 -c:a libmp3lame -b:a 320k", "mastered_320.mp3", "audio/mpeg"
    return "-ar 48000 -ac 2 -c:a pcm_s16le", "mastered.wav", "audio/wav"

# === изменено ===
def _xfade_duration(prev_len: float, next_len: float) -> float:
    d = min(_XFADE_SEC, prev_len * 0.25, next_len * 0.25)
    return float(max(_XFADE_MIN, d))

# === изменено ===
def _sanitize_sections(sections: list[dict]) -> list[dict]:
    """
    1) сортируем
    2) снэпим start следующей секции к end предыдущей, если они рядом (убираем микро-дырки/перехлёсты)
    3) гарантируем минимальную длину
    """
    if not sections:
        return []
    secs = sorted(sections, key=lambda s: float(s.get("start", 0.0)))

    out = []
    for i, s in enumerate(secs):
        st = max(0.0, float(s.get("start", 0.0)))
        en = max(st + 0.02, float(s.get("end", 0.0)))

        if out:
            prev = out[-1]
            prev_en = float(prev["end"])
            # если граница рядом - снэпим, чтобы не было дырок/двойных кусков
            if abs(st - prev_en) <= _SNAP_SEC:
                st = prev_en
                en = max(st + 0.02, en)

        ns = dict(s)
        ns["start"] = float(st)
        ns["end"] = float(en)
        out.append(ns)
    return out

def _build_section_filter_complex(section_params: dict) -> str:
    """
    ONLY pre-loudnorm part per section: pre_clean + (eq/comp/widen) WITHOUT loudnorm.
    Final loudnorm is global 2-pass after stitching.

    === изменено ===
    Правильная схема:
      - НЕ расширяем сегменты влево/вправо
      - НЕ используем concat вместе с acrossfade
      - делаем последовательный acrossfade: (s0+s1)->x1; (x1+s2)->x2; ...
    """
    base = section_params["base_params"]
    sections = _sanitize_sections(section_params["sections"])

    base_chain = build_smart_chain(base)
    pre_chain, _ = _strip_loudnorm(base_chain)

    if not sections:
        return f"[0:a]{_PRE_CLEAN_CHAIN},{pre_chain}[aout]"

    seg_filters = []
    seg_labels = []

    for i, s in enumerate(sections):
        sp = s["params"]
        ch = build_smart_chain(sp)
        ch_pre, _ = _strip_loudnorm(ch)

        start = float(s["start"])
        end = float(s["end"])
        lbl = f"s{i}"

        seg_filters.append(
            f"[0:a]atrim=start={start}:end={end},asetpts=PTS-STARTPTS,{_PRE_CLEAN_CHAIN},{ch_pre}[{lbl}]"
        )
        seg_labels.append(lbl)

    if len(seg_labels) == 1:
        return ";".join(seg_filters + [f"[{seg_labels[0]}]anull[aout]"])

    xfade_filters = []
    cur = seg_labels[0]

    for i in range(1, len(seg_labels)):
        prev = sections[i - 1]
        nxt = sections[i]
        prev_len = float(prev["end"] - prev["start"])
        next_len = float(nxt["end"] - nxt["start"])

        # если вдруг между секциями есть большая дырка - acrossfade только навредит
        gap = float(nxt["start"] - prev["end"])
        if gap > _MAX_GAP_FOR_XFADE:
            # fallback: просто склеиваем через concat=2 (без overlap)
            out_lbl = "aout" if i == (len(seg_labels) - 1) else f"x{i}"
            xfade_filters.append(
                f"[{cur}][{seg_labels[i]}]concat=n=2:v=0:a=1[{out_lbl}]"
            )
            cur = out_lbl
            continue

        d = _xfade_duration(prev_len, next_len)
        out_lbl = "aout" if i == (len(seg_labels) - 1) else f"x{i}"
        xfade_filters.append(
            f"[{cur}][{seg_labels[i]}]acrossfade=d={d}:c1=tri:c2=tri[{out_lbl}]"
        )
        cur = out_lbl

    return ";".join(seg_filters + xfade_filters)

def _render_master(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    sec = analyze_sections(in_path, target_sr=48000)
    global_a = sec["global"]
    sections = sec.get("sections") or []

    sp = decide_smart_params_with_sections(
        global_analysis=global_a,
        sections=sections,
        intensity=(intensity or "balanced"),
        tone_mode=(tone or "balanced"),
    )

    interm = os.path.join(td, "intermediate.wav")
    fc = _build_section_filter_complex(sp)
    cmd = f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} -filter_complex "{fc}" -map "[aout]" -ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(interm)}'
    _run(cmd)

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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify
import os, tempfile, requests, re
from urllib.parse import urlparse, parse_qs
from analyze_mastering import run_analysis

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
    """
    Convert Google Drive share/view link -> direct download endpoint.
    Handles both /file/d/<id>/view and open?id=<id>
    """
    fid = gdrive_file_id(url)
    if not fid:
        return url
    return f"https://drive.google.com/uc?export=download&id={fid}"

def guess_ext(url: str, content_type: str | None) -> str:
    """
    Ensure we save files WITH extension so librosa/soundfile backends work.
    """
    u = (url or "").lower()
    if ".wav" in u: return ".wav"
    if ".mp3" in u: return ".mp3"
    if ".m4a" in u: return ".m4a"
    if content_type:
        ct = content_type.lower()
        if "audio/wav" in ct or "audio/x-wav" in ct: return ".wav"
        if "audio/mpeg" in ct: return ".mp3"
        if "audio/mp4" in ct or "audio/x-m4a" in ct: return ".m4a"
    # default
    return ".wav"

def download_file(url: str, out_path: str, timeout: int = 120) -> tuple[int, str]:
    """
    Download a file, following redirects.
    If Google Drive returns an interstitial confirmation page, try to pass confirm token.
    Returns: (bytes, final_url)
    """
    sess = requests.Session()
    r = sess.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()

    # Google Drive sometimes returns HTML with confirm token
    ct = (r.headers.get("Content-Type") or "").lower()
    if "text/html" in ct and "drive.google.com" in (r.url or ""):
        # try to find confirm token in cookies
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

    # still HTML? then it's not a direct audio file
    if "text/html" in ct:
        raise RuntimeError(f"Downloaded HTML instead of audio. URL probably not direct-download. final_url={r.url}")

    with open(out_path, "wb") as f:
        f.write(r.content)

    return len(r.content), (r.url or url)

# --- routes ---

@app.get("/")
def root():
    return jsonify({"ok": True, "service": "analysis_mastering_api", "endpoints": ["/health", "/analyze"]})

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.get("/analyze")
def analyze():
    before = request.args.get("before")
    after = request.args.get("after")
    if not before or not after:
        return jsonify({"error": "provide ?before=<url>&after=<url>"}), 400

    # convert gdrive view links to direct
    if is_gdrive(before):
        before = gdrive_direct(before)
    if is_gdrive(after):
        after = gdrive_direct(after)

    try:
        with tempfile.TemporaryDirectory() as td:
            # download BEFORE
            b_tmp = os.path.join(td, "before.tmp")
            b_size, b_final = download_file(before, b_tmp)
            # decide ext by final url / content type (we only have final url now)
            b_ext = guess_ext(b_final, None)
            b_path = os.path.join(td, "before" + b_ext)
            os.replace(b_tmp, b_path)

            # download AFTER
            a_tmp = os.path.join(td, "after.tmp")
            a_size, a_final = download_file(after, a_tmp)
            a_ext = guess_ext(a_final, None)
            a_path = os.path.join(td, "after" + a_ext)
            os.replace(a_tmp, a_path)

            report, suggestion = run_analysis(b_path, a_path, os.path.join(td, "out"))
            return jsonify({
                "report": report,
                "preset_suggestion": suggestion,
                "debug": {
                    "before_bytes": b_size,
                    "after_bytes": a_size,
                    "before_final_url": b_final,
                    "after_final_url": a_final,
                    "before_file": os.path.basename(b_path),
                    "after_file": os.path.basename(a_path),
                }
            })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

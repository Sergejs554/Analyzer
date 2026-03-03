#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify
import os, tempfile, requests, re
from analyze_mastering import run_analysis

app = Flask(__name__)

MAX_MB = int(os.environ.get("MAX_DOWNLOAD_MB", "200"))  # защита от гигабайт

GDRIVE_FILE_RX = re.compile(r"(?:https?://)?drive\.google\.com/file/d/([A-Za-z0-9_-]+)")
GDRIVE_UC_RX   = re.compile(r"(?:https?://)?drive\.google\.com/uc\?(?:.*&)?id=([A-Za-z0-9_-]+)")

def gdrive_direct(url: str) -> str:
    m = GDRIVE_FILE_RX.search(url) or GDRIVE_UC_RX.search(url)
    if not m:
        return url
    file_id = m.group(1)
    return f"https://drive.google.com/uc?export=download&id={file_id}"

def download_file(url: str, dst_path: str, timeout: int = 120) -> int:
    """
    Robust download:
    - follows redirects
    - handles Google Drive confirm token for large files
    - streams to disk
    """
    url = url.strip()
    # Normalize google drive links
    url = gdrive_direct(url)

    sess = requests.Session()

    def _stream_get(u: str):
        return sess.get(u, stream=True, allow_redirects=True, timeout=timeout)

    r = _stream_get(url)

    # If Google Drive returns HTML confirmation page, extract confirm token and retry
    ct = (r.headers.get("Content-Type") or "").lower()
    if "text/html" in ct:
        html = r.text
        # patterns that appear in Drive confirm pages
        m = re.search(r"confirm=([0-9A-Za-z_]+)", html)
        if m:
            token = m.group(1)
            sep = "&" if "?" in url else "?"
            url2 = f"{url}{sep}confirm={token}"
            r = _stream_get(url2)

    r.raise_for_status()

    total = 0
    with open(dst_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1 << 15):
            if not chunk:
                continue
            total += len(chunk)
            if total > MAX_MB * 1024 * 1024:
                raise RuntimeError(f"Remote file too big (> {MAX_MB} MB)")
            f.write(chunk)

    return total

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def root():
    return jsonify({"ok": True, "routes": ["/health", "/analyze?before=<url>&after=<url>"]})

@app.get("/analyze")
def analyze():
    before = request.args.get("before")
    after = request.args.get("after")
    if not before or not after:
        return jsonify({"error":"provide ?before=<url>&after=<url>"}), 400

    try:
        with tempfile.TemporaryDirectory() as td:
            b_path = os.path.join(td, "before")
            a_path = os.path.join(td, "after")

            download_file(before, b_path)
            download_file(after, a_path)

            report, suggestion = run_analysis(b_path, a_path, os.path.join(td, "out"))
            return jsonify({"report": report, "preset_suggestion": suggestion})

    except Exception as e:
        # чтобы Railway логи показывали причину
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

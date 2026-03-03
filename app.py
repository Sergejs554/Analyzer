#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify
import os, tempfile, requests, re
from analyze_mastering import run_analysis

app = Flask(__name__)

MAX_MB = int(os.environ.get("MAX_DOWNLOAD_MB", "200"))

GDRIVE_FILE_RX = re.compile(r"(?:https?://)?drive\.google\.com/file/d/([A-Za-z0-9_-]+)")
GDRIVE_UC_RX   = re.compile(r"(?:https?://)?drive\.google\.com/uc\?(?:.*&)?id=([A-Za-z0-9_-]+)")

def gdrive_direct(url: str) -> str:
    m = GDRIVE_FILE_RX.search(url) or GDRIVE_UC_RX.search(url)
    if not m:
        return url
    file_id = m.group(1)
    return f"https://drive.google.com/uc?export=download&id={file_id}"

def detect_ext(first_bytes: bytes) -> str:
    # WAV
    if len(first_bytes) >= 12 and first_bytes[0:4] == b"RIFF" and first_bytes[8:12] == b"WAVE":
        return ".wav"
    # FLAC
    if first_bytes.startswith(b"fLaC"):
        return ".flac"
    # OGG
    if first_bytes.startswith(b"OggS"):
        return ".ogg"
    # MP3 (ID3 or frame sync)
    if first_bytes.startswith(b"ID3") or (len(first_bytes) >= 2 and first_bytes[0] == 0xFF and (first_bytes[1] & 0xE0) == 0xE0):
        return ".mp3"
    # MP4/M4A (ftyp)
    if len(first_bytes) >= 8 and first_bytes[4:8] == b"ftyp":
        return ".m4a"
    return ".bin"

def download_file(url: str, dst_base_path: str, timeout: int = 120) -> str:
    """
    Robust download:
    - handles Google Drive confirm token
    - streams to disk
    - detects format and saves with proper extension
    Returns final filepath.
    """
    url = gdrive_direct(url.strip())
    sess = requests.Session()

    def _stream_get(u: str):
        return sess.get(u, stream=True, allow_redirects=True, timeout=timeout)

    r = _stream_get(url)

    # Google Drive confirm token
    ct = (r.headers.get("Content-Type") or "").lower()
    if "text/html" in ct:
        html = r.text
        m = re.search(r"confirm=([0-9A-Za-z_]+)", html)
        if m:
            token = m.group(1)
            sep = "&" if "?" in url else "?"
            r = _stream_get(f"{url}{sep}confirm={token}")

    r.raise_for_status()

    # read a little for format detection
    it = r.iter_content(chunk_size=1 << 15)
    first = b""
    try:
        first = next(it)
    except StopIteration:
        raise RuntimeError("Empty download")

    ext = detect_ext(first[:16])
    final_path = dst_base_path + ext

    total = 0
    with open(final_path, "wb") as f:
        f.write(first)
        total += len(first)

        for chunk in it:
            if not chunk:
                continue
            total += len(chunk)
            if total > MAX_MB * 1024 * 1024:
                raise RuntimeError(f"Remote file too big (> {MAX_MB} MB)")
            f.write(chunk)

    return final_path

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
            b_base = os.path.join(td, "before")
            a_base = os.path.join(td, "after")

            b_path = download_file(before, b_base)
            a_path = download_file(after, a_base)

            report, suggestion = run_analysis(b_path, a_path, os.path.join(td, "out"))
            return jsonify({"report": report, "preset_suggestion": suggestion})

    except Exception as e:
        return jsonify({"error": repr(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify
import os, tempfile, requests
from urllib.parse import urlparse
from analyze_mastering import run_analysis

app = Flask(__name__)

# === изменено ===
MAX_DOWNLOAD_MB = int(os.getenv("MAX_DOWNLOAD_MB", "60"))  # лимит на каждый файл

ALLOWED_EXT = (".wav", ".mp3", ".m4a", ".flac", ".aac", ".ogg")

def _safe_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def _ext_from_url(u: str) -> str:
    path = urlparse(u).path.lower()
    for ext in ALLOWED_EXT:
        if path.endswith(ext):
            return ext
    # если не распознали – пусть будет wav, так чаще всего ок
    return ".wav"

def _download_to(url: str, dst_path: str, timeout: int = 120) -> None:
    with requests.get(url, stream=True, timeout=timeout) as r:
        if r.status_code != 200:
            raise RuntimeError(f"download failed: {url} status={r.status_code}")

        # если сервер отдаёт длину – проверим заранее
        cl = r.headers.get("Content-Length")
        if cl:
            try:
                if int(cl) > MAX_DOWNLOAD_MB * 1024 * 1024:
                    raise RuntimeError(f"file too big (> {MAX_DOWNLOAD_MB} MB): {url}")
            except Exception:
                pass

        total = 0
        with open(dst_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):
                if not chunk:
                    continue
                total += len(chunk)
                if total > MAX_DOWNLOAD_MB * 1024 * 1024:
                    raise RuntimeError(f"file too big (> {MAX_DOWNLOAD_MB} MB): {url}")
                f.write(chunk)
# === конец изменения ===

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/analyze")
def analyze():
    before = request.args.get("before")
    after = request.args.get("after")
    if not before or not after:
        return jsonify({"error": "provide ?before=<url>&after=<url>"}), 400

    # === изменено ===
    if not _safe_url(before) or not _safe_url(after):
        return jsonify({"error": "invalid url (must be http/https)"}), 400
    # === конец изменения ===

    with tempfile.TemporaryDirectory() as td:
        # === изменено ===
        b_ext = _ext_from_url(before)
        a_ext = _ext_from_url(after)
        b_path = os.path.join(td, f"before{b_ext}")
        a_path = os.path.join(td, f"after{a_ext}")
        # download
        try:
            _download_to(before, b_path)
            _download_to(after, a_path)
        except Exception as e:
            return jsonify({"error": str(e)}), 502
        # === конец изменения ===

        # run
        report, suggestion = run_analysis(b_path, a_path, os.path.join(td, "out"))
        return jsonify({"report": report, "preset_suggestion": suggestion})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

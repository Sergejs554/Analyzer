#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify
import os, tempfile, requests
from analyze_mastering import run_analysis

app = Flask(__name__)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/analyze")
def analyze():
    before = request.args.get("before")
    after = request.args.get("after")
    if not before or not after:
        return jsonify({"error":"provide ?before=<url>&after=<url>"}), 400

    with tempfile.TemporaryDirectory() as td:
        b_path = os.path.join(td, "before")
        a_path = os.path.join(td, "after")
        # download
        for url, path in [(before, b_path), (after, a_path)]:
            r = requests.get(url, timeout=120)
            if r.status_code != 200:
                return jsonify({"error": f"download failed: {url}", "status": r.status_code}), 502
            with open(path, "wb") as f:
                f.write(r.content)
        # run
        report, suggestion = run_analysis(b_path, a_path, os.path.join(td, "out"))
        return jsonify({"report": report, "preset_suggestion": suggestion})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

# sm/render.py

import inspect
import os
import shlex
import subprocess
from dataclasses import fields, is_dataclass
from typing import Any

from .analysis import analyze_sm_input
from .selector import select_sm_profiles
from .router import build_sm_router_summary
from .contracts import SmartMasterDebugBundle
from .precondition import build_neutral_preclean_chain
from .dsp.assembler import assemble_sm_dsp_blueprint
from .dsp.render_builder import build_dsp_render_plan
from .dsp.executor import execute_dsp_render_plan


def _run(cmd: str) -> tuple[str, str]:
    p = subprocess.run(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if p.returncode != 0:
        raise RuntimeError(p.stderr.decode("utf-8", errors="ignore")[:4000])
    return (
        p.stdout.decode("utf-8", errors="ignore"),
        p.stderr.decode("utf-8", errors="ignore"),
    )


def _normalize_tone(x: str) -> str:
    x = (x or "balanced").lower().strip()
    return x if x in ("warm", "balanced", "bright") else "balanced"


def _normalize_intensity(x: str) -> str:
    x = (x or "balanced").lower().strip()
    if x in ("low", "soft"):
        return "low"
    if x in ("high", "hard"):
        return "high"
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


def _render_neutral_preclean(input_path: str, td: str, enable_afftdn: bool = False) -> str:
    prepared_path = os.path.join(td, "sm_prepared_input.wav")
    chain = build_neutral_preclean_chain(enable_afftdn=enable_afftdn)

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(input_path)} '
        f'-af "{chain}" -ar 48000 -ac 2 -c:a pcm_s16le {shlex.quote(prepared_path)}'
    )
    _run(cmd)
    return prepared_path


def _build_debug_bundle(
    analysis: Any,
    selection: Any,
    router: Any,
    dsp: Any,
    render_plan: Any,
    render_execution_report: Any,
):
    payload = {
        "analysis": analysis,
        "selection": selection,
        "router": router,
        "dsp": dsp,
        "render_plan": render_plan,
        "render_execution_report": render_execution_report,
    }

    if is_dataclass(SmartMasterDebugBundle):
        allowed = {f.name for f in fields(SmartMasterDebugBundle)}
        payload = {k: v for k, v in payload.items() if k in allowed}
        return SmartMasterDebugBundle(**payload)

    try:
        sig = inspect.signature(SmartMasterDebugBundle)
        allowed = set(sig.parameters.keys())
        payload = {k: v for k, v in payload.items() if k in allowed}
        return SmartMasterDebugBundle(**payload)
    except Exception:
        return SmartMasterDebugBundle(
            analysis=analysis,
            selection=selection,
            router=router,
        )


def render_sm_core_v1(
    input_path: str,
    tone: str,
    intensity: str,
    fmt: str,
    td: str,
    use_neutral_preclean: bool = True,
    enable_afftdn: bool = False,
):
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    analysis_input_path = input_path
    if use_neutral_preclean:
        analysis_input_path = _render_neutral_preclean(
            input_path=input_path,
            td=td,
            enable_afftdn=enable_afftdn,
        )

    analysis = analyze_sm_input(analysis_input_path)

    analysis.global_flags["render_input_path"] = input_path
    analysis.global_flags["analysis_input_path"] = analysis_input_path
    analysis.global_flags["neutral_preclean_applied"] = bool(use_neutral_preclean)
    analysis.global_flags["neutral_preclean_afftdn"] = bool(enable_afftdn)
    analysis.global_flags["tone"] = tone
    analysis.global_flags["intensity"] = intensity
    analysis.global_flags["fmt"] = fmt

    selection = select_sm_profiles(analysis, tone, intensity)
    router = build_sm_router_summary(analysis, selection)
    dsp = assemble_sm_dsp_blueprint(analysis, router)
    render_plan = build_dsp_render_plan(dsp)

    render_execution_report = execute_dsp_render_plan(
        render_plan=render_plan,
        input_path=analysis_input_path,
    )

    analysis.global_flags["render_execution_backend"] = "execute_dsp_render_plan"
    if isinstance(render_execution_report, dict):
        analysis.global_flags["render_execution_status"] = render_execution_report.get("status", "unknown")
    else:
        analysis.global_flags["render_execution_status"] = "unknown"

    return _build_debug_bundle(
        analysis=analysis,
        selection=selection,
        router=router,
        dsp=dsp,
        render_plan=render_plan,
        render_execution_report=render_execution_report,
    )

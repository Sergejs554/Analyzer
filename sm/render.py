# sm/render.py

import inspect
import os
import shlex
import subprocess
from dataclasses import fields, is_dataclass
from typing import Any, Dict

from .analysis import analyze_sm_input
from .selector import select_sm_profiles
from .router import build_sm_router_summary
from .contracts import SmartMasterDebugBundle
from .precondition import build_neutral_preclean_chain
from .dsp.assembler import assemble_sm_dsp_blueprint
from .dsp.render_builder import build_dsp_render_plan


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


def _call_with_supported_kwargs(fn, kwargs: Dict[str, Any]):
    sig = inspect.signature(fn)

    accepts_var_kw = any(
        p.kind == inspect.Parameter.VAR_KEYWORD
        for p in sig.parameters.values()
    )

    if accepts_var_kw:
        return fn(**kwargs)

    filtered = {
        name: value
        for name, value in kwargs.items()
        if name in sig.parameters
    }
    return fn(**filtered)


def _execute_render_plan_debug(
    render_plan: Any,
    prepared_input_path: str,
    td: str,
    fmt: str,
) -> Dict[str, Any]:
    try:
        from .dsp import executor as executor_module
    except Exception as exc:
        return {
            "executor_status": "missing",
            "executor_backend": None,
            "notes": [
                "executor module import failed",
                str(exc)[:500],
            ],
        }

    base_kwargs: Dict[str, Any] = {
        "render_plan": render_plan,
        "plan": render_plan,
        "input_path": prepared_input_path,
        "source_path": prepared_input_path,
        "prepared_input_path": prepared_input_path,
        "prepared_path": prepared_input_path,
        "td": td,
        "temp_dir": td,
        "workdir": td,
        "fmt": fmt,
        "output_format": fmt,
        "dry_run": True,
        "debug": True,
        "return_debug": True,
        "return_report": True,
    }

    candidate_names = [
        "execute_dsp_render_plan",
        "execute_render_plan",
        "run_dsp_render_plan",
        "run_render_plan",
        "build_render_execution_report",
        "build_execution_report",
        "execute_render_plan_debug",
        "execute_render_plan_dry",
    ]

    for name in candidate_names:
        fn = getattr(executor_module, name, None)
        if not callable(fn):
            continue

        try:
            result = _call_with_supported_kwargs(fn, base_kwargs)
            if result is None:
                result = {
                    "executor_status": "ok",
                    "executor_backend": name,
                    "notes": ["executor returned None"],
                }
            elif isinstance(result, dict):
                result.setdefault("executor_status", "ok")
                result.setdefault("executor_backend", name)
            else:
                result = {
                    "executor_status": "ok",
                    "executor_backend": name,
                    "result_type": type(result).__name__,
                    "result_repr": repr(result)[:1000],
                }
            return result
        except Exception as exc:
            return {
                "executor_status": "error",
                "executor_backend": name,
                "notes": [
                    "executor call failed",
                    str(exc)[:1000],
                ],
            }

    return {
        "executor_status": "missing_callable",
        "executor_backend": None,
        "notes": [
            "no supported executor callable found",
        ],
    }


def _build_debug_bundle(
    analysis: Any,
    selection: Any,
    router: Any,
    dsp: Any,
    render_plan: Any,
    render_execution: Any,
):
    payload = {
        "analysis": analysis,
        "selection": selection,
        "router": router,
        "dsp": dsp,
        "render_plan": render_plan,
        "render_execution": render_execution,
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

    render_execution = _execute_render_plan_debug(
        render_plan=render_plan,
        prepared_input_path=analysis_input_path,
        td=td,
        fmt=fmt,
    )

    if isinstance(render_execution, dict):
        analysis.global_flags["render_execution_status"] = render_execution.get("executor_status")
        analysis.global_flags["render_execution_backend"] = render_execution.get("executor_backend")

    return _build_debug_bundle(
        analysis=analysis,
        selection=selection,
        router=router,
        dsp=dsp,
        render_plan=render_plan,
        render_execution=render_execution,
    )

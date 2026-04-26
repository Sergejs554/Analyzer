# sm/dsp_roles.py

from __future__ import annotations

from .contracts import RoleExecutionPlan


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _get_float(plan: RoleExecutionPlan, name: str, fallback: float) -> float:
    try:
        value = getattr(plan, name, fallback)
        if value is None:
            return fallback
        return float(value)
    except Exception:
        return fallback


def _get_str(plan: RoleExecutionPlan, name: str, fallback: str = "") -> str:
    try:
        value = getattr(plan, name, fallback)
        if value is None:
            return fallback
        return str(value)
    except Exception:
        return fallback


def _amount(plan: RoleExecutionPlan) -> float:
    """
    Real musical role activity.
    Router decides amount/cap/dynamic_scale.
    This layer must translate that into audible DSP, not silence it.
    """
    requested = _get_float(plan, "execution_amount", 0.0)
    cap = _get_float(plan, "execution_cap", 1.0)
    dyn = _get_float(plan, "dynamic_scale", 0.65)

    if cap <= 0.0001:
        norm = 0.0
    else:
        norm = _clamp(requested / cap, 0.0, 1.0)

    return _clamp((norm * 0.55) + (dyn * 0.45), 0.0, 1.0)


def _enabled(plan: RoleExecutionPlan) -> bool:
    return bool(getattr(plan, "enabled", False)) and _amount(plan) > 0.01


def _chain(*parts: str) -> str:
    clean = [p for p in parts if p and p != "anull"]
    if not clean:
        return "anull"
    return ",".join(clean)


def _eq(freq: float, width: float, gain: float) -> str:
    return (
        f"equalizer=f={freq:.1f}:"
        f"width_type=o:"
        f"width={width:.3f}:"
        f"g={gain:.3f}"
    )


def _low_shelf(freq: float, gain: float) -> str:
    """
    FFmpeg bass filter as a broad low support shelf.
    Conservative by default, but audible enough for mastering character.
    """
    return f"bass=f={freq:.1f}:width_type=o:width=0.650:g={gain:.3f}"


def _high_shelf(freq: float, gain: float) -> str:
    return f"treble=f={freq:.1f}:width_type=o:width=0.650:g={gain:.3f}"


def _soft_glue(amount: float, *, mix: float = 1.0) -> str:
    """
    Gentle mastering glue.
    Not a limiter, not a crusher.
    Used to make support/projection feel finished.
    """
    threshold = -18.0 + (amount * 4.0)
    ratio = 1.18 + (amount * 0.42)
    attack = 18.0 - (amount * 5.0)
    release = 130.0 + (amount * 45.0)

    return (
        "acompressor="
        f"threshold={threshold:.3f}dB:"
        f"ratio={ratio:.3f}:"
        f"attack={attack:.3f}:"
        f"release={release:.3f}:"
        f"makeup=0:"
        f"mix={_clamp(mix, 0.0, 1.0):.3f}"
    )


def _safe_softclip(amount: float) -> str:
    """
    Tiny density stage for projection/spark.
    Must be audible as polish, not distortion.
    """
    drive = 0.25 + (amount * 0.85)
    return f"asoftclip=type=tanh:param={drive:.3f}:oversample=2"


def build_body_anchor_role(plan: RoleExecutionPlan) -> str:
    """
    Body Anchor:
    - restores useful body foundation
    - gives low vocal/body mass
    - does not rebuild mud
    """
    if not _enabled(plan):
        return "anull"

    a = _amount(plan)
    mode = _get_str(plan, "target_band_mode", "")

    if mode == "body_restrain":
        return _chain(
            _eq(260.0, 0.950, -(0.18 + 0.42 * a)),
            _eq(390.0, 1.050, -(0.10 + 0.28 * a)),
        )

    if mode == "body_hold":
        body_gain = 0.35 + (0.70 * a)
        low_gain = 0.18 + (0.36 * a)
        glue_mix = 0.08 + (0.10 * a)
    else:
        body_gain = 0.55 + (1.05 * a)
        low_gain = 0.25 + (0.58 * a)
        glue_mix = 0.10 + (0.14 * a)

    return _chain(
        _low_shelf(95.0, low_gain),
        _eq(175.0, 0.820, body_gain),
        _eq(245.0, 1.050, body_gain * 0.45),
        _soft_glue(a, mix=glue_mix),
    )


def build_body_bridge_role(plan: RoleExecutionPlan) -> str:
    """
    Body Bridge:
    - connects bass to body
    - prevents low end from feeling separate
    - does not smear 200-400 Hz
    """
    if not _enabled(plan):
        return "anull"

    a = _amount(plan)
    mode = _get_str(plan, "target_band_mode", "")

    if mode == "bridge_restrain":
        return _chain(
            _eq(145.0, 0.850, 0.10 + (0.25 * a)),
            _eq(285.0, 1.150, -(0.16 + 0.38 * a)),
            _soft_glue(a, mix=0.06 + (0.07 * a)),
        )

    if mode == "bridge_hold":
        bridge_gain = 0.24 + (0.55 * a)
        glue_mix = 0.07 + (0.08 * a)
    else:
        bridge_gain = 0.38 + (0.82 * a)
        glue_mix = 0.09 + (0.12 * a)

    return _chain(
        _eq(118.0, 0.720, bridge_gain * 0.65),
        _eq(155.0, 0.850, bridge_gain),
        _eq(320.0, 1.150, -(0.08 + 0.28 * a)),
        _soft_glue(a, mix=glue_mix),
    )


def build_buildup_cleanup_role(plan: RoleExecutionPlan) -> str:
    """
    Buildup Cleanup:
    - separates mud/buildup from useful body
    - prepares room for projection
    - must not become the main sound
    """
    if not _enabled(plan):
        return "anull"

    a = _amount(plan)
    mode = _get_str(plan, "target_band_mode", "")

    if mode == "cleanup_micro":
        cut_250 = -(0.18 + 0.36 * a)
        cut_360 = -(0.10 + 0.24 * a)
        deharsh = -(0.06 + 0.16 * a)
    elif mode == "cleanup_guarded":
        cut_250 = -(0.28 + 0.62 * a)
        cut_360 = -(0.16 + 0.42 * a)
        deharsh = -(0.10 + 0.25 * a)
    else:
        cut_250 = -(0.40 + 0.95 * a)
        cut_360 = -(0.24 + 0.62 * a)
        deharsh = -(0.12 + 0.30 * a)

    return _chain(
        _eq(245.0, 1.050, cut_250),
        _eq(365.0, 1.250, cut_360),
        _eq(520.0, 1.350, cut_360 * 0.45),
        _eq(4300.0, 1.800, deharsh),
    )


def build_upper_body_guard_role(plan: RoleExecutionPlan) -> str:
    """
    Upper Body Guard:
    - controls boxiness after cleanup
    - protects body-to-mid transition
    - avoids a hollow hole between body and presence
    """
    if not _enabled(plan):
        return "anull"

    a = _amount(plan)
    mode = _get_str(plan, "target_band_mode", "")

    if mode == "guard_transition_support":
        return _chain(
            _eq(420.0, 1.050, -(0.10 + 0.28 * a)),
            _eq(720.0, 0.900, 0.08 + (0.22 * a)),
            _eq(1150.0, 0.850, 0.08 + (0.18 * a)),
        )

    if mode == "guard_hold":
        return _chain(
            _eq(420.0, 1.150, -(0.10 + 0.26 * a)),
            _eq(620.0, 1.000, -(0.06 + 0.18 * a)),
        )

    return _chain(
        _eq(330.0, 1.100, -(0.14 + 0.36 * a)),
        _eq(460.0, 1.200, -(0.14 + 0.42 * a)),
        _eq(760.0, 0.950, 0.06 + (0.18 * a)),
    )


def build_projection_handoff_role(plan: RoleExecutionPlan) -> str:
    """
    Projection Handoff:
    - this is the main studio-forward character
    - must stay audible
    - not brightness hype, but center-forward mastered feel
    """
    if not _enabled(plan):
        return "anull"

    a = _amount(plan)
    mode = _get_str(plan, "target_band_mode", "")

    if mode == "projection_clamp":
        return _chain(
            _eq(2600.0, 0.850, 0.18 + (0.38 * a)),
            _eq(4300.0, 1.700, -(0.16 + 0.34 * a)),
        )

    if mode == "projection_mild":
        presence = 0.48 + (0.88 * a)
        density = 0.20 + (0.42 * a)
        deharsh = -(0.10 + 0.28 * a)
    else:
        presence = 0.72 + (1.18 * a)
        density = 0.30 + (0.62 * a)
        deharsh = -(0.12 + 0.34 * a)

    return _chain(
        _eq(1850.0, 0.720, density),
        _eq(2450.0, 0.760, presence),
        _eq(3300.0, 0.900, presence * 0.42),
        _eq(4700.0, 1.650, deharsh),
        _safe_softclip(a * 0.55),
    )


def build_finish_spark_role(plan: RoleExecutionPlan) -> str:
    """
    Finish Spark:
    - final studio polish
    - air, gloss, width feeling through top balance
    - not a replacement for projection
    """
    if not _enabled(plan):
        return "anull"

    a = _amount(plan)
    mode = _get_str(plan, "target_band_mode", "")

    if mode in {"spark_off", "off"}:
        return "anull"

    if mode == "spark_micro":
        air = 0.18 + (0.42 * a)
        shine = 0.10 + (0.26 * a)
        deess = -(0.08 + 0.20 * a)
        clip_amount = a * 0.28
    else:
        air = 0.28 + (0.62 * a)
        shine = 0.16 + (0.38 * a)
        deess = -(0.10 + 0.24 * a)
        clip_amount = a * 0.38

    return _chain(
        _high_shelf(9200.0, air),
        _eq(6800.0, 1.200, shine),
        _eq(7600.0, 1.600, deess),
        _safe_softclip(clip_amount),
    )

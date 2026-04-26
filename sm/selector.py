# sm/selector.py

from dataclasses import dataclass

from .contracts import (
    RoleProfileSelection,
    SelectedRoleProfile,
    SmartMasterAnalysis,
)
from .enums import (
    AnchorState,
    BridgeState,
    CleanupReadiness,
    ProjectionReadiness,
    ProjectionState,
    RiskLevel,
    RoleName,
    TransitionState,
    UpperBodyShape,
)


@dataclass
class SelectorContext:
    schema: SmartMasterAnalysis
    tone: str
    intensity: str

    intensity_scale: float
    tone_projection_scale: float
    tone_cleanup_scale: float

    top_safe: bool
    top_guarded: bool
    top_fragile: bool

    body_fragile: bool
    body_weak: bool
    foundation_missing: bool

    bridge_broken: bool
    bridge_gluey: bool
    bridge_gap_risky: bool

    cleanup_heavy_needed: bool
    cleanup_guarded: bool

    boxy_active: bool
    transition_fragile: bool
    underprojected: bool
    overpushed: bool

    dense_good_candidate: bool
    dirty_dense_candidate: bool
    thin_candidate: bool

    primary_correction_lane: str = "none"
    secondary_support_lane: str = "none"
    primary_benefit_lane: str = "none"


def _has(v) -> bool:
    return v is not None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _risk_ge(risk: RiskLevel, level: RiskLevel) -> bool:
    order = {
        RiskLevel.LOW: 0,
        RiskLevel.MEDIUM: 1,
        RiskLevel.HIGH: 2,
    }
    return order[risk] >= order[level]


def _intensity_scale(intensity: str) -> float:
    x = (intensity or "balanced").lower().strip()
    if x == "low":
        return 0.92
    if x == "high":
        return 1.12
    return 1.00


def _tone_projection_scale(tone: str) -> float:
    x = (tone or "balanced").lower().strip()
    if x == "warm":
        return 0.96
    if x == "bright":
        return 1.06
    return 1.00


def _tone_cleanup_scale(tone: str) -> float:
    x = (tone or "balanced").lower().strip()
    if x == "warm":
        return 0.94
    if x == "bright":
        return 1.02
    return 1.00


def _make_profile(
    role: RoleName,
    profile_name: str,
    reason: str,
    amount: float,
    cap: float,
    enabled: bool = True,
    forced_clamp: bool = False,
    notes: list[str] | None = None,
) -> SelectedRoleProfile:
    return SelectedRoleProfile(
        role=role,
        profile_name=profile_name,
        reason=reason,
        amount=amount,
        cap=cap,
        enabled=enabled,
        forced_clamp=forced_clamp,
        notes=notes[:] if notes else [],
    )


def _quiet_room_score(ctx: SelectorContext) -> float:
    m = ctx.schema.metrics

    integrated_lufs = getattr(m, "integrated_lufs", -10.0)
    true_peak_dbtp = getattr(m, "true_peak_dbtp", -1.0)
    limiter_stress_proxy = getattr(m, "limiter_stress_proxy", 0.0)
    near_clip_ratio = getattr(m, "near_clip_ratio", 0.0)

    lufs_room = _clamp((-9.8 - integrated_lufs) / 4.5, 0.0, 1.0)
    peak_room = _clamp((-0.6 - true_peak_dbtp) / 2.4, 0.0, 1.0)
    stress_room = _clamp((0.96 - limiter_stress_proxy) / 0.28, 0.0, 1.0)
    clip_room = _clamp((0.006 - near_clip_ratio) / 0.006, 0.0, 1.0)

    return _clamp(
        (lufs_room * 0.42)
        + (peak_room * 0.24)
        + (stress_room * 0.22)
        + (clip_room * 0.12),
        0.0,
        1.0,
    )


def _hard_emergency(ctx: SelectorContext) -> bool:
    m = ctx.schema.metrics
    d = ctx.schema.derived
    p = ctx.schema.projection

    true_peak_dbtp = getattr(m, "true_peak_dbtp", -1.0)
    near_clip_ratio = getattr(m, "near_clip_ratio", 0.0)
    crest_db = getattr(m, "crest_db", 10.0)
    punch_proxy = getattr(m, "punch_proxy", 10.0)

    top_push = getattr(d, "top_push_safety_proxy", None)

    hard_clip = true_peak_dbtp >= 2.20 or near_clip_ratio >= 0.018
    punch_collapse = crest_db < 8.2 and punch_proxy < 9.2

    top_collapse = (
        p.harshness_risk == RiskLevel.HIGH
        and p.sibilance_risk == RiskLevel.HIGH
        and top_push is not None
        and top_push < 0.34
    )

    return bool(hard_clip or punch_collapse or top_collapse)


def build_selector_context(schema: SmartMasterAnalysis, tone: str, intensity: str) -> SelectorContext:
    m = schema.metrics
    d = schema.derived
    a = schema.anchor
    b = schema.bridge
    c = schema.cleanup
    g = schema.guard
    p = schema.projection

    intensity_scale = _intensity_scale(intensity)
    tone_projection_scale = _tone_projection_scale(tone)
    tone_cleanup_scale = _tone_cleanup_scale(tone)

    top_safe = (
        p.harshness_risk == RiskLevel.LOW
        and p.sibilance_risk == RiskLevel.LOW
        and (d.top_push_safety_proxy is None or d.top_push_safety_proxy >= 0.60)
    )
    top_guarded = (
        not top_safe
        and (d.top_push_safety_proxy is None or d.top_push_safety_proxy >= 0.42)
    )

    # ВАЖНО:
    # Старый баг был: top_fragile = not top_guarded.
    # Это делало top_fragile=True даже при top_safe=True.
    top_fragile = (not top_safe) and (not top_guarded)

    foundation_missing = not a.foundation_present
    body_fragile = a.fragility in (RiskLevel.MEDIUM, RiskLevel.HIGH)
    body_weak = (
        a.state == AnchorState.DEFICIENT
        or foundation_missing
        or (_has(d.center_body_support_proxy) and d.center_body_support_proxy < 0.44)
        or (_has(m.low_body_150_300_db) and m.low_body_150_300_db < 31.0)
    )

    bridge_broken = b.state == BridgeState.BROKEN
    bridge_gluey = b.state == BridgeState.OVERGLUED or _risk_ge(b.glue_risk, RiskLevel.MEDIUM)
    bridge_gap_risky = _risk_ge(b.gap_risk, RiskLevel.MEDIUM)

    cleanup_heavy_needed = (
        c.readiness == CleanupReadiness.SAFE
        and c.buildup_risk == RiskLevel.HIGH
    )
    cleanup_guarded = c.readiness == CleanupReadiness.GUARDED

    boxy_active = g.shape == UpperBodyShape.BOXY
    transition_fragile = g.transition_state in (TransitionState.WEAK, TransitionState.THINNING)

    underprojected = p.state == ProjectionState.UNDERPROJECTED
    overpushed = p.state == ProjectionState.OVERPUSHED

    dense_good_candidate = (
        not body_fragile
        and not body_weak
        and not bridge_broken
        and c.readiness != CleanupReadiness.DENIED
        and c.buildup_risk in (RiskLevel.MEDIUM, RiskLevel.HIGH)
        and not boxy_active
        and not transition_fragile
        and (_has(d.center_body_support_proxy) is False or d.center_body_support_proxy >= 0.54)
        and (_has(d.body_to_mid_handoff_proxy) is False or d.body_to_mid_handoff_proxy >= 0.54)
        and (_has(m.mud_to_body_db) is False or m.mud_to_body_db < 0.10)
    )

    dirty_dense_candidate = (
        c.buildup_risk == RiskLevel.HIGH
        and (boxy_active or (_has(m.mud_to_body_db) and m.mud_to_body_db >= -0.05))
        and (bridge_gluey or (_has(m.lowmid_buildup_ratio_db) and m.lowmid_buildup_ratio_db >= 17.2))
    )

    thin_candidate = body_weak or bridge_broken or transition_fragile

    ctx = SelectorContext(
        schema=schema,
        tone=tone,
        intensity=intensity,
        intensity_scale=intensity_scale,
        tone_projection_scale=tone_projection_scale,
        tone_cleanup_scale=tone_cleanup_scale,
        top_safe=top_safe,
        top_guarded=top_guarded,
        top_fragile=top_fragile,
        body_fragile=body_fragile,
        body_weak=body_weak,
        foundation_missing=foundation_missing,
        bridge_broken=bridge_broken,
        bridge_gluey=bridge_gluey,
        bridge_gap_risky=bridge_gap_risky,
        cleanup_heavy_needed=cleanup_heavy_needed,
        cleanup_guarded=cleanup_guarded,
        boxy_active=boxy_active,
        transition_fragile=transition_fragile,
        underprojected=underprojected,
        overpushed=overpushed,
        dense_good_candidate=dense_good_candidate,
        dirty_dense_candidate=dirty_dense_candidate,
        thin_candidate=thin_candidate,
    )

    ctx.primary_correction_lane = choose_primary_correction_lane(ctx)
    ctx.secondary_support_lane = choose_secondary_support_lane(ctx)
    ctx.primary_benefit_lane = choose_primary_benefit_lane(ctx)
    return ctx


def choose_primary_correction_lane(ctx: SelectorContext) -> str:
    if ctx.body_weak or ctx.bridge_broken:
        return "anchor_bridge"

    if ctx.cleanup_heavy_needed and ctx.dirty_dense_candidate:
        return "cleanup"

    if ctx.boxy_active or ctx.transition_fragile:
        return "guard"

    if ctx.underprojected:
        return "projection"

    return "stability_hold"


def choose_secondary_support_lane(ctx: SelectorContext) -> str:
    if ctx.primary_correction_lane == "anchor_bridge":
        if ctx.cleanup_heavy_needed and not ctx.body_fragile:
            return "cleanup_support"
        if ctx.boxy_active or ctx.transition_fragile:
            return "guard_support"
        return "projection_support"

    if ctx.primary_correction_lane == "cleanup":
        if ctx.boxy_active or ctx.transition_fragile:
            return "guard_support"
        return "projection_support"

    if ctx.primary_correction_lane == "guard":
        if ctx.cleanup_heavy_needed and not ctx.body_fragile:
            return "cleanup_support"
        return "projection_support"

    if ctx.primary_correction_lane == "projection":
        if ctx.boxy_active or ctx.transition_fragile:
            return "guard_support"
        if ctx.cleanup_heavy_needed and not ctx.body_fragile:
            return "cleanup_support"
        return "none"

    if ctx.boxy_active:
        return "guard_support"

    return "projection_support"


def choose_primary_benefit_lane(ctx: SelectorContext) -> str:
    # В polish-ветке cleanup не является главным benefit.
    # Cleanup подготавливает место. Главная магия - body/bridge + projection + finish.
    if ctx.thin_candidate:
        return "body_gain"

    if ctx.underprojected:
        return "forward_gain"

    if ctx.top_safe or ctx.top_guarded:
        return "forward_gain"

    return "forward_gain"


def select_anchor_profile(ctx: SelectorContext) -> SelectedRoleProfile:
    a = ctx.schema.anchor
    d = ctx.schema.derived
    room = _quiet_room_score(ctx)

    notes = [
        f"correction={ctx.primary_correction_lane}",
        f"support={ctx.secondary_support_lane}",
        f"benefit={ctx.primary_benefit_lane}",
    ]

    if a.stop:
        return _make_profile(
            RoleName.ANCHOR,
            "anchor_restrain_upper_body",
            "anchor stop fallback",
            0.16,
            0.26,
            forced_clamp=True,
            notes=notes + ["anchor stop but body floor retained"],
        )

    if a.state == AnchorState.EXCESSIVE:
        return _make_profile(
            RoleName.ANCHOR,
            "anchor_restrain_upper_body",
            "anchor excessive",
            0.20,
            0.32,
            notes=notes + ["excessive anchor mass shaped, not killed"],
        )

    restore_needed = (
        a.state == AnchorState.DEFICIENT
        or ctx.foundation_missing
        or ctx.primary_correction_lane == "anchor_bridge"
        or ctx.primary_benefit_lane == "body_gain"
    )

    if restore_needed:
        amount = 0.28 * ctx.intensity_scale
        amount += 0.03 * room

        if a.state == AnchorState.DEFICIENT or ctx.foundation_missing:
            amount += 0.05
        if ctx.primary_benefit_lane == "body_gain":
            amount += 0.04
        if _has(d.center_body_support_proxy) and d.center_body_support_proxy < 0.40:
            amount += 0.03
        if ctx.body_fragile:
            amount -= 0.02

        return _make_profile(
            RoleName.ANCHOR,
            "anchor_restore_controlled",
            "anchor restore path",
            _clamp(amount, 0.24, 0.44),
            0.50,
            notes=notes + ["body support prioritized"],
        )

    if a.fragility == RiskLevel.HIGH:
        return _make_profile(
            RoleName.ANCHOR,
            "anchor_hold_safe",
            "anchor fragile",
            0.16,
            0.28,
            forced_clamp=True,
            notes=notes + ["high fragility but body hold retained"],
        )

    if a.fragility == RiskLevel.MEDIUM:
        return _make_profile(
            RoleName.ANCHOR,
            "anchor_hold_safe",
            "anchor mildly fragile",
            0.18,
            0.30,
            notes=notes + ["medium fragility"],
        )

    return _make_profile(
        RoleName.ANCHOR,
        "anchor_hold_safe",
        "anchor balanced",
        0.20 + (0.02 * room),
        0.32,
        notes=notes + ["default body floor"],
    )


def select_bridge_profile(ctx: SelectorContext) -> SelectedRoleProfile:
    b = ctx.schema.bridge
    d = ctx.schema.derived
    room = _quiet_room_score(ctx)

    notes = [
        f"correction={ctx.primary_correction_lane}",
        f"support={ctx.secondary_support_lane}",
        f"benefit={ctx.primary_benefit_lane}",
    ]

    if b.stop:
        return _make_profile(
            RoleName.BRIDGE,
            "bridge_restrain_glue",
            "bridge stop fallback",
            0.15,
            0.25,
            forced_clamp=True,
            notes=notes + ["bridge stop but handoff floor retained"],
        )

    if b.state == BridgeState.OVERGLUED or b.glue_risk == RiskLevel.HIGH:
        return _make_profile(
            RoleName.BRIDGE,
            "bridge_restrain_glue",
            "bridge glue excess",
            0.18,
            0.30,
            notes=notes + ["glue shaped, bridge not killed"],
        )

    restore_needed = (
        b.state == BridgeState.BROKEN
        or ctx.primary_correction_lane == "anchor_bridge"
        or ctx.primary_benefit_lane == "body_gain"
        or ctx.bridge_gap_risky
    )

    if restore_needed:
        amount = 0.22 * ctx.intensity_scale
        amount += 0.02 * room

        if b.state == BridgeState.BROKEN:
            amount += 0.06
        if ctx.primary_benefit_lane == "body_gain":
            amount += 0.04
        if _has(d.body_to_mid_handoff_proxy) and d.body_to_mid_handoff_proxy < 0.42:
            amount += 0.03
        if ctx.bridge_gluey:
            amount -= 0.03

        return _make_profile(
            RoleName.BRIDGE,
            "bridge_restore_controlled",
            "bridge restore path",
            _clamp(amount, 0.20, 0.42),
            0.46,
            notes=notes + ["bridge support prioritized"],
        )

    if b.glue_risk == RiskLevel.MEDIUM:
        return _make_profile(
            RoleName.BRIDGE,
            "bridge_hold_safe",
            "bridge balanced but glue-prone",
            0.15,
            0.26,
            notes=notes + ["conservative bridge hold"],
        )

    return _make_profile(
        RoleName.BRIDGE,
        "bridge_hold_safe",
        "bridge balanced",
        0.17 + (0.02 * room),
        0.30,
        notes=notes + ["default bridge floor"],
    )


def select_cleanup_profile(ctx: SelectorContext) -> SelectedRoleProfile:
    c = ctx.schema.cleanup
    d = ctx.schema.derived

    notes = [
        f"correction={ctx.primary_correction_lane}",
        f"support={ctx.secondary_support_lane}",
        f"benefit={ctx.primary_benefit_lane}",
        "cleanup is preparation, not main polish benefit",
    ]

    if c.stop or c.readiness == CleanupReadiness.DENIED:
        return _make_profile(
            RoleName.CLEANUP,
            "cleanup_micro_corrective",
            "cleanup denied/stop",
            0.08,
            0.16,
            forced_clamp=True,
            notes=notes + ["cleanup denied -> micro only"],
        )

    if c.readiness == CleanupReadiness.GUARDED:
        amount = 0.16 * ctx.intensity_scale * ctx.tone_cleanup_scale
        if c.buildup_risk == RiskLevel.HIGH:
            amount += 0.03

        return _make_profile(
            RoleName.CLEANUP,
            "cleanup_guarded_safe",
            "cleanup guarded",
            _clamp(amount, 0.15, 0.28),
            0.34,
            notes=notes + ["guarded cleanup separates mud without drying body"],
        )

    dense_allowed = True
    if ctx.body_fragile:
        dense_allowed = False
        notes.append("body fragility blocks dense cleanup")
    if ctx.bridge_broken:
        dense_allowed = False
        notes.append("broken bridge blocks dense cleanup")
    if _has(d.center_body_support_proxy) and d.center_body_support_proxy < 0.50:
        dense_allowed = False
        notes.append("center-body support too weak")
    if _has(d.body_to_mid_handoff_proxy) and d.body_to_mid_handoff_proxy < 0.48:
        dense_allowed = False
        notes.append("handoff too weak")

    if (
        dense_allowed
        and ctx.primary_correction_lane == "cleanup"
        and c.buildup_risk == RiskLevel.HIGH
    ):
        amount = 0.30 * ctx.intensity_scale * ctx.tone_cleanup_scale
        return _make_profile(
            RoleName.CLEANUP,
            "cleanup_focused_dense",
            "cleanup primary preparation",
            _clamp(amount, 0.26, 0.42),
            0.50,
            notes=notes + ["dirty track cleanup, but projection remains mandatory"],
        )

    if (
        ctx.secondary_support_lane == "cleanup_support"
        or c.buildup_risk in (RiskLevel.MEDIUM, RiskLevel.HIGH)
    ):
        amount = 0.18 * ctx.intensity_scale * ctx.tone_cleanup_scale
        if c.buildup_risk == RiskLevel.HIGH:
            amount += 0.03

        return _make_profile(
            RoleName.CLEANUP,
            "cleanup_guarded_safe",
            "cleanup controlled",
            _clamp(amount, 0.17, 0.30),
            0.36,
            notes=notes + ["cleanup retained as preparation lane"],
        )

    return _make_profile(
        RoleName.CLEANUP,
        "cleanup_micro_corrective",
        "cleanup only minor",
        0.10,
        0.18,
        notes=notes + ["minor prep cleanup only"],
    )


def select_guard_profile(
    ctx: SelectorContext,
    cleanup_profile: SelectedRoleProfile,
) -> SelectedRoleProfile:
    g = ctx.schema.guard
    d = ctx.schema.derived

    notes = [
        f"correction={ctx.primary_correction_lane}",
        f"support={ctx.secondary_support_lane}",
        f"benefit={ctx.primary_benefit_lane}",
    ]

    if g.stop:
        return _make_profile(
            RoleName.GUARD,
            "guard_transition_support_safe",
            "guard stop fallback",
            0.18,
            0.28,
            forced_clamp=True,
            notes=notes + ["guard stop"],
        )

    if g.transition_state in (TransitionState.THINNING, TransitionState.WEAK):
        return _make_profile(
            RoleName.GUARD,
            "guard_transition_support_safe",
            "guard transition support",
            0.20,
            0.32,
            notes=notes + ["transition support priority"],
        )

    if g.shape == UpperBodyShape.BOXY:
        if _has(d.body_to_mid_handoff_proxy) and d.body_to_mid_handoff_proxy < 0.52:
            return _make_profile(
                RoleName.GUARD,
                "guard_transition_support_safe",
                "guard boxy but handoff fragile",
                0.18,
                0.30,
                notes=notes + ["prefer support over subtractive guard"],
            )

        amount = 0.18
        if ctx.primary_correction_lane == "guard":
            amount += 0.04
        elif ctx.secondary_support_lane == "guard_support":
            amount += 0.02

        if cleanup_profile.profile_name == "cleanup_focused_dense":
            amount += 0.01

        return _make_profile(
            RoleName.GUARD,
            "guard_boxiness_controlled",
            "guard boxiness control",
            _clamp(amount, 0.18, 0.32),
            0.34,
            notes=notes + ["boxiness managed without blocking projection"],
        )

    return _make_profile(
        RoleName.GUARD,
        "guard_hold_safe",
        "guard stable",
        0.15,
        0.26,
        notes=notes + ["guard hold"],
    )


def select_projection_profile(
    ctx: SelectorContext,
    cleanup_profile: SelectedRoleProfile,
    guard_profile: SelectedRoleProfile,
) -> SelectedRoleProfile:
    p = ctx.schema.projection
    d = ctx.schema.derived
    room = _quiet_room_score(ctx)

    notes = [
        f"correction={ctx.primary_correction_lane}",
        f"support={ctx.secondary_support_lane}",
        f"benefit={ctx.primary_benefit_lane}",
        "projection is mandatory polish magic",
    ]

    if p.stop or p.readiness == ProjectionReadiness.DENIED:
        return _make_profile(
            RoleName.PROJECTION,
            "projection_clamp_safe",
            "projection denied/stop",
            0.18,
            0.28,
            forced_clamp=True,
            notes=notes + ["projection denied -> clamp, not off"],
        )

    if p.state == ProjectionState.OVERPUSHED:
        return _make_profile(
            RoleName.PROJECTION,
            "projection_clamp_safe",
            "projection overpushed",
            0.18,
            0.28,
            forced_clamp=True,
            notes=notes + ["already forward -> shape only"],
        )

    if p.readiness == ProjectionReadiness.GUARDED or ctx.top_fragile:
        amount = 0.24 * ctx.intensity_scale * ctx.tone_projection_scale
        amount += 0.04 * room
        if ctx.underprojected:
            amount += 0.03

        return _make_profile(
            RoleName.PROJECTION,
            "projection_mild_safe",
            "projection guarded",
            _clamp(amount, 0.22, 0.34),
            0.40,
            notes=notes + ["guarded projection still audible"],
        )

    dense_allowed = True

    if not ctx.top_safe:
        dense_allowed = False
        notes.append("top not safe enough for dense")
    if _has(d.top_push_safety_proxy) and d.top_push_safety_proxy < 0.66:
        dense_allowed = False
        notes.append("top push safety below dense threshold")
    if _has(d.body_to_mid_handoff_proxy) and d.body_to_mid_handoff_proxy < 0.55:
        dense_allowed = False
        notes.append("handoff not strong enough for dense")

    if cleanup_profile.profile_name == "cleanup_focused_dense":
        dense_allowed = False
        notes.append("dense cleanup makes projection controlled, not off")

    if guard_profile.profile_name == "guard_boxiness_controlled":
        dense_allowed = False
        notes.append("active box control makes projection controlled, not off")

    if ctx.transition_fragile:
        dense_allowed = False
        notes.append("transition fragility blocks dense projection")

    if dense_allowed:
        amount = 0.30 * ctx.intensity_scale * ctx.tone_projection_scale
        amount += 0.04 * room
        if ctx.primary_correction_lane == "projection":
            amount += 0.04
        if ctx.underprojected:
            amount += 0.03

        return _make_profile(
            RoleName.PROJECTION,
            "projection_controlled_dense",
            "projection primary musical reveal",
            _clamp(amount, 0.28, 0.46),
            0.54,
            notes=notes + ["forward reveal active"],
        )

    amount = 0.24 * ctx.intensity_scale * ctx.tone_projection_scale
    amount += 0.04 * room

    if ctx.primary_benefit_lane == "forward_gain" or ctx.secondary_support_lane == "projection_support":
        amount += 0.03
    if ctx.underprojected:
        amount += 0.03

    return _make_profile(
        RoleName.PROJECTION,
        "projection_mild_safe",
        "projection controlled floor",
        _clamp(amount, 0.22, 0.36),
        0.42,
        notes=notes + ["mandatory forward floor"],
    )


def select_spark_profile(
    ctx: SelectorContext,
    cleanup_profile: SelectedRoleProfile,
    guard_profile: SelectedRoleProfile,
    projection_profile: SelectedRoleProfile,
) -> SelectedRoleProfile:
    p = ctx.schema.projection
    d = ctx.schema.derived
    room = _quiet_room_score(ctx)

    notes = [
        f"correction={ctx.primary_correction_lane}",
        f"support={ctx.secondary_support_lane}",
        f"benefit={ctx.primary_benefit_lane}",
        "spark is mandatory finish floor except hard emergency",
    ]

    if _hard_emergency(ctx):
        return _make_profile(
            RoleName.SPARK,
            "finish_spark_off",
            "hard emergency",
            0.0,
            0.0,
            enabled=False,
            forced_clamp=True,
            notes=notes + ["spark disabled only by hard emergency"],
        )

    if p.stop or p.readiness == ProjectionReadiness.DENIED:
        return _make_profile(
            RoleName.SPARK,
            "finish_spark_micro_safe",
            "projection denied but finish floor retained",
            0.10 + (0.02 * room),
            0.20,
            notes=notes + ["micro finish only"],
        )

    if p.harshness_risk == RiskLevel.HIGH or p.sibilance_risk == RiskLevel.HIGH:
        return _make_profile(
            RoleName.SPARK,
            "finish_spark_micro_safe",
            "top risk guarded",
            0.12 + (0.02 * room),
            0.22,
            notes=notes + ["spark guarded by top risk"],
        )

    if projection_profile.profile_name == "projection_controlled_dense" and ctx.top_safe:
        amount = 0.18 * ctx.intensity_scale
        amount += 0.03 * room

        if _has(d.top_push_safety_proxy) and d.top_push_safety_proxy < 0.72:
            return _make_profile(
                RoleName.SPARK,
                "finish_spark_micro_safe",
                "top push safety moderate",
                0.14 + (0.02 * room),
                0.24,
                notes=notes + ["spark kept micro by top push safety"],
            )

        return _make_profile(
            RoleName.SPARK,
            "finish_spark_controlled_excited",
            "spark allowed",
            _clamp(amount, 0.17, 0.30),
            0.34,
            notes=notes + ["finish lane safely established"],
        )

    amount = 0.14 * ctx.intensity_scale
    amount += 0.03 * room

    if guard_profile.profile_name == "guard_boxiness_controlled":
        amount -= 0.01
        notes.append("active guard keeps spark slightly restrained")

    if cleanup_profile.profile_name == "cleanup_focused_dense":
        amount -= 0.01
        notes.append("dense cleanup keeps spark controlled, not off")

    return _make_profile(
        RoleName.SPARK,
        "finish_spark_micro_safe",
        "mandatory spark micro floor",
        _clamp(amount, 0.12, 0.24),
        0.28,
        notes=notes + ["mandatory finish floor"],
    )


def apply_stack_rules(
    ctx: SelectorContext,
    selection: RoleProfileSelection,
) -> RoleProfileSelection:
    anchor = selection.anchor
    bridge = selection.bridge
    cleanup = selection.cleanup
    guard = selection.guard
    projection = selection.projection
    spark = selection.spark

    # 1. Never run aggressive triple-stack, but do not kill magic.
    if (
        cleanup.profile_name == "cleanup_focused_dense"
        and projection.profile_name == "projection_controlled_dense"
        and spark.profile_name == "finish_spark_controlled_excited"
    ):
        projection = _make_profile(
            RoleName.PROJECTION,
            "projection_mild_safe",
            "triple-stack controlled",
            0.26,
            0.40,
            notes=projection.notes + ["downgraded by no-overstack rule, not disabled"],
        )
        spark = _make_profile(
            RoleName.SPARK,
            "finish_spark_micro_safe",
            "triple-stack controlled",
            0.14,
            0.26,
            notes=spark.notes + ["spark micro by no-overstack rule"],
        )

    # 2. Cleanup-primary cannot force projection off.
    if (
        ctx.primary_correction_lane == "cleanup"
        and projection.profile_name == "projection_controlled_dense"
    ):
        projection = _make_profile(
            RoleName.PROJECTION,
            "projection_mild_safe",
            "cleanup-primary projection controlled",
            0.26,
            0.40,
            notes=projection.notes + ["cleanup-primary controls projection density, not projection existence"],
        )

    # 3. Guard-active keeps spark micro, not off.
    if (
        guard.profile_name == "guard_boxiness_controlled"
        and spark.profile_name == "finish_spark_controlled_excited"
    ):
        spark = _make_profile(
            RoleName.SPARK,
            "finish_spark_micro_safe",
            "guard-active spark controlled",
            0.14,
            0.26,
            notes=spark.notes + ["downgraded by active-guard rule, not disabled"],
        )

    # 4. Thin material cannot take subtractive double-hit.
    if ctx.thin_candidate:
        if cleanup.profile_name == "cleanup_focused_dense":
            cleanup = _make_profile(
                RoleName.CLEANUP,
                "cleanup_guarded_safe",
                "thin-track cleanup clamp",
                0.18,
                0.30,
                notes=cleanup.notes + ["thin-track subtractive clamp"],
            )
        if guard.profile_name == "guard_boxiness_controlled" and ctx.transition_fragile:
            guard = _make_profile(
                RoleName.GUARD,
                "guard_transition_support_safe",
                "thin-track guard support bias",
                0.18,
                0.30,
                notes=guard.notes + ["thin-track support bias"],
            )

    # 5. Broken bridge + dense cleanup is not allowed.
    if ctx.bridge_broken and cleanup.profile_name == "cleanup_focused_dense":
        cleanup = _make_profile(
            RoleName.CLEANUP,
            "cleanup_guarded_safe",
            "bridge-broken cleanup clamp",
            0.18,
            0.30,
            notes=cleanup.notes + ["broken bridge blocks dense cleanup"],
        )

    # 6. Body fragility blocks dense cleanup.
    if ctx.body_fragile and cleanup.profile_name == "cleanup_focused_dense":
        cleanup = _make_profile(
            RoleName.CLEANUP,
            "cleanup_guarded_safe",
            "body-fragile cleanup clamp",
            0.18,
            0.30,
            notes=cleanup.notes + ["body fragility clamp"],
        )

    # 7. Overglued bridge limits anchor+bridge simultaneous restore, but does not kill support.
    if (
        bridge.profile_name == "bridge_restrain_glue"
        and anchor.profile_name == "anchor_restore_controlled"
    ):
        anchor = _make_profile(
            RoleName.ANCHOR,
            "anchor_hold_safe",
            "overglued-bridge anchor moderation",
            0.18,
            0.30,
            notes=anchor.notes + ["anchor moderated by glue restraint"],
        )

    return RoleProfileSelection(
        anchor=anchor,
        bridge=bridge,
        cleanup=cleanup,
        guard=guard,
        projection=projection,
        spark=spark,
    )


def enforce_benefit_floor(
    ctx: SelectorContext,
    selection: RoleProfileSelection,
) -> RoleProfileSelection:
    anchor = selection.anchor
    bridge = selection.bridge
    cleanup = selection.cleanup
    guard = selection.guard
    projection = selection.projection
    spark = selection.spark

    # Body/bridge floor.
    if anchor.amount < 0.16 and bridge.amount < 0.14:
        if ctx.bridge_broken or ctx.bridge_gap_risky:
            bridge = _make_profile(
                RoleName.BRIDGE,
                "bridge_restore_controlled",
                "mandatory bridge floor",
                0.20,
                0.34,
                notes=bridge.notes + ["mandatory bridge floor"],
            )
        else:
            anchor = _make_profile(
                RoleName.ANCHOR,
                "anchor_hold_safe",
                "mandatory body floor",
                0.18,
                0.30,
                notes=anchor.notes + ["mandatory body floor"],
            )

    # Cleanup floor remains preparation only.
    if cleanup.amount < 0.08 or not cleanup.enabled:
        cleanup = _make_profile(
            RoleName.CLEANUP,
            "cleanup_micro_corrective",
            "mandatory prep cleanup floor",
            0.10,
            0.18,
            notes=cleanup.notes + ["mandatory prep floor"],
        )

    # Projection floor is mandatory in polish branch.
    if projection.amount < 0.22 or not projection.enabled:
        if ctx.top_fragile or projection.profile_name == "projection_clamp_safe":
            projection = _make_profile(
                RoleName.PROJECTION,
                "projection_clamp_safe",
                "mandatory projection clamp floor",
                0.18,
                0.28,
                notes=projection.notes + ["mandatory projection clamp floor"],
            )
        else:
            projection = _make_profile(
                RoleName.PROJECTION,
                "projection_mild_safe",
                "mandatory projection floor",
                0.24,
                0.40,
                notes=projection.notes + ["mandatory projection floor"],
            )

    # Spark floor is mandatory except hard emergency.
    if (not spark.enabled or spark.amount < 0.12) and not _hard_emergency(ctx):
        spark = _make_profile(
            RoleName.SPARK,
            "finish_spark_micro_safe",
            "mandatory spark floor",
            0.14,
            0.26,
            notes=spark.notes + ["mandatory spark floor"],
        )

    return RoleProfileSelection(
        anchor=anchor,
        bridge=bridge,
        cleanup=cleanup,
        guard=guard,
        projection=projection,
        spark=spark,
    )


def select_sm_profiles(schema: SmartMasterAnalysis, tone: str, intensity: str) -> RoleProfileSelection:
    ctx = build_selector_context(schema, tone, intensity)

    anchor = select_anchor_profile(ctx)
    bridge = select_bridge_profile(ctx)
    cleanup = select_cleanup_profile(ctx)
    guard = select_guard_profile(ctx, cleanup)
    projection = select_projection_profile(ctx, cleanup, guard)
    spark = select_spark_profile(ctx, cleanup, guard, projection)

    selection = RoleProfileSelection(
        anchor=anchor,
        bridge=bridge,
        cleanup=cleanup,
        guard=guard,
        projection=projection,
        spark=spark,
    )

    selection = apply_stack_rules(ctx, selection)
    selection = enforce_benefit_floor(ctx, selection)
    return selection

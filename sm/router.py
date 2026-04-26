# sm/router.py

from __future__ import annotations

from dataclasses import replace

from .contracts import (
    RoleExecutionPlan,
    RoleProfileSelection,
    RouterContext,
    SelectedRoleProfile,
    SmartMasterAnalysis,
    SmartMasterExecutionBlueprint,
)
from .enums import RoleName, RiskLevel


CORRECTION_LANE_TO_ROLE_KEYS = {
    "anchor_bridge": {"anchor", "bridge"},
    "cleanup": {"cleanup"},
    "guard": {"guard"},
    "projection": {"projection"},
    "stability_hold": set(),
    "none": set(),
}

SUPPORT_LANE_TO_ROLE_KEYS = {
    "body_support": {"anchor", "bridge"},
    "cleanup_support": {"cleanup"},
    "guard_support": {"guard"},
    "projection_support": {"projection"},
    "finish_support": {"spark"},
    "none": set(),
}

BENEFIT_LANE_TO_ROLE_KEYS = {
    "body_gain": {"anchor", "bridge"},
    "forward_gain": {"projection"},
    "finish_gain": {"spark"},
    "clarity_gain": {"cleanup", "guard"},
    "none": set(),
}

ENERGY_ORDER = {
    "off": 0,
    "micro": 1,
    "mild": 2,
    "controlled": 3,
    "dense": 4,
}

# Musical order, not old defensive order.
# Cleanup prepares. Guard shapes. Anchor/bridge support. Projection makes the master. Spark finishes.
ASSEMBLY_ORDER = [
    "cleanup",
    "guard",
    "anchor",
    "bridge",
    "projection",
    "spark",
]


def _has(v) -> bool:
    return v is not None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _safe(v, fallback: float) -> float:
    if v is None:
        return fallback
    try:
        return float(v)
    except Exception:
        return fallback


def _risk_ge(risk: RiskLevel, level: RiskLevel) -> bool:
    order = {
        RiskLevel.LOW: 0,
        RiskLevel.MEDIUM: 1,
        RiskLevel.HIGH: 2,
    }
    return order.get(risk, 0) >= order[level]


def _role_key(role: RoleName | str) -> str:
    if hasattr(role, "value"):
        return str(role.value).strip().lower()
    return str(role).strip().lower()


def _extract_note_value(notes: list[str], prefix: str) -> str | None:
    p = f"{prefix}="
    for note in notes or []:
        if isinstance(note, str) and note.startswith(p):
            return note.split("=", 1)[1].strip()
    return None


def _append_unique(values: list[str], extra: list[str]) -> list[str]:
    out = list(values or [])
    for item in extra or []:
        if item and item not in out:
            out.append(item)
    return out


def _collect_lane_from_selection(selection: RoleProfileSelection, prefix: str) -> str | None:
    counts: dict[str, int] = {}
    for field in ASSEMBLY_ORDER:
        role_sel = getattr(selection, field, None)
        if role_sel is None:
            continue
        value = _extract_note_value(role_sel.notes, prefix)
        if value:
            counts[value] = counts.get(value, 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda kv: kv[1])[0]


def _fallback_primary_correction_lane(selection: RoleProfileSelection) -> str:
    cleanup = getattr(selection, "cleanup", None)
    guard = getattr(selection, "guard", None)
    anchor = getattr(selection, "anchor", None)
    bridge = getattr(selection, "bridge", None)
    projection = getattr(selection, "projection", None)

    if cleanup and cleanup.enabled and cleanup.amount >= 0.22:
        return "cleanup"

    if guard and guard.enabled and guard.amount >= 0.20:
        return "guard"

    if (
        (anchor and anchor.enabled and anchor.amount >= 0.24)
        or (bridge and bridge.enabled and bridge.amount >= 0.22)
    ):
        return "anchor_bridge"

    if projection and projection.enabled and projection.amount >= 0.24:
        return "projection"

    return "stability_hold"


def _fallback_support_lane(selection: RoleProfileSelection) -> str:
    anchor = getattr(selection, "anchor", None)
    bridge = getattr(selection, "bridge", None)
    guard = getattr(selection, "guard", None)
    projection = getattr(selection, "projection", None)
    spark = getattr(selection, "spark", None)
    cleanup = getattr(selection, "cleanup", None)

    if (
        (anchor and anchor.enabled and anchor.amount >= 0.14)
        or (bridge and bridge.enabled and bridge.amount >= 0.14)
    ):
        return "body_support"

    if projection and projection.enabled and projection.amount >= 0.14:
        return "projection_support"

    if spark and spark.enabled and spark.amount > 0.0:
        return "finish_support"

    if guard and guard.enabled and guard.amount >= 0.14:
        return "guard_support"

    if cleanup and cleanup.enabled and cleanup.amount >= 0.14:
        return "cleanup_support"

    return "none"


def _fallback_benefit_lane(selection: RoleProfileSelection) -> str:
    projection = getattr(selection, "projection", None)
    spark = getattr(selection, "spark", None)
    anchor = getattr(selection, "anchor", None)
    bridge = getattr(selection, "bridge", None)
    cleanup = getattr(selection, "cleanup", None)
    guard = getattr(selection, "guard", None)

    # Benefit is musical gain. Cleanup is not the main benefit by default.
    if projection and projection.enabled and projection.amount >= 0.12:
        return "forward_gain"

    if spark and spark.enabled and spark.amount > 0.0:
        return "finish_gain"

    if (
        (anchor and anchor.enabled and anchor.amount >= 0.14)
        or (bridge and bridge.enabled and bridge.amount >= 0.14)
    ):
        return "body_gain"

    if (
        (cleanup and cleanup.enabled and cleanup.amount >= 0.26)
        or (guard and guard.enabled and guard.amount >= 0.24)
    ):
        return "clarity_gain"

    return "forward_gain"


def _analysis_quiet_score(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics

    integrated_lufs = _safe(getattr(m, "integrated_lufs", None), -12.0)
    true_peak_dbtp = _safe(getattr(m, "true_peak_dbtp", None), -1.0)
    crest_db = _safe(getattr(m, "crest_db", None), 10.0)
    punch_proxy = _safe(getattr(m, "punch_proxy", None), 10.0)

    quiet_lufs = _clamp((-11.0 - integrated_lufs) / 5.0, 0.0, 1.0)
    peak_room = _clamp((-1.6 - true_peak_dbtp) / 3.0, 0.0, 1.0)
    punch_room = _clamp((min(crest_db, punch_proxy) - 8.8) / 4.0, 0.0, 1.0)

    return _clamp(
        (quiet_lufs * 0.50)
        + (peak_room * 0.30)
        + (punch_room * 0.20),
        0.0,
        1.0,
    )


def _top_emergency(analysis: SmartMasterAnalysis) -> bool:
    m = analysis.metrics
    p = analysis.projection

    harshness_index = _safe(getattr(m, "harshness_index", None), 0.0)
    sibilance_index = _safe(getattr(m, "sibilance_index", None), 0.0)
    harsh_to_mid_db = _safe(getattr(m, "harsh_to_mid_db", None), -6.0)
    sibilance_db = _safe(getattr(m, "sibilance_5k_9k_db", None), 15.5)

    return (
        p.harshness_risk == RiskLevel.HIGH
        and p.sibilance_risk == RiskLevel.HIGH
        and (
            harshness_index >= 0.86
            or sibilance_index >= 0.86
            or harsh_to_mid_db > -2.2
            or sibilance_db > 18.4
        )
    )


def _delivery_emergency(analysis: SmartMasterAnalysis) -> bool:
    m = analysis.metrics

    true_peak_dbtp = _safe(getattr(m, "true_peak_dbtp", None), -1.0)
    limiter_stress_proxy = _safe(getattr(m, "limiter_stress_proxy", None), 0.0)
    near_clip_ratio = _safe(getattr(m, "near_clip_ratio", None), 0.0)
    crest_db = _safe(getattr(m, "crest_db", None), 10.0)
    punch_proxy = _safe(getattr(m, "punch_proxy", None), 10.0)

    return (
        true_peak_dbtp >= 0.35
        or limiter_stress_proxy >= 1.18
        or near_clip_ratio >= 0.012
        or min(crest_db, punch_proxy) <= 7.2
    )


def build_router_context(
    analysis: SmartMasterAnalysis,
    selection: RoleProfileSelection,
) -> RouterContext:
    m = analysis.metrics
    d = analysis.derived
    a = analysis.anchor
    b = analysis.bridge
    c = analysis.cleanup
    g = analysis.guard
    p = analysis.projection

    primary_correction_lane = (
        _collect_lane_from_selection(selection, "correction")
        or _fallback_primary_correction_lane(selection)
    )
    secondary_support_lane = (
        _collect_lane_from_selection(selection, "support")
        or _fallback_support_lane(selection)
    )
    primary_benefit_lane = (
        _collect_lane_from_selection(selection, "benefit")
        or _fallback_benefit_lane(selection)
    )

    top_safe = (
        p.harshness_risk == RiskLevel.LOW
        and p.sibilance_risk == RiskLevel.LOW
        and (_has(d.top_push_safety_proxy) is False or d.top_push_safety_proxy >= 0.60)
    )
    top_guarded = (
        (not top_safe)
        and (_has(d.top_push_safety_proxy) is False or d.top_push_safety_proxy >= 0.42)
    )
    top_fragile = (not top_safe) and (not top_guarded)

    foundation_missing = not a.foundation_present
    body_fragile = _risk_ge(a.fragility, RiskLevel.MEDIUM)
    body_weak = (
        foundation_missing
        or (_has(d.center_body_support_proxy) and d.center_body_support_proxy < 0.44)
        or (_has(m.low_body_150_300_db) and m.low_body_150_300_db < 31.0)
    )

    bridge_broken = _role_key(b.state) == "broken"
    bridge_gluey = _role_key(b.state) == "overglued" or _risk_ge(b.glue_risk, RiskLevel.MEDIUM)
    bridge_gap_risky = _risk_ge(b.gap_risk, RiskLevel.MEDIUM)

    cleanup_heavy_needed = (
        _role_key(c.readiness) == "safe"
        and c.buildup_risk == RiskLevel.HIGH
    )
    cleanup_guarded = _role_key(c.readiness) == "guarded"

    boxy_active = _role_key(g.shape) == "boxy"
    transition_fragile = _role_key(g.transition_state) in {"weak", "thinning"}

    underprojected = _role_key(p.state) == "underprojected"
    overpushed = _role_key(p.state) == "overpushed"

    dense_good_candidate = (
        not body_fragile
        and not body_weak
        and not bridge_broken
        and not boxy_active
        and not transition_fragile
        and (_has(d.center_body_support_proxy) is False or d.center_body_support_proxy >= 0.54)
        and (_has(d.body_to_mid_handoff_proxy) is False or d.body_to_mid_handoff_proxy >= 0.54)
        and (_has(m.mud_to_body_db) is False or m.mud_to_body_db < 0.10)
    )

    dirty_dense_candidate = (
        cleanup_heavy_needed
        and (
            boxy_active
            or (_has(m.mud_to_body_db) and m.mud_to_body_db >= -0.05)
            or (_has(m.lowmid_buildup_ratio_db) and m.lowmid_buildup_ratio_db >= 17.2)
        )
    )

    thin_candidate = body_weak or bridge_broken or transition_fragile

    return RouterContext(
        analysis=analysis,
        selection=selection,
        primary_correction_lane=primary_correction_lane,
        secondary_support_lane=secondary_support_lane,
        primary_benefit_lane=primary_benefit_lane,
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
        dirty_dense_candidate=dirty_dense_candidate,
        dense_good_candidate=dense_good_candidate,
        thin_candidate=thin_candidate,
    )


def normalize_role_rank(
    ctx: RouterContext,
    role: RoleName,
    role_selection: SelectedRoleProfile,
) -> str:
    if not role_selection.enabled:
        return "off"

    key = _role_key(role)

    if key == "cleanup":
        # Cleanup may be primary correction, but it is never allowed to become the master benefit.
        if ctx.primary_correction_lane == "cleanup":
            return "primary"
        if ctx.cleanup_heavy_needed or ctx.cleanup_guarded:
            return "support"
        return "restrained"

    if key == "guard":
        if ctx.primary_correction_lane == "guard":
            return "support"
        if ctx.boxy_active or ctx.transition_fragile:
            return "support"
        return "restrained"

    if key in {"anchor", "bridge"}:
        if key in CORRECTION_LANE_TO_ROLE_KEYS.get(ctx.primary_correction_lane, set()):
            return "primary"
        if key in SUPPORT_LANE_TO_ROLE_KEYS.get(ctx.secondary_support_lane, set()):
            return "support"
        if key in BENEFIT_LANE_TO_ROLE_KEYS.get(ctx.primary_benefit_lane, set()):
            return "support"
        if ctx.body_weak or ctx.foundation_missing or ctx.bridge_broken or ctx.bridge_gap_risky:
            return "support"
        return "restrained"

    if key == "projection":
        # Projection is the main musical reveal path. It should not be demoted by cleanup/guard.
        if ctx.overpushed and ctx.top_fragile:
            return "support"
        if ctx.underprojected:
            return "primary"
        if ctx.primary_benefit_lane == "forward_gain":
            return "primary"
        if role_selection.amount >= 0.12:
            return "support"
        return "support"

    if key == "spark":
        # Spark is not the master by itself, but it is the finish layer.
        if ctx.primary_benefit_lane == "finish_gain":
            return "support"
        if role_selection.amount > 0.0:
            return "support"
        return "restrained"

    return "restrained"


def _energy_from_amount(amount: float) -> str:
    if amount <= 0.0:
        return "off"
    if amount < 0.10:
        return "micro"
    if amount < 0.22:
        return "mild"
    if amount < 0.34:
        return "controlled"
    return "dense"


def _cap_energy_for_rank(energy_class: str, role_rank: str) -> str:
    if role_rank == "off":
        return "off"

    # Only non-musical support is capped hard.
    # Projection and spark are handled later by musical laws, not by a generic kill switch.
    if role_rank == "restrained" and ENERGY_ORDER[energy_class] > ENERGY_ORDER["mild"]:
        return "mild"

    if role_rank == "support" and ENERGY_ORDER[energy_class] > ENERGY_ORDER["controlled"]:
        return "controlled"

    return energy_class


def normalize_energy_class(
    role_rank: str,
    requested_amount: float,
) -> str:
    energy = _energy_from_amount(requested_amount)
    return _cap_energy_for_rank(energy, role_rank)


def derive_target_band_mode(
    ctx: RouterContext,
    role_selection: SelectedRoleProfile,
    role_rank: str,
) -> str:
    p = role_selection.profile_name

    if p == "anchor_restore_controlled":
        return "body_restore" if role_rank in {"primary", "support"} else "body_hold"
    if p == "anchor_hold_safe":
        if ctx.body_weak or ctx.foundation_missing:
            return "body_restore"
        return "body_hold"
    if p == "anchor_restrain_upper_body":
        return "body_restrain"

    if p == "bridge_restore_controlled":
        return "bridge_restore" if role_rank in {"primary", "support"} else "bridge_hold"
    if p == "bridge_hold_safe":
        if ctx.bridge_broken or ctx.bridge_gap_risky:
            return "bridge_restore"
        return "bridge_hold"
    if p == "bridge_restrain_glue":
        return "bridge_restrain"

    if p == "cleanup_focused_dense":
        if ctx.cleanup_heavy_needed and role_rank == "primary":
            return "cleanup_dense"
        if ctx.cleanup_heavy_needed or ctx.cleanup_guarded:
            return "cleanup_guarded"
        return "cleanup_micro"
    if p == "cleanup_guarded_safe":
        return "cleanup_guarded" if ctx.cleanup_heavy_needed or ctx.cleanup_guarded else "cleanup_micro"
    if p == "cleanup_micro_corrective":
        return "cleanup_micro"

    if p == "guard_boxiness_controlled":
        if ctx.boxy_active:
            return "guard_boxiness"
        if ctx.transition_fragile:
            return "guard_transition_support"
        return "guard_hold"
    if p == "guard_transition_support_safe":
        return "guard_transition_support" if ctx.transition_fragile else "guard_hold"
    if p == "guard_hold_safe":
        return "guard_hold"

    if p == "projection_controlled_dense":
        if ctx.overpushed and ctx.top_fragile:
            return "projection_mild"
        if ctx.underprojected:
            return "projection_dense" if not ctx.top_fragile else "projection_mild"
        return "projection_dense" if role_rank == "primary" and not ctx.top_fragile else "projection_mild"
    if p == "projection_mild_safe":
        return "projection_mild"
    if p == "projection_clamp_safe":
        return "projection_clamp" if ctx.top_fragile and ctx.overpushed else "projection_mild"

    if p == "finish_spark_controlled_excited":
        if ctx.top_safe and not ctx.overpushed:
            return "spark_excited"
        return "spark_micro"
    if p == "finish_spark_micro_safe":
        return "spark_micro"
    if p == "finish_spark_off":
        return "spark_micro"

    return "off"


def derive_protection_mode(
    ctx: RouterContext,
    role_selection: SelectedRoleProfile,
    role_rank: str,
    target_band_mode: str,
) -> str:
    if target_band_mode == "body_restore":
        return "body_restore_guarded"
    if target_band_mode == "body_hold":
        return "body_strict"
    if target_band_mode == "body_restrain":
        return "upper_body_restrain_only"

    if target_band_mode == "bridge_restore":
        return "gap_restore_guarded"
    if target_band_mode == "bridge_hold":
        return "bridge_strict"
    if target_band_mode == "bridge_restrain":
        return "glue_strict"

    if target_band_mode == "cleanup_dense":
        return "body_bridge_guarded"
    if target_band_mode == "cleanup_guarded":
        return "body_ultra_guarded"
    if target_band_mode == "cleanup_micro":
        return "micro_only"

    if target_band_mode == "guard_boxiness":
        return "anti_hole"
    if target_band_mode == "guard_transition_support":
        return "transition_support_only"
    if target_band_mode == "guard_hold":
        return "anti_hole"

    if target_band_mode == "projection_dense":
        return "body_link_required"
    if target_band_mode == "projection_mild":
        if ctx.top_fragile:
            return "top_strict"
        return "top_guarded"
    if target_band_mode == "projection_clamp":
        return "top_strict"

    if target_band_mode == "spark_excited":
        return "spark_guarded"
    if target_band_mode == "spark_micro":
        return "spark_micro_only"
    if target_band_mode in {"spark_off", "off"}:
        return "off"

    return "off"


def _derive_primitives_for_plan(
    role: RoleName,
    target_band_mode: str,
    protection_mode: str,
    role_rank: str,
    energy_class: str,
) -> tuple[list[str], list[str]]:
    key = _role_key(role)

    allowed: list[str] = []
    forbidden: set[str] = set()

    if key == "anchor":
        if target_band_mode == "body_restore":
            allowed = [
                "controlled_bell_boost",
                "dynamic_body_support_boost",
                "restrained_parallel_fill",
            ]
        elif target_band_mode == "body_hold":
            allowed = [
                "dynamic_body_support_boost",
                "restrained_parallel_fill",
            ]
        elif target_band_mode == "body_restrain":
            allowed = [
                "restrained_parallel_fill",
            ]
        forbidden.update([
            "micro_air_shelf",
            "micro_top_texture",
            "micro_width_high_only",
            "broad_presence_contour",
            "dynamic_presence_lift",
        ])

    elif key == "bridge":
        if target_band_mode == "bridge_restore":
            allowed = [
                "restrained_parallel_handoff_support",
                "transient_safe_support_compression",
            ]
        elif target_band_mode == "bridge_hold":
            allowed = [
                "restrained_parallel_handoff_support",
            ]
        elif target_band_mode == "bridge_restrain":
            allowed = [
                "transient_safe_support_compression",
            ]
        forbidden.update([
            "micro_air_shelf",
            "micro_top_texture",
            "micro_width_high_only",
            "broad_presence_contour",
            "dynamic_presence_lift",
        ])

    elif key == "cleanup":
        if target_band_mode == "cleanup_dense":
            allowed = [
                "dynamic_bell_cut",
                "dynamic_wide_cut",
                "restrained_static_cut",
                "dynamic_tilt_down",
                "local_antiharsh_control",
            ]
        elif target_band_mode == "cleanup_guarded":
            allowed = [
                "dynamic_bell_cut",
                "restrained_static_cut",
                "local_antiharsh_control",
            ]
        elif target_band_mode == "cleanup_micro":
            allowed = [
                "dynamic_bell_cut",
                "restrained_static_cut",
            ]
        forbidden.update([
            "restrained_parallel_fill",
            "micro_air_shelf",
            "micro_top_texture",
            "micro_width_high_only",
            "broad_presence_contour",
        ])

    elif key == "guard":
        if target_band_mode in {"guard_boxiness", "guard_transition_support"}:
            allowed = [
                "dynamic_bell_cut",
                "restrained_static_cut",
            ]
        elif target_band_mode == "guard_hold":
            allowed = [
                "restrained_static_cut",
            ]
        forbidden.update([
            "micro_air_shelf",
            "micro_top_texture",
            "micro_width_high_only",
            "broad_presence_contour",
            "dynamic_presence_lift",
        ])

    elif key == "projection":
        if target_band_mode == "projection_dense":
            allowed = [
                "broad_presence_contour",
                "dynamic_presence_lift",
                "projection_local_deharsh",
                "band_limited_soft_saturation",
                "controlled_harmonic_density",
            ]
        elif target_band_mode == "projection_mild":
            allowed = [
                "broad_presence_contour",
                "dynamic_presence_lift",
                "projection_local_deharsh",
                "controlled_harmonic_density",
            ]
        elif target_band_mode == "projection_clamp":
            allowed = [
                "projection_local_deharsh",
                "broad_presence_contour",
            ]
        forbidden.update([
            "restrained_parallel_fill",
            "dynamic_tilt_down",
            "micro_air_shelf",
            "micro_width_high_only",
        ])

    elif key == "spark":
        if target_band_mode == "spark_excited":
            allowed = [
                "micro_air_shelf",
                "micro_top_texture",
                "protected_high_side_polish",
                "micro_width_high_only",
                "local_desibilance_control",
            ]
        elif target_band_mode == "spark_micro":
            allowed = [
                "micro_air_shelf",
                "micro_top_texture",
                "protected_high_side_polish",
                "local_desibilance_control",
            ]
        else:
            allowed = []
        forbidden.update([
            "controlled_bell_boost",
            "dynamic_body_support_boost",
            "dynamic_bell_cut",
            "dynamic_wide_cut",
            "restrained_static_cut",
            "dynamic_tilt_down",
            "broad_presence_contour",
            "dynamic_presence_lift",
            "band_limited_soft_saturation",
            "controlled_harmonic_density",
        ])

    if role_rank == "restrained":
        allowed = [
            x for x in allowed
            if x not in {
                "dynamic_tilt_down",
                "band_limited_soft_saturation",
                "micro_width_high_only",
            }
        ]

    if energy_class == "micro":
        allowed = [
            x for x in allowed
            if x in {
                "dynamic_bell_cut",
                "restrained_static_cut",
                "dynamic_body_support_boost",
                "restrained_parallel_fill",
                "restrained_parallel_handoff_support",
                "broad_presence_contour",
                "projection_local_deharsh",
                "micro_air_shelf",
                "micro_top_texture",
                "protected_high_side_polish",
                "local_desibilance_control",
            }
        ]

    if protection_mode == "top_strict":
        forbidden.update({
            "band_limited_soft_saturation",
            "micro_width_high_only",
            "micro_top_texture",
        })
        allowed = [
            x for x in allowed
            if x not in {
                "band_limited_soft_saturation",
                "micro_width_high_only",
                "micro_top_texture",
            }
        ]

    if protection_mode == "spark_micro_only":
        allowed = [
            x for x in allowed
            if x in {
                "micro_air_shelf",
                "micro_top_texture",
                "protected_high_side_polish",
                "local_desibilance_control",
            }
        ]

    return allowed, sorted(forbidden)


def _rank_scale(role_rank: str) -> float:
    if role_rank == "primary":
        return 1.00
    if role_rank == "support":
        return 0.84
    if role_rank == "restrained":
        return 0.56
    return 0.00


def _energy_scale(energy_class: str) -> float:
    if energy_class == "dense":
        return 1.00
    if energy_class == "controlled":
        return 0.86
    if energy_class == "mild":
        return 0.70
    if energy_class == "micro":
        return 0.48
    return 0.00


def _protection_ceiling(protection_mode: str) -> float:
    ceilings = {
        "body_restore_guarded": 0.90,
        "body_strict": 0.86,
        "upper_body_restrain_only": 0.74,
        "gap_restore_guarded": 0.86,
        "bridge_strict": 0.84,
        "glue_strict": 0.68,
        "body_bridge_guarded": 0.78,
        "body_ultra_guarded": 0.68,
        "micro_only": 0.52,
        "anti_hole": 0.78,
        "transition_support_only": 0.76,
        "body_link_required": 0.86,
        "top_guarded": 0.76,
        "top_strict": 0.62,
        "spark_guarded": 0.58,
        "spark_micro_only": 0.42,
        "off": 0.0,
    }
    return ceilings.get(protection_mode, 0.66)


def build_role_execution_plan(
    ctx: RouterContext,
    role_selection: SelectedRoleProfile,
) -> RoleExecutionPlan:
    role = role_selection.role
    role_rank = normalize_role_rank(ctx, role, role_selection)
    requested_amount = role_selection.amount if role_selection.enabled else 0.0
    requested_cap = role_selection.cap if role_selection.enabled else 0.0

    energy_class = normalize_energy_class(role_rank, requested_amount)
    target_band_mode = derive_target_band_mode(ctx, role_selection, role_rank)
    protection_mode = derive_protection_mode(ctx, role_selection, role_rank, target_band_mode)

    allowed_primitives, forbidden_primitives = _derive_primitives_for_plan(
        role=role,
        target_band_mode=target_band_mode,
        protection_mode=protection_mode,
        role_rank=role_rank,
        energy_class=energy_class,
    )

    rank_cap_scale = {
        "primary": 1.00,
        "support": 0.90,
        "restrained": 0.72,
        "off": 0.00,
    }[role_rank]

    execution_cap = requested_cap * rank_cap_scale
    execution_amount = min(requested_amount, execution_cap)
    dynamic_scale = min(
        _rank_scale(role_rank) * _energy_scale(energy_class),
        _protection_ceiling(protection_mode),
    )

    if not role_selection.enabled or role_rank == "off":
        execution_cap = 0.0
        execution_amount = 0.0
        dynamic_scale = 0.0
        target_band_mode = "off"
        protection_mode = "off"
        allowed_primitives = []
        forbidden_primitives = []
        energy_class = "off"

    notes = list(role_selection.notes or [])
    notes.extend([
        f"role_rank={role_rank}",
        f"energy_class={energy_class}",
        f"target_band_mode={target_band_mode}",
        f"protection_mode={protection_mode}",
    ])

    return RoleExecutionPlan(
        role=role,
        enabled=role_selection.enabled and role_rank != "off",
        profile_name=role_selection.profile_name,
        role_rank=role_rank,
        energy_class=energy_class,
        requested_amount=requested_amount,
        requested_cap=requested_cap,
        execution_amount=execution_amount,
        execution_cap=execution_cap,
        dynamic_scale=dynamic_scale,
        target_band_mode=target_band_mode,
        protection_mode=protection_mode,
        allowed_primitives=allowed_primitives,
        forbidden_primitives=forbidden_primitives,
        interaction_tags=[],
        notes=notes,
    )


def _recompute_energy_class(role_rank: str, execution_amount: float) -> str:
    return normalize_energy_class(role_rank, execution_amount)


def _rewrite_plan(
    plan: RoleExecutionPlan,
    *,
    enabled: bool | None = None,
    role_rank: str | None = None,
    min_amount: float | None = None,
    min_cap: float | None = None,
    max_amount: float | None = None,
    max_cap: float | None = None,
    min_dynamic: float | None = None,
    max_dynamic: float | None = None,
    target_band_mode: str | None = None,
    protection_mode: str | None = None,
    add_tags: list[str] | None = None,
    add_notes: list[str] | None = None,
) -> RoleExecutionPlan:
    new_enabled = plan.enabled if enabled is None else enabled
    new_role_rank = role_rank or plan.role_rank

    execution_cap = plan.execution_cap
    if min_cap is not None:
        execution_cap = max(execution_cap, min_cap)
    if max_cap is not None:
        execution_cap = min(execution_cap, max_cap)

    execution_amount = plan.execution_amount
    if min_amount is not None:
        execution_amount = max(execution_amount, min_amount)
    if max_amount is not None:
        execution_amount = min(execution_amount, max_amount)

    execution_amount = min(execution_amount, execution_cap)

    new_target_band_mode = target_band_mode or plan.target_band_mode
    new_protection_mode = protection_mode or plan.protection_mode

    dynamic_scale = plan.dynamic_scale
    if min_dynamic is not None:
        dynamic_scale = max(dynamic_scale, min_dynamic)
    if max_dynamic is not None:
        dynamic_scale = min(dynamic_scale, max_dynamic)

    dynamic_scale = min(dynamic_scale, _protection_ceiling(new_protection_mode))

    if not new_enabled:
        execution_cap = 0.0
        execution_amount = 0.0
        dynamic_scale = 0.0
        new_target_band_mode = "off"
        new_protection_mode = "off"
        new_role_rank = "off"

    energy_class = _recompute_energy_class(new_role_rank, execution_amount)

    allowed_primitives, forbidden_primitives = _derive_primitives_for_plan(
        role=plan.role,
        target_band_mode=new_target_band_mode,
        protection_mode=new_protection_mode,
        role_rank=new_role_rank,
        energy_class=energy_class,
    )

    interaction_tags = _append_unique(list(plan.interaction_tags or []), add_tags or [])
    notes = _append_unique(list(plan.notes or []), add_notes or [])

    return replace(
        plan,
        enabled=new_enabled,
        role_rank=new_role_rank,
        execution_amount=execution_amount,
        execution_cap=execution_cap,
        dynamic_scale=dynamic_scale,
        target_band_mode=new_target_band_mode,
        protection_mode=new_protection_mode,
        energy_class=energy_class,
        allowed_primitives=allowed_primitives,
        forbidden_primitives=forbidden_primitives,
        interaction_tags=interaction_tags,
        notes=notes,
    )


def apply_interaction_clamps(
    ctx: RouterContext,
    anchor: RoleExecutionPlan,
    bridge: RoleExecutionPlan,
    cleanup: RoleExecutionPlan,
    guard: RoleExecutionPlan,
    projection: RoleExecutionPlan,
    spark: RoleExecutionPlan,
) -> tuple[
    RoleExecutionPlan,
    RoleExecutionPlan,
    RoleExecutionPlan,
    RoleExecutionPlan,
    RoleExecutionPlan,
    RoleExecutionPlan,
    list[str],
]:
    global_notes: list[str] = []

    quiet_score = _analysis_quiet_score(ctx.analysis)
    top_emergency = _top_emergency(ctx.analysis)
    delivery_emergency = _delivery_emergency(ctx.analysis)

    # ------------------------------------------------------------
    # 1. Cleanup law
    # Cleanup prepares space. It never kills projection.
    # ------------------------------------------------------------
    if ctx.body_fragile or ctx.body_weak or ctx.bridge_broken or ctx.bridge_gap_risky:
        if cleanup.enabled and cleanup.target_band_mode == "cleanup_dense":
            cleanup = _rewrite_plan(
                cleanup,
                max_amount=0.24,
                max_cap=0.36,
                max_dynamic=0.64,
                target_band_mode="cleanup_guarded",
                protection_mode="body_bridge_guarded",
                add_tags=["cleanup_preserve_body_bridge"],
                add_notes=[
                    "cleanup dense reshaped to guarded because body/bridge must survive",
                    "cleanup does not demote projection",
                ],
            )
            global_notes.append("cleanup reshaped to guarded without blocking projection")

    if cleanup.enabled:
        cleanup = _rewrite_plan(
            cleanup,
            add_tags=["cleanup_is_preparation_not_benefit"],
            add_notes=[
                "cleanup prepares controlled space for support/projection",
                "cleanup is not allowed to become the main musical benefit",
            ],
        )

    # ------------------------------------------------------------
    # 2. Guard law
    # Guard shapes body transition. It never kills projection.
    # ------------------------------------------------------------
    if guard.enabled:
        guard = _rewrite_plan(
            guard,
            add_tags=["guard_shapes_not_blocks"],
            add_notes=[
                "guard preserves body-to-mid form",
                "guard does not demote projection",
            ],
        )

    if ctx.transition_fragile and guard.enabled and guard.target_band_mode == "guard_boxiness":
        guard = _rewrite_plan(
            guard,
            max_amount=0.24,
            max_cap=0.36,
            max_dynamic=0.70,
            target_band_mode="guard_transition_support",
            protection_mode="transition_support_only",
            add_tags=["transition_support_bias"],
            add_notes=["transition fragility biases guard toward continuity, not hollowing"],
        )
        global_notes.append("guard biased toward transition support")

    # ------------------------------------------------------------
    # 3. Support law
    # Body and bridge support are part of the master character.
    # They are shaped, not killed.
    # ------------------------------------------------------------
    if ctx.body_weak or ctx.foundation_missing:
        anchor = _rewrite_plan(
            anchor,
            enabled=True,
            role_rank="support" if anchor.role_rank == "off" else anchor.role_rank,
            min_amount=0.18,
            min_cap=0.28,
            min_dynamic=0.58,
            target_band_mode="body_restore",
            protection_mode="body_restore_guarded",
            add_tags=["mandatory_body_support"],
            add_notes=["body/foundation weakness forces musical body support"],
        )
        global_notes.append("anchor forced/kept for body support")

    elif anchor.enabled and anchor.target_band_mode == "off":
        anchor = _rewrite_plan(
            anchor,
            enabled=True,
            role_rank="restrained",
            min_amount=0.10,
            min_cap=0.18,
            min_dynamic=0.42,
            target_band_mode="body_hold",
            protection_mode="body_strict",
            add_tags=["body_hold_floor"],
            add_notes=["minimal body hold floor"],
        )

    if ctx.bridge_broken or ctx.bridge_gap_risky:
        bridge = _rewrite_plan(
            bridge,
            enabled=True,
            role_rank="support" if bridge.role_rank == "off" else bridge.role_rank,
            min_amount=0.16,
            min_cap=0.26,
            min_dynamic=0.54,
            target_band_mode="bridge_restore",
            protection_mode="gap_restore_guarded",
            add_tags=["mandatory_bridge_support"],
            add_notes=["bridge/gap risk forces bass-to-body continuity support"],
        )
        global_notes.append("bridge forced/kept for bass-to-body continuity")

    elif ctx.bridge_gluey:
        bridge = _rewrite_plan(
            bridge,
            enabled=True,
            role_rank="support" if bridge.role_rank == "off" else bridge.role_rank,
            min_amount=0.10,
            min_cap=0.20,
            max_amount=0.22,
            max_cap=0.34,
            max_dynamic=0.68,
            target_band_mode="bridge_restrain",
            protection_mode="glue_strict",
            add_tags=["bridge_glue_shape_not_kill"],
            add_notes=["glue-prone bridge is restrained, not killed"],
        )
        global_notes.append("bridge glue shaped without killing support")

    # ------------------------------------------------------------
    # 4. Projection mandatory law
    # Projection is the core studio-forward block.
    # It is never disabled just because cleanup/guard are active.
    # ------------------------------------------------------------
    if top_emergency:
        projection = _rewrite_plan(
            projection,
            enabled=True,
            role_rank="support",
            min_amount=0.10,
            min_cap=0.20,
            max_amount=0.18,
            max_cap=0.28,
            min_dynamic=0.34,
            max_dynamic=0.54,
            target_band_mode="projection_clamp",
            protection_mode="top_strict",
            add_tags=["projection_emergency_clamp_not_off"],
            add_notes=["extreme top risk clamps projection shape instead of switching master reveal off"],
        )
        global_notes.append("projection emergency-clamped, not killed")

    elif ctx.underprojected:
        projection = _rewrite_plan(
            projection,
            enabled=True,
            role_rank="primary",
            min_amount=0.30 if ctx.top_safe else 0.24,
            min_cap=0.42 if ctx.top_safe else 0.34,
            min_dynamic=0.72 if ctx.top_safe else 0.62,
            target_band_mode="projection_dense" if ctx.top_safe else "projection_mild",
            protection_mode="body_link_required" if ctx.top_safe else "top_guarded",
            add_tags=["mandatory_projection_reveal"],
            add_notes=["underprojected track requires real projection, not spark substitution"],
        )
        global_notes.append("underprojected track forces musical projection")

    elif ctx.overpushed:
        projection = _rewrite_plan(
            projection,
            enabled=True,
            role_rank="support" if projection.role_rank == "off" else projection.role_rank,
            min_amount=0.14,
            min_cap=0.24,
            min_dynamic=0.48,
            max_amount=0.24,
            max_cap=0.36,
            target_band_mode="projection_mild",
            protection_mode="top_guarded" if not ctx.top_fragile else "top_strict",
            add_tags=["overpushed_projection_shape"],
            add_notes=["overpushed track keeps projection but changes shape"],
        )
        global_notes.append("overpushed projection shaped, not disabled")

    elif quiet_score >= 0.42:
        projection = _rewrite_plan(
            projection,
            enabled=True,
            role_rank="primary" if projection.role_rank in {"off", "restrained"} else projection.role_rank,
            min_amount=0.28,
            min_cap=0.40,
            min_dynamic=0.68,
            target_band_mode="projection_dense" if ctx.top_safe else "projection_mild",
            protection_mode="body_link_required" if ctx.top_safe else "top_guarded",
            add_tags=["quiet_track_reveal_floor"],
            add_notes=["quiet track needs audible musical reveal and forward studio build"],
        )
        global_notes.append("quiet track projection floor active")

    else:
        projection = _rewrite_plan(
            projection,
            enabled=True,
            role_rank="support" if projection.role_rank == "off" else projection.role_rank,
            min_amount=0.18,
            min_cap=0.30,
            min_dynamic=0.54,
            target_band_mode=projection.target_band_mode if projection.target_band_mode != "off" else "projection_mild",
            protection_mode=projection.protection_mode if projection.protection_mode != "off" else "top_guarded",
            add_tags=["projection_always_present"],
            add_notes=["projection has a musical floor in polish branch"],
        )

    # Cleanup/guard can inform projection protection, but cannot demote it.
    if cleanup.enabled:
        projection = _rewrite_plan(
            projection,
            add_tags=["post_cleanup_projection_required"],
            add_notes=["cleanup created space; projection must use that space musically"],
        )

    if guard.enabled:
        projection = _rewrite_plan(
            projection,
            protection_mode="body_link_required" if projection.target_band_mode == "projection_dense" and not ctx.top_fragile else projection.protection_mode,
            add_tags=["projection_body_link_after_guard"],
            add_notes=["projection follows guarded body shape instead of bypassing body"],
        )

    # ------------------------------------------------------------
    # 5. Spark mandatory finish law
    # Spark is finish. It is protected, not normally switched off.
    # ------------------------------------------------------------
    if top_emergency or delivery_emergency:
        spark = _rewrite_plan(
            spark,
            enabled=False,
            add_tags=["spark_off_only_emergency"],
            add_notes=["spark disabled only by true emergency condition"],
        )
        global_notes.append("spark disabled by emergency only")

    elif ctx.top_safe and projection.enabled and projection.execution_amount >= 0.24 and not ctx.overpushed:
        spark = _rewrite_plan(
            spark,
            enabled=True,
            role_rank="support" if spark.role_rank == "off" else spark.role_rank,
            min_amount=0.12,
            min_cap=0.22,
            min_dynamic=0.44,
            target_band_mode="spark_excited",
            protection_mode="spark_guarded",
            add_tags=["spark_finish_character"],
            add_notes=["safe projected track gets audible protected finish spark"],
        )
        global_notes.append("spark excited finish active")

    else:
        spark = _rewrite_plan(
            spark,
            enabled=True,
            role_rank="support" if spark.role_rank == "off" else spark.role_rank,
            min_amount=0.07,
            min_cap=0.14,
            min_dynamic=0.30,
            target_band_mode="spark_micro",
            protection_mode="spark_micro_only",
            add_tags=["spark_micro_finish_floor"],
            add_notes=["spark remains as protected finish floor"],
        )
        global_notes.append("spark micro finish floor active")

    if ctx.top_fragile and spark.enabled:
        spark = _rewrite_plan(
            spark,
            max_amount=0.10,
            max_cap=0.16,
            max_dynamic=0.34,
            target_band_mode="spark_micro",
            protection_mode="spark_micro_only",
            add_tags=["top_fragile_spark_protected"],
            add_notes=["top fragility removes risky sparkle behavior but keeps protected finish"],
        )
        global_notes.append("top fragile spark protected, not killed")

    # ------------------------------------------------------------
    # 6. Final musical identity note
    # Delivery is not handled here as a creative suppressor.
    # True output safety belongs to delivery/DSP terminal stage.
    # ------------------------------------------------------------
    if delivery_emergency:
        global_notes.append("delivery emergency detected: terminal delivery must protect output")
    else:
        global_notes.append("delivery is not allowed to suppress creative polish blocks in router")

    global_notes.extend([
        "router_policy=musical_dispatcher_not_guardian",
        "cleanup_prepares_space",
        "support_preserves_body_and_bass_bridge",
        "projection_is_mandatory_master_reveal",
        "spark_is_mandatory_finish_except_emergency",
        "risk_changes_shape_not_off",
    ])

    return anchor, bridge, cleanup, guard, projection, spark, global_notes


def compile_execution_blueprint(
    ctx: RouterContext,
    anchor: RoleExecutionPlan,
    bridge: RoleExecutionPlan,
    cleanup: RoleExecutionPlan,
    guard: RoleExecutionPlan,
    projection: RoleExecutionPlan,
    spark: RoleExecutionPlan,
    global_notes: list[str],
) -> SmartMasterExecutionBlueprint:
    notes = [
        f"primary_correction_lane={ctx.primary_correction_lane}",
        f"secondary_support_lane={ctx.secondary_support_lane}",
        f"primary_benefit_lane={ctx.primary_benefit_lane}",
    ]
    notes.extend(global_notes)

    return SmartMasterExecutionBlueprint(
        anchor=anchor,
        bridge=bridge,
        cleanup=cleanup,
        guard=guard,
        projection=projection,
        spark=spark,
        primary_correction_lane=ctx.primary_correction_lane,
        secondary_support_lane=ctx.secondary_support_lane,
        primary_benefit_lane=ctx.primary_benefit_lane,
        assembly_order=ASSEMBLY_ORDER[:],
        global_notes=notes,
    )


def build_sm_execution_blueprint(
    analysis: SmartMasterAnalysis,
    selection: RoleProfileSelection,
) -> SmartMasterExecutionBlueprint:
    ctx = build_router_context(analysis, selection)

    cleanup = build_role_execution_plan(ctx, selection.cleanup)
    guard = build_role_execution_plan(ctx, selection.guard)
    anchor = build_role_execution_plan(ctx, selection.anchor)
    bridge = build_role_execution_plan(ctx, selection.bridge)
    projection = build_role_execution_plan(ctx, selection.projection)
    spark = build_role_execution_plan(ctx, selection.spark)

    anchor, bridge, cleanup, guard, projection, spark, global_notes = apply_interaction_clamps(
        ctx,
        anchor,
        bridge,
        cleanup,
        guard,
        projection,
        spark,
    )

    return compile_execution_blueprint(
        ctx,
        anchor,
        bridge,
        cleanup,
        guard,
        projection,
        spark,
        global_notes,
    )


def build_sm_router_summary(
    analysis: SmartMasterAnalysis,
    selection: RoleProfileSelection,
) -> SmartMasterExecutionBlueprint:
    return build_sm_execution_blueprint(analysis, selection)

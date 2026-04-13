# sm/router.py

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
    "cleanup_support": {"cleanup"},
    "guard_support": {"guard"},
    "projection_support": {"projection"},
    "none": set(),
}

BENEFIT_LANE_TO_ROLE_KEYS = {
    "body_gain": {"anchor", "bridge"},
    "clarity_gain": {"cleanup", "guard"},
    "forward_gain": {"projection"},
    "finish_gain": {"spark"},
    "none": set(),
}

ENERGY_ORDER = {
    "off": 0,
    "micro": 1,
    "mild": 2,
    "controlled": 3,
    "dense": 4,
}

ASSEMBLY_ORDER = [
    "anchor",
    "bridge",
    "cleanup",
    "guard",
    "projection",
    "spark",
]


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
    ranked = []
    for field in ASSEMBLY_ORDER:
        role_sel = getattr(selection, field, None)
        if role_sel is None or not role_sel.enabled:
            continue
        ranked.append((role_sel.amount, field))
    if not ranked:
        return "none"
    ranked.sort(reverse=True)
    top_field = ranked[0][1]
    if top_field in ("anchor", "bridge"):
        return "anchor_bridge"
    if top_field == "cleanup":
        return "cleanup"
    if top_field == "guard":
        return "guard"
    if top_field == "projection":
        return "projection"
    return "stability_hold"


def _fallback_support_lane(selection: RoleProfileSelection) -> str:
    guard = getattr(selection, "guard", None)
    projection = getattr(selection, "projection", None)
    cleanup = getattr(selection, "cleanup", None)

    if guard and guard.enabled and guard.amount >= 0.18:
        return "guard_support"
    if projection and projection.enabled and projection.amount >= 0.16:
        return "projection_support"
    if cleanup and cleanup.enabled and cleanup.amount >= 0.18:
        return "cleanup_support"
    return "none"


def _fallback_benefit_lane(selection: RoleProfileSelection) -> str:
    spark = getattr(selection, "spark", None)
    projection = getattr(selection, "projection", None)
    cleanup = getattr(selection, "cleanup", None)
    anchor = getattr(selection, "anchor", None)
    bridge = getattr(selection, "bridge", None)

    if spark and spark.enabled and spark.amount > 0.0:
        return "finish_gain"
    if projection and projection.enabled and projection.amount >= 0.16:
        return "forward_gain"
    if cleanup and cleanup.enabled and cleanup.amount >= 0.18:
        return "clarity_gain"
    if (anchor and anchor.enabled and anchor.amount >= 0.22) or (bridge and bridge.enabled and bridge.amount >= 0.20):
        return "body_gain"
    return "forward_gain"


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

    primary_correction_lane = _collect_lane_from_selection(selection, "correction") or _fallback_primary_correction_lane(selection)
    secondary_support_lane = _collect_lane_from_selection(selection, "support") or _fallback_support_lane(selection)
    primary_benefit_lane = _collect_lane_from_selection(selection, "benefit") or _fallback_benefit_lane(selection)

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
        and (boxy_active or (_has(m.mud_to_body_db) and m.mud_to_body_db >= -0.05))
        and (bridge_gluey or (_has(m.lowmid_buildup_ratio_db) and m.lowmid_buildup_ratio_db >= 17.2))
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

    if key in CORRECTION_LANE_TO_ROLE_KEYS.get(ctx.primary_correction_lane, set()):
        return "primary"

    if key in SUPPORT_LANE_TO_ROLE_KEYS.get(ctx.secondary_support_lane, set()):
        return "support"

    if key in BENEFIT_LANE_TO_ROLE_KEYS.get(ctx.primary_benefit_lane, set()):
        return "support"

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
        return "body_hold"
    if p == "anchor_restrain_upper_body":
        return "body_restrain"

    if p == "bridge_restore_controlled":
        return "bridge_restore" if role_rank in {"primary", "support"} else "bridge_hold"
    if p == "bridge_hold_safe":
        return "bridge_hold"
    if p == "bridge_restrain_glue":
        return "bridge_restrain"

    if p == "cleanup_focused_dense":
        if role_rank == "primary":
            return "cleanup_dense"
        if role_rank == "support":
            return "cleanup_guarded"
        return "cleanup_micro"
    if p == "cleanup_guarded_safe":
        return "cleanup_guarded"
    if p == "cleanup_micro_corrective":
        return "cleanup_micro"

    if p == "guard_boxiness_controlled":
        return "guard_boxiness" if role_rank in {"primary", "support"} else "guard_hold"
    if p == "guard_transition_support_safe":
        return "guard_transition_support"
    if p == "guard_hold_safe":
        return "guard_hold"

    if p == "projection_controlled_dense":
        return "projection_dense" if role_rank == "primary" else "projection_mild"
    if p == "projection_mild_safe":
        return "projection_mild"
    if p == "projection_clamp_safe":
        return "projection_clamp"

    if p == "finish_spark_controlled_excited":
        return "spark_excited" if role_rank in {"primary", "support"} else "spark_micro"
    if p == "finish_spark_micro_safe":
        return "spark_micro"
    if p == "finish_spark_off":
        return "spark_off"

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
        return "top_guarded"
    if target_band_mode == "projection_clamp":
        return "top_strict"

    if target_band_mode == "spark_excited":
        return "spark_guarded"
    if target_band_mode == "spark_micro":
        return "spark_micro_only"
    if target_band_mode == "spark_off":
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
        if target_band_mode in {"body_restore", "body_hold"}:
            allowed = ["bell_boost", "dynamic_bell_boost", "parallel_band_fill"]
        elif target_band_mode == "body_restrain":
            allowed = ["dynamic_bell_cut", "bell_cut"]
        forbidden.update(["micro_air_shelf", "micro_top_texture", "micro_width_layer", "broad_presence_lift"])

    elif key == "bridge":
        if target_band_mode in {"bridge_restore", "bridge_hold"}:
            allowed = ["dynamic_bell_boost", "parallel_band_shape", "transient_safe_compression"]
        elif target_band_mode == "bridge_restrain":
            allowed = ["dynamic_bell_cut", "transient_safe_compression"]
        forbidden.update(["micro_air_shelf", "micro_top_texture", "micro_width_layer", "broad_presence_lift"])

    elif key == "cleanup":
        if target_band_mode == "cleanup_dense":
            allowed = ["bell_cut", "dynamic_bell_cut", "wide_tilt"]
        elif target_band_mode == "cleanup_guarded":
            allowed = ["dynamic_bell_cut", "bell_cut", "wide_tilt"]
        else:
            allowed = ["dynamic_bell_cut", "bell_cut"]
        forbidden.update(["parallel_band_fill", "micro_air_shelf", "micro_top_texture", "broad_presence_lift"])

    elif key == "guard":
        if target_band_mode == "guard_transition_support":
            allowed = ["dynamic_bell_boost", "parallel_band_shape"]
        else:
            allowed = ["dynamic_bell_cut", "parallel_band_shape"]
        forbidden.update(["micro_air_shelf", "micro_top_texture", "micro_width_layer", "broad_presence_lift"])

    elif key == "projection":
        if target_band_mode == "projection_clamp":
            allowed = ["broad_presence_lift", "dynamic_bell_boost"]
        else:
            allowed = ["broad_presence_lift", "dynamic_bell_boost", "restrained_soft_saturation"]
        forbidden.update(["parallel_band_fill", "bell_cut", "micro_air_shelf", "micro_width_layer"])

    elif key == "spark":
        if target_band_mode == "spark_off":
            allowed = []
        else:
            allowed = ["micro_air_shelf", "micro_top_texture", "micro_width_layer"]
        forbidden.update([
            "bell_boost",
            "bell_cut",
            "dynamic_bell_boost",
            "dynamic_bell_cut",
            "parallel_band_fill",
            "parallel_band_shape",
            "broad_presence_lift",
            "wide_tilt",
            "transient_safe_compression",
        ])

    if role_rank == "restrained":
        allowed = [x for x in allowed if x not in {"wide_tilt", "restrained_soft_saturation", "parallel_band_fill"}]

    if energy_class == "micro":
        allowed = [x for x in allowed if x in {
            "dynamic_bell_cut",
            "dynamic_bell_boost",
            "micro_air_shelf",
            "micro_top_texture",
            "micro_width_layer",
            "broad_presence_lift",
        }]
        if not allowed and target_band_mode != "spark_off":
            if key in {"anchor", "bridge", "projection"}:
                allowed = ["dynamic_bell_boost"]
            elif key in {"cleanup", "guard"}:
                allowed = ["dynamic_bell_cut"]

    if protection_mode == "top_strict":
        forbidden.update({"restrained_soft_saturation", "micro_top_texture", "micro_air_shelf", "micro_width_layer"})

    if protection_mode == "spark_micro_only":
        allowed = [x for x in allowed if x in {"micro_air_shelf", "micro_top_texture", "micro_width_layer"}]

    return allowed, sorted(forbidden)


def _rank_scale(role_rank: str) -> float:
    if role_rank == "primary":
        return 1.00
    if role_rank == "support":
        return 0.78
    if role_rank == "restrained":
        return 0.52
    return 0.00


def _energy_scale(energy_class: str) -> float:
    if energy_class == "dense":
        return 1.00
    if energy_class == "controlled":
        return 0.84
    if energy_class == "mild":
        return 0.68
    if energy_class == "micro":
        return 0.44
    return 0.00


def _protection_ceiling(protection_mode: str) -> float:
    ceilings = {
        "body_restore_guarded": 0.82,
        "body_strict": 0.82,
        "upper_body_restrain_only": 0.70,
        "gap_restore_guarded": 0.78,
        "bridge_strict": 0.80,
        "glue_strict": 0.62,
        "body_bridge_guarded": 0.72,
        "body_ultra_guarded": 0.62,
        "micro_only": 0.46,
        "anti_hole": 0.76,
        "transition_support_only": 0.74,
        "body_link_required": 0.72,
        "top_guarded": 0.66,
        "top_strict": 0.58,
        "spark_guarded": 0.46,
        "spark_micro_only": 0.32,
        "off": 0.0,
    }
    return ceilings.get(protection_mode, 0.60)


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
        "support": 0.86,
        "restrained": 0.68,
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


def _apply_plan_clamp(
    plan: RoleExecutionPlan,
    *,
    max_amount: float | None = None,
    max_cap: float | None = None,
    max_dynamic: float | None = None,
    force_target_band_mode: str | None = None,
    force_protection_mode: str | None = None,
    force_enabled: bool | None = None,
    add_tags: list[str] | None = None,
    add_notes: list[str] | None = None,
) -> RoleExecutionPlan:
    enabled = plan.enabled if force_enabled is None else force_enabled

    execution_cap = plan.execution_cap
    if max_cap is not None:
        execution_cap = min(execution_cap, max_cap)

    execution_amount = plan.execution_amount
    if max_amount is not None:
        execution_amount = min(execution_amount, max_amount)
    execution_amount = min(execution_amount, execution_cap)

    dynamic_scale = plan.dynamic_scale
    if max_dynamic is not None:
        dynamic_scale = min(dynamic_scale, max_dynamic)

    target_band_mode = force_target_band_mode or plan.target_band_mode
    protection_mode = force_protection_mode or plan.protection_mode

    if not enabled:
        execution_amount = 0.0
        execution_cap = 0.0
        dynamic_scale = 0.0
        target_band_mode = "off"
        protection_mode = "off"

    energy_class = _recompute_energy_class(plan.role_rank, execution_amount)
    allowed_primitives, forbidden_primitives = _derive_primitives_for_plan(
        role=plan.role,
        target_band_mode=target_band_mode,
        protection_mode=protection_mode,
        role_rank=plan.role_rank,
        energy_class=energy_class,
    )

    interaction_tags = list(plan.interaction_tags)
    if add_tags:
        for tag in add_tags:
            if tag not in interaction_tags:
                interaction_tags.append(tag)

    notes = list(plan.notes)
    if add_notes:
        notes.extend(add_notes)

    return replace(
        plan,
        enabled=enabled,
        execution_amount=execution_amount,
        execution_cap=execution_cap,
        dynamic_scale=dynamic_scale,
        target_band_mode=target_band_mode,
        protection_mode=protection_mode,
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

    if ctx.body_fragile or ctx.body_weak:
        if cleanup.target_band_mode == "cleanup_dense":
            cleanup = _apply_plan_clamp(
                cleanup,
                max_amount=0.22,
                max_cap=0.34,
                max_dynamic=0.58,
                force_target_band_mode="cleanup_guarded",
                force_protection_mode="body_ultra_guarded",
                add_tags=["body_protects_cleanup"],
                add_notes=["clamped by body weakness / fragility"],
            )
            global_notes.append("cleanup dense clamped by body protection")

    if ctx.bridge_broken or ctx.bridge_gap_risky:
        if cleanup.target_band_mode == "cleanup_dense":
            cleanup = _apply_plan_clamp(
                cleanup,
                max_amount=0.20,
                max_cap=0.32,
                max_dynamic=0.54,
                force_target_band_mode="cleanup_guarded",
                force_protection_mode="body_bridge_guarded",
                add_tags=["bridge_protects_cleanup"],
                add_notes=["clamped by bridge broken / gap risk"],
            )
            global_notes.append("cleanup clamped by bridge protection")

    if ctx.bridge_gluey:
        bridge = _apply_plan_clamp(
            bridge,
            max_amount=0.20,
            max_cap=0.32,
            max_dynamic=0.60,
            add_tags=["glue_clamp"],
            add_notes=["glue tendency limits bridge energy"],
        )
        if anchor.target_band_mode == "body_restore":
            anchor = _apply_plan_clamp(
                anchor,
                max_amount=0.24,
                max_cap=0.36,
                max_dynamic=0.74,
                add_tags=["anchor_moderated_by_glue"],
                add_notes=["anchor restore moderated by glue-prone bridge"],
            )
        global_notes.append("glue clamp active")

    if ctx.primary_correction_lane == "cleanup" and projection.target_band_mode == "projection_dense":
        projection = _apply_plan_clamp(
            projection,
            max_amount=0.18,
            max_cap=0.30,
            max_dynamic=0.56,
            force_target_band_mode="projection_mild",
            force_protection_mode="top_guarded",
            add_tags=["cleanup_projection_clamp"],
            add_notes=["projection softened because cleanup is primary"],
        )
        global_notes.append("projection softened by cleanup-primary rule")

    if guard.target_band_mode == "guard_boxiness" and projection.target_band_mode == "projection_dense":
        projection = _apply_plan_clamp(
            projection,
            max_amount=0.18,
            max_cap=0.30,
            max_dynamic=0.56,
            force_target_band_mode="projection_mild",
            force_protection_mode="top_guarded",
            add_tags=["guard_projection_clamp"],
            add_notes=["projection softened by active boxiness control"],
        )
        global_notes.append("projection softened by active guard")

    if ctx.underprojected:
        if spark.enabled:
            spark = _apply_plan_clamp(
                spark,
                force_enabled=False,
                add_tags=["underprojected_blocks_spark"],
                add_notes=["spark disabled because projection is underprojected"],
            )
            global_notes.append("spark blocked by underprojected state")
    elif projection.target_band_mode != "projection_dense" and spark.target_band_mode == "spark_excited":
        spark = _apply_plan_clamp(
            spark,
            max_amount=0.08,
            max_cap=0.16,
            max_dynamic=0.32,
            force_target_band_mode="spark_micro",
            force_protection_mode="spark_micro_only",
            add_tags=["projection_not_established_blocks_excited_spark"],
            add_notes=["spark downgraded because projection is not fully established"],
        )
        global_notes.append("spark downgraded because projection not fully established")

    if ctx.top_fragile:
        projection = _apply_plan_clamp(
            projection,
            max_amount=0.16,
            max_cap=0.28,
            max_dynamic=0.52,
            force_target_band_mode="projection_mild" if projection.enabled else "off",
            force_protection_mode="top_strict" if projection.enabled else "off",
            add_tags=["top_fragile_projection_clamp"],
            add_notes=["top fragility clamps projection"],
        )
        if spark.enabled:
            spark = _apply_plan_clamp(
                spark,
                force_enabled=False,
                add_tags=["top_fragile_blocks_spark"],
                add_notes=["spark disabled by top fragility"],
            )
        global_notes.append("top fragility clamp active")

    if ctx.thin_candidate:
        if cleanup.target_band_mode == "cleanup_dense":
            cleanup = _apply_plan_clamp(
                cleanup,
                max_amount=0.20,
                max_cap=0.30,
                max_dynamic=0.54,
                force_target_band_mode="cleanup_guarded",
                force_protection_mode="body_ultra_guarded",
                add_tags=["thin_track_cleanup_clamp"],
                add_notes=["thin-track subtractive clamp on cleanup"],
            )
        if guard.target_band_mode == "guard_boxiness" and ctx.transition_fragile:
            guard = _apply_plan_clamp(
                guard,
                max_amount=0.20,
                max_cap=0.30,
                max_dynamic=0.62,
                force_target_band_mode="guard_transition_support",
                force_protection_mode="transition_support_only",
                add_tags=["thin_track_guard_support_bias"],
                add_notes=["thin-track bias toward transition support"],
            )
        global_notes.append("thin-track subtractive clamp considered")

    if spark.enabled and projection.enabled:
        spark = _apply_plan_clamp(
            spark,
            max_dynamic=min(spark.dynamic_scale, projection.dynamic_scale * 0.85),
            add_tags=["spark_bound_to_projection"],
            add_notes=["spark dynamic scale bound to projection"],
        )

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

    anchor = build_role_execution_plan(ctx, selection.anchor)
    bridge = build_role_execution_plan(ctx, selection.bridge)
    cleanup = build_role_execution_plan(ctx, selection.cleanup)
    guard = build_role_execution_plan(ctx, selection.guard)
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

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import List, Optional

from ..contracts import SmartMasterAnalysis
from ..enums import RiskLevel
from .contracts import DSPActiveClamp, DSPExecutionBlueprint, RoleDSPStack
from .primitives import PRIMITIVE_REGISTRY
from .role_specs import get_role_mode_spec


ALL_PRIMITIVE_NAMES = sorted(PRIMITIVE_REGISTRY.keys())


@dataclass
class DSPRiskContext:
    foundation_missing: bool
    body_fragile: bool
    body_weak: bool

    bridge_broken: bool
    bridge_gluey: bool
    bridge_gap_risky: bool

    cleanup_heavy_needed: bool
    thin_candidate: bool

    boxy_active: bool
    transition_fragile: bool

    underprojected: bool
    overpushed: bool

    top_safe: bool
    top_guarded: bool
    top_fragile: bool

    delivery_overbudget: bool
    delivery_extreme: bool


def _has(v) -> bool:
    return v is not None


def _risk_ge(risk: RiskLevel, level: RiskLevel) -> bool:
    order = {
        RiskLevel.LOW: 0,
        RiskLevel.MEDIUM: 1,
        RiskLevel.HIGH: 2,
    }
    return order[risk] >= order[level]


def _enum_value(v) -> str:
    if hasattr(v, "value"):
        return str(v.value).strip().lower()
    return str(v).strip().lower()


def _uniq(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        if not value:
            continue
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _enabled(stack: Optional[RoleDSPStack]) -> bool:
    return stack is not None and stack.enabled


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _read(obj, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _derive_risk_context(analysis: SmartMasterAnalysis) -> DSPRiskContext:
    m = analysis.metrics
    d = analysis.derived
    a = analysis.anchor
    b = analysis.bridge
    c = analysis.cleanup
    g = analysis.guard
    p = analysis.projection

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

    bridge_broken = _enum_value(b.state) == "broken"
    bridge_gluey = _enum_value(b.state) == "overglued" or _risk_ge(b.glue_risk, RiskLevel.MEDIUM)
    bridge_gap_risky = _risk_ge(b.gap_risk, RiskLevel.MEDIUM)

    cleanup_heavy_needed = (
        _enum_value(c.readiness) == "safe"
        and c.buildup_risk == RiskLevel.HIGH
    )

    boxy_active = _enum_value(g.shape) == "boxy"
    transition_fragile = _enum_value(g.transition_state) in {"weak", "thinning"}

    underprojected = _enum_value(p.state) == "underprojected"
    overpushed = _enum_value(p.state) == "overpushed"

    thin_candidate = body_weak or bridge_broken or transition_fragile

    true_peak_dbtp = float(_read(m, "true_peak_dbtp", -1.0) or -1.0)
    integrated_lufs = float(_read(m, "integrated_lufs", -12.0) or -12.0)
    limiter_stress_proxy = float(_read(m, "limiter_stress_proxy", 0.0) or 0.0)
    near_clip_ratio = float(_read(m, "near_clip_ratio", 0.0) or 0.0)
    crest_db = float(_read(m, "crest_db", 10.0) or 10.0)
    punch_proxy = float(_read(m, "punch_proxy", 10.0) or 10.0)

    tp_hot = _clamp((true_peak_dbtp - 0.20) / 1.60, 0.0, 1.0)
    loud_hot = _clamp((integrated_lufs + 8.20) / 2.60, 0.0, 1.0)
    stress_hot = _clamp((limiter_stress_proxy - 0.98) / 0.22, 0.0, 1.0)
    clip_hot = _clamp(near_clip_ratio / 0.0080, 0.0, 1.0)
    punch_fragility = _clamp((10.6 - min(crest_db, punch_proxy)) / 2.0, 0.0, 1.0)

    delivery_pressure = _clamp(
        (tp_hot * 0.34)
        + (loud_hot * 0.14)
        + (stress_hot * 0.28)
        + (clip_hot * 0.14)
        + (punch_fragility * 0.10),
        0.0,
        1.0,
    )

    delivery_overbudget = delivery_pressure >= 0.58
    delivery_extreme = delivery_pressure >= 0.78

    return DSPRiskContext(
        foundation_missing=foundation_missing,
        body_fragile=body_fragile,
        body_weak=body_weak,
        bridge_broken=bridge_broken,
        bridge_gluey=bridge_gluey,
        bridge_gap_risky=bridge_gap_risky,
        cleanup_heavy_needed=cleanup_heavy_needed,
        thin_candidate=thin_candidate,
        boxy_active=boxy_active,
        transition_fragile=transition_fragile,
        underprojected=underprojected,
        overpushed=overpushed,
        top_safe=top_safe,
        top_guarded=top_guarded,
        top_fragile=top_fragile,
        delivery_overbudget=delivery_overbudget,
        delivery_extreme=delivery_extreme,
    )


def _projection_established(
    contour_stack: Optional[RoleDSPStack],
    ctx: DSPRiskContext,
) -> bool:
    if not _enabled(contour_stack):
        return False
    if ctx.underprojected:
        return False
    if ctx.top_fragile:
        return False
    if contour_stack.target_band_mode not in {"projection_dense", "projection_mild"}:
        return False
    return contour_stack.execution_amount >= 0.12


def _refresh_permissions(stack: RoleDSPStack) -> RoleDSPStack:
    if not stack.enabled or stack.target_band_mode == "off":
        return replace(
            stack,
            allowed_primitive_names=[],
            forbidden_primitive_names=ALL_PRIMITIVE_NAMES[:],
            safety_tags=_uniq(list(stack.safety_tags or []) + ["disabled_stack"]),
        )

    mode_spec = get_role_mode_spec(stack.role, stack.target_band_mode)

    matched_template = None
    for template in mode_spec.stack_templates:
        if template.stack_name == stack.stack_name:
            matched_template = template
            break
    if matched_template is None:
        for template in mode_spec.stack_templates:
            if template.stack_kind == stack.stack_kind:
                matched_template = template
                break

    if matched_template is None:
        return replace(
            stack,
            allowed_primitive_names=[],
            forbidden_primitive_names=ALL_PRIMITIVE_NAMES[:],
            notes=_uniq(list(stack.notes or []) + ["no_matching_role_stack_template"]),
        )

    allowed = matched_template.allowed_primitive_names[:]
    forbidden = sorted(name for name in ALL_PRIMITIVE_NAMES if name not in set(allowed))

    safety_tags = _uniq(
        list(stack.safety_tags or [])
        + list(mode_spec.required_safety_tags or [])
        + list(matched_template.required_safety_tags or [])
    )

    return replace(
        stack,
        allowed_primitive_names=allowed,
        forbidden_primitive_names=forbidden,
        safety_tags=safety_tags,
    )


def _drop_allowed_primitives(
    stack: Optional[RoleDSPStack],
    primitive_names: List[str],
    *,
    clamp_name: str,
    reason: str,
) -> Optional[RoleDSPStack]:
    if not _enabled(stack):
        return stack

    to_drop = set(primitive_names)
    old_allowed = list(stack.allowed_primitive_names or [])
    new_allowed = [name for name in old_allowed if name not in to_drop]

    blocked_actions = list(stack.blocked_actions or [])
    for name in old_allowed:
        if name in to_drop and name not in blocked_actions:
            blocked_actions.append(name)

    forbidden = _uniq(list(stack.forbidden_primitive_names or []) + list(to_drop))
    active_clamps = _uniq(list(stack.active_clamps or []) + [clamp_name])
    notes = _uniq(list(stack.notes or []) + [reason])

    return replace(
        stack,
        allowed_primitive_names=new_allowed,
        forbidden_primitive_names=forbidden,
        blocked_actions=blocked_actions,
        active_clamps=active_clamps,
        notes=notes,
    )


def _clamp_stack(
    stack: Optional[RoleDSPStack],
    *,
    clamp_name: str,
    reason: str,
    max_amount: Optional[float] = None,
    max_cap: Optional[float] = None,
    max_dynamic: Optional[float] = None,
    force_enabled: Optional[bool] = None,
    force_target_band_mode: Optional[str] = None,
    force_protection_mode: Optional[str] = None,
) -> Optional[RoleDSPStack]:
    if stack is None:
        return None

    enabled = stack.enabled if force_enabled is None else force_enabled
    target_band_mode = stack.target_band_mode if force_target_band_mode is None else force_target_band_mode
    protection_mode = stack.protection_mode if force_protection_mode is None else force_protection_mode

    execution_cap = stack.execution_cap
    execution_amount = stack.execution_amount
    dynamic_scale = stack.dynamic_scale

    if max_cap is not None:
        execution_cap = min(execution_cap, max_cap)

    if max_amount is not None:
        execution_amount = min(execution_amount, max_amount)

    execution_amount = min(execution_amount, execution_cap)

    if max_dynamic is not None:
        dynamic_scale = min(dynamic_scale, max_dynamic)

    blocked_actions = list(stack.blocked_actions or [])
    notes = _uniq(list(stack.notes or []) + [reason])
    active_clamps = _uniq(list(stack.active_clamps or []) + [clamp_name])

    if not enabled:
        blocked_actions = _uniq(blocked_actions + list(stack.allowed_primitive_names or []))
        patched = replace(
            stack,
            enabled=False,
            target_band_mode="off",
            protection_mode="off",
            execution_amount=0.0,
            execution_cap=0.0,
            dynamic_scale=0.0,
            active_clamps=active_clamps,
            blocked_actions=blocked_actions,
            notes=notes,
        )
        return _refresh_permissions(patched)

    patched = replace(
        stack,
        enabled=True,
        target_band_mode=target_band_mode,
        protection_mode=protection_mode,
        execution_amount=execution_amount,
        execution_cap=execution_cap,
        dynamic_scale=dynamic_scale,
        active_clamps=active_clamps,
        blocked_actions=blocked_actions,
        notes=notes,
    )
    return _refresh_permissions(patched)


def _make_clamp(
    *,
    clamp_name: str,
    severity: str,
    reason: str,
    target_roles: List[str],
    actions: dict,
    notes: Optional[List[str]] = None,
) -> DSPActiveClamp:
    return DSPActiveClamp(
        clamp_name=clamp_name,
        severity=severity,
        source="role_interaction",
        reason=reason,
        target_roles=target_roles,
        target_primitives=[],
        actions=actions,
        notes=notes or [],
    )


def apply_dsp_clamps(
    blueprint: DSPExecutionBlueprint,
    analysis: SmartMasterAnalysis,
) -> DSPExecutionBlueprint:
    ctx = _derive_risk_context(analysis)

    cleanup = blueprint.cleanup_stack
    guard = blueprint.guard_stack
    anchor = blueprint.anchor_parallel_stack
    bridge = blueprint.bridge_parallel_stack
    projection_contour = blueprint.projection_contour_stack
    projection_assist = blueprint.projection_assist_stack
    spark = blueprint.spark_stack
    delivery = blueprint.delivery_stack

    active_clamps: List[DSPActiveClamp] = list(blueprint.active_clamps or [])
    blocked_actions: List[str] = list(blueprint.blocked_actions or [])
    safety_notes: List[str] = list(blueprint.safety_notes or [])
    notes: List[str] = list(blueprint.notes or [])

    def register(clamp: DSPActiveClamp) -> None:
        active_clamps.append(clamp)

    if _enabled(cleanup) and cleanup.target_band_mode == "cleanup_dense" and (ctx.body_fragile or ctx.body_weak):
        cleanup = _clamp_stack(
            cleanup,
            clamp_name="body_protection_cleanup_clamp",
            reason="body weakness / fragility forces guarded cleanup",
            max_amount=0.22,
            max_cap=0.34,
            max_dynamic=0.58,
            force_target_band_mode="cleanup_guarded",
            force_protection_mode="body_ultra_guarded",
        )
        register(_make_clamp(
            clamp_name="body_protection_cleanup_clamp",
            severity="high",
            reason="Body fragility or weakness blocks dense cleanup.",
            target_roles=["cleanup"],
            actions={"force_target_band_mode": "cleanup_guarded"},
        ))
        safety_notes.append("cleanup dense blocked by body protection")

    if _enabled(cleanup) and cleanup.target_band_mode == "cleanup_dense" and (ctx.bridge_broken or ctx.bridge_gap_risky):
        cleanup = _clamp_stack(
            cleanup,
            clamp_name="bridge_protection_cleanup_clamp",
            reason="bridge risk forces guarded cleanup",
            max_amount=0.20,
            max_cap=0.32,
            max_dynamic=0.54,
            force_target_band_mode="cleanup_guarded",
            force_protection_mode="body_bridge_guarded",
        )
        register(_make_clamp(
            clamp_name="bridge_protection_cleanup_clamp",
            severity="high",
            reason="Bridge break/gap risk blocks dense cleanup in V1.",
            target_roles=["cleanup"],
            actions={"force_target_band_mode": "cleanup_guarded"},
        ))
        safety_notes.append("cleanup guarded by bridge protection")

    if ctx.bridge_gluey:
        if _enabled(bridge):
            bridge = _clamp_stack(
                bridge,
                clamp_name="bridge_glue_clamp",
                reason="gluey bridge restrains bridge support energy",
                max_amount=0.20,
                max_cap=0.32,
                max_dynamic=0.60,
                force_target_band_mode="bridge_restrain",
                force_protection_mode="glue_strict",
            )
        if _enabled(anchor) and anchor.target_band_mode == "body_restore":
            anchor = _clamp_stack(
                anchor,
                clamp_name="anchor_moderated_by_bridge_glue",
                reason="anchor restore moderated because bridge is glue-prone",
                max_amount=0.24,
                max_cap=0.36,
                max_dynamic=0.74,
                force_target_band_mode="body_hold",
                force_protection_mode="body_strict",
            )
        register(_make_clamp(
            clamp_name="bridge_glue_clamp",
            severity="medium",
            reason="Glue-prone bridge suppresses bridge-first and anchor-second support energy.",
            target_roles=["bridge", "anchor"],
            actions={"bridge_mode": "bridge_restrain", "anchor_mode": "body_hold"},
        ))
        safety_notes.append("bridge glue clamp active")

    if _enabled(cleanup) and cleanup.role_rank == "primary":
        if _enabled(projection_contour) and projection_contour.target_band_mode == "projection_dense":
            projection_contour = _clamp_stack(
                projection_contour,
                clamp_name="cleanup_primary_projection_clamp",
                reason="cleanup-primary pass softens projection contour",
                max_amount=0.18,
                max_cap=0.30,
                max_dynamic=0.56,
                force_target_band_mode="projection_mild",
                force_protection_mode="top_guarded",
            )
        if _enabled(projection_assist):
            projection_assist = _clamp_stack(
                projection_assist,
                clamp_name="cleanup_primary_projection_assist_clamp",
                reason="cleanup-primary pass softens projection assist",
                max_amount=0.12,
                max_cap=0.20,
                max_dynamic=0.44,
                force_target_band_mode="projection_mild",
                force_protection_mode="top_guarded",
            )
        register(_make_clamp(
            clamp_name="cleanup_primary_projection_clamp",
            severity="medium",
            reason="Primary cleanup lane prevents dense projection in same pass.",
            target_roles=["projection"],
            actions={"projection_mode": "projection_mild"},
        ))
        safety_notes.append("projection softened by cleanup-primary law")

    if _enabled(guard) and guard.target_band_mode in {"guard_boxiness", "guard_transition_support"} and guard.execution_amount >= 0.18:
        if _enabled(projection_contour) and projection_contour.target_band_mode == "projection_dense":
            projection_contour = _clamp_stack(
                projection_contour,
                clamp_name="guard_projection_density_clamp",
                reason="active guard softens projection contour",
                max_amount=0.16,
                max_cap=0.28,
                max_dynamic=0.52,
                force_target_band_mode="projection_mild",
                force_protection_mode="top_guarded",
            )
        if _enabled(projection_assist):
            projection_assist = _clamp_stack(
                projection_assist,
                clamp_name="guard_projection_assist_clamp",
                reason="active guard softens projection assist",
                max_amount=0.10,
                max_cap=0.16,
                max_dynamic=0.36,
                force_target_band_mode="projection_mild",
                force_protection_mode="top_guarded",
            )
        register(_make_clamp(
            clamp_name="guard_projection_density_clamp",
            severity="medium",
            reason="Guard-active pass limits projection density.",
            target_roles=["guard", "projection"],
            actions={"projection_mode": "projection_mild"},
        ))
        safety_notes.append("projection softened by active guard")

    if ctx.thin_candidate:
        if _enabled(cleanup) and cleanup.target_band_mode == "cleanup_dense":
            cleanup = _clamp_stack(
                cleanup,
                clamp_name="thin_track_cleanup_clamp",
                reason="thin-track law forces guarded cleanup",
                max_amount=0.20,
                max_cap=0.30,
                max_dynamic=0.54,
                force_target_band_mode="cleanup_guarded",
                force_protection_mode="body_ultra_guarded",
            )
        if _enabled(guard) and guard.target_band_mode == "guard_boxiness" and ctx.transition_fragile:
            guard = _clamp_stack(
                guard,
                clamp_name="thin_track_guard_bias",
                reason="thin-track law biases guard toward transition support",
                max_amount=0.20,
                max_cap=0.30,
                max_dynamic=0.62,
                force_target_band_mode="guard_transition_support",
                force_protection_mode="transition_support_only",
            )
        register(_make_clamp(
            clamp_name="thin_track_global_clamp",
            severity="high",
            reason="Thin-track law prevents subtractive double-hit and aggressive structure loss.",
            target_roles=["cleanup", "guard"],
            actions={"bias": "guarded"},
        ))
        safety_notes.append("thin-track clamp active")

    projection_established = _projection_established(projection_contour, ctx)

    if _enabled(spark):
        if ctx.underprojected:
            spark = _clamp_stack(
                spark,
                clamp_name="underprojected_blocks_spark",
                reason="spark disabled because projection is underprojected",
                force_enabled=False,
            )
            register(_make_clamp(
                clamp_name="underprojected_blocks_spark",
                severity="high",
                reason="Spark cannot substitute for missing projection.",
                target_roles=["spark"],
                actions={"enabled": False},
            ))
            safety_notes.append("spark blocked by underprojected state")
        elif not projection_established:
            spark = _clamp_stack(
                spark,
                clamp_name="projection_not_established_spark_micro",
                reason="spark downgraded because projection is not established",
                max_amount=0.06,
                max_cap=0.12,
                max_dynamic=0.24,
                force_target_band_mode="spark_micro",
                force_protection_mode="spark_micro_only",
            )
            register(_make_clamp(
                clamp_name="projection_not_established_spark_micro",
                severity="medium",
                reason="Spark stays micro until projection is established.",
                target_roles=["spark"],
                actions={"spark_mode": "spark_micro"},
            ))
            safety_notes.append("spark downgraded because projection not established")

    if ctx.top_fragile:
        if _enabled(projection_contour):
            projection_contour = _clamp_stack(
                projection_contour,
                clamp_name="top_fragility_projection_clamp",
                reason="top fragility forces projection clamp mode",
                max_amount=0.12,
                max_cap=0.22,
                max_dynamic=0.40,
                force_target_band_mode="projection_clamp",
                force_protection_mode="top_strict",
            )
        if _enabled(projection_assist):
            projection_assist = _clamp_stack(
                projection_assist,
                clamp_name="top_fragility_disables_projection_assist",
                reason="top fragility disables projection assist",
                force_enabled=False,
            )
        if _enabled(spark):
            spark = _clamp_stack(
                spark,
                clamp_name="top_fragility_blocks_spark",
                reason="top fragility disables spark",
                force_enabled=False,
            )
        register(_make_clamp(
            clamp_name="top_fragility_global_clamp",
            severity="high",
            reason="Top fragility blocks spark and hard-clamps projection.",
            target_roles=["projection", "spark"],
            actions={"projection_mode": "projection_clamp", "spark_enabled": False},
        ))
        safety_notes.append("top fragility clamp active")

    projection_established = _projection_established(projection_contour, ctx)
    if _enabled(spark) and (ctx.foundation_missing or ctx.body_weak or (not projection_established) or ctx.top_fragile):
        spark = _drop_allowed_primitives(
            spark,
            ["micro_width_high_only"],
            clamp_name="width_law_clamp",
            reason="width law removed high-only width primitive",
        )
        register(_make_clamp(
            clamp_name="width_law_clamp",
            severity="medium",
            reason="Width cannot run without foundation, body support, safe top, and established projection.",
            target_roles=["spark"],
            actions={"drop_primitives": ["micro_width_high_only"]},
        ))
        safety_notes.append("width law clamp active")

    if ctx.delivery_overbudget:
        if _enabled(spark):
            spark = _clamp_stack(
                spark,
                clamp_name="delivery_budget_spark_trim",
                reason="delivery budget trims spark first",
                max_amount=0.06,
                max_cap=0.10,
                max_dynamic=0.24,
                force_target_band_mode="spark_micro",
                force_protection_mode="spark_micro_only",
            )
        if _enabled(projection_assist):
            projection_assist = _clamp_stack(
                projection_assist,
                clamp_name="delivery_budget_projection_assist_trim",
                reason="delivery budget trims projection assist second",
                max_amount=0.08,
                max_cap=0.14,
                max_dynamic=0.34,
                force_target_band_mode="projection_mild",
                force_protection_mode="top_guarded",
            )
        if _enabled(anchor):
            anchor = _clamp_stack(
                anchor,
                clamp_name="delivery_budget_anchor_trim",
                reason="delivery budget trims anchor support third",
                max_amount=0.18,
                max_cap=0.26,
                max_dynamic=0.48,
            )
        if _enabled(bridge):
            bridge = _clamp_stack(
                bridge,
                clamp_name="delivery_budget_bridge_trim",
                reason="delivery budget trims bridge support third",
                max_amount=0.16,
                max_cap=0.24,
                max_dynamic=0.44,
            )
        if ctx.delivery_extreme and _enabled(projection_contour):
            projection_contour = _clamp_stack(
                projection_contour,
                clamp_name="delivery_budget_projection_contour_trim",
                reason="extreme delivery budget also trims projection contour",
                max_amount=0.16,
                max_cap=0.28,
                max_dynamic=0.50,
                force_target_band_mode="projection_mild",
                force_protection_mode="top_guarded",
            )

        register(_make_clamp(
            clamp_name="delivery_budget_global_clamp",
            severity="high" if ctx.delivery_extreme else "medium",
            reason="Delivery budget trims finish first, projection assist second, support third.",
            target_roles=["spark", "projection", "anchor", "bridge"],
            actions={"budget": "trim_to_safe"},
        ))
        safety_notes.append("delivery budget clamp active")

    for stack in [cleanup, guard, anchor, bridge, projection_contour, projection_assist, spark, delivery]:
        if stack is None:
            continue
        blocked_actions.extend(stack.blocked_actions or [])

    patched = replace(
        blueprint,
        cleanup_stack=_refresh_permissions(cleanup) if cleanup is not None else None,
        guard_stack=_refresh_permissions(guard) if guard is not None else None,
        anchor_parallel_stack=_refresh_permissions(anchor) if anchor is not None else None,
        bridge_parallel_stack=_refresh_permissions(bridge) if bridge is not None else None,
        projection_contour_stack=_refresh_permissions(projection_contour) if projection_contour is not None else None,
        projection_assist_stack=_refresh_permissions(projection_assist) if projection_assist is not None else None,
        spark_stack=_refresh_permissions(spark) if spark is not None else None,
        delivery_stack=_refresh_permissions(delivery) if delivery is not None else None,
        active_clamps=active_clamps,
        blocked_actions=_uniq(blocked_actions),
        safety_notes=_uniq(safety_notes),
        stage_plans=[],
        recombine_plans=[],
        notes=_uniq(notes + ["dsp_clamps_applied", "graph_requires_reattach_after_clamps"]),
    )
    return patched

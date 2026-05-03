from __future__ import annotations

from dataclasses import dataclass, replace
from typing import List, Optional

from ..contracts import SmartMasterAnalysis
from ..enums import RiskLevel
from .contracts import DSPActiveClamp, DSPExecutionBlueprint, RoleDSPStack
from .primitives import PRIMITIVE_REGISTRY
from .role_specs import get_role_mode_spec


ALL_PRIMITIVE_NAMES = sorted(PRIMITIVE_REGISTRY.keys())

POLISH_SOUND_FIRST = True

TOP_EMERGENCY_SCORE = 0.66
TOP_HARD_EMERGENCY_SCORE = 0.78

DELIVERY_OBSERVED_SCORE = 0.70
DELIVERY_EXTREME_SCORE = 0.86


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
    top_emergency: bool
    top_hard_emergency: bool

    delivery_overbudget: bool
    delivery_extreme: bool

    studio_density_score: float
    dirt_buildup_score: float
    body_restore_score: float
    bridge_restore_score: float
    top_emergency_score: float
    delivery_pressure_score: float
    punch_preserve_score: float


def _has(v) -> bool:
    return v is not None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _safe(v, fallback: float) -> float:
    if v is None:
        return fallback
    try:
        return float(v)
    except Exception:
        return fallback


def _read(obj, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _risk_ge(risk: RiskLevel, level: RiskLevel) -> bool:
    order = {
        RiskLevel.LOW: 0,
        RiskLevel.MEDIUM: 1,
        RiskLevel.HIGH: 2,
    }
    return order.get(risk, 0) >= order.get(level, 0)


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
    return stack is not None and bool(stack.enabled)


def _metric(analysis: SmartMasterAnalysis, name: str, fallback: float) -> float:
    return _safe(_read(analysis.metrics, name, fallback), fallback)


def _derived(analysis: SmartMasterAnalysis, name: str, fallback: Optional[float] = None) -> Optional[float]:
    value = _read(analysis.derived, name, fallback)
    if value is None:
        return None
    return _safe(value, fallback if fallback is not None else 0.0)


def _dirt_buildup_score(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics
    c = analysis.cleanup
    g = analysis.guard
    b = analysis.bridge

    lowmid_buildup_ratio_db = _safe(_read(m, "lowmid_buildup_ratio_db", 12.0), 12.0)
    mud_to_body_db = _safe(_read(m, "mud_to_body_db", -1.0), -1.0)
    mud_200_500_db = _safe(_read(m, "mud_200_500_db", 33.5), 33.5)
    lowmid_buildup_200_400_db = _safe(_read(m, "lowmid_buildup_200_400_db", 33.5), 33.5)
    body_150_400_db = _safe(_read(m, "body_150_400_db", 33.5), 33.5)

    ratio_score = _clamp((lowmid_buildup_ratio_db - 14.2) / 6.8, 0.0, 1.0)
    mud_relation_score = _clamp((mud_to_body_db + 0.45) / 1.65, 0.0, 1.0)
    absolute_mud_score = _clamp((mud_200_500_db - 35.2) / 4.2, 0.0, 1.0)
    cluster_score = _clamp((lowmid_buildup_200_400_db - body_150_400_db + 0.35) / 2.1, 0.0, 1.0)

    cleanup_bonus = 0.18 if c.buildup_risk == RiskLevel.HIGH else 0.08 if c.buildup_risk == RiskLevel.MEDIUM else 0.0
    boxy_bonus = 0.16 if _enum_value(g.shape) == "boxy" else 0.0
    glue_bonus = 0.10 if _enum_value(b.state) == "overglued" or _risk_ge(b.glue_risk, RiskLevel.MEDIUM) else 0.0

    return _clamp(
        (ratio_score * 0.25)
        + (mud_relation_score * 0.30)
        + (absolute_mud_score * 0.18)
        + (cluster_score * 0.15)
        + cleanup_bonus
        + boxy_bonus
        + glue_bonus,
        0.0,
        1.0,
    )


def _body_restore_score(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics
    d = analysis.derived
    a = analysis.anchor

    center_body_support_proxy = _read(d, "center_body_support_proxy", None)
    body_to_mid_handoff_proxy = _read(d, "body_to_mid_handoff_proxy", None)

    body_150_400_db = _safe(_read(m, "body_150_400_db", 33.5), 33.5)
    low_body_150_300_db = _safe(_read(m, "low_body_150_300_db", 33.0), 33.0)
    lowmid_120_300_db = _safe(_read(m, "lowmid_120_300_db", 33.0), 33.0)
    crest_db = _safe(_read(m, "crest_db", 10.0), 10.0)
    punch_proxy = _safe(_read(m, "punch_proxy", 10.0), 10.0)

    center_weak = 0.0
    if center_body_support_proxy is not None:
        center_weak = _clamp((0.54 - float(center_body_support_proxy)) / 0.34, 0.0, 1.0)

    handoff_weak = 0.0
    if body_to_mid_handoff_proxy is not None:
        handoff_weak = _clamp((0.50 - float(body_to_mid_handoff_proxy)) / 0.30, 0.0, 1.0)

    body_abs_weak = _clamp((32.0 - body_150_400_db) / 4.0, 0.0, 1.0)
    low_body_weak = _clamp((31.8 - low_body_150_300_db) / 4.0, 0.0, 1.0)
    lowmid_weak = _clamp((31.6 - lowmid_120_300_db) / 4.0, 0.0, 1.0)
    punch_fragile = _clamp((9.2 - min(crest_db, punch_proxy)) / 2.8, 0.0, 1.0)

    anchor_bonus = 0.20 if _enum_value(a.state) == "deficient" else 0.0
    foundation_bonus = 0.18 if not a.foundation_present else 0.0
    fragility_bonus = 0.12 if a.fragility == RiskLevel.MEDIUM else 0.20 if a.fragility == RiskLevel.HIGH else 0.0

    return _clamp(
        (center_weak * 0.28)
        + (handoff_weak * 0.12)
        + (body_abs_weak * 0.18)
        + (low_body_weak * 0.16)
        + (lowmid_weak * 0.08)
        + (punch_fragile * 0.08)
        + anchor_bonus
        + foundation_bonus
        + fragility_bonus,
        0.0,
        1.0,
    )


def _bridge_restore_score(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics
    d = analysis.derived
    b = analysis.bridge

    body_to_mid_handoff_proxy = _read(d, "body_to_mid_handoff_proxy", None)

    bass_to_body_db = _safe(_read(m, "bass_to_body_db", 5.0), 5.0)
    sub_to_body_db = _safe(_read(m, "sub_to_body_db", 5.0), 5.0)
    low_foundation_ratio_db = _safe(_read(m, "low_foundation_ratio_db", 5.0), 5.0)

    handoff_weak = 0.0
    if body_to_mid_handoff_proxy is not None:
        handoff_weak = _clamp((0.52 - float(body_to_mid_handoff_proxy)) / 0.34, 0.0, 1.0)

    detached_bass = _clamp((bass_to_body_db - 7.4) / 4.8, 0.0, 1.0)
    sub_gap = _clamp((2.8 - sub_to_body_db) / 3.2, 0.0, 1.0)
    foundation_gap = _clamp((3.2 - low_foundation_ratio_db) / 3.8, 0.0, 1.0)

    broken_bonus = 0.26 if _enum_value(b.state) == "broken" else 0.0
    gap_bonus = 0.16 if _risk_ge(b.gap_risk, RiskLevel.MEDIUM) else 0.0

    return _clamp(
        (handoff_weak * 0.30)
        + (detached_bass * 0.24)
        + (sub_gap * 0.12)
        + (foundation_gap * 0.08)
        + broken_bonus
        + gap_bonus,
        0.0,
        1.0,
    )


def _studio_density_score(analysis: SmartMasterAnalysis, dirt_score: float) -> float:
    m = analysis.metrics
    d = analysis.derived

    center_body_support_proxy = _read(d, "center_body_support_proxy", None)
    body_to_mid_handoff_proxy = _read(d, "body_to_mid_handoff_proxy", None)

    body_150_400_db = _safe(_read(m, "body_150_400_db", 33.0), 33.0)
    low_body_150_300_db = _safe(_read(m, "low_body_150_300_db", 33.0), 33.0)
    lowmid_120_300_db = _safe(_read(m, "lowmid_120_300_db", 33.0), 33.0)
    mud_to_body_db = _safe(_read(m, "mud_to_body_db", -1.0), -1.0)
    crest_db = _safe(_read(m, "crest_db", 10.0), 10.0)
    punch_proxy = _safe(_read(m, "punch_proxy", 10.0), 10.0)
    plr_proxy_db = _safe(_read(m, "plr_proxy_db", 10.0), 10.0)

    body_present = _clamp((body_150_400_db - 29.8) / 5.2, 0.0, 1.0)
    low_body_present = _clamp((low_body_150_300_db - 29.8) / 5.0, 0.0, 1.0)
    lowmid_present = _clamp((lowmid_120_300_db - 30.2) / 5.0, 0.0, 1.0)

    center_score = 0.62
    if center_body_support_proxy is not None:
        center_score = _clamp(float(center_body_support_proxy), 0.0, 1.0)

    handoff_score = 0.62
    if body_to_mid_handoff_proxy is not None:
        handoff_score = _clamp(float(body_to_mid_handoff_proxy), 0.0, 1.0)

    mud_is_not_dominant = _clamp((-0.05 - mud_to_body_db) / 1.45, 0.0, 1.0)
    punch_ok = _clamp((min(crest_db, punch_proxy, plr_proxy_db) - 9.0) / 4.5, 0.0, 1.0)

    return _clamp(
        (body_present * 0.18)
        + (low_body_present * 0.14)
        + (lowmid_present * 0.10)
        + (center_score * 0.20)
        + (handoff_score * 0.14)
        + (mud_is_not_dominant * 0.14)
        + (punch_ok * 0.10)
        - (dirt_score * 0.22),
        0.0,
        1.0,
    )


def _top_emergency_score(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics
    d = analysis.derived
    p = analysis.projection

    true_peak_dbtp = _safe(_read(m, "true_peak_dbtp", -1.0), -1.0)
    near_clip_ratio = _safe(_read(m, "near_clip_ratio", 0.0), 0.0)
    crest_db = _safe(_read(m, "crest_db", 10.0), 10.0)
    punch_proxy = _safe(_read(m, "punch_proxy", 10.0), 10.0)

    harshness_index = _safe(_read(m, "harshness_index", -10.0), -10.0)
    sibilance_index = _safe(_read(m, "sibilance_index", 0.0), 0.0)
    harsh_to_mid_db = _safe(_read(m, "harsh_to_mid_db", -7.0), -7.0)
    harsh_2p5k_6k_db = _safe(_read(m, "harsh_2p5k_6k_db", 16.0), 16.0)
    sibilance_5k_9k_db = _safe(_read(m, "sibilance_5k_9k_db", 15.0), 15.0)

    top_push = _read(d, "top_push_safety_proxy", None)

    hard_clip_score = _clamp((true_peak_dbtp - 0.85) / 1.35, 0.0, 1.0)
    near_clip_score = _clamp((near_clip_ratio - 0.006) / 0.014, 0.0, 1.0)
    punch_collapse_score = _clamp((8.2 - min(crest_db, punch_proxy)) / 2.2, 0.0, 1.0)

    harsh_score = _clamp((harshness_index + 6.0) / 4.0, 0.0, 1.0)
    sib_index_score = _clamp((sibilance_index - 6.0) / 3.2, 0.0, 1.0)
    harsh_relation_score = _clamp((harsh_to_mid_db + 2.4) / 2.8, 0.0, 1.0)
    harsh_band_score = _clamp((harsh_2p5k_6k_db - 20.0) / 4.0, 0.0, 1.0)
    sib_band_score = _clamp((sibilance_5k_9k_db - 18.4) / 3.2, 0.0, 1.0)

    risk_pair = (
        p.harshness_risk == RiskLevel.HIGH
        and p.sibilance_risk == RiskLevel.HIGH
    )

    risk_pair_bonus = 0.08 if risk_pair else 0.0

    top_push_collapse = 0.0
    if risk_pair and top_push is not None:
        top_push_collapse = _clamp((0.30 - float(top_push)) / 0.22, 0.0, 1.0)

    top_danger = _clamp(
        (harsh_score * 0.16)
        + (sib_index_score * 0.20)
        + (harsh_relation_score * 0.14)
        + (harsh_band_score * 0.12)
        + (sib_band_score * 0.16)
        + (top_push_collapse * 0.14)
        + risk_pair_bonus,
        0.0,
        1.0,
    )

    return _clamp(
        (hard_clip_score * 0.22)
        + (near_clip_score * 0.16)
        + (punch_collapse_score * 0.18)
        + (top_danger * 0.44),
        0.0,
        1.0,
    )


def _delivery_pressure_score(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics

    true_peak_dbtp = _safe(_read(m, "true_peak_dbtp", -1.0), -1.0)
    integrated_lufs = _safe(_read(m, "integrated_lufs", -12.0), -12.0)
    limiter_stress_proxy = _safe(_read(m, "limiter_stress_proxy", 0.0), 0.0)
    near_clip_ratio = _safe(_read(m, "near_clip_ratio", 0.0), 0.0)

    tp_hot = _clamp((true_peak_dbtp - 0.30) / 1.70, 0.0, 1.0)
    loud_hot = _clamp((integrated_lufs + 8.0) / 2.80, 0.0, 1.0)
    stress_hot = _clamp((limiter_stress_proxy - 1.04) / 0.30, 0.0, 1.0)
    clip_hot = _clamp((near_clip_ratio - 0.0040) / 0.0120, 0.0, 1.0)

    return _clamp(
        (tp_hot * 0.36)
        + (loud_hot * 0.16)
        + (stress_hot * 0.28)
        + (clip_hot * 0.20),
        0.0,
        1.0,
    )


def _punch_preserve_score(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics

    crest_db = _safe(_read(m, "crest_db", 10.0), 10.0)
    punch_proxy = _safe(_read(m, "punch_proxy", 10.0), 10.0)
    plr_proxy_db = _safe(_read(m, "plr_proxy_db", 10.0), 10.0)
    lra_ebu = _safe(_read(m, "lra_ebu", 3.0), 3.0)

    crest_score = _clamp((crest_db - 8.5) / 5.0, 0.0, 1.0)
    punch_score = _clamp((punch_proxy - 9.0) / 5.0, 0.0, 1.0)
    plr_score = _clamp((plr_proxy_db - 8.5) / 5.0, 0.0, 1.0)
    lra_score = _clamp((lra_ebu - 1.8) / 3.5, 0.0, 1.0)

    return _clamp(
        (crest_score * 0.34)
        + (punch_score * 0.34)
        + (plr_score * 0.22)
        + (lra_score * 0.10),
        0.0,
        1.0,
    )


def _derive_risk_context(analysis: SmartMasterAnalysis) -> DSPRiskContext:
    m = analysis.metrics
    d = analysis.derived
    a = analysis.anchor
    b = analysis.bridge
    c = analysis.cleanup
    g = analysis.guard
    p = analysis.projection

    dirt_buildup_score = _dirt_buildup_score(analysis)
    body_restore_score = _body_restore_score(analysis)
    bridge_restore_score = _bridge_restore_score(analysis)
    studio_density_score = _studio_density_score(analysis, dirt_buildup_score)
    top_emergency_score = _top_emergency_score(analysis)
    delivery_pressure_score = _delivery_pressure_score(analysis)
    punch_preserve_score = _punch_preserve_score(analysis)

    top_hard_emergency = top_emergency_score >= TOP_HARD_EMERGENCY_SCORE
    top_emergency = top_emergency_score >= TOP_EMERGENCY_SCORE

    top_safe = (
        not top_emergency
        and top_emergency_score < 0.24
        and p.harshness_risk == RiskLevel.LOW
        and p.sibilance_risk == RiskLevel.LOW
        and (_has(_read(d, "top_push_safety_proxy", None)) is False or _read(d, "top_push_safety_proxy", None) >= 0.56)
    )

    top_guarded = (
        not top_safe
        and not top_hard_emergency
        and top_emergency_score < TOP_EMERGENCY_SCORE
    )

    top_fragile = not top_safe and not top_guarded

    foundation_missing = not a.foundation_present

    body_fragile = (
        _risk_ge(a.fragility, RiskLevel.MEDIUM)
        or body_restore_score >= 0.52
    )

    body_weak = (
        foundation_missing
        or body_restore_score >= 0.58
        or (_has(_read(d, "center_body_support_proxy", None)) and _read(d, "center_body_support_proxy", None) < 0.40)
        or (_has(_read(m, "low_body_150_300_db", None)) and _read(m, "low_body_150_300_db", None) < 30.6)
    )

    bridge_broken = (
        _enum_value(b.state) == "broken"
        or bridge_restore_score >= 0.70
    )

    bridge_gluey = (
        _enum_value(b.state) == "overglued"
        or _risk_ge(b.glue_risk, RiskLevel.MEDIUM)
    )

    bridge_gap_risky = (
        _risk_ge(b.gap_risk, RiskLevel.MEDIUM)
        or bridge_restore_score >= 0.48
    )

    cleanup_heavy_needed = (
        _enum_value(c.readiness) == "safe"
        and c.buildup_risk == RiskLevel.HIGH
        and dirt_buildup_score >= 0.58
        and studio_density_score < 0.62
    )

    boxy_active = _enum_value(g.shape) == "boxy"
    transition_fragile = _enum_value(g.transition_state) in {"weak", "thinning"}

    underprojected = _enum_value(p.state) == "underprojected"
    overpushed = _enum_value(p.state) == "overpushed"

    thin_candidate = body_weak or bridge_broken or transition_fragile

    delivery_overbudget = delivery_pressure_score >= DELIVERY_OBSERVED_SCORE
    delivery_extreme = delivery_pressure_score >= DELIVERY_EXTREME_SCORE

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
        top_emergency=top_emergency,
        top_hard_emergency=top_hard_emergency,
        delivery_overbudget=delivery_overbudget,
        delivery_extreme=delivery_extreme,
        studio_density_score=studio_density_score,
        dirt_buildup_score=dirt_buildup_score,
        body_restore_score=body_restore_score,
        bridge_restore_score=bridge_restore_score,
        top_emergency_score=top_emergency_score,
        delivery_pressure_score=delivery_pressure_score,
        punch_preserve_score=punch_preserve_score,
    )


def _refresh_permissions(stack: RoleDSPStack) -> RoleDSPStack:
    if not stack.enabled or stack.target_band_mode == "off":
        return replace(
            stack,
            allowed_primitive_names=[],
            forbidden_primitive_names=ALL_PRIMITIVE_NAMES[:],
            safety_tags=_uniq(list(stack.safety_tags or []) + ["disabled_stack"]),
        )

    try:
        mode_spec = get_role_mode_spec(stack.role, stack.target_band_mode)
    except Exception:
        allowed = [
            name for name in list(stack.allowed_primitive_names or [])
            if name not in set(stack.blocked_actions or [])
            and name not in set(stack.forbidden_primitive_names or [])
        ]
        forbidden = sorted(name for name in ALL_PRIMITIVE_NAMES if name not in set(allowed))
        return replace(
            stack,
            allowed_primitive_names=allowed,
            forbidden_primitive_names=forbidden,
            notes=_uniq(list(stack.notes or []) + ["role_spec_unavailable_permissions_preserved"]),
        )

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
        allowed = [
            name for name in list(stack.allowed_primitive_names or [])
            if name not in set(stack.blocked_actions or [])
            and name not in set(stack.forbidden_primitive_names or [])
        ]
        forbidden = sorted(name for name in ALL_PRIMITIVE_NAMES if name not in set(allowed))
        return replace(
            stack,
            allowed_primitive_names=allowed,
            forbidden_primitive_names=forbidden,
            notes=_uniq(list(stack.notes or []) + ["no_matching_role_stack_template_permissions_preserved"]),
        )

    template_allowed = matched_template.allowed_primitive_names[:]

    blocked = set(stack.blocked_actions or [])
    explicit_forbidden = set(stack.forbidden_primitive_names or [])

    allowed = [
        name for name in template_allowed
        if name not in blocked and name not in explicit_forbidden
    ]

    forbidden = sorted(
        name for name in ALL_PRIMITIVE_NAMES
        if name not in set(allowed)
    )

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


def _sound_first_apply_dsp_clamps(
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

    notes.extend(
        [
            f"clamp_studio_density_score={round(ctx.studio_density_score, 4)}",
            f"clamp_dirt_buildup_score={round(ctx.dirt_buildup_score, 4)}",
            f"clamp_body_restore_score={round(ctx.body_restore_score, 4)}",
            f"clamp_bridge_restore_score={round(ctx.bridge_restore_score, 4)}",
            f"clamp_top_emergency_score={round(ctx.top_emergency_score, 4)}",
            f"clamp_delivery_pressure_score={round(ctx.delivery_pressure_score, 4)}",
            f"clamp_punch_preserve_score={round(ctx.punch_preserve_score, 4)}",
        ]
    )

    if _enabled(cleanup) and cleanup.target_band_mode == "cleanup_dense":
        should_guard_cleanup = (
            ctx.body_restore_score >= 0.70
            or ctx.bridge_restore_score >= 0.70
            or (
                ctx.thin_candidate
                and ctx.studio_density_score < 0.52
            )
        )

        if should_guard_cleanup:
            cleanup = _clamp_stack(
                cleanup,
                clamp_name="sound_first_cleanup_structural_safety_clamp",
                reason="dense cleanup reduced only by real body/bridge structural danger",
                max_amount=0.24,
                max_cap=0.36,
                max_dynamic=0.62,
                force_target_band_mode="cleanup_guarded",
                force_protection_mode="body_bridge_guarded",
            )

            register(
                _make_clamp(
                    clamp_name="sound_first_cleanup_structural_safety_clamp",
                    severity="medium",
                    reason="Dense cleanup reduced only when body/bridge structure is genuinely unsafe.",
                    target_roles=["cleanup"],
                    actions={"cleanup": "dense_to_guarded"},
                    notes=[
                        "Cleanup may prepare space.",
                        "Cleanup must not remove useful body or bass-to-body bridge.",
                    ],
                )
            )
            safety_notes.append("sound-first: cleanup structural safety clamp active")

    if ctx.top_emergency:
        if _enabled(projection_contour):
            if ctx.top_hard_emergency:
                projection_contour = _clamp_stack(
                    projection_contour,
                    clamp_name="top_hard_emergency_projection_contour_clamp",
                    reason="hard top emergency clamps projection contour",
                    max_amount=0.18,
                    max_cap=0.28,
                    max_dynamic=0.46,
                    force_target_band_mode="projection_clamp",
                    force_protection_mode="top_strict",
                )
            else:
                projection_contour = _clamp_stack(
                    projection_contour,
                    clamp_name="top_emergency_projection_contour_mild_guard",
                    reason="top emergency guards projection contour without killing musical reveal",
                    max_amount=0.26,
                    max_cap=0.38,
                    max_dynamic=0.58,
                    force_target_band_mode="projection_mild",
                    force_protection_mode="top_guarded",
                )

        if _enabled(projection_assist):
            if ctx.top_hard_emergency:
                projection_assist = _clamp_stack(
                    projection_assist,
                    clamp_name="top_hard_emergency_projection_assist_off",
                    reason="hard top emergency disables projection assist only as emergency",
                    force_enabled=False,
                )
            else:
                projection_assist = _clamp_stack(
                    projection_assist,
                    clamp_name="top_emergency_projection_assist_trim",
                    reason="top emergency trims projection assist without killing main projection",
                    max_amount=0.10,
                    max_cap=0.18,
                    max_dynamic=0.38,
                    force_target_band_mode="projection_mild",
                    force_protection_mode="top_guarded",
                )

        if _enabled(spark):
            if ctx.top_hard_emergency:
                spark = _clamp_stack(
                    spark,
                    clamp_name="top_hard_emergency_spark_off",
                    reason="hard top emergency disables spark",
                    force_enabled=False,
                )
            else:
                spark = _clamp_stack(
                    spark,
                    clamp_name="top_emergency_spark_micro",
                    reason="top emergency downgrades spark to protected micro",
                    max_amount=0.08,
                    max_cap=0.14,
                    max_dynamic=0.28,
                    force_target_band_mode="spark_micro",
                    force_protection_mode="spark_micro_only",
                )
                spark = _drop_allowed_primitives(
                    spark,
                    ["micro_width_high_only", "micro_top_texture"],
                    clamp_name="top_emergency_spark_risk_primitives_drop",
                    reason="top emergency removes risky spark primitives but keeps protected finish if possible",
                )

        register(
            _make_clamp(
                clamp_name="top_emergency_global_clamp",
                severity="high" if ctx.top_hard_emergency else "medium",
                reason="Only real top emergency may clamp projection/spark in sound-first mode.",
                target_roles=["projection", "spark"],
                actions={
                    "projection_contour": "mild_or_clamp",
                    "projection_assist": "trim_or_off",
                    "spark": "micro_or_off",
                },
                notes=[
                    f"top_emergency_score={round(ctx.top_emergency_score, 4)}",
                    "Single weak proxy is not enough for top emergency.",
                ],
            )
        )
        safety_notes.append("sound-first: real top emergency clamp active")

    if _enabled(spark):
        remove_width = (
            ctx.foundation_missing
            or ctx.top_emergency
            or ctx.top_hard_emergency
        )

        if remove_width:
            spark = _drop_allowed_primitives(
                spark,
                ["micro_width_high_only"],
                clamp_name="sound_first_width_law_clamp",
                reason="width removed only because foundation is missing or top emergency is active",
            )
            register(
                _make_clamp(
                    clamp_name="sound_first_width_law_clamp",
                    severity="medium",
                    reason="Width cannot run without foundation or during top emergency.",
                    target_roles=["spark"],
                    actions={"drop_primitives": ["micro_width_high_only"]},
                )
            )
            safety_notes.append("sound-first: width law clamp active")

    if ctx.delivery_overbudget or ctx.delivery_extreme:
        register(
            _make_clamp(
                clamp_name="delivery_pressure_observed_no_music_trim",
                severity="high" if ctx.delivery_extreme else "medium",
                reason="Delivery pressure observed, but musical stacks remain untouched. Terminal delivery must solve peak safety without acting as creative handbrake.",
                target_roles=["delivery"],
                actions={
                    "anchor": "untouched",
                    "bridge": "untouched",
                    "cleanup": "untouched",
                    "guard": "untouched",
                    "projection": "untouched_unless_top_emergency",
                    "spark": "untouched_unless_top_emergency",
                    "delivery": "terminal_safety_only",
                },
                notes=[
                    f"delivery_pressure_score={round(ctx.delivery_pressure_score, 4)}",
                    "No anchor trim.",
                    "No bridge trim.",
                    "No projection trim.",
                    "No spark trim unless top emergency also active.",
                ],
            )
        )
        safety_notes.append("sound-first: delivery pressure observed, musical stacks untouched")

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
        notes=_uniq(
            notes
            + [
                "dsp_clamps_applied",
                "sound_first_clamp_mode_v2",
                "clamps_are_emergency_layer_not_router",
                "delivery_cannot_trim_musical_stacks",
                "top_proxy_alone_cannot_trigger_emergency",
                "graph_requires_reattach_after_clamps",
            ]
        ),
    )
    return patched


def _legacy_apply_dsp_clamps(
    blueprint: DSPExecutionBlueprint,
    analysis: SmartMasterAnalysis,
) -> DSPExecutionBlueprint:
    patched = _sound_first_apply_dsp_clamps(blueprint, analysis)
    return replace(
        patched,
        notes=_uniq(
            list(patched.notes or [])
            + [
                "legacy_clamp_mode_redirected_to_sound_first",
                "legacy_music_trimming_disabled_for_sm_branch",
            ]
        ),
    )


def apply_dsp_clamps(
    blueprint: DSPExecutionBlueprint,
    analysis: SmartMasterAnalysis,
) -> DSPExecutionBlueprint:
    if POLISH_SOUND_FIRST:
        return _sound_first_apply_dsp_clamps(blueprint, analysis)
    return _legacy_apply_dsp_clamps(blueprint, analysis)

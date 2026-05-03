from __future__ import annotations

from dataclasses import replace
from typing import Any, Dict, List, Optional, Tuple

from ..contracts import SmartMasterAnalysis, SmartMasterExecutionBlueprint
from ..enums import RoleName
from .clamps import apply_dsp_clamps
from .contracts import DSPExecutionBlueprint, RoleDSPStack
from .graph import attach_graph_to_blueprint
from .primitive_instances import attach_primitive_instances_to_blueprint
from .role_specs import RoleModeSpec, RoleStackTemplate, get_role_mode_spec


def _read(obj: Any, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


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


def _enabled(plan) -> bool:
    return plan is not None and bool(plan.enabled)


def _effective_mode(plan) -> str:
    if not _enabled(plan):
        return "off"
    return str(plan.target_band_mode or "off")


def _value(v: Any) -> str:
    if hasattr(v, "value"):
        return str(v.value).strip().lower()
    return str(v).strip().lower()


def _metric_f(analysis: Optional[SmartMasterAnalysis], key: str, default: float = 0.0) -> float:
    if analysis is None:
        return default
    metrics = _read(analysis, "metrics", {}) or {}
    try:
        value = _read(metrics, key, default)
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _derived_f(analysis: Optional[SmartMasterAnalysis], key: str, default: float = 0.0) -> float:
    if analysis is None:
        return default
    derived = _read(analysis, "derived", {}) or {}
    try:
        value = _read(derived, key, default)
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _analysis_projection_value(
    analysis: Optional[SmartMasterAnalysis],
    key: str,
    default: str = "low",
) -> str:
    if analysis is None:
        return default
    projection = _read(analysis, "projection", {}) or {}
    return _value(_read(projection, key, default))


def _stack_activity(stack: Optional[RoleDSPStack]) -> float:
    if stack is None or not stack.enabled:
        return 0.0

    if stack.execution_cap <= 1e-9:
        amount_norm = 0.0
    else:
        amount_norm = _clamp(stack.execution_amount / stack.execution_cap, 0.0, 1.0)

    return _clamp((amount_norm * 0.58) + (stack.dynamic_scale * 0.42), 0.0, 1.0)


def _track_context(analysis: Optional[SmartMasterAnalysis]) -> Dict[str, Any]:
    if analysis is None:
        return {
            "profile": "unknown",
            "is_quiet_open": False,
            "is_studio_dense_preserve": False,
            "is_dirty_buildup": False,
            "is_body_weak": False,
            "is_top_hot_musical": False,
            "is_real_top_emergency": False,
            "is_punch_fragile": False,
            "is_loud_hot": False,
        }

    integrated_lufs = _metric_f(analysis, "integrated_lufs", -14.0)
    true_peak_dbtp = _metric_f(analysis, "true_peak_dbtp", -1.0)
    near_clip_ratio = _metric_f(analysis, "near_clip_ratio", 0.0)
    crest_db = _metric_f(analysis, "crest_db", 10.0)
    punch_proxy = _metric_f(analysis, "punch_proxy", 10.0)
    plr_proxy_db = _metric_f(analysis, "plr_proxy_db", 10.0)

    body_150_400_db = _metric_f(analysis, "body_150_400_db", 0.0)
    low_body_150_300_db = _metric_f(analysis, "low_body_150_300_db", 0.0)
    lowmid_buildup_ratio_db = _metric_f(analysis, "lowmid_buildup_ratio_db", 0.0)
    mud_to_body_db = _metric_f(analysis, "mud_to_body_db", 0.0)
    bass_to_body_db = _metric_f(analysis, "bass_to_body_db", 0.0)
    low_foundation_ratio_db = _metric_f(analysis, "low_foundation_ratio_db", 0.0)
    presence_to_body_db = _metric_f(analysis, "presence_to_body_db", -18.0)
    harshness_index = _metric_f(analysis, "harshness_index", -12.0)
    sibilance_index = _metric_f(analysis, "sibilance_index", -2.0)
    limiter_stress_proxy = _metric_f(analysis, "limiter_stress_proxy", 0.0)

    center_body_support_proxy = _derived_f(analysis, "center_body_support_proxy", 0.5)
    body_to_mid_handoff_proxy = _derived_f(analysis, "body_to_mid_handoff_proxy", 0.5)
    top_push_safety_proxy = _derived_f(analysis, "top_push_safety_proxy", 0.5)

    harshness_risk = _analysis_projection_value(analysis, "harshness_risk", "low")
    sibilance_risk = _analysis_projection_value(analysis, "sibilance_risk", "low")

    is_loud_hot = integrated_lufs > -9.3
    is_peak_hot = true_peak_dbtp > -0.30
    is_limiter_hot = limiter_stress_proxy > 1.18 or near_clip_ratio > 0.0025

    is_punch_healthy = crest_db >= 12.0 and punch_proxy >= 12.5 and plr_proxy_db >= 10.8
    is_punch_fragile = crest_db < 10.2 or punch_proxy < 10.8 or plr_proxy_db < 9.8

    useful_body_present = (
        body_150_400_db >= 29.0
        and low_body_150_300_db >= 29.0
        and center_body_support_proxy >= 0.38
    )

    bridge_connected = (
        body_to_mid_handoff_proxy >= 0.38
        and bass_to_body_db >= 4.0
        and low_foundation_ratio_db >= 7.0
    )

    is_body_weak = (
        center_body_support_proxy < 0.34
        or body_to_mid_handoff_proxy < 0.34
        or body_150_400_db < 29.0
    )

    mud_is_real = (
        mud_to_body_db >= 0.90
        or (lowmid_buildup_ratio_db >= 18.5 and mud_to_body_db >= 0.35)
        or (lowmid_buildup_ratio_db >= 20.5 and not is_punch_healthy)
    )

    studio_density_signature = (
        is_punch_healthy
        and useful_body_present
        and bridge_connected
        and mud_to_body_db <= 0.60
        and lowmid_buildup_ratio_db <= 19.8
    )

    is_dirty_buildup = (
        mud_is_real
        and not studio_density_signature
    )

    is_studio_dense_preserve = (
        studio_density_signature
        and integrated_lufs >= -13.2
        and integrated_lufs <= -8.2
    )

    is_quiet_open = (
        integrated_lufs <= -12.6
        and is_punch_healthy
        and near_clip_ratio < 0.0015
        and top_push_safety_proxy >= 0.55
        and harshness_index <= -9.0
        and sibilance_index <= 2.2
    )

    is_top_hot_musical = (
        top_push_safety_proxy < 0.36
        and is_punch_healthy
        and near_clip_ratio < 0.0035
        and true_peak_dbtp < 1.60
        and sibilance_index < 5.8
    )

    destructive_top_condition = (
        harshness_risk == "high"
        and sibilance_risk == "high"
        and top_push_safety_proxy < 0.16
        and sibilance_index >= 5.8
        and harshness_index > -8.0
    )

    true_clip_emergency = (
        true_peak_dbtp >= 2.40
        or near_clip_ratio >= 0.020
    )

    punch_collapse_emergency = (
        crest_db < 7.8
        and punch_proxy < 8.8
        and plr_proxy_db < 8.5
    )

    is_real_top_emergency = bool(
        true_clip_emergency
        or punch_collapse_emergency
        or destructive_top_condition
    )

    if is_quiet_open:
        profile = "quiet_open_lift"
    elif is_studio_dense_preserve:
        profile = "studio_dense_preserve"
    elif is_dirty_buildup:
        profile = "dirty_buildup_cleanup"
    elif is_body_weak:
        profile = "body_bridge_restore"
    elif is_top_hot_musical:
        profile = "top_hot_musical_preserve"
    else:
        profile = "balanced_polish"

    return {
        "profile": profile,
        "is_quiet_open": is_quiet_open,
        "is_studio_dense_preserve": is_studio_dense_preserve,
        "is_dirty_buildup": is_dirty_buildup,
        "is_body_weak": is_body_weak,
        "is_top_hot_musical": is_top_hot_musical,
        "is_real_top_emergency": is_real_top_emergency,
        "is_punch_fragile": is_punch_fragile,
        "is_loud_hot": is_loud_hot,
        "is_peak_hot": is_peak_hot,
        "is_limiter_hot": is_limiter_hot,
        "is_punch_healthy": is_punch_healthy,
        "top_push_safety_proxy": top_push_safety_proxy,
        "center_body_support_proxy": center_body_support_proxy,
        "body_to_mid_handoff_proxy": body_to_mid_handoff_proxy,
        "mud_is_real": mud_is_real,
        "studio_density_signature": studio_density_signature,
    }


def _context_notes(analysis: Optional[SmartMasterAnalysis]) -> List[str]:
    ctx = _track_context(analysis)
    return _uniq(
        [
            f"sm_context={ctx['profile']}",
            f"quiet_open={ctx['is_quiet_open']}",
            f"studio_dense_preserve={ctx['is_studio_dense_preserve']}",
            f"dirty_buildup={ctx['is_dirty_buildup']}",
            f"body_weak={ctx['is_body_weak']}",
            f"top_hot_musical={ctx['is_top_hot_musical']}",
            f"real_top_emergency={ctx['is_real_top_emergency']}",
            f"punch_fragile={ctx['is_punch_fragile']}",
        ]
    )


def _default_split_for_mode(role: RoleName, target_band_mode: str) -> Dict[str, Tuple[float, float, float]]:
    if role == RoleName.PROJECTION:
        if target_band_mode == "projection_dense":
            return {
                "projection_contour_stack": (0.88, 0.96, 1.00),
                "projection_assist_stack": (0.68, 0.86, 1.00),
            }
        if target_band_mode == "projection_mild":
            return {
                "projection_contour_stack": (0.92, 1.00, 1.00),
                "projection_assist_stack": (0.70, 0.88, 0.96),
            }
        if target_band_mode == "projection_clamp":
            return {
                "projection_contour_stack": (1.00, 1.00, 1.00),
            }
        return {}

    if role == RoleName.SPARK:
        return {
            "spark_finish_stack": (1.00, 1.00, 1.00),
        }

    if role == RoleName.ANCHOR:
        return {
            "anchor_parallel_stack": (1.00, 1.00, 1.00),
        }

    if role == RoleName.BRIDGE:
        return {
            "bridge_parallel_stack": (1.00, 1.00, 1.00),
        }

    if role == RoleName.CLEANUP:
        return {
            "cleanup_core_stack": (1.00, 1.00, 1.00),
        }

    if role == RoleName.GUARD:
        return {
            "guard_core_stack": (1.00, 1.00, 1.00),
        }

    return {}


def _stack_safety_tags(
    plan,
    mode_spec: RoleModeSpec,
    template: RoleStackTemplate,
) -> List[str]:
    tags: List[str] = []
    tags.extend(mode_spec.required_safety_tags or [])
    tags.extend(template.required_safety_tags or [])

    role_key = str(plan.role.value if hasattr(plan.role, "value") else plan.role).lower()

    if role_key in {"cleanup", "guard"}:
        tags.append("body_sensitive")

    if role_key in {"anchor", "bridge"}:
        tags.append("body_sensitive")
        tags.append("bridge_sensitive")

    if role_key in {"projection", "spark"}:
        tags.append("top_sensitive")

    if template.path_type == "finish":
        tags.append("finish_sensitive")

    if template.path_type in {"parallel", "finish", "delivery"}:
        tags.append("delivery_sensitive")

    return _uniq(tags)


def _stack_notes(
    plan,
    mode_spec: RoleModeSpec,
    template: RoleStackTemplate,
) -> List[str]:
    notes: List[str] = []
    notes.extend(plan.notes or [])
    notes.extend(mode_spec.notes or [])
    notes.extend(template.notes or [])

    notes.extend(
        [
            f"router_profile={plan.profile_name}",
            f"router_role_rank={plan.role_rank}",
            f"router_energy_class={plan.energy_class}",
            f"stack_kind={template.stack_kind}",
            f"path_type={template.path_type}",
            f"tap_point={template.default_tap_point}",
        ]
    )

    if getattr(plan, "interaction_tags", None):
        notes.append(f"interaction_tags={','.join(plan.interaction_tags)}")

    if getattr(plan, "allowed_primitives", None):
        notes.append(f"router_allowed_primitives={','.join(plan.allowed_primitives)}")

    if getattr(plan, "forbidden_primitives", None):
        notes.append(f"router_forbidden_primitives={','.join(plan.forbidden_primitives)}")

    return _uniq(notes)


ALL_PRIMITIVE_NAMES: List[str] = []


def _set_all_primitive_names_from_mode_spec() -> None:
    global ALL_PRIMITIVE_NAMES
    if ALL_PRIMITIVE_NAMES:
        return

    from .primitives import list_primitive_names

    ALL_PRIMITIVE_NAMES = list_primitive_names()


def _build_role_stack(
    plan,
    mode_spec: RoleModeSpec,
    template: RoleStackTemplate,
) -> RoleDSPStack:
    split_map = _default_split_for_mode(plan.role, mode_spec.target_band_mode)
    amount_scale, cap_scale, dyn_mul = split_map.get(template.stack_name, (1.0, 1.0, 1.0))

    execution_amount = _clamp(plan.execution_amount * amount_scale, 0.0, plan.execution_cap)
    execution_cap = _clamp(plan.execution_cap * cap_scale, 0.0, plan.execution_cap)
    execution_amount = min(execution_amount, execution_cap)

    dynamic_scale = _clamp(plan.dynamic_scale * dyn_mul, 0.0, 1.0)

    allowed = list(template.allowed_primitive_names or [])
    forbidden = sorted(name for name in ALL_PRIMITIVE_NAMES if name not in set(allowed))

    return RoleDSPStack(
        role=plan.role,
        enabled=bool(plan.enabled),
        stack_name=template.stack_name,
        stack_kind=template.stack_kind,
        path_type=template.path_type,
        tap_point=template.default_tap_point,
        output_node=template.output_node,
        recombine_target=template.recombine_target,
        role_rank=plan.role_rank,
        target_band_mode=mode_spec.target_band_mode,
        protection_mode=plan.protection_mode,
        requested_amount=plan.requested_amount,
        requested_cap=plan.requested_cap,
        execution_amount=execution_amount,
        execution_cap=execution_cap,
        dynamic_scale=dynamic_scale,
        allowed_primitive_names=allowed,
        forbidden_primitive_names=forbidden,
        primitive_instances=[],
        safety_tags=_stack_safety_tags(plan, mode_spec, template),
        active_clamps=[],
        blocked_actions=[],
        notes=_stack_notes(plan, mode_spec, template),
    )


def _expand_role_plan_to_stacks(plan) -> List[RoleDSPStack]:
    if plan is None:
        return []

    _set_all_primitive_names_from_mode_spec()

    mode_spec = get_role_mode_spec(plan.role, _effective_mode(plan))
    if not plan.enabled or mode_spec.target_band_mode == "off" or not mode_spec.stack_templates:
        return []

    stacks: List[RoleDSPStack] = []
    for template in sorted(mode_spec.stack_templates, key=lambda x: x.preferred_order):
        stacks.append(_build_role_stack(plan, mode_spec, template))
    return stacks


def _index_role_stacks(stacks: List[RoleDSPStack]) -> Dict[str, RoleDSPStack]:
    return {stack.stack_name: stack for stack in stacks if stack is not None}


def _is_projection_hard_emergency(analysis: Optional[SmartMasterAnalysis]) -> bool:
    return bool(_track_context(analysis).get("is_real_top_emergency", False))


def _is_spark_hard_emergency(analysis: Optional[SmartMasterAnalysis]) -> bool:
    return bool(_track_context(analysis).get("is_real_top_emergency", False))


def _restore_mandatory_projection_floor(
    *,
    router_blueprint: SmartMasterExecutionBlueprint,
    projection_stacks: List[RoleDSPStack],
    analysis: Optional[SmartMasterAnalysis],
) -> Tuple[List[RoleDSPStack], List[str]]:
    projection_idx = _index_role_stacks(projection_stacks)
    contour_stack = projection_idx.get("projection_contour_stack")
    assist_stack = projection_idx.get("projection_assist_stack")

    ctx = _track_context(analysis)
    hard_emergency = bool(ctx["is_real_top_emergency"])

    existing_mode = str(getattr(contour_stack, "target_band_mode", "") or "")
    existing_activity = _stack_activity(contour_stack)

    projection_is_missing = contour_stack is None or not contour_stack.enabled
    projection_is_too_weak = (
        contour_stack is not None
        and contour_stack.enabled
        and existing_mode != "projection_clamp"
        and (
            contour_stack.execution_amount < 0.24
            or contour_stack.dynamic_scale < 0.62
            or existing_activity < 0.62
        )
    )
    projection_was_false_clamped = (
        contour_stack is not None
        and contour_stack.enabled
        and existing_mode == "projection_clamp"
        and not hard_emergency
    )
    assist_is_missing_when_musical = (
        not hard_emergency
        and (
            assist_stack is None
            or not assist_stack.enabled
            or assist_stack.execution_amount < 0.12
            or _stack_activity(assist_stack) < 0.48
        )
    )

    if (
        not projection_is_missing
        and not projection_is_too_weak
        and not projection_was_false_clamped
        and not assist_is_missing_when_musical
    ):
        return projection_stacks, []

    projection_plan = router_blueprint.projection
    if projection_plan is None:
        return projection_stacks, ["projection_not_restored_no_router_plan"]

    old_tags = list(getattr(projection_plan, "interaction_tags", []) or [])
    old_notes = list(getattr(projection_plan, "notes", []) or [])

    if hard_emergency:
        target_band_mode = "projection_clamp"
        profile_name = "projection_clamp_safe"
        protection_mode = "top_strict"
        floor_amount = 0.16
        floor_cap = 0.24
        floor_dynamic = 0.46
        energy_class = "mild"
        role_rank = "support"
    elif ctx["is_quiet_open"]:
        target_band_mode = "projection_dense"
        profile_name = "projection_controlled_dense"
        protection_mode = "body_link_required"
        floor_amount = 0.40
        floor_cap = 0.56
        floor_dynamic = 0.84
        energy_class = "dense"
        role_rank = "primary"
    elif ctx["is_studio_dense_preserve"] or ctx["is_top_hot_musical"]:
        target_band_mode = "projection_mild"
        profile_name = "projection_mild_safe"
        protection_mode = "top_guarded"
        floor_amount = 0.25
        floor_cap = 0.38
        floor_dynamic = 0.62
        energy_class = "mild"
        role_rank = "support"
    else:
        target_band_mode = "projection_mild"
        profile_name = "projection_mild_safe"
        protection_mode = "top_guarded"
        floor_amount = 0.31
        floor_cap = 0.44
        floor_dynamic = 0.70
        energy_class = "controlled"
        role_rank = "primary"

    if existing_mode == "projection_dense" and not hard_emergency and not ctx["is_studio_dense_preserve"]:
        target_band_mode = "projection_dense"
        profile_name = "projection_controlled_dense"
        protection_mode = "body_link_required"
        floor_amount = max(floor_amount, 0.36)
        floor_cap = max(floor_cap, 0.50)
        floor_dynamic = max(floor_dynamic, 0.76)
        energy_class = "controlled"
        role_rank = "primary"

    patched_projection_plan = replace(
        projection_plan,
        enabled=True,
        profile_name=profile_name,
        role_rank=role_rank,
        energy_class=energy_class,
        requested_amount=max(float(_read(projection_plan, "requested_amount", 0.0) or 0.0), floor_amount),
        requested_cap=max(float(_read(projection_plan, "requested_cap", 0.0) or 0.0), floor_cap),
        execution_amount=max(float(_read(projection_plan, "execution_amount", 0.0) or 0.0), floor_amount),
        execution_cap=max(float(_read(projection_plan, "execution_cap", 0.0) or 0.0), floor_cap),
        dynamic_scale=max(float(_read(projection_plan, "dynamic_scale", 0.0) or 0.0), floor_dynamic),
        target_band_mode=target_band_mode,
        protection_mode=protection_mode,
        allowed_primitives=[
            "broad_presence_contour",
            "dynamic_presence_lift",
            "projection_local_deharsh",
            "band_limited_soft_saturation",
            "controlled_harmonic_density",
        ],
        forbidden_primitives=[],
        interaction_tags=_uniq(old_tags + ["assembler_restored_mandatory_projection_floor"]),
        notes=_uniq(
            old_notes
            + _context_notes(analysis)
            + [
                "assembler restored mandatory projection floor",
                "projection is primary musical benefit in SM",
                "cleanup creates space; projection must use that space musically",
                "projection can become guarded or clamp only in real hard emergency",
                "false top clamp is restored to musical guarded projection",
            ]
        ),
    )

    return _expand_role_plan_to_stacks(patched_projection_plan), ["assembler_restored_mandatory_projection_floor"]


def _restore_mandatory_spark_floor(
    *,
    router_blueprint: SmartMasterExecutionBlueprint,
    projection_contour_stack: Optional[RoleDSPStack],
    spark_stacks: List[RoleDSPStack],
    analysis: Optional[SmartMasterAnalysis],
) -> Tuple[List[RoleDSPStack], List[str]]:
    spark_idx = _index_role_stacks(spark_stacks)
    existing_spark = spark_idx.get("spark_finish_stack")
    ctx = _track_context(analysis)

    if existing_spark is not None and existing_spark.enabled:
        if existing_spark.execution_amount >= 0.16 and existing_spark.dynamic_scale >= 0.50:
            return spark_stacks, []

    if projection_contour_stack is None or not projection_contour_stack.enabled:
        return spark_stacks, ["spark_not_restored_no_projection_carrier"]

    if ctx["is_real_top_emergency"]:
        return spark_stacks, ["spark_off_hard_emergency_only"]

    spark_plan = router_blueprint.spark
    if spark_plan is None:
        return spark_stacks, ["spark_not_restored_no_router_plan"]

    old_tags = list(getattr(spark_plan, "interaction_tags", []) or [])
    old_notes = list(getattr(spark_plan, "notes", []) or [])

    if ctx["is_quiet_open"]:
        target_band_mode = "spark_excited"
        profile_name = "finish_spark_controlled_excited"
        protection_mode = "spark_guarded"
        floor_amount = 0.20
        floor_cap = 0.34
        floor_dynamic = 0.58
    elif ctx["is_studio_dense_preserve"] or ctx["is_top_hot_musical"]:
        target_band_mode = "spark_micro"
        profile_name = "finish_spark_micro_safe"
        protection_mode = "spark_micro_only"
        floor_amount = 0.14
        floor_cap = 0.26
        floor_dynamic = 0.46
    else:
        target_band_mode = "spark_micro"
        profile_name = "finish_spark_micro_safe"
        protection_mode = "spark_micro_only"
        floor_amount = 0.18
        floor_cap = 0.30
        floor_dynamic = 0.54

    patched_spark_plan = replace(
        spark_plan,
        enabled=True,
        profile_name=profile_name,
        role_rank="support",
        energy_class="mild",
        requested_amount=max(float(_read(spark_plan, "requested_amount", 0.0) or 0.0), floor_amount),
        requested_cap=max(float(_read(spark_plan, "requested_cap", 0.0) or 0.0), floor_cap),
        execution_amount=max(float(_read(spark_plan, "execution_amount", 0.0) or 0.0), floor_amount),
        execution_cap=max(float(_read(spark_plan, "execution_cap", 0.0) or 0.0), floor_cap),
        dynamic_scale=max(float(_read(spark_plan, "dynamic_scale", 0.0) or 0.0), floor_dynamic),
        target_band_mode=target_band_mode,
        protection_mode=protection_mode,
        allowed_primitives=[
            "micro_air_shelf",
            "micro_top_texture",
            "protected_high_side_polish",
            "micro_width_high_only",
            "local_desibilance_control",
        ],
        forbidden_primitives=[],
        interaction_tags=_uniq(old_tags + ["assembler_restored_mandatory_spark_floor"]),
        notes=_uniq(
            old_notes
            + _context_notes(analysis)
            + [
                "assembler restored mandatory polish spark floor",
                "spark is finish magic, not optional silence",
                "spark can be disabled only by hard emergency",
                "micro spark must remain small but audible",
            ]
        ),
    )

    return _expand_role_plan_to_stacks(patched_spark_plan), ["assembler_restored_mandatory_spark_floor"]


def _restore_enabled_support_floor(
    *,
    plan,
    current_stacks: List[RoleDSPStack],
    stack_name: str,
    floor_amount: float,
    floor_cap: float,
    floor_dynamic: float,
    floor_activity: float,
    note_tag: str,
) -> Tuple[List[RoleDSPStack], List[str]]:
    if not _enabled(plan) or _effective_mode(plan) == "off":
        return current_stacks, []

    idx = _index_role_stacks(current_stacks)
    stack = idx.get(stack_name)

    too_weak = (
        stack is None
        or not stack.enabled
        or stack.execution_amount < floor_amount
        or stack.execution_cap < floor_cap
        or stack.dynamic_scale < floor_dynamic
        or _stack_activity(stack) < floor_activity
    )

    if not too_weak:
        return current_stacks, []

    old_tags = list(getattr(plan, "interaction_tags", []) or [])
    old_notes = list(getattr(plan, "notes", []) or [])

    patched_plan = replace(
        plan,
        enabled=True,
        requested_amount=max(float(_read(plan, "requested_amount", 0.0) or 0.0), floor_amount),
        requested_cap=max(float(_read(plan, "requested_cap", 0.0) or 0.0), floor_cap),
        execution_amount=max(float(_read(plan, "execution_amount", 0.0) or 0.0), floor_amount),
        execution_cap=max(float(_read(plan, "execution_cap", 0.0) or 0.0), floor_cap),
        dynamic_scale=max(float(_read(plan, "dynamic_scale", 0.0) or 0.0), floor_dynamic),
        interaction_tags=_uniq(old_tags + [note_tag]),
        notes=_uniq(
            old_notes
            + [
                note_tag,
                "support floor restored after clamps",
                "support preserves body and bass-to-body bridge instead of disappearing",
            ]
        ),
    )

    return _expand_role_plan_to_stacks(patched_plan), [note_tag]


def _derive_support_recombine_gain_db(
    anchor_stack: Optional[RoleDSPStack],
    bridge_stack: Optional[RoleDSPStack],
) -> float:
    total_amount = 0.0
    activity_sum = 0.0
    active_count = 0

    for stack in [anchor_stack, bridge_stack]:
        if stack is None or not stack.enabled:
            continue
        total_amount += stack.execution_amount
        activity_sum += _stack_activity(stack)
        active_count += 1

    if active_count <= 0:
        return 0.0

    activity_avg = activity_sum / active_count

    return _clamp(
        -0.02 + (total_amount * 0.74) + (activity_avg * 0.18),
        0.00,
        0.46,
    )


def _derive_projection_assist_blend(
    projection_assist_stack: Optional[RoleDSPStack],
) -> float:
    if projection_assist_stack is None or not projection_assist_stack.enabled:
        return 0.0

    activity = _stack_activity(projection_assist_stack)
    mode = str(projection_assist_stack.target_band_mode or "")

    if mode == "projection_dense":
        return _clamp(0.30 + (activity * 0.28), 0.30, 0.58)

    if mode == "projection_mild":
        return _clamp(0.22 + (activity * 0.22), 0.22, 0.46)

    if mode == "projection_clamp":
        return 0.0

    return _clamp(0.18 + (activity * 0.20), 0.18, 0.38)


def _derive_spark_blend(
    spark_stack: Optional[RoleDSPStack],
) -> float:
    if spark_stack is None or not spark_stack.enabled:
        return 0.0

    activity = _stack_activity(spark_stack)
    mode = str(spark_stack.target_band_mode or "")

    if mode == "spark_excited":
        return _clamp(0.18 + (activity * 0.18), 0.18, 0.36)

    if mode == "spark_micro":
        return _clamp(0.12 + (activity * 0.12), 0.12, 0.26)

    return _clamp(0.10 + (activity * 0.10), 0.10, 0.20)


def _delivery_role_value():
    return getattr(RoleName, "DELIVERY", "delivery")


def _delivery_strategy(analysis: SmartMasterAnalysis) -> Dict[str, Any]:
    ctx = _track_context(analysis)

    integrated_lufs = _metric_f(analysis, "integrated_lufs", -14.0)
    true_peak_dbtp = _metric_f(analysis, "true_peak_dbtp", -1.0)
    near_clip_ratio = _metric_f(analysis, "near_clip_ratio", 0.0)
    crest_db = _metric_f(analysis, "crest_db", 10.0)
    punch_proxy = _metric_f(analysis, "punch_proxy", 10.0)
    plr_proxy_db = _metric_f(analysis, "plr_proxy_db", 10.0)
    limiter_stress_proxy = _metric_f(analysis, "limiter_stress_proxy", 0.0)

    tp_over_margin = max(0.0, true_peak_dbtp + 1.10)
    peak_danger = true_peak_dbtp > -0.75 or near_clip_ratio > 0.001
    severe_peak_danger = true_peak_dbtp >= 1.40 or near_clip_ratio >= 0.010
    punch_fragile = crest_db < 10.2 or punch_proxy < 10.8 or plr_proxy_db < 9.8

    if ctx["is_real_top_emergency"] or severe_peak_danger:
        name = "hard_peak_protection"
        amount = 0.46 + min(0.20, tp_over_margin * 0.08)
        dynamic = 0.58 + min(0.14, tp_over_margin * 0.06)
        max_lift_db = 0.0
        protect_punch = True
    elif ctx["is_quiet_open"]:
        name = "quiet_open_controlled_lift"
        amount = 0.22
        dynamic = 0.44
        max_lift_db = 1.40
        protect_punch = True
    elif ctx["is_studio_dense_preserve"] or ctx["is_top_hot_musical"]:
        name = "studio_dense_peak_preserve"
        amount = 0.18 + min(0.10, tp_over_margin * 0.05)
        dynamic = 0.40 + min(0.08, tp_over_margin * 0.04)
        max_lift_db = 0.35
        protect_punch = True
    elif integrated_lufs < -13.5 and not peak_danger:
        name = "quiet_safe_lift"
        amount = 0.24
        dynamic = 0.46
        max_lift_db = 1.20
        protect_punch = True
    elif peak_danger:
        name = "peak_margin_protection"
        amount = 0.28 + min(0.16, tp_over_margin * 0.07)
        dynamic = 0.48 + min(0.10, tp_over_margin * 0.05)
        max_lift_db = 0.20
        protect_punch = True
    else:
        name = "transparent_finalize"
        amount = 0.20
        dynamic = 0.42
        max_lift_db = 0.55
        protect_punch = punch_fragile

    if punch_fragile:
        amount -= 0.04
        dynamic -= 0.06
        max_lift_db = min(max_lift_db, 0.30)

    if limiter_stress_proxy > 1.25 and not ctx["is_studio_dense_preserve"]:
        amount += 0.05
        dynamic += 0.04

    return {
        "name": name,
        "amount": _clamp(amount, 0.12, 0.66),
        "dynamic": _clamp(dynamic, 0.32, 0.72),
        "max_lift_db": _clamp(max_lift_db, 0.0, 1.6),
        "protect_punch": protect_punch,
        "peak_danger": peak_danger,
        "severe_peak_danger": severe_peak_danger,
    }


def _derive_delivery_execution_amount(analysis: SmartMasterAnalysis) -> float:
    return float(_delivery_strategy(analysis)["amount"])


def _derive_delivery_dynamic_scale(analysis: SmartMasterAnalysis) -> float:
    return float(_delivery_strategy(analysis)["dynamic"])


def _build_delivery_stack(
    analysis: SmartMasterAnalysis,
) -> RoleDSPStack:
    _set_all_primitive_names_from_mode_spec()

    metrics = _read(analysis, "metrics", {}) or {}
    strategy = _delivery_strategy(analysis)

    true_peak_dbtp = float(_read(metrics, "true_peak_dbtp", -1.0) or -1.0)
    integrated_lufs = float(_read(metrics, "integrated_lufs", -14.0) or -14.0)
    limiter_stress_proxy = float(_read(metrics, "limiter_stress_proxy", 0.0) or 0.0)
    near_clip_ratio = float(_read(metrics, "near_clip_ratio", 0.0) or 0.0)
    crest_db = float(_read(metrics, "crest_db", 10.0) or 10.0)
    punch_proxy = float(_read(metrics, "punch_proxy", 10.0) or 10.0)
    plr_proxy_db = float(_read(metrics, "plr_proxy_db", 10.0) or 10.0)

    execution_amount = _derive_delivery_execution_amount(analysis)
    dynamic_scale = _derive_delivery_dynamic_scale(analysis)

    allowed = ["output_gain_trim", "true_peak_limiter"]
    forbidden = sorted(name for name in ALL_PRIMITIVE_NAMES if name not in set(allowed))

    notes = [
        "delivery is terminal protection, not creative polish",
        "delivery is transparent finalize, not musical handbrake",
        "headroom first, limiter second",
        "no tone shaping inside delivery",
        "no width moves inside delivery",
        "no extra sparkle inside delivery",
        "delivery should preserve forward delta, crest, punch and PLR",
        f"delivery_strategy={strategy['name']}",
        f"delivery_max_lift_db={round(float(strategy['max_lift_db']), 3)}",
        f"delivery_protect_punch={strategy['protect_punch']}",
        f"delivery_peak_danger={strategy['peak_danger']}",
        f"delivery_severe_peak_danger={strategy['severe_peak_danger']}",
        f"analysis_true_peak_dbtp={round(true_peak_dbtp, 4)}",
        f"analysis_integrated_lufs={round(integrated_lufs, 4)}",
        f"analysis_limiter_stress_proxy={round(limiter_stress_proxy, 4)}",
        f"analysis_near_clip_ratio={round(near_clip_ratio, 6)}",
        f"analysis_crest_db={round(crest_db, 4)}",
        f"analysis_punch_proxy={round(punch_proxy, 4)}",
        f"analysis_plr_proxy_db={round(plr_proxy_db, 4)}",
    ]
    notes.extend(_context_notes(analysis))

    if strategy["protect_punch"]:
        notes.append("punch_crest_plr_preservation_required")

    return RoleDSPStack(
        role=_delivery_role_value(),
        enabled=True,
        stack_name="delivery_protect_stack",
        stack_kind="delivery_core",
        path_type="delivery",
        tap_point="finish_stage_out",
        output_node="final_output",
        recombine_target=None,
        role_rank="terminal",
        target_band_mode="fullband_delivery",
        protection_mode="true_peak_delivery_safe",
        requested_amount=execution_amount,
        requested_cap=1.00,
        execution_amount=execution_amount,
        execution_cap=1.00,
        dynamic_scale=dynamic_scale,
        allowed_primitive_names=allowed,
        forbidden_primitive_names=forbidden,
        primitive_instances=[],
        safety_tags=_uniq(
            [
                "delivery_sensitive",
                "tp_sensitive",
                "punch_sensitive",
                "codec_sensitive",
            ]
        ),
        active_clamps=[],
        blocked_actions=[],
        notes=_uniq(notes),
    )


def _filter_false_top_emergency_clamps(
    clamps: List[Any],
    analysis: SmartMasterAnalysis,
) -> List[Any]:
    if _track_context(analysis)["is_real_top_emergency"]:
        return clamps

    out: List[Any] = []
    for clamp in clamps or []:
        if isinstance(clamp, dict):
            name = str(clamp.get("clamp_name", "") or "").lower()
            reason = str(clamp.get("reason", "") or "").lower()
            if "top_emergency" in name or "top emergency" in reason:
                continue
        else:
            value = str(clamp).lower()
            if "top_emergency" in value or "top emergency" in value:
                continue
        out.append(clamp)
    return out


def build_dsp_execution_blueprint(
    router_blueprint: SmartMasterExecutionBlueprint,
    analysis: Optional[SmartMasterAnalysis] = None,
) -> DSPExecutionBlueprint:
    anchor_stacks = _expand_role_plan_to_stacks(router_blueprint.anchor)
    bridge_stacks = _expand_role_plan_to_stacks(router_blueprint.bridge)
    cleanup_stacks = _expand_role_plan_to_stacks(router_blueprint.cleanup)
    guard_stacks = _expand_role_plan_to_stacks(router_blueprint.guard)
    projection_stacks = _expand_role_plan_to_stacks(router_blueprint.projection)
    spark_stacks = _expand_role_plan_to_stacks(router_blueprint.spark)

    anchor_stacks, anchor_restore_notes = _restore_enabled_support_floor(
        plan=router_blueprint.anchor,
        current_stacks=anchor_stacks,
        stack_name="anchor_parallel_stack",
        floor_amount=0.18,
        floor_cap=0.30,
        floor_dynamic=0.42,
        floor_activity=0.46,
        note_tag="assembler_restored_anchor_support_floor",
    )

    bridge_stacks, bridge_restore_notes = _restore_enabled_support_floor(
        plan=router_blueprint.bridge,
        current_stacks=bridge_stacks,
        stack_name="bridge_parallel_stack",
        floor_amount=0.14,
        floor_cap=0.24,
        floor_dynamic=0.40,
        floor_activity=0.42,
        note_tag="assembler_restored_bridge_handoff_floor",
    )

    projection_stacks, projection_restore_notes = _restore_mandatory_projection_floor(
        router_blueprint=router_blueprint,
        projection_stacks=projection_stacks,
        analysis=analysis,
    )

    anchor_idx = _index_role_stacks(anchor_stacks)
    bridge_idx = _index_role_stacks(bridge_stacks)
    cleanup_idx = _index_role_stacks(cleanup_stacks)
    guard_idx = _index_role_stacks(guard_stacks)
    projection_idx = _index_role_stacks(projection_stacks)

    cleanup_stack = cleanup_idx.get("cleanup_core_stack")
    guard_stack = guard_idx.get("guard_core_stack")
    anchor_parallel_stack = anchor_idx.get("anchor_parallel_stack")
    bridge_parallel_stack = bridge_idx.get("bridge_parallel_stack")
    projection_contour_stack = projection_idx.get("projection_contour_stack")
    projection_assist_stack = projection_idx.get("projection_assist_stack")

    spark_stacks, spark_restore_notes = _restore_mandatory_spark_floor(
        router_blueprint=router_blueprint,
        projection_contour_stack=projection_contour_stack,
        spark_stacks=spark_stacks,
        analysis=analysis,
    )

    spark_idx = _index_role_stacks(spark_stacks)
    spark_stack = spark_idx.get("spark_finish_stack")

    delivery_stack = _build_delivery_stack(analysis) if analysis is not None else None

    blueprint_notes = list(router_blueprint.global_notes or [])
    blueprint_notes.extend(
        [
            "assembler_initialized",
            "role_specs_expanded",
            "mandatory_projection_floor_enabled",
            "mandatory_spark_floor_enabled",
            "mandatory_support_floor_enabled",
            "musical_blend_floors_enabled",
        ]
    )

    if analysis is not None:
        blueprint_notes.extend(_context_notes(analysis))

    blueprint_notes.extend(anchor_restore_notes)
    blueprint_notes.extend(bridge_restore_notes)
    blueprint_notes.extend(projection_restore_notes)
    blueprint_notes.extend(spark_restore_notes)

    if delivery_stack is None:
        blueprint_notes.append("delivery_stack_pending_true_peak_stage")
    else:
        blueprint_notes.extend(
            [
                "delivery_true_peak_stage_attached",
                "delivery_transparent_finalize_policy",
                "delivery_no_creative_tone_shaping",
                "delivery_not_global_handbrake",
            ]
        )

    blueprint = DSPExecutionBlueprint(
        blueprint_name="sm_dsp_execution_v1",
        prepared_input_node="prepared_input",
        final_output_node="final_output",
        cleanup_stack=cleanup_stack,
        guard_stack=guard_stack,
        anchor_parallel_stack=anchor_parallel_stack,
        bridge_parallel_stack=bridge_parallel_stack,
        projection_contour_stack=projection_contour_stack,
        projection_assist_stack=projection_assist_stack,
        spark_stack=spark_stack,
        delivery_stack=delivery_stack,
        support_recombine_gain_db=_derive_support_recombine_gain_db(
            anchor_parallel_stack,
            bridge_parallel_stack,
        ),
        projection_assist_blend=_derive_projection_assist_blend(
            projection_assist_stack,
        ),
        spark_blend=_derive_spark_blend(
            spark_stack,
        ),
        active_clamps=[],
        blocked_actions=[],
        safety_notes=[],
        notes=_uniq(blueprint_notes),
    )
    return blueprint


def _enforce_post_clamp_musical_floors(
    *,
    blueprint: DSPExecutionBlueprint,
    router_blueprint: SmartMasterExecutionBlueprint,
    analysis: SmartMasterAnalysis,
) -> DSPExecutionBlueprint:
    ctx = _track_context(analysis)

    notes: List[str] = [
        "post_clamp_musical_floor_pass_enabled",
        f"post_clamp_context={ctx['profile']}",
    ]

    anchor_stacks, anchor_notes = _restore_enabled_support_floor(
        plan=router_blueprint.anchor,
        current_stacks=[blueprint.anchor_parallel_stack] if blueprint.anchor_parallel_stack is not None else [],
        stack_name="anchor_parallel_stack",
        floor_amount=0.18,
        floor_cap=0.30,
        floor_dynamic=0.42,
        floor_activity=0.46,
        note_tag="post_clamp_restored_anchor_support_floor",
    )

    bridge_stacks, bridge_notes = _restore_enabled_support_floor(
        plan=router_blueprint.bridge,
        current_stacks=[blueprint.bridge_parallel_stack] if blueprint.bridge_parallel_stack is not None else [],
        stack_name="bridge_parallel_stack",
        floor_amount=0.14,
        floor_cap=0.24,
        floor_dynamic=0.40,
        floor_activity=0.42,
        note_tag="post_clamp_restored_bridge_handoff_floor",
    )

    projection_stacks, projection_notes = _restore_mandatory_projection_floor(
        router_blueprint=router_blueprint,
        projection_stacks=[
            stack
            for stack in [
                blueprint.projection_contour_stack,
                blueprint.projection_assist_stack,
            ]
            if stack is not None
        ],
        analysis=analysis,
    )

    projection_idx = _index_role_stacks(projection_stacks)
    projection_contour_stack = projection_idx.get(
        "projection_contour_stack",
        blueprint.projection_contour_stack,
    )
    projection_assist_stack = projection_idx.get(
        "projection_assist_stack",
        blueprint.projection_assist_stack,
    )

    spark_stacks, spark_notes = _restore_mandatory_spark_floor(
        router_blueprint=router_blueprint,
        projection_contour_stack=projection_contour_stack,
        spark_stacks=[blueprint.spark_stack] if blueprint.spark_stack is not None else [],
        analysis=analysis,
    )

    anchor_idx = _index_role_stacks(anchor_stacks)
    bridge_idx = _index_role_stacks(bridge_stacks)
    spark_idx = _index_role_stacks(spark_stacks)

    anchor_parallel_stack = anchor_idx.get("anchor_parallel_stack", blueprint.anchor_parallel_stack)
    bridge_parallel_stack = bridge_idx.get("bridge_parallel_stack", blueprint.bridge_parallel_stack)
    spark_stack = spark_idx.get("spark_finish_stack", blueprint.spark_stack)

    notes.extend(anchor_notes)
    notes.extend(bridge_notes)
    notes.extend(projection_notes)
    notes.extend(spark_notes)

    if not ctx["is_real_top_emergency"]:
        notes.append("false_top_emergency_clamps_removed_after_context_check")

    filtered_active_clamps = _filter_false_top_emergency_clamps(
        list(blueprint.active_clamps or []),
        analysis,
    )

    filtered_safety_notes = list(blueprint.safety_notes or [])
    if not ctx["is_real_top_emergency"]:
        filtered_safety_notes = [
            note
            for note in filtered_safety_notes
            if "top emergency" not in str(note).lower()
            and "top_emergency" not in str(note).lower()
        ]

    return replace(
        blueprint,
        anchor_parallel_stack=anchor_parallel_stack,
        bridge_parallel_stack=bridge_parallel_stack,
        projection_contour_stack=projection_contour_stack,
        projection_assist_stack=projection_assist_stack,
        spark_stack=spark_stack,
        support_recombine_gain_db=_derive_support_recombine_gain_db(
            anchor_parallel_stack,
            bridge_parallel_stack,
        ),
        projection_assist_blend=_derive_projection_assist_blend(
            projection_assist_stack,
        ),
        spark_blend=_derive_spark_blend(
            spark_stack,
        ),
        active_clamps=filtered_active_clamps,
        safety_notes=_uniq(filtered_safety_notes),
        notes=_uniq(list(blueprint.notes or []) + notes),
    )


def assemble_sm_dsp_blueprint(
    analysis: SmartMasterAnalysis,
    router_blueprint: SmartMasterExecutionBlueprint,
) -> DSPExecutionBlueprint:
    blueprint = build_dsp_execution_blueprint(
        router_blueprint,
        analysis=analysis,
    )
    blueprint = apply_dsp_clamps(blueprint, analysis)
    blueprint = _enforce_post_clamp_musical_floors(
        blueprint=blueprint,
        router_blueprint=router_blueprint,
        analysis=analysis,
    )
    blueprint = attach_primitive_instances_to_blueprint(blueprint, analysis)
    blueprint = attach_graph_to_blueprint(blueprint)
    return replace(
        blueprint,
        notes=_uniq(
            list(blueprint.notes or [])
            + [
                "assembler_completed",
                "premium_v1_topology_attached",
                "primitive_instances_ready",
                "sm_music_first_architecture_v3",
                "context_aware_delivery_and_false_top_clamp_recovery",
                "post_clamp_projection_support_spark_floors_locked",
            ]
        ),
    )

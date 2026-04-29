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


def _stack_activity(stack: Optional[RoleDSPStack]) -> float:
    if stack is None or not stack.enabled:
        return 0.0

    if stack.execution_cap <= 1e-9:
        amount_norm = 0.0
    else:
        amount_norm = _clamp(stack.execution_amount / stack.execution_cap, 0.0, 1.0)

    return _clamp((amount_norm * 0.58) + (stack.dynamic_scale * 0.42), 0.0, 1.0)


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

    notes.extend([
        f"router_profile={plan.profile_name}",
        f"router_role_rank={plan.role_rank}",
        f"router_energy_class={plan.energy_class}",
        f"stack_kind={template.stack_kind}",
        f"path_type={template.path_type}",
        f"tap_point={template.default_tap_point}",
    ])

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
    if analysis is None:
        return False

    metrics = _read(analysis, "metrics", {}) or {}
    derived = _read(analysis, "derived", {}) or {}
    projection = _read(analysis, "projection", {}) or {}

    true_peak_dbtp = float(_read(metrics, "true_peak_dbtp", -1.0) or -1.0)
    near_clip_ratio = float(_read(metrics, "near_clip_ratio", 0.0) or 0.0)
    crest_db = float(_read(metrics, "crest_db", 10.0) or 10.0)
    punch_proxy = float(_read(metrics, "punch_proxy", 10.0) or 10.0)

    harshness_risk = _value(_read(projection, "harshness_risk", "low"))
    sibilance_risk = _value(_read(projection, "sibilance_risk", "low"))
    top_push_safety_proxy = _read(derived, "top_push_safety_proxy", None)

    top_safety_collapse = (
        top_push_safety_proxy is not None
        and float(top_push_safety_proxy) < 0.32
    )

    double_top_collapse = (
        harshness_risk == "high"
        and sibilance_risk == "high"
        and top_safety_collapse
    )

    hard_clip_emergency = true_peak_dbtp >= 2.40 or near_clip_ratio >= 0.020
    punch_collapse = crest_db < 8.0 and punch_proxy < 9.0

    return bool(double_top_collapse or hard_clip_emergency or punch_collapse)


def _restore_mandatory_projection_floor(
    *,
    router_blueprint: SmartMasterExecutionBlueprint,
    projection_stacks: List[RoleDSPStack],
    analysis: Optional[SmartMasterAnalysis],
) -> Tuple[List[RoleDSPStack], List[str]]:
    projection_idx = _index_role_stacks(projection_stacks)
    contour_stack = projection_idx.get("projection_contour_stack")
    assist_stack = projection_idx.get("projection_assist_stack")

    hard_emergency = _is_projection_hard_emergency(analysis)

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
    assist_is_missing_when_musical = (
        not hard_emergency
        and (
            assist_stack is None
            or not assist_stack.enabled
            or assist_stack.execution_amount < 0.12
            or _stack_activity(assist_stack) < 0.48
        )
    )

    if not projection_is_missing and not projection_is_too_weak and not assist_is_missing_when_musical:
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
        floor_amount = 0.18
        floor_cap = 0.28
        floor_dynamic = 0.50
        energy_class = "mild"
    else:
        target_band_mode = "projection_mild"
        profile_name = "projection_mild_safe"
        protection_mode = "top_guarded"
        floor_amount = 0.31
        floor_cap = 0.44
        floor_dynamic = 0.70
        energy_class = "controlled"

    if existing_mode == "projection_dense" and not hard_emergency:
        target_band_mode = "projection_dense"
        profile_name = "projection_controlled_dense"
        protection_mode = "body_link_required"
        floor_amount = 0.36
        floor_cap = 0.50
        floor_dynamic = 0.76
        energy_class = "controlled"

    patched_projection_plan = replace(
        projection_plan,
        enabled=True,
        profile_name=profile_name,
        role_rank="primary",
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
            + [
                "assembler restored mandatory projection floor",
                "projection is primary musical benefit in SM",
                "cleanup creates space; projection must use that space musically",
                "projection can become guarded or clamp in hard emergency, but not silent",
            ]
        ),
    )

    return _expand_role_plan_to_stacks(patched_projection_plan), ["assembler_restored_mandatory_projection_floor"]


def _is_spark_hard_emergency(analysis: Optional[SmartMasterAnalysis]) -> bool:
    if analysis is None:
        return False

    metrics = _read(analysis, "metrics", {}) or {}
    derived = _read(analysis, "derived", {}) or {}
    projection = _read(analysis, "projection", {}) or {}

    true_peak_dbtp = float(_read(metrics, "true_peak_dbtp", -1.0) or -1.0)
    near_clip_ratio = float(_read(metrics, "near_clip_ratio", 0.0) or 0.0)
    crest_db = float(_read(metrics, "crest_db", 10.0) or 10.0)
    punch_proxy = float(_read(metrics, "punch_proxy", 10.0) or 10.0)

    harshness_risk = _value(_read(projection, "harshness_risk", "low"))
    sibilance_risk = _value(_read(projection, "sibilance_risk", "low"))
    top_push_safety_proxy = _read(derived, "top_push_safety_proxy", None)

    top_safety_collapse = (
        top_push_safety_proxy is not None
        and float(top_push_safety_proxy) < 0.34
    )

    double_top_emergency = (
        harshness_risk == "high"
        and sibilance_risk == "high"
        and top_safety_collapse
    )

    hard_clip_emergency = true_peak_dbtp >= 2.20 or near_clip_ratio >= 0.018
    punch_collapse = crest_db < 8.2 and punch_proxy < 9.2

    return bool(double_top_emergency or hard_clip_emergency or punch_collapse)


def _restore_mandatory_spark_floor(
    *,
    router_blueprint: SmartMasterExecutionBlueprint,
    projection_contour_stack: Optional[RoleDSPStack],
    spark_stacks: List[RoleDSPStack],
    analysis: Optional[SmartMasterAnalysis],
) -> Tuple[List[RoleDSPStack], List[str]]:
    spark_idx = _index_role_stacks(spark_stacks)
    existing_spark = spark_idx.get("spark_finish_stack")

    if existing_spark is not None and existing_spark.enabled:
        if existing_spark.execution_amount >= 0.16 and existing_spark.dynamic_scale >= 0.50:
            return spark_stacks, []

    if projection_contour_stack is None or not projection_contour_stack.enabled:
        return spark_stacks, ["spark_not_restored_no_projection_carrier"]

    if _is_spark_hard_emergency(analysis):
        return spark_stacks, ["spark_off_hard_emergency_only"]

    spark_plan = router_blueprint.spark
    if spark_plan is None:
        return spark_stacks, ["spark_not_restored_no_router_plan"]

    old_tags = list(getattr(spark_plan, "interaction_tags", []) or [])
    old_notes = list(getattr(spark_plan, "notes", []) or [])

    patched_spark_plan = replace(
        spark_plan,
        enabled=True,
        profile_name="finish_spark_micro_safe",
        role_rank="support",
        energy_class="mild",
        requested_amount=max(float(_read(spark_plan, "requested_amount", 0.0) or 0.0), 0.18),
        requested_cap=max(float(_read(spark_plan, "requested_cap", 0.0) or 0.0), 0.30),
        execution_amount=max(float(_read(spark_plan, "execution_amount", 0.0) or 0.0), 0.18),
        execution_cap=max(float(_read(spark_plan, "execution_cap", 0.0) or 0.0), 0.30),
        dynamic_scale=max(float(_read(spark_plan, "dynamic_scale", 0.0) or 0.0), 0.54),
        target_band_mode="spark_micro",
        protection_mode="spark_micro_only",
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
        return _clamp(0.26 + (activity * 0.24), 0.26, 0.50)

    if mode == "projection_clamp":
        return 0.0

    return _clamp(0.20 + (activity * 0.20), 0.20, 0.40)


def _derive_spark_blend(
    spark_stack: Optional[RoleDSPStack],
) -> float:
    if spark_stack is None or not spark_stack.enabled:
        return 0.0

    activity = _stack_activity(spark_stack)
    mode = str(spark_stack.target_band_mode or "")

    if mode == "spark_excited":
        return _clamp(0.20 + (activity * 0.18), 0.20, 0.38)

    if mode == "spark_micro":
        return _clamp(0.16 + (activity * 0.14), 0.16, 0.30)

    return _clamp(0.12 + (activity * 0.10), 0.12, 0.22)


def _delivery_role_value():
    return getattr(RoleName, "DELIVERY", "delivery")


def _derive_delivery_execution_amount(analysis: SmartMasterAnalysis) -> float:
    metrics = _read(analysis, "metrics", {}) or {}

    true_peak_dbtp = float(_read(metrics, "true_peak_dbtp", -1.0) or -1.0)
    integrated_lufs = float(_read(metrics, "integrated_lufs", -14.0) or -14.0)
    limiter_stress_proxy = float(_read(metrics, "limiter_stress_proxy", 0.0) or 0.0)
    near_clip_ratio = float(_read(metrics, "near_clip_ratio", 0.0) or 0.0)
    crest_db = float(_read(metrics, "crest_db", 10.0) or 10.0)
    punch_proxy = float(_read(metrics, "punch_proxy", 10.0) or 10.0)

    tp_hot = max(0.0, true_peak_dbtp + 1.0)
    loud_hot = max(0.0, integrated_lufs + 9.3)
    stress_hot = max(0.0, limiter_stress_proxy - 0.92)
    clip_hot = min(1.0, near_clip_ratio * 70.0)

    amount = 0.34
    amount += tp_hot * 0.14
    amount += loud_hot * 0.03
    amount += stress_hot * 0.22
    amount += clip_hot * 0.08

    if punch_proxy < 10.8 or crest_db < 10.2:
        amount -= 0.05

    return _clamp(amount, 0.28, 0.78)


def _derive_delivery_dynamic_scale(analysis: SmartMasterAnalysis) -> float:
    metrics = _read(analysis, "metrics", {}) or {}

    true_peak_dbtp = float(_read(metrics, "true_peak_dbtp", -1.0) or -1.0)
    limiter_stress_proxy = float(_read(metrics, "limiter_stress_proxy", 0.0) or 0.0)
    near_clip_ratio = float(_read(metrics, "near_clip_ratio", 0.0) or 0.0)
    crest_db = float(_read(metrics, "crest_db", 10.0) or 10.0)
    punch_proxy = float(_read(metrics, "punch_proxy", 10.0) or 10.0)

    tp_hot = max(0.0, true_peak_dbtp + 1.0)
    stress_hot = max(0.0, limiter_stress_proxy - 0.92)
    clip_hot = min(1.0, near_clip_ratio * 70.0)

    dynamic_scale = 0.52
    dynamic_scale += tp_hot * 0.10
    dynamic_scale += stress_hot * 0.14
    dynamic_scale += clip_hot * 0.05

    if punch_proxy < 10.8 or crest_db < 10.2:
        dynamic_scale -= 0.07

    return _clamp(dynamic_scale, 0.42, 0.82)


def _build_delivery_stack(
    analysis: SmartMasterAnalysis,
) -> RoleDSPStack:
    _set_all_primitive_names_from_mode_spec()

    metrics = _read(analysis, "metrics", {}) or {}

    true_peak_dbtp = float(_read(metrics, "true_peak_dbtp", -1.0) or -1.0)
    integrated_lufs = float(_read(metrics, "integrated_lufs", -14.0) or -14.0)
    limiter_stress_proxy = float(_read(metrics, "limiter_stress_proxy", 0.0) or 0.0)
    near_clip_ratio = float(_read(metrics, "near_clip_ratio", 0.0) or 0.0)
    crest_db = float(_read(metrics, "crest_db", 10.0) or 10.0)
    punch_proxy = float(_read(metrics, "punch_proxy", 10.0) or 10.0)

    execution_amount = _derive_delivery_execution_amount(analysis)
    dynamic_scale = _derive_delivery_dynamic_scale(analysis)

    allowed = ["output_gain_trim", "true_peak_limiter"]
    forbidden = sorted(name for name in ALL_PRIMITIVE_NAMES if name not in set(allowed))

    notes = [
        "delivery is terminal protection, not creative polish",
        "headroom first, limiter second",
        "no tone shaping inside delivery",
        "no width moves inside delivery",
        "no extra sparkle inside delivery",
        "delivery should preserve forward delta, not auto-dim hot material",
        f"analysis_true_peak_dbtp={round(true_peak_dbtp, 4)}",
        f"analysis_integrated_lufs={round(integrated_lufs, 4)}",
        f"analysis_limiter_stress_proxy={round(limiter_stress_proxy, 4)}",
        f"analysis_near_clip_ratio={round(near_clip_ratio, 6)}",
    ]

    if punch_proxy < 10.8 or crest_db < 10.2:
        notes.append("punch_fragile_delivery_moderation")

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
        active_clamps=["delivery_budget_global_clamp"],
        blocked_actions=[],
        notes=_uniq(notes),
    )


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
                "delivery_headroom_first_policy",
                "delivery_no_creative_tone_shaping",
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
    notes: List[str] = ["post_clamp_musical_floor_pass_enabled"]

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
            stack for stack in [
                blueprint.projection_contour_stack,
                blueprint.projection_assist_stack,
            ]
            if stack is not None
        ],
        analysis=analysis,
    )

    projection_idx = _index_role_stacks(projection_stacks)
    projection_contour_stack = projection_idx.get("projection_contour_stack", blueprint.projection_contour_stack)
    projection_assist_stack = projection_idx.get("projection_assist_stack", blueprint.projection_assist_stack)

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
                "sm_music_first_architecture_v2",
                "post_clamp_projection_support_spark_floors_locked",
            ]
        ),
    )

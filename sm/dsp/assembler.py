# sm/dsp/assembler.py

from __future__ import annotations

from dataclasses import replace
from typing import Dict, List, Optional, Tuple

from ..contracts import SmartMasterAnalysis, SmartMasterExecutionBlueprint
from ..enums import RoleName
from .clamps import apply_dsp_clamps
from .contracts import DSPExecutionBlueprint, RoleDSPStack
from .graph import attach_graph_to_blueprint
from .role_specs import RoleModeSpec, RoleStackTemplate, get_role_mode_spec


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


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


def _default_split_for_mode(role: RoleName, target_band_mode: str) -> Dict[str, Tuple[float, float, float]]:
    """
    Returns per-stack scaling:
    stack_name -> (amount_scale, cap_scale, dynamic_scale_multiplier)

    This is where we keep premium V1 role execution discipline:
    - single-stack roles generally keep full router execution values
    - projection splits contour vs assist deliberately
    - spark remains micro by nature
    """
    if role == RoleName.PROJECTION:
        if target_band_mode == "projection_dense":
            return {
                "projection_contour_stack": (0.72, 0.78, 1.00),
                "projection_assist_stack": (0.28, 0.34, 0.72),
            }
        if target_band_mode == "projection_mild":
            return {
                "projection_contour_stack": (0.84, 0.90, 1.00),
                "projection_assist_stack": (0.16, 0.22, 0.58),
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


ALL_PRIMITIVE_NAMES: List[str] = []


def _set_all_primitive_names_from_mode_spec() -> None:
    global ALL_PRIMITIVE_NAMES
    if ALL_PRIMITIVE_NAMES:
        return

    from .primitives import list_primitive_names
    ALL_PRIMITIVE_NAMES = list_primitive_names()


def _expand_role_plan_to_stacks(
    plan,
) -> List[RoleDSPStack]:
    _set_all_primitive_names_from_mode_spec()

    mode_spec = get_role_mode_spec(plan.role, _effective_mode(plan))
    if not plan.enabled or mode_spec.target_band_mode == "off" or not mode_spec.stack_templates:
        return []

    stacks: List[RoleDSPStack] = []
    for template in sorted(mode_spec.stack_templates, key=lambda x: x.preferred_order):
        stacks.append(_build_role_stack(plan, mode_spec, template))
    return stacks


def _index_role_stacks(stacks: List[RoleDSPStack]) -> Dict[str, RoleDSPStack]:
    return {stack.stack_name: stack for stack in stacks}


def _derive_support_recombine_gain_db(
    anchor_stack: Optional[RoleDSPStack],
    bridge_stack: Optional[RoleDSPStack],
) -> float:
    total = 0.0
    if anchor_stack is not None and anchor_stack.enabled:
        total += anchor_stack.execution_amount
    if bridge_stack is not None and bridge_stack.enabled:
        total += bridge_stack.execution_amount

    # Conservative premium support recombine.
    # More support allowed -> less attenuation, but never becomes hype.
    return _clamp(-0.90 + (total * 1.35), -1.00, -0.10)


def _derive_projection_assist_blend(
    projection_assist_stack: Optional[RoleDSPStack],
) -> float:
    if projection_assist_stack is None or not projection_assist_stack.enabled:
        return 0.0

    return _clamp(
        (projection_assist_stack.execution_amount * 0.55)
        + (projection_assist_stack.dynamic_scale * 0.04),
        0.02,
        0.18,
    )


def _derive_spark_blend(
    spark_stack: Optional[RoleDSPStack],
) -> float:
    if spark_stack is None or not spark_stack.enabled:
        return 0.0

    return _clamp(
        (spark_stack.execution_amount * 0.42)
        + (spark_stack.dynamic_scale * 0.02),
        0.01,
        0.08,
    )


def build_dsp_execution_blueprint(
    router_blueprint: SmartMasterExecutionBlueprint,
) -> DSPExecutionBlueprint:
    anchor_stacks = _expand_role_plan_to_stacks(router_blueprint.anchor)
    bridge_stacks = _expand_role_plan_to_stacks(router_blueprint.bridge)
    cleanup_stacks = _expand_role_plan_to_stacks(router_blueprint.cleanup)
    guard_stacks = _expand_role_plan_to_stacks(router_blueprint.guard)
    projection_stacks = _expand_role_plan_to_stacks(router_blueprint.projection)
    spark_stacks = _expand_role_plan_to_stacks(router_blueprint.spark)

    anchor_idx = _index_role_stacks(anchor_stacks)
    bridge_idx = _index_role_stacks(bridge_stacks)
    cleanup_idx = _index_role_stacks(cleanup_stacks)
    guard_idx = _index_role_stacks(guard_stacks)
    projection_idx = _index_role_stacks(projection_stacks)
    spark_idx = _index_role_stacks(spark_stacks)

    cleanup_stack = cleanup_idx.get("cleanup_core_stack")
    guard_stack = guard_idx.get("guard_core_stack")
    anchor_parallel_stack = anchor_idx.get("anchor_parallel_stack")
    bridge_parallel_stack = bridge_idx.get("bridge_parallel_stack")
    projection_contour_stack = projection_idx.get("projection_contour_stack")
    projection_assist_stack = projection_idx.get("projection_assist_stack")
    spark_stack = spark_idx.get("spark_finish_stack")

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
        delivery_stack=None,
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
        notes=_uniq(
            list(router_blueprint.global_notes or [])
            + [
                "assembler_initialized",
                "role_specs_expanded",
                "delivery_stack_pending_true_peak_stage",
            ]
        ),
    )
    return blueprint


def assemble_sm_dsp_blueprint(
    analysis: SmartMasterAnalysis,
    router_blueprint: SmartMasterExecutionBlueprint,
) -> DSPExecutionBlueprint:
    """
    Premium V1 assembly order:
    1. Expand router execution plans into DSP role stacks
    2. Apply inter-role DSP clamps
    3. Attach fixed topology graph
    """
    blueprint = build_dsp_execution_blueprint(router_blueprint)
    blueprint = apply_dsp_clamps(blueprint, analysis)
    blueprint = attach_graph_to_blueprint(blueprint)

    return replace(
        blueprint,
        notes=_uniq(
            list(blueprint.notes or [])
            + [
                "assembler_completed",
                "premium_v1_topology_attached",
            ]
        ),
    )

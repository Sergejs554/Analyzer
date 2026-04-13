# sm/dsp/graph.py

from __future__ import annotations

from dataclasses import replace
from typing import List, Optional

from ..enums import RoleName
from .contracts import (
    DSPExecutionBlueprint,
    DSPRecombinePlan,
    DSPStagePlan,
    RoleDSPStack,
)
from .role_specs import (
    NODE_CLEANUP_OUT,
    NODE_FINISH_OUT,
    NODE_GUARD_OUT,
    NODE_PREPARED_INPUT,
    NODE_PROJECTION_OUT,
    NODE_SUPPORT_BUS,
    NODE_SUPPORT_OUT,
)


STAGE_ORDER = [
    "cleanup_core",
    "guard_core",
    "support_assembly",
    "projection_assembly",
    "finish_assembly",
    "delivery_protect",
]


def _enabled(stack: Optional[RoleDSPStack]) -> bool:
    return stack is not None and stack.enabled


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


def _collect_safety_tags(*stacks: Optional[RoleDSPStack]) -> List[str]:
    tags: List[str] = []
    for stack in stacks:
        if stack is None:
            continue
        tags.extend(stack.safety_tags or [])
    return _uniq(tags)


def _collect_active_clamps(*stacks: Optional[RoleDSPStack]) -> List[str]:
    clamps: List[str] = []
    for stack in stacks:
        if stack is None:
            continue
        clamps.extend(stack.active_clamps or [])
    return _uniq(clamps)


def _collect_role_stacks(*stacks: Optional[RoleDSPStack]) -> List[RoleDSPStack]:
    out: List[RoleDSPStack] = []
    for stack in stacks:
        if stack is not None and stack.enabled:
            out.append(stack)
    return out


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _safe_blend(x: float) -> float:
    return _clamp(x, 0.0, 1.0)


def _passthrough_recombine(
    recombine_name: str,
    source_node: str,
    target_node: str,
    *,
    notes: Optional[List[str]] = None,
) -> DSPRecombinePlan:
    return DSPRecombinePlan(
        recombine_name=recombine_name,
        recombine_mode="sum",
        source_nodes=[source_node],
        target_node=target_node,
        gain_db=0.0,
        blend=1.0,
        notes=notes or ["passthrough"],
    )


def _single_source_recombine(
    recombine_name: str,
    source_node: str,
    target_node: str,
    recombine_mode: str,
    *,
    gain_db: float = 0.0,
    blend: float = 1.0,
    safety_tags: Optional[List[str]] = None,
    active_clamps: Optional[List[str]] = None,
    notes: Optional[List[str]] = None,
) -> DSPRecombinePlan:
    return DSPRecombinePlan(
        recombine_name=recombine_name,
        recombine_mode=recombine_mode,
        source_nodes=[source_node],
        target_node=target_node,
        gain_db=gain_db,
        blend=_safe_blend(blend),
        safety_tags=safety_tags or [],
        active_clamps=active_clamps or [],
        notes=notes or [],
    )


def _multi_source_recombine(
    recombine_name: str,
    source_nodes: List[str],
    target_node: str,
    recombine_mode: str,
    *,
    gain_db: float = 0.0,
    blend: float = 1.0,
    safety_tags: Optional[List[str]] = None,
    active_clamps: Optional[List[str]] = None,
    notes: Optional[List[str]] = None,
) -> DSPRecombinePlan:
    return DSPRecombinePlan(
        recombine_name=recombine_name,
        recombine_mode=recombine_mode,
        source_nodes=source_nodes,
        target_node=target_node,
        gain_db=gain_db,
        blend=_safe_blend(blend),
        safety_tags=safety_tags or [],
        active_clamps=active_clamps or [],
        notes=notes or [],
    )


def _build_cleanup_stage(
    cleanup_stack: Optional[RoleDSPStack],
) -> DSPStagePlan:
    stacks = _collect_role_stacks(cleanup_stack)
    safety_tags = _collect_safety_tags(cleanup_stack)
    active_clamps = _collect_active_clamps(cleanup_stack)

    recombines: List[DSPRecombinePlan] = []
    notes: List[str] = []

    if not stacks:
        recombines.append(
            _passthrough_recombine(
                "cleanup_passthrough",
                NODE_PREPARED_INPUT,
                NODE_CLEANUP_OUT,
                notes=["cleanup disabled -> prepared_input passes through"],
            )
        )
        notes.append("cleanup stage passthrough")
    else:
        stack = stacks[0]
        if stack.output_node != NODE_CLEANUP_OUT:
            recombines.append(
                _single_source_recombine(
                    "cleanup_finalize",
                    stack.output_node,
                    NODE_CLEANUP_OUT,
                    "sum",
                    safety_tags=safety_tags,
                    active_clamps=active_clamps,
                    notes=["cleanup output normalized to cleanup_core_out"],
                )
            )
        notes.append("cleanup runs in-place on prepared input")

    return DSPStagePlan(
        stage_name="cleanup_core",
        stage_kind="corrective",
        input_node=NODE_PREPARED_INPUT,
        output_node=NODE_CLEANUP_OUT,
        role_order=[RoleName.CLEANUP],
        role_stacks=stacks,
        recombine_plans=recombines,
        active_clamps=active_clamps,
        safety_tags=safety_tags,
        notes=notes,
    )


def _build_guard_stage(
    guard_stack: Optional[RoleDSPStack],
) -> DSPStagePlan:
    stacks = _collect_role_stacks(guard_stack)
    safety_tags = _collect_safety_tags(guard_stack)
    active_clamps = _collect_active_clamps(guard_stack)

    recombines: List[DSPRecombinePlan] = []
    notes: List[str] = []

    if not stacks:
        recombines.append(
            _passthrough_recombine(
                "guard_passthrough",
                NODE_CLEANUP_OUT,
                NODE_GUARD_OUT,
                notes=["guard disabled -> cleanup output passes through"],
            )
        )
        notes.append("guard stage passthrough")
    else:
        stack = stacks[0]
        if stack.output_node != NODE_GUARD_OUT:
            recombines.append(
                _single_source_recombine(
                    "guard_finalize",
                    stack.output_node,
                    NODE_GUARD_OUT,
                    "sum",
                    safety_tags=safety_tags,
                    active_clamps=active_clamps,
                    notes=["guard output normalized to guard_core_out"],
                )
            )
        notes.append("guard runs in-place after cleanup")

    return DSPStagePlan(
        stage_name="guard_core",
        stage_kind="corrective",
        input_node=NODE_CLEANUP_OUT,
        output_node=NODE_GUARD_OUT,
        role_order=[RoleName.GUARD],
        role_stacks=stacks,
        recombine_plans=recombines,
        active_clamps=active_clamps,
        safety_tags=safety_tags,
        notes=notes,
    )


def _build_support_stage(
    anchor_stack: Optional[RoleDSPStack],
    bridge_stack: Optional[RoleDSPStack],
    support_recombine_gain_db: float,
) -> DSPStagePlan:
    stacks = _collect_role_stacks(anchor_stack, bridge_stack)
    safety_tags = _collect_safety_tags(anchor_stack, bridge_stack)
    active_clamps = _collect_active_clamps(anchor_stack, bridge_stack)

    recombines: List[DSPRecombinePlan] = []
    notes: List[str] = []

    support_sources: List[str] = []
    if _enabled(anchor_stack):
        support_sources.append(anchor_stack.output_node)
    if _enabled(bridge_stack):
        support_sources.append(bridge_stack.output_node)

    if not stacks:
        recombines.append(
            _passthrough_recombine(
                "support_passthrough",
                NODE_GUARD_OUT,
                NODE_SUPPORT_OUT,
                notes=["anchor/bridge disabled -> guard output passes through"],
            )
        )
        notes.append("support stage passthrough")
    else:
        recombines.append(
            _multi_source_recombine(
                "support_bus_sum",
                support_sources,
                NODE_SUPPORT_BUS,
                "guarded_sum",
                gain_db=0.0,
                blend=1.0,
                safety_tags=safety_tags,
                active_clamps=active_clamps,
                notes=[
                    "support layers summed into dedicated support bus",
                    "support bus collects micro-hold layers before guarded reinjection",
                ],
            )
        )
        recombines.append(
            _multi_source_recombine(
                "support_to_main",
                [NODE_GUARD_OUT, NODE_SUPPORT_BUS],
                NODE_SUPPORT_OUT,
                "guarded_sum",
                gain_db=support_recombine_gain_db,
                blend=1.0,
                safety_tags=safety_tags,
                active_clamps=active_clamps,
                notes=[
                    "support bus recombined under guard-aware constraints",
                    f"support_recombine_gain_db={support_recombine_gain_db}",
                ],
            )
        )
        notes.append("anchor and bridge run as disciplined parallel support")
        notes.append(f"support recombine gain applied: {support_recombine_gain_db} dB")

    return DSPStagePlan(
        stage_name="support_assembly",
        stage_kind="support",
        input_node=NODE_GUARD_OUT,
        output_node=NODE_SUPPORT_OUT,
        role_order=[RoleName.ANCHOR, RoleName.BRIDGE],
        role_stacks=stacks,
        recombine_plans=recombines,
        active_clamps=active_clamps,
        safety_tags=safety_tags,
        notes=notes,
    )


def _build_projection_stage(
    projection_contour_stack: Optional[RoleDSPStack],
    projection_assist_stack: Optional[RoleDSPStack],
    projection_assist_blend: float,
) -> DSPStagePlan:
    stacks = _collect_role_stacks(projection_contour_stack, projection_assist_stack)
    safety_tags = _collect_safety_tags(projection_contour_stack, projection_assist_stack)
    active_clamps = _collect_active_clamps(projection_contour_stack, projection_assist_stack)

    recombines: List[DSPRecombinePlan] = []
    notes: List[str] = []

    contour_on = _enabled(projection_contour_stack)
    assist_on = _enabled(projection_assist_stack)

    if not stacks:
        recombines.append(
            _passthrough_recombine(
                "projection_passthrough",
                NODE_SUPPORT_OUT,
                NODE_PROJECTION_OUT,
                notes=["projection disabled -> support output passes through"],
            )
        )
        notes.append("projection stage passthrough")
    elif contour_on and not assist_on:
        source = projection_contour_stack.output_node
        if source != NODE_PROJECTION_OUT:
            recombines.append(
                _single_source_recombine(
                    "projection_contour_finalize",
                    source,
                    NODE_PROJECTION_OUT,
                    "sum",
                    safety_tags=safety_tags,
                    active_clamps=active_clamps,
                    notes=["projection contour becomes projection stage output"],
                )
            )
        notes.append("projection contour only")
    elif contour_on and assist_on:
        recombines.append(
            _multi_source_recombine(
                "projection_assist_sum",
                [projection_contour_stack.output_node, projection_assist_stack.output_node],
                NODE_PROJECTION_OUT,
                "assist_sum",
                gain_db=0.0,
                blend=projection_assist_blend,
                safety_tags=safety_tags,
                active_clamps=active_clamps,
                notes=[
                    "projection contour and assist recombined under protected blend",
                    f"projection_assist_blend={projection_assist_blend}",
                ],
            )
        )
        notes.append("projection runs as contour + assist split architecture")
        notes.append(f"projection assist blend applied: {projection_assist_blend}")
    else:
        recombines.append(
            _multi_source_recombine(
                "projection_assist_fallback_sum",
                [NODE_SUPPORT_OUT, projection_assist_stack.output_node],
                NODE_PROJECTION_OUT,
                "assist_sum",
                gain_db=0.0,
                blend=projection_assist_blend,
                safety_tags=safety_tags,
                active_clamps=active_clamps,
                notes=[
                    "assist exists without contour -> fallback uses support output as main carrier",
                    f"projection_assist_blend={projection_assist_blend}",
                ],
            )
        )
        notes.append("projection assist fallback mode")

    return DSPStagePlan(
        stage_name="projection_assembly",
        stage_kind="projection",
        input_node=NODE_SUPPORT_OUT,
        output_node=NODE_PROJECTION_OUT,
        role_order=[RoleName.PROJECTION],
        role_stacks=stacks,
        recombine_plans=recombines,
        active_clamps=active_clamps,
        safety_tags=safety_tags,
        notes=notes,
    )


def _build_finish_stage(
    spark_stack: Optional[RoleDSPStack],
    spark_blend: float,
) -> DSPStagePlan:
    stacks = _collect_role_stacks(spark_stack)
    safety_tags = _collect_safety_tags(spark_stack)
    active_clamps = _collect_active_clamps(spark_stack)

    recombines: List[DSPRecombinePlan] = []
    notes: List[str] = []

    if not stacks:
        recombines.append(
            _passthrough_recombine(
                "finish_passthrough",
                NODE_PROJECTION_OUT,
                NODE_FINISH_OUT,
                notes=["spark disabled -> projection output passes through"],
            )
        )
        notes.append("finish stage passthrough")
    else:
        recombines.append(
            _multi_source_recombine(
                "spark_finish_sum",
                [NODE_PROJECTION_OUT, spark_stack.output_node],
                NODE_FINISH_OUT,
                "finish_sum",
                gain_db=0.0,
                blend=spark_blend,
                safety_tags=safety_tags,
                active_clamps=active_clamps,
                notes=[
                    "spark blends last as protected finish micro-layer",
                    f"spark_blend={spark_blend}",
                ],
            )
        )
        notes.append("spark runs only after projection is established")
        notes.append(f"spark blend applied: {spark_blend}")

    return DSPStagePlan(
        stage_name="finish_assembly",
        stage_kind="finish",
        input_node=NODE_PROJECTION_OUT,
        output_node=NODE_FINISH_OUT,
        role_order=[RoleName.SPARK],
        role_stacks=stacks,
        recombine_plans=recombines,
        active_clamps=active_clamps,
        safety_tags=safety_tags,
        notes=notes,
    )


def _build_delivery_stage(
    delivery_stack: Optional[RoleDSPStack],
    final_output_node: str,
) -> DSPStagePlan:
    stacks = _collect_role_stacks(delivery_stack)
    safety_tags = _collect_safety_tags(delivery_stack)
    active_clamps = _collect_active_clamps(delivery_stack)

    recombines: List[DSPRecombinePlan] = []
    notes: List[str] = []

    if not stacks:
        recombines.append(
            _passthrough_recombine(
                "delivery_passthrough",
                NODE_FINISH_OUT,
                final_output_node,
                notes=["delivery stack absent -> finish output becomes final output"],
            )
        )
        notes.append("delivery stage passthrough")
    else:
        stack = stacks[0]
        if stack.output_node != final_output_node:
            recombines.append(
                _single_source_recombine(
                    "delivery_finalize",
                    stack.output_node,
                    final_output_node,
                    "sum",
                    safety_tags=safety_tags,
                    active_clamps=active_clamps,
                    notes=["delivery output normalized to final output node"],
                )
            )
        notes.append("delivery protect stage finalizes output safety")

    return DSPStagePlan(
        stage_name="delivery_protect",
        stage_kind="delivery",
        input_node=NODE_FINISH_OUT,
        output_node=final_output_node,
        role_order=[],
        role_stacks=stacks,
        recombine_plans=recombines,
        active_clamps=active_clamps,
        safety_tags=safety_tags,
        notes=notes,
    )


def build_stage_plans(
    *,
    cleanup_stack: Optional[RoleDSPStack],
    guard_stack: Optional[RoleDSPStack],
    anchor_parallel_stack: Optional[RoleDSPStack],
    bridge_parallel_stack: Optional[RoleDSPStack],
    projection_contour_stack: Optional[RoleDSPStack],
    projection_assist_stack: Optional[RoleDSPStack],
    spark_stack: Optional[RoleDSPStack],
    delivery_stack: Optional[RoleDSPStack],
    support_recombine_gain_db: float = 0.0,
    projection_assist_blend: float = 1.0,
    spark_blend: float = 1.0,
    final_output_node: str = "final_output",
) -> List[DSPStagePlan]:
    stages = [
        _build_cleanup_stage(cleanup_stack),
        _build_guard_stage(guard_stack),
        _build_support_stage(
            anchor_parallel_stack,
            bridge_parallel_stack,
            support_recombine_gain_db,
        ),
        _build_projection_stage(
            projection_contour_stack,
            projection_assist_stack,
            projection_assist_blend,
        ),
        _build_finish_stage(
            spark_stack,
            spark_blend,
        ),
        _build_delivery_stage(delivery_stack, final_output_node),
    ]
    return stages


def flatten_recombine_plans(stage_plans: List[DSPStagePlan]) -> List[DSPRecombinePlan]:
    recombines: List[DSPRecombinePlan] = []
    for stage in stage_plans:
        recombines.extend(stage.recombine_plans or [])
    return recombines


def validate_graph_topology(blueprint: DSPExecutionBlueprint) -> None:
    if blueprint.prepared_input_node != NODE_PREPARED_INPUT:
        raise ValueError(
            f"Unexpected prepared_input_node: {blueprint.prepared_input_node} != {NODE_PREPARED_INPUT}"
        )

    if blueprint.cleanup_stack is not None and blueprint.cleanup_stack.enabled:
        if blueprint.cleanup_stack.tap_point != NODE_PREPARED_INPUT:
            raise ValueError("Cleanup stack must tap from prepared_input")

    if blueprint.guard_stack is not None and blueprint.guard_stack.enabled:
        if blueprint.guard_stack.tap_point != NODE_CLEANUP_OUT:
            raise ValueError("Guard stack must tap from cleanup_core_out")

    if blueprint.anchor_parallel_stack is not None and blueprint.anchor_parallel_stack.enabled:
        if blueprint.anchor_parallel_stack.tap_point != NODE_GUARD_OUT:
            raise ValueError("Anchor stack must tap from guard_core_out")

    if blueprint.bridge_parallel_stack is not None and blueprint.bridge_parallel_stack.enabled:
        if blueprint.bridge_parallel_stack.tap_point != NODE_GUARD_OUT:
            raise ValueError("Bridge stack must tap from guard_core_out")

    if blueprint.projection_contour_stack is not None and blueprint.projection_contour_stack.enabled:
        if blueprint.projection_contour_stack.tap_point != NODE_SUPPORT_OUT:
            raise ValueError("Projection contour stack must tap from support_stage_out")

    if blueprint.projection_assist_stack is not None and blueprint.projection_assist_stack.enabled:
        if blueprint.projection_assist_stack.tap_point != "projection_contour_out":
            raise ValueError("Projection assist stack must tap from projection_contour_out")

    if blueprint.spark_stack is not None and blueprint.spark_stack.enabled:
        if blueprint.spark_stack.tap_point != NODE_PROJECTION_OUT:
            raise ValueError("Spark stack must tap from projection_stage_out")

    if blueprint.projection_assist_stack is not None and blueprint.projection_assist_stack.enabled:
        if blueprint.projection_contour_stack is None or not blueprint.projection_contour_stack.enabled:
            raise ValueError("Projection assist cannot be enabled without projection contour in premium topology")

    if blueprint.spark_stack is not None and blueprint.spark_stack.enabled:
        if blueprint.projection_contour_stack is None or not blueprint.projection_contour_stack.enabled:
            raise ValueError("Spark cannot be enabled without projection contour in premium topology")


def attach_graph_to_blueprint(
    blueprint: DSPExecutionBlueprint,
) -> DSPExecutionBlueprint:
    stage_plans = build_stage_plans(
        cleanup_stack=blueprint.cleanup_stack,
        guard_stack=blueprint.guard_stack,
        anchor_parallel_stack=blueprint.anchor_parallel_stack,
        bridge_parallel_stack=blueprint.bridge_parallel_stack,
        projection_contour_stack=blueprint.projection_contour_stack,
        projection_assist_stack=blueprint.projection_assist_stack,
        spark_stack=blueprint.spark_stack,
        delivery_stack=blueprint.delivery_stack,
        support_recombine_gain_db=blueprint.support_recombine_gain_db,
        projection_assist_blend=blueprint.projection_assist_blend,
        spark_blend=blueprint.spark_blend,
        final_output_node=blueprint.final_output_node,
    )

    patched = replace(
        blueprint,
        stage_plans=stage_plans,
        recombine_plans=flatten_recombine_plans(stage_plans),
        notes=_uniq(
            list(blueprint.notes or [])
            + [
                "graph_attached",
                "fixed_stage_order_v1",
                "support_runs_after_guard",
                "projection_runs_after_support",
                "spark_runs_after_projection",
                "delivery_runs_last",
                "recombine_gain_blend_attached",
            ]
        ),
    )

    validate_graph_topology(patched)
    return patched

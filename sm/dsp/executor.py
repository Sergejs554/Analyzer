# sm/dsp/executor.py

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Iterable, List, Optional, Set


DEFAULT_AVAILABLE_BACKENDS: Set[str] = {"ffmpeg_safe"}


def _to_plain(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _to_plain(asdict(value))

    if isinstance(value, dict):
        return {str(k): _to_plain(v) for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        return [_to_plain(v) for v in value]

    return value


def _as_dict(value: Any) -> Dict[str, Any]:
    value = _to_plain(value)
    if isinstance(value, dict):
        return value
    return {}


def _as_list(value: Any) -> List[Any]:
    value = _to_plain(value)
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _as_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        x = value.strip().lower()
        if x in {"1", "true", "yes", "on"}:
            return True
        if x in {"0", "false", "no", "off"}:
            return False
    return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _copy_list_of_dicts(value: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in _as_list(value):
        out.append(_as_dict(item))
    return out


def _collect_enabled_ops(stacks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ops: List[Dict[str, Any]] = []
    for stack in stacks:
        if not _as_bool(stack.get("enabled"), True):
            continue
        for op in _copy_list_of_dicts(stack.get("ops")):
            if _as_bool(op.get("enabled"), True):
                ops.append(op)
    return ops


def _build_op_execution(
    op: Dict[str, Any],
    available_backends: Set[str],
) -> Dict[str, Any]:
    enabled = _as_bool(op.get("enabled"), True)
    backend_hint = _as_str(op.get("backend_hint"), "custom_dsp_required")
    backend_ready = enabled and backend_hint in available_backends

    if not enabled:
        status = "disabled"
    elif backend_ready:
        status = "ready_now"
    else:
        status = "pending_backend"

    return {
        "instance_name": _as_str(op.get("instance_name")),
        "primitive_name": _as_str(op.get("primitive_name")),
        "primitive_class": _as_str(op.get("primitive_class")),
        "op_kind": _as_str(op.get("op_kind")),
        "role": _as_str(op.get("role")),
        "stack_name": _as_str(op.get("stack_name")),
        "target_band_mode": _as_str(op.get("target_band_mode")),
        "enabled": enabled,
        "status": status,
        "backend_hint": backend_hint,
        "backend_ready": backend_ready,
        "path_type": _as_str(op.get("path_type")),
        "phase_policy": _as_str(op.get("phase_policy")),
        "band_scope": _as_str(op.get("band_scope")),
        "channel_mode": _as_str(op.get("channel_mode")),
        "detector_mode": _as_str(op.get("detector_mode")),
        "activity": _as_float(op.get("activity"), 0.0),
        "amount_norm": _as_float(op.get("amount_norm"), 0.0),
        "params": _as_dict(op.get("params")),
        "notes": _as_list(op.get("notes")),
        "safety_tags": _as_list(op.get("safety_tags")),
    }


def _build_stack_execution(
    stack: Dict[str, Any],
    available_backends: Set[str],
    stage_input_ready: bool,
) -> Dict[str, Any]:
    enabled = _as_bool(stack.get("enabled"), True)
    ops = [_build_op_execution(op, available_backends) for op in _copy_list_of_dicts(stack.get("ops"))]

    ready_now_count = sum(1 for op in ops if op["status"] == "ready_now")
    pending_backend_count = sum(1 for op in ops if op["status"] == "pending_backend")
    disabled_count = sum(1 for op in ops if op["status"] == "disabled")

    active_ops = sum(1 for op in ops if op["enabled"])

    requires_custom_dsp = pending_backend_count > 0
    fully_ready_now = (
        enabled
        and stage_input_ready
        and active_ops > 0
        and pending_backend_count == 0
    )

    if not enabled:
        execution_state = "disabled"
    elif not stage_input_ready:
        execution_state = "blocked_no_stage_input"
    elif active_ops == 0:
        execution_state = "empty_stack"
    elif pending_backend_count == 0:
        execution_state = "ready_now"
    else:
        execution_state = "pending_custom_backend"

    output_node_materialized = fully_ready_now

    if active_ops == 0 and enabled and stage_input_ready:
        output_node_materialized = True

    return {
        "stack_name": _as_str(stack.get("stack_name")),
        "role": _as_str(stack.get("role")),
        "role_rank": _as_str(stack.get("role_rank")),
        "stack_kind": _as_str(stack.get("stack_kind")),
        "render_mode": _as_str(stack.get("render_mode")),
        "path_type": _as_str(stack.get("path_type")),
        "tap_point": _as_str(stack.get("tap_point")),
        "output_node": _as_str(stack.get("output_node")),
        "recombine_target": _as_str(stack.get("recombine_target")),
        "target_band_mode": _as_str(stack.get("target_band_mode")),
        "protection_mode": _as_str(stack.get("protection_mode")),
        "enabled": enabled,
        "stage_input_ready": stage_input_ready,
        "execution_state": execution_state,
        "requires_custom_dsp": requires_custom_dsp,
        "fully_ready_now": fully_ready_now,
        "output_node_materialized": output_node_materialized,
        "requested_amount": _as_float(stack.get("requested_amount"), 0.0),
        "requested_cap": _as_float(stack.get("requested_cap"), 0.0),
        "execution_amount": _as_float(stack.get("execution_amount"), 0.0),
        "execution_cap": _as_float(stack.get("execution_cap"), 0.0),
        "dynamic_scale": _as_float(stack.get("dynamic_scale"), 0.0),
        "active_op_count": active_ops,
        "ready_now_op_count": ready_now_count,
        "pending_backend_op_count": pending_backend_count,
        "disabled_op_count": disabled_count,
        "notes": _as_list(stack.get("notes")),
        "active_clamps": _as_list(stack.get("active_clamps")),
        "ops": ops,
    }


def _build_recombine_execution(
    recombine: Dict[str, Any],
    materialized_nodes: Set[str],
) -> Dict[str, Any]:
    source_nodes = [_as_str(x) for x in _as_list(recombine.get("source_nodes")) if _as_str(x)]
    target_node = _as_str(recombine.get("target_node"))
    missing_sources = [src for src in source_nodes if src not in materialized_nodes]
    source_ready = len(missing_sources) == 0

    if source_ready:
        status = "ready_now"
    else:
        status = "blocked_missing_sources"

    return {
        "recombine_name": _as_str(recombine.get("recombine_name")),
        "recombine_mode": _as_str(recombine.get("recombine_mode")),
        "render_recombine_kind": _as_str(recombine.get("render_recombine_kind")),
        "source_nodes": source_nodes,
        "target_node": target_node,
        "blend": _as_float(recombine.get("blend"), 1.0),
        "gain_db": _as_float(recombine.get("gain_db"), 0.0),
        "status": status,
        "source_ready": source_ready,
        "missing_sources": missing_sources,
        "notes": _as_list(recombine.get("notes")),
        "active_clamps": _as_list(recombine.get("active_clamps")),
        "safety_tags": _as_list(recombine.get("safety_tags")),
    }


def _materialize_stack_outputs(
    stack_records: List[Dict[str, Any]],
    materialized_nodes: Set[str],
) -> None:
    for stack in stack_records:
        output_node = _as_str(stack.get("output_node"))
        if output_node and _as_bool(stack.get("output_node_materialized"), False):
            materialized_nodes.add(output_node)


def _materialize_recombine_outputs(
    recombine_records: List[Dict[str, Any]],
    materialized_nodes: Set[str],
) -> None:
    for item in recombine_records:
        target_node = _as_str(item.get("target_node"))
        if target_node and _as_bool(item.get("source_ready"), False):
            materialized_nodes.add(target_node)


def build_render_execution_report(
    render_plan: Any,
    available_backends: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    plan = _as_dict(render_plan)
    if not plan:
        raise TypeError("render_plan must be a dict-like object or dataclass")

    available_backend_set: Set[str] = set(available_backends or DEFAULT_AVAILABLE_BACKENDS)

    prepared_input_node = _as_str(plan.get("prepared_input_node"), "prepared_input")
    final_output_node = _as_str(plan.get("final_output_node"), "final_output")
    stages = _copy_list_of_dicts(plan.get("stages"))

    materialized_nodes: Set[str] = {prepared_input_node}
    stage_reports: List[Dict[str, Any]] = []

    total_stack_count = 0
    total_op_count = 0
    total_ready_now_ops = 0
    total_pending_backend_ops = 0
    total_recombine_count = 0
    total_ready_recombine = 0

    for stage in stages:
        input_node = _as_str(stage.get("input_node"))
        output_node = _as_str(stage.get("output_node"))
        stage_input_ready = input_node in materialized_nodes

        stack_records: List[Dict[str, Any]] = []
        for stack in _copy_list_of_dicts(stage.get("stacks")):
            stack_record = _build_stack_execution(
                stack=stack,
                available_backends=available_backend_set,
                stage_input_ready=stage_input_ready,
            )
            stack_records.append(stack_record)

        _materialize_stack_outputs(stack_records, materialized_nodes)

        recombine_records: List[Dict[str, Any]] = []
        for recombine in _copy_list_of_dicts(stage.get("recombine")):
            recombine_record = _build_recombine_execution(
                recombine=recombine,
                materialized_nodes=materialized_nodes,
            )
            recombine_records.append(recombine_record)

        _materialize_recombine_outputs(recombine_records, materialized_nodes)

        if output_node and not recombine_records:
            if stage_input_ready and not stack_records:
                materialized_nodes.add(output_node)

            if len(stack_records) == 1 and _as_bool(stack_records[0].get("output_node_materialized"), False):
                materialized_nodes.add(output_node)

        stage_output_ready = output_node in materialized_nodes

        enabled_stack_count = sum(1 for x in stack_records if _as_bool(x.get("enabled"), True))
        stage_op_count = sum(int(x.get("active_op_count", 0)) for x in stack_records)
        stage_ready_now_op_count = sum(int(x.get("ready_now_op_count", 0)) for x in stack_records)
        stage_pending_backend_op_count = sum(int(x.get("pending_backend_op_count", 0)) for x in stack_records)

        stage_requires_custom_dsp = stage_pending_backend_op_count > 0
        stage_ready_recombine_count = sum(1 for x in recombine_records if _as_bool(x.get("source_ready"), False))

        if stage_requires_custom_dsp:
            stage_execution_state = "pending_custom_backend"
        elif stage_input_ready and stage_output_ready:
            stage_execution_state = "ready_now"
        elif not stage_input_ready:
            stage_execution_state = "blocked_no_input"
        else:
            stage_execution_state = "graph_incomplete"

        stage_report = {
            "stage_name": _as_str(stage.get("stage_name")),
            "stage_kind": _as_str(stage.get("stage_kind")),
            "input_node": input_node,
            "output_node": output_node,
            "role_order": _as_list(stage.get("role_order")),
            "notes": _as_list(stage.get("notes")),
            "safety_tags": _as_list(stage.get("safety_tags")),
            "active_clamps": _as_list(stage.get("active_clamps")),
            "requires_custom_dsp": stage_requires_custom_dsp,
            "stage_input_ready": stage_input_ready,
            "stage_output_ready": stage_output_ready,
            "stage_execution_state": stage_execution_state,
            "enabled_stack_count": enabled_stack_count,
            "active_op_count": stage_op_count,
            "ready_now_op_count": stage_ready_now_op_count,
            "pending_backend_op_count": stage_pending_backend_op_count,
            "recombine_count": len(recombine_records),
            "ready_recombine_count": stage_ready_recombine_count,
            "stacks": stack_records,
            "recombine": recombine_records,
        }
        stage_reports.append(stage_report)

        total_stack_count += len(stack_records)
        total_op_count += stage_op_count
        total_ready_now_ops += stage_ready_now_op_count
        total_pending_backend_ops += stage_pending_backend_op_count
        total_recombine_count += len(recombine_records)
        total_ready_recombine += stage_ready_recombine_count

    final_output_ready = final_output_node in materialized_nodes
    executable_now = final_output_ready and total_pending_backend_ops == 0

    if executable_now:
        overall_state = "ready_now"
    elif total_pending_backend_ops > 0:
        overall_state = "pending_custom_backend"
    elif not final_output_ready:
        overall_state = "graph_incomplete"
    else:
        overall_state = "blocked"

    return {
        "executor_name": "sm_executor_v1",
        "plan_name": _as_str(plan.get("plan_name")),
        "prepared_input_node": prepared_input_node,
        "final_output_node": final_output_node,
        "node_order": _as_list(plan.get("node_order")),
        "available_backends": sorted(available_backend_set),
        "overall_state": overall_state,
        "executable_now": executable_now,
        "requires_custom_dsp": total_pending_backend_ops > 0,
        "final_output_ready": final_output_ready,
        "materialized_nodes": sorted(materialized_nodes),
        "support_recombine_gain_db": _as_float(plan.get("support_recombine_gain_db"), 0.0),
        "projection_assist_blend": _as_float(plan.get("projection_assist_blend"), 0.0),
        "spark_blend": _as_float(plan.get("spark_blend"), 0.0),
        "active_clamps": _as_list(plan.get("active_clamps")),
        "safety_notes": _as_list(plan.get("safety_notes")),
        "notes": _as_list(plan.get("notes")),
        "counts": {
            "stage_count": len(stage_reports),
            "stack_count": total_stack_count,
            "op_count": total_op_count,
            "ready_now_op_count": total_ready_now_ops,
            "pending_backend_op_count": total_pending_backend_ops,
            "recombine_count": total_recombine_count,
            "ready_recombine_count": total_ready_recombine,
        },
        "stages": stage_reports,
    }


def render_plan_requires_custom_dsp(render_plan: Any) -> bool:
    report = build_render_execution_report(render_plan)
    return _as_bool(report.get("requires_custom_dsp"), True)


def summarize_render_execution(render_plan: Any) -> Dict[str, Any]:
    report = build_render_execution_report(render_plan)

    return {
        "executor_name": report["executor_name"],
        "overall_state": report["overall_state"],
        "executable_now": report["executable_now"],
        "requires_custom_dsp": report["requires_custom_dsp"],
        "final_output_ready": report["final_output_ready"],
        "counts": report["counts"],
        "materialized_nodes": report["materialized_nodes"],
    }

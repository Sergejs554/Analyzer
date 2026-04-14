# sm/dsp/render_builder.py

from __future__ import annotations

from typing import Any, Dict, List


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


def _clean_params(params: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (params or {}).items() if v is not None}


def _backend_hint(primitive_class: str, path_type: str) -> str:
    if primitive_class in {
        "static_eq_cut",
        "presence_contour",
    }:
        return "ffmpeg_safe"
    if primitive_class in {
        "dynamic_eq_cut",
        "dynamic_harsh_control",
        "dynamic_presence_boost",
        "projection_safety_control",
        "parallel_fill",
        "support_compression",
        "harmonic_density",
        "top_texture",
        "side_polish",
        "width_high_only",
    }:
        return "custom_dsp_required"
    if path_type == "delivery":
        return "delivery_backend"
    return "custom_dsp_required"


def _op_kind_from_instance(instance: Dict[str, Any]) -> str:
    primitive_class = instance.get("primitive_class", "")

    mapping = {
        "dynamic_eq_cut": "dynamic_eq",
        "static_eq_cut": "static_eq",
        "dynamic_harsh_control": "dynamic_eq",
        "presence_contour": "broad_eq",
        "dynamic_presence_boost": "dynamic_eq",
        "projection_safety_control": "dynamic_eq",
        "parallel_fill": "parallel_eq_fill",
        "support_compression": "parallel_compressor",
        "harmonic_density": "band_limited_saturation",
        "top_texture": "band_limited_texture",
        "side_polish": "high_side_polish",
        "width_high_only": "high_only_width",
        "true_peak_limiter": "true_peak_limiter",
        "output_trim": "output_trim",
        "final_balance": "final_balance_guard",
    }
    return mapping.get(primitive_class, primitive_class or "unknown")


def _normalize_instance(instance: Dict[str, Any]) -> Dict[str, Any]:
    primitive_class = instance.get("primitive_class", "")
    path_type = instance.get("path_type", "")
    params = _clean_params(instance.get("params", {}))

    return {
        "instance_name": instance.get("instance_name"),
        "primitive_name": instance.get("primitive_name"),
        "primitive_class": primitive_class,
        "op_kind": _op_kind_from_instance(instance),
        "backend_hint": _backend_hint(primitive_class, path_type),
        "enabled": bool(instance.get("enabled", True)),
        "role": instance.get("role"),
        "stack_name": instance.get("stack_name"),
        "stack_kind": instance.get("stack_kind"),
        "path_type": path_type,
        "target_band_mode": instance.get("target_band_mode"),
        "protection_mode": instance.get("protection_mode"),
        "channel_scope": instance.get("channel_scope"),
        "channel_mode": instance.get("channel_mode"),
        "band_scope": instance.get("band_scope"),
        "detector_mode": instance.get("detector_mode"),
        "phase_policy": instance.get("phase_policy"),
        "amount_norm": instance.get("amount_norm"),
        "activity": instance.get("activity"),
        "dynamic_scale": instance.get("dynamic_scale"),
        "params": params,
        "safety_tags": list(instance.get("safety_tags", [])),
        "notes": list(instance.get("notes", [])),
    }


def _stack_execution_mode(stack: Dict[str, Any]) -> str:
    path_type = stack.get("path_type")
    stack_kind = stack.get("stack_kind")

    if not stack.get("enabled", False):
        return "disabled"

    if path_type == "inplace":
        return "serial_inplace"
    if path_type == "parallel":
        if stack_kind == "projection_assist":
            return "parallel_assist_return"
        return "parallel_return"
    if path_type == "finish":
        return "finish_micro_return"
    if path_type == "delivery":
        return "delivery_serial"
    return "unknown"


def _normalize_stack(stack: Dict[str, Any]) -> Dict[str, Any]:
    instances = [_normalize_instance(x) for x in stack.get("primitive_instances", [])]

    requires_custom_dsp = any(
        op.get("backend_hint") == "custom_dsp_required"
        for op in instances
    )

    return {
        "role": stack.get("role"),
        "role_rank": stack.get("role_rank"),
        "enabled": bool(stack.get("enabled", False)),
        "stack_name": stack.get("stack_name"),
        "stack_kind": stack.get("stack_kind"),
        "path_type": stack.get("path_type"),
        "tap_point": stack.get("tap_point"),
        "output_node": stack.get("output_node"),
        "recombine_target": stack.get("recombine_target"),
        "target_band_mode": stack.get("target_band_mode"),
        "protection_mode": stack.get("protection_mode"),
        "requested_amount": stack.get("requested_amount"),
        "requested_cap": stack.get("requested_cap"),
        "execution_amount": stack.get("execution_amount"),
        "execution_cap": stack.get("execution_cap"),
        "dynamic_scale": stack.get("dynamic_scale"),
        "safety_tags": list(stack.get("safety_tags", [])),
        "active_clamps": list(stack.get("active_clamps", [])),
        "blocked_actions": list(stack.get("blocked_actions", [])),
        "render_mode": _stack_execution_mode(stack),
        "requires_custom_dsp": requires_custom_dsp,
        "ops": instances,
        "notes": list(stack.get("notes", [])),
    }


def _normalize_recombine_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    recombine_mode = plan.get("recombine_mode", "sum")
    blend = float(plan.get("blend", 1.0))
    gain_db = float(plan.get("gain_db", 0.0))

    render_recombine_kind = {
        "sum": "passthrough_or_sum",
        "guarded_sum": "guarded_parallel_sum",
        "assist_sum": "assist_blend_sum",
        "finish_sum": "finish_blend_sum",
    }.get(recombine_mode, recombine_mode)

    return {
        "recombine_name": plan.get("recombine_name"),
        "recombine_mode": recombine_mode,
        "render_recombine_kind": render_recombine_kind,
        "source_nodes": list(plan.get("source_nodes", [])),
        "target_node": plan.get("target_node"),
        "gain_db": gain_db,
        "blend": blend,
        "active_clamps": list(plan.get("active_clamps", [])),
        "safety_tags": list(plan.get("safety_tags", [])),
        "notes": list(plan.get("notes", [])),
    }


def _normalize_stage(stage: Dict[str, Any]) -> Dict[str, Any]:
    role_stacks = [_normalize_stack(x) for x in stage.get("role_stacks", [])]
    recombine_plans = [_normalize_recombine_plan(x) for x in stage.get("recombine_plans", [])]

    requires_custom_dsp = any(x.get("requires_custom_dsp", False) for x in role_stacks)

    return {
        "stage_name": stage.get("stage_name"),
        "stage_kind": stage.get("stage_kind"),
        "input_node": stage.get("input_node"),
        "output_node": stage.get("output_node"),
        "role_order": list(stage.get("role_order", [])),
        "active_clamps": list(stage.get("active_clamps", [])),
        "safety_tags": list(stage.get("safety_tags", [])),
        "requires_custom_dsp": requires_custom_dsp,
        "stacks": role_stacks,
        "recombine": recombine_plans,
        "notes": list(stage.get("notes", [])),
    }


def _collect_node_order(stage_plans: List[Dict[str, Any]], final_output_node: str) -> List[str]:
    ordered: List[str] = []
    for stage in stage_plans:
        input_node = stage.get("input_node")
        output_node = stage.get("output_node")
        if input_node:
            ordered.append(input_node)
        if output_node:
            ordered.append(output_node)
    if final_output_node:
        ordered.append(final_output_node)
    return _uniq(ordered)


def validate_render_plan(render_plan: Dict[str, Any]) -> None:
    stages = render_plan.get("stages", [])
    if not stages:
        raise ValueError("Render plan has no stages")

    previous_output = None
    for idx, stage in enumerate(stages):
        input_node = stage.get("input_node")
        output_node = stage.get("output_node")

        if not input_node or not output_node:
            raise ValueError(f"Stage {stage.get('stage_name')} is missing input/output node")

        if idx == 0:
            if input_node != render_plan.get("prepared_input_node"):
                raise ValueError("First render stage must start from prepared_input_node")
        else:
            if previous_output is not None and input_node != previous_output:
                raise ValueError(
                    f"Stage chain mismatch: {stage.get('stage_name')} input_node={input_node} "
                    f"but previous output_node={previous_output}"
                )

        previous_output = output_node

    if previous_output != render_plan.get("final_output_node"):
        raise ValueError(
            f"Last render stage output {previous_output} does not match final_output_node "
            f"{render_plan.get('final_output_node')}"
        )


def build_dsp_render_plan(blueprint) -> Dict[str, Any]:
    stage_plans = [_normalize_stage(x) for x in getattr(blueprint, "stage_plans", [])]

    render_plan = {
        "plan_name": "sm_render_plan_v1",
        "prepared_input_node": getattr(blueprint, "prepared_input_node", "prepared_input"),
        "final_output_node": getattr(blueprint, "final_output_node", "final_output"),
        "support_recombine_gain_db": getattr(blueprint, "support_recombine_gain_db", 0.0),
        "projection_assist_blend": getattr(blueprint, "projection_assist_blend", 0.0),
        "spark_blend": getattr(blueprint, "spark_blend", 0.0),
        "active_clamps": list(getattr(blueprint, "active_clamps", [])),
        "safety_notes": list(getattr(blueprint, "safety_notes", [])),
        "stages": stage_plans,
        "node_order": _collect_node_order(
            stage_plans,
            getattr(blueprint, "final_output_node", "final_output"),
        ),
        "notes": _uniq(
            list(getattr(blueprint, "notes", []))
            + [
                "render_builder_attached",
                "stage_order_locked_from_graph",
                "render_plan_ready_for_executor",
            ]
        ),
    }

    validate_render_plan(render_plan)
    return render_plan

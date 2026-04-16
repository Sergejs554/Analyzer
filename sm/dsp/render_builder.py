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


def _read(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _clean_params(params: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (params or {}).items() if v is not None}


def _backend_hint(primitive_class: str, path_type: str) -> str:
    if path_type == "delivery":
        return "delivery_backend"

    if primitive_class in {
        "static_eq_cut",
        "presence_contour",
        "air_shelf",
    }:
        return "ffmpeg_safe"

    if primitive_class in {
        "dynamic_eq_cut",
        "dynamic_eq_wide_cut",
        "dynamic_harsh_control",
        "dynamic_sibilance_control",
        "dynamic_tilt",
        "support_eq_boost",
        "dynamic_support_boost",
        "parallel_fill",
        "parallel_handoff_support",
        "support_compression",
        "dynamic_presence_boost",
        "projection_safety_control",
        "band_limited_saturation",
        "harmonic_density",
        "top_texture",
        "side_top_polish",
        "high_only_width",
        "true_peak_limiter",
        "output_trim",
        "ceiling_trim",
        "final_balance_guard",
    }:
        return "custom_dsp_required"

    return "custom_dsp_required"


def _op_kind_from_instance(instance: Any) -> str:
    primitive_class = _read(instance, "primitive_class", "")

    mapping = {
        "dynamic_eq_cut": "dynamic_eq",
        "dynamic_eq_wide_cut": "dynamic_eq",
        "static_eq_cut": "static_eq",
        "dynamic_harsh_control": "dynamic_eq",
        "dynamic_sibilance_control": "dynamic_eq",
        "dynamic_tilt": "dynamic_tilt",
        "support_eq_boost": "parallel_eq_boost",
        "dynamic_support_boost": "dynamic_eq_boost",
        "parallel_fill": "parallel_eq_fill",
        "parallel_handoff_support": "parallel_eq_fill",
        "support_compression": "parallel_compressor",
        "presence_contour": "broad_eq",
        "dynamic_presence_boost": "dynamic_eq",
        "projection_safety_control": "dynamic_eq",
        "band_limited_saturation": "band_limited_saturation",
        "harmonic_density": "band_limited_saturation",
        "air_shelf": "high_shelf",
        "top_texture": "band_limited_texture",
        "side_top_polish": "high_side_polish",
        "high_only_width": "high_only_width",
        "true_peak_limiter": "true_peak_limiter",
        "output_trim": "output_trim",
        "ceiling_trim": "ceiling_trim",
        "final_balance_guard": "final_balance_guard",
    }
    return mapping.get(primitive_class, primitive_class or "unknown")


def _normalize_instance(instance: Any) -> Dict[str, Any]:
    primitive_class = _read(instance, "primitive_class", "")
    path_type = _read(instance, "path_type", "")
    params = _clean_params(_read(instance, "params", {}) or {})
    backend_hint = _backend_hint(primitive_class, path_type)

    return {
        "instance_name": _read(instance, "instance_name"),
        "primitive_name": _read(instance, "primitive_name"),
        "primitive_class": primitive_class,
        "op_kind": _op_kind_from_instance(instance),
        "backend_hint": backend_hint,
        "enabled": bool(_read(instance, "enabled", True)),
        "role": _read(instance, "role"),
        "stack_name": _read(instance, "stack_name"),
        "stack_kind": _read(instance, "stack_kind"),
        "path_type": path_type,
        "target_band_mode": _read(instance, "target_band_mode"),
        "protection_mode": _read(instance, "protection_mode"),
        "channel_scope": _read(instance, "channel_scope"),
        "channel_mode": _read(instance, "channel_mode"),
        "band_scope": _read(instance, "band_scope"),
        "detector_mode": _read(instance, "detector_mode"),
        "phase_policy": _read(instance, "phase_policy"),
        "amount_norm": _read(instance, "amount_norm"),
        "activity": _read(instance, "activity"),
        "dynamic_scale": _read(instance, "dynamic_scale"),
        "params": params,
        "safety_tags": list(_read(instance, "safety_tags", []) or []),
        "notes": list(_read(instance, "notes", []) or []),
    }


def _stack_execution_mode(stack: Any) -> str:
    path_type = _read(stack, "path_type")
    stack_kind = _read(stack, "stack_kind")

    if not _read(stack, "enabled", False):
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


def _normalize_stack(stack: Any) -> Dict[str, Any]:
    instances = [_normalize_instance(x) for x in (_read(stack, "primitive_instances", []) or [])]

    requires_custom_dsp = any(
        op.get("backend_hint") in {"custom_dsp_required", "delivery_backend"}
        for op in instances
    )

    return {
        "role": _read(stack, "role"),
        "role_rank": _read(stack, "role_rank"),
        "enabled": bool(_read(stack, "enabled", False)),
        "stack_name": _read(stack, "stack_name"),
        "stack_kind": _read(stack, "stack_kind"),
        "path_type": _read(stack, "path_type"),
        "tap_point": _read(stack, "tap_point"),
        "output_node": _read(stack, "output_node"),
        "recombine_target": _read(stack, "recombine_target"),
        "target_band_mode": _read(stack, "target_band_mode"),
        "protection_mode": _read(stack, "protection_mode"),
        "requested_amount": _read(stack, "requested_amount"),
        "requested_cap": _read(stack, "requested_cap"),
        "execution_amount": _read(stack, "execution_amount"),
        "execution_cap": _read(stack, "execution_cap"),
        "dynamic_scale": _read(stack, "dynamic_scale"),
        "safety_tags": list(_read(stack, "safety_tags", []) or []),
        "active_clamps": list(_read(stack, "active_clamps", []) or []),
        "blocked_actions": list(_read(stack, "blocked_actions", []) or []),
        "render_mode": _stack_execution_mode(stack),
        "requires_custom_dsp": requires_custom_dsp,
        "ops": instances,
        "notes": list(_read(stack, "notes", []) or []),
    }


def _normalize_recombine_plan(plan: Any) -> Dict[str, Any]:
    recombine_mode = _read(plan, "recombine_mode", "sum")
    blend = float(_read(plan, "blend", 1.0))
    gain_db = float(_read(plan, "gain_db", 0.0))

    render_recombine_kind = {
        "sum": "passthrough_or_sum",
        "guarded_sum": "guarded_parallel_sum",
        "assist_sum": "assist_blend_sum",
        "finish_sum": "finish_blend_sum",
    }.get(recombine_mode, recombine_mode)

    return {
        "recombine_name": _read(plan, "recombine_name"),
        "recombine_mode": recombine_mode,
        "render_recombine_kind": render_recombine_kind,
        "source_nodes": list(_read(plan, "source_nodes", []) or []),
        "target_node": _read(plan, "target_node"),
        "gain_db": gain_db,
        "blend": blend,
        "active_clamps": list(_read(plan, "active_clamps", []) or []),
        "safety_tags": list(_read(plan, "safety_tags", []) or []),
        "notes": list(_read(plan, "notes", []) or []),
    }


def _normalize_stage(stage: Any) -> Dict[str, Any]:
    role_stacks = [_normalize_stack(x) for x in (_read(stage, "role_stacks", []) or [])]
    recombine_plans = [_normalize_recombine_plan(x) for x in (_read(stage, "recombine_plans", []) or [])]

    requires_custom_dsp = any(x.get("requires_custom_dsp", False) for x in role_stacks)

    return {
        "stage_name": _read(stage, "stage_name"),
        "stage_kind": _read(stage, "stage_kind"),
        "input_node": _read(stage, "input_node"),
        "output_node": _read(stage, "output_node"),
        "role_order": list(_read(stage, "role_order", []) or []),
        "active_clamps": list(_read(stage, "active_clamps", []) or []),
        "safety_tags": list(_read(stage, "safety_tags", []) or []),
        "requires_custom_dsp": requires_custom_dsp,
        "stacks": role_stacks,
        "recombine": recombine_plans,
        "notes": list(_read(stage, "notes", []) or []),
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


def build_dsp_render_plan(blueprint: Any) -> Dict[str, Any]:
    stage_plans = [_normalize_stage(x) for x in (_read(blueprint, "stage_plans", []) or [])]

    render_plan = {
        "plan_name": "sm_render_plan_v1",
        "prepared_input_node": _read(blueprint, "prepared_input_node", "prepared_input"),
        "final_output_node": _read(blueprint, "final_output_node", "final_output"),
        "support_recombine_gain_db": _read(blueprint, "support_recombine_gain_db", 0.0),
        "projection_assist_blend": _read(blueprint, "projection_assist_blend", 0.0),
        "spark_blend": _read(blueprint, "spark_blend", 0.0),
        "active_clamps": list(_read(blueprint, "active_clamps", []) or []),
        "safety_notes": list(_read(blueprint, "safety_notes", []) or []),
        "stages": stage_plans,
        "node_order": _collect_node_order(
            stage_plans,
            _read(blueprint, "final_output_node", "final_output"),
        ),
        "notes": _uniq(
            list(_read(blueprint, "notes", []) or [])
            + [
                "render_builder_attached",
                "stage_order_locked_from_graph",
                "render_plan_ready_for_executor",
            ]
        ),
    }

    validate_render_plan(render_plan)
    return render_plan

from __future__ import annotations

from typing import Any


def _read(obj: Any, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def build_render_execution_report(render_plan: Any, input_path: str | None = None) -> dict:
    node_order = list(_read(render_plan, "node_order", []) or [])
    stages = list(_read(render_plan, "stages", []) or [])
    prepared_input_node = _read(render_plan, "prepared_input_node", "prepared_input")
    final_output_node = _read(render_plan, "final_output_node", "final_output")

    node_paths: dict[str, str | None] = {name: None for name in node_order}
    if input_path:
        node_paths[prepared_input_node] = input_path

    report: dict[str, Any] = {
        "status": "ok",
        "plan_name": _read(render_plan, "plan_name", "sm_render_plan_v1"),
        "prepared_input_node": prepared_input_node,
        "final_output_node": final_output_node,
        "node_order": node_order,
        "node_paths": node_paths,
        "stage_reports": [],
        "unsupported_ops": [],
        "executed_op_count": 0,
        "pending_custom_op_count": 0,
        "safety_notes": list(_read(render_plan, "safety_notes", []) or []),
        "notes": list(_read(render_plan, "notes", []) or []),
    }

    current_path = input_path

    for stage in stages:
        stage_name = _read(stage, "stage_name", "unknown_stage")
        input_node = _read(stage, "input_node")
        output_node = _read(stage, "output_node")
        stacks = list(_read(stage, "stacks", []) or [])
        recombine = list(_read(stage, "recombine", []) or [])

        stage_report: dict[str, Any] = {
            "stage_name": stage_name,
            "stage_kind": _read(stage, "stage_kind"),
            "input_node": input_node,
            "output_node": output_node,
            "requires_custom_dsp": bool(_read(stage, "requires_custom_dsp", False)),
            "active_clamps": list(_read(stage, "active_clamps", []) or []),
            "safety_tags": list(_read(stage, "safety_tags", []) or []),
            "stack_reports": [],
            "recombine_reports": [],
            "resolved_input_path": node_paths.get(input_node),
            "resolved_output_path": None,
        }

        for stack in stacks:
            stack_report: dict[str, Any] = {
                "stack_name": _read(stack, "stack_name"),
                "role": _read(stack, "role"),
                "render_mode": _read(stack, "render_mode"),
                "requires_custom_dsp": bool(_read(stack, "requires_custom_dsp", False)),
                "ops": [],
            }

            ops = list(_read(stack, "ops", []) or [])
            for op in ops:
                backend_hint = _read(op, "backend_hint", "unknown")
                executed = backend_hint == "ffmpeg_safe"

                op_report = {
                    "instance_name": _read(op, "instance_name"),
                    "primitive_name": _read(op, "primitive_name"),
                    "op_kind": _read(op, "op_kind"),
                    "backend_hint": backend_hint,
                    "executed": executed,
                    "pending_reason": None if executed else "custom_dsp_pending",
                    "params": dict(_read(op, "params", {}) or {}),
                }
                stack_report["ops"].append(op_report)

                if executed:
                    report["executed_op_count"] += 1
                else:
                    report["pending_custom_op_count"] += 1
                    report["unsupported_ops"].append(
                        {
                            "stage_name": stage_name,
                            "stack_name": _read(stack, "stack_name"),
                            "instance_name": _read(op, "instance_name"),
                            "primitive_name": _read(op, "primitive_name"),
                            "op_kind": _read(op, "op_kind"),
                            "backend_hint": backend_hint,
                        }
                    )

            stage_report["stack_reports"].append(stack_report)

        for rec in recombine:
            stage_report["recombine_reports"].append(
                {
                    "recombine_name": _read(rec, "recombine_name"),
                    "render_recombine_kind": _read(rec, "render_recombine_kind"),
                    "source_nodes": list(_read(rec, "source_nodes", []) or []),
                    "target_node": _read(rec, "target_node"),
                    "blend": _read(rec, "blend"),
                    "gain_db": _read(rec, "gain_db"),
                }
            )

        if output_node:
            stage_report["resolved_output_path"] = current_path
            node_paths[output_node] = current_path

        report["stage_reports"].append(stage_report)

    if report["pending_custom_op_count"] > 0:
        report["status"] = "partial_custom_dsp_pending"

    report["final_output_path"] = node_paths.get(final_output_node)
    return report


def execute_dsp_render_plan(render_plan: Any, input_path: str | None = None) -> dict:
    return build_render_execution_report(
        render_plan=render_plan,
        input_path=input_path,
    )

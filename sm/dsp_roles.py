# sm/dsp_roles.py

from .contracts import RoleExecutionPlan


def build_body_anchor_role(plan: RoleExecutionPlan) -> str:
    if not plan.enabled:
        return "anull"
    return "anull"


def build_body_bridge_role(plan: RoleExecutionPlan) -> str:
    if not plan.enabled:
        return "anull"
    return "anull"


def build_buildup_cleanup_role(plan: RoleExecutionPlan) -> str:
    if not plan.enabled:
        return "anull"
    return "anull"


def build_upper_body_guard_role(plan: RoleExecutionPlan) -> str:
    if not plan.enabled:
        return "anull"
    return "anull"


def build_projection_handoff_role(plan: RoleExecutionPlan) -> str:
    if not plan.enabled:
        return "anull"
    return "anull"


def build_finish_spark_role(plan: RoleExecutionPlan) -> str:
    if not plan.enabled:
        return "anull"
    return "anull"

# sm/router.py

from .contracts import (
    RoleProfileSelection,
    RoleExecutionPlan,
    SmartMasterRouterSummary,
)
from .profiles import PROFILE_DEFAULTS


def _plan_from_selected(selected) -> RoleExecutionPlan:
    defaults = PROFILE_DEFAULTS[selected.profile_name]
    return RoleExecutionPlan(
        role=selected.role,
        profile_name=selected.profile_name,
        enabled=selected.enabled,
        amount=selected.amount,
        cap=selected.cap,
        dynamic_scale=defaults["dynamic_scale"],
        target_band_mode=defaults["target_band_mode"],
        protection_mode=defaults["protection_mode"],
        notes=selected.notes[:],
    )


def build_sm_router_summary(selection: RoleProfileSelection) -> SmartMasterRouterSummary:
    return SmartMasterRouterSummary(
        anchor=_plan_from_selected(selection.anchor),
        bridge=_plan_from_selected(selection.bridge),
        cleanup=_plan_from_selected(selection.cleanup),
        guard=_plan_from_selected(selection.guard),
        projection=_plan_from_selected(selection.projection),
        spark=_plan_from_selected(selection.spark),
    )

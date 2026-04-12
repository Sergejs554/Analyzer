# sm/selector.py

from .contracts import RoleProfileSelection, SelectedRoleProfile, SmartMasterAnalysis
from .enums import RoleName


def select_anchor_profile(schema: SmartMasterAnalysis, tone: str, intensity: str) -> SelectedRoleProfile:
    packet = schema.anchor
    if packet.stop:
        return SelectedRoleProfile(RoleName.ANCHOR, "anchor_restrain_upper_body", "anchor stop fallback", 0.18, 0.28)
    if packet.state.value == "deficient":
        return SelectedRoleProfile(RoleName.ANCHOR, "anchor_restore_controlled", "anchor deficient", 0.34, 0.48)
    if packet.state.value == "excessive":
        return SelectedRoleProfile(RoleName.ANCHOR, "anchor_restrain_upper_body", "anchor excessive", 0.22, 0.34)
    return SelectedRoleProfile(RoleName.ANCHOR, "anchor_hold_safe", "anchor balanced", 0.18, 0.30)


def select_bridge_profile(schema: SmartMasterAnalysis, tone: str, intensity: str) -> SelectedRoleProfile:
    packet = schema.bridge
    if packet.stop:
        return SelectedRoleProfile(RoleName.BRIDGE, "bridge_restrain_glue", "bridge stop fallback", 0.16, 0.28)
    if packet.state.value == "broken":
        return SelectedRoleProfile(RoleName.BRIDGE, "bridge_restore_controlled", "bridge broken", 0.30, 0.44)
    if packet.state.value == "overglued":
        return SelectedRoleProfile(RoleName.BRIDGE, "bridge_restrain_glue", "bridge overglued", 0.22, 0.36)
    return SelectedRoleProfile(RoleName.BRIDGE, "bridge_hold_safe", "bridge balanced", 0.16, 0.28)


def select_cleanup_profile(schema: SmartMasterAnalysis, tone: str, intensity: str) -> SelectedRoleProfile:
    packet = schema.cleanup
    if packet.stop or packet.readiness.value == "denied":
        return SelectedRoleProfile(RoleName.CLEANUP, "cleanup_micro_corrective", "cleanup denied/stop", 0.10, 0.18)
    if packet.readiness.value == "guarded":
        return SelectedRoleProfile(RoleName.CLEANUP, "cleanup_guarded_safe", "cleanup guarded", 0.20, 0.34)
    return SelectedRoleProfile(RoleName.CLEANUP, "cleanup_focused_dense", "cleanup safe", 0.42, 0.60)


def select_guard_profile(schema: SmartMasterAnalysis, tone: str, intensity: str) -> SelectedRoleProfile:
    packet = schema.guard
    if packet.stop or packet.transition_state.value == "thinning":
        return SelectedRoleProfile(RoleName.GUARD, "guard_transition_support_safe", "guard thinning fallback", 0.20, 0.32)
    if packet.shape.value == "boxy":
        return SelectedRoleProfile(RoleName.GUARD, "guard_boxiness_controlled", "guard boxy", 0.24, 0.38)
    return SelectedRoleProfile(RoleName.GUARD, "guard_hold_safe", "guard stable", 0.16, 0.28)


def select_projection_profile(schema: SmartMasterAnalysis, tone: str, intensity: str) -> SelectedRoleProfile:
    packet = schema.projection
    if packet.stop or packet.readiness.value == "denied":
        return SelectedRoleProfile(RoleName.PROJECTION, "projection_clamp_safe", "projection denied/stop", 0.10, 0.18)
    if packet.readiness.value == "guarded":
        return SelectedRoleProfile(RoleName.PROJECTION, "projection_mild_safe", "projection guarded", 0.18, 0.30)
    if packet.state.value == "overpushed":
        return SelectedRoleProfile(RoleName.PROJECTION, "projection_clamp_safe", "projection overpushed", 0.12, 0.22)
    return SelectedRoleProfile(RoleName.PROJECTION, "projection_controlled_dense", "projection ready", 0.38, 0.54)


def select_spark_profile(schema: SmartMasterAnalysis, tone: str, intensity: str) -> SelectedRoleProfile:
    packet = schema.projection
    if packet.readiness.value != "ready":
        return SelectedRoleProfile(RoleName.SPARK, "finish_spark_off", "projection not ready", 0.0, 0.0, enabled=False)
    if packet.harshness_risk.value == "high" or packet.sibilance_risk.value == "high":
        return SelectedRoleProfile(RoleName.SPARK, "finish_spark_micro_safe", "top risk guarded", 0.08, 0.16)
    return SelectedRoleProfile(RoleName.SPARK, "finish_spark_controlled_excited", "spark allowed", 0.22, 0.34)


def select_sm_profiles(schema: SmartMasterAnalysis, tone: str, intensity: str) -> RoleProfileSelection:
    return RoleProfileSelection(
        anchor=select_anchor_profile(schema, tone, intensity),
        bridge=select_bridge_profile(schema, tone, intensity),
        cleanup=select_cleanup_profile(schema, tone, intensity),
        guard=select_guard_profile(schema, tone, intensity),
        projection=select_projection_profile(schema, tone, intensity),
        spark=select_spark_profile(schema, tone, intensity),
    )

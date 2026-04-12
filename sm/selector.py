# sm/selector.py

from .contracts import (
    RoleProfileSelection,
    SelectedRoleProfile,
    SmartMasterAnalysis,
)
from .enums import RoleName, RiskLevel, UpperBodyShape, TransitionState, ProjectionState


def _has(v) -> bool:
    return v is not None


def _risk_ge(risk: RiskLevel, level: RiskLevel) -> bool:
    order = {
        RiskLevel.LOW: 0,
        RiskLevel.MEDIUM: 1,
        RiskLevel.HIGH: 2,
    }
    return order[risk] >= order[level]


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _intensity_scale(intensity: str) -> float:
    intensity = (intensity or "balanced").lower().strip()
    if intensity == "low":
        return 0.90
    if intensity == "high":
        return 1.08
    return 1.00


def _tone_projection_scale(tone: str) -> float:
    tone = (tone or "balanced").lower().strip()
    if tone == "warm":
        return 0.94
    if tone == "bright":
        return 1.04
    return 1.00


def _tone_cleanup_scale(tone: str) -> float:
    tone = (tone or "balanced").lower().strip()
    if tone == "warm":
        return 0.96
    if tone == "bright":
        return 1.04
    return 1.00


def _make_profile(
    role: RoleName,
    profile_name: str,
    reason: str,
    amount: float,
    cap: float,
    enabled: bool = True,
    forced_clamp: bool = False,
    notes: list[str] | None = None,
) -> SelectedRoleProfile:
    return SelectedRoleProfile(
        role=role,
        profile_name=profile_name,
        reason=reason,
        amount=amount,
        cap=cap,
        enabled=enabled,
        forced_clamp=forced_clamp,
        notes=notes[:] if notes else [],
    )


def select_anchor_profile(schema: SmartMasterAnalysis, tone: str, intensity: str) -> SelectedRoleProfile:
    packet = schema.anchor
    derived = schema.derived
    notes: list[str] = []

    if packet.stop:
        return _make_profile(
            RoleName.ANCHOR,
            "anchor_restrain_upper_body",
            "anchor stop fallback",
            0.16,
            0.26,
            forced_clamp=True,
            notes=["anchor stop"],
        )

    if packet.state.value == "deficient":
        amount = 0.34 * _intensity_scale(intensity)
        cap = 0.48
        if _has(derived.center_body_support_proxy) and derived.center_body_support_proxy < 0.40:
            amount += 0.03
            notes.append("center-body support weak -> slightly stronger anchor restore")
        return _make_profile(
            RoleName.ANCHOR,
            "anchor_restore_controlled",
            "anchor deficient",
            _clamp(amount, 0.28, 0.42),
            cap,
            notes=notes,
        )

    if packet.state.value == "excessive":
        return _make_profile(
            RoleName.ANCHOR,
            "anchor_restrain_upper_body",
            "anchor excessive",
            0.20,
            0.32,
            notes=["excessive anchor mass"],
        )

    if not packet.foundation_present:
        return _make_profile(
            RoleName.ANCHOR,
            "anchor_restore_controlled",
            "foundation missing",
            0.28,
            0.40,
            notes=["foundation not fully present"],
        )

    if packet.fragility == RiskLevel.HIGH:
        return _make_profile(
            RoleName.ANCHOR,
            "anchor_hold_safe",
            "anchor fragile",
            0.14,
            0.24,
            forced_clamp=True,
            notes=["high anchor fragility"],
        )

    if packet.fragility == RiskLevel.MEDIUM:
        return _make_profile(
            RoleName.ANCHOR,
            "anchor_hold_safe",
            "anchor mildly fragile",
            0.16,
            0.28,
            notes=["medium anchor fragility"],
        )

    return _make_profile(
        RoleName.ANCHOR,
        "anchor_hold_safe",
        "anchor balanced",
        0.18,
        0.30,
    )


def select_bridge_profile(schema: SmartMasterAnalysis, tone: str, intensity: str) -> SelectedRoleProfile:
    packet = schema.bridge
    derived = schema.derived
    notes: list[str] = []

    if packet.stop:
        return _make_profile(
            RoleName.BRIDGE,
            "bridge_restrain_glue",
            "bridge stop fallback",
            0.14,
            0.24,
            forced_clamp=True,
            notes=["bridge stop"],
        )

    if packet.state.value == "broken":
        amount = 0.30 * _intensity_scale(intensity)
        if _has(derived.body_to_mid_handoff_proxy) and derived.body_to_mid_handoff_proxy < 0.40:
            amount += 0.03
            notes.append("weak handoff -> slightly stronger bridge restore")
        return _make_profile(
            RoleName.BRIDGE,
            "bridge_restore_controlled",
            "bridge broken",
            _clamp(amount, 0.26, 0.40),
            0.44,
            notes=notes,
        )

    if packet.state.value == "overglued":
        return _make_profile(
            RoleName.BRIDGE,
            "bridge_restrain_glue",
            "bridge overglued",
            0.20,
            0.32,
            notes=["bridge glue excess"],
        )

    if packet.glue_risk == RiskLevel.HIGH:
        return _make_profile(
            RoleName.BRIDGE,
            "bridge_restrain_glue",
            "bridge glue risk high",
            0.18,
            0.30,
            notes=["prevent low/body stickiness"],
        )

    if packet.gap_risk == RiskLevel.MEDIUM:
        return _make_profile(
            RoleName.BRIDGE,
            "bridge_restore_controlled",
            "bridge gap risk medium",
            0.22,
            0.34,
            notes=["bridge needs mild restore"],
        )

    if packet.glue_risk == RiskLevel.MEDIUM:
        return _make_profile(
            RoleName.BRIDGE,
            "bridge_hold_safe",
            "bridge balanced but glue-prone",
            0.14,
            0.24,
            notes=["bridge held conservatively due to glue risk"],
        )

    return _make_profile(
        RoleName.BRIDGE,
        "bridge_hold_safe",
        "bridge balanced",
        0.16,
        0.28,
    )


def select_cleanup_profile(schema: SmartMasterAnalysis, tone: str, intensity: str) -> SelectedRoleProfile:
    packet = schema.cleanup
    anchor = schema.anchor
    bridge = schema.bridge
    derived = schema.derived
    notes: list[str] = []

    tone_scale = _tone_cleanup_scale(tone)
    intensity_scale = _intensity_scale(intensity)

    if packet.stop or packet.readiness.value == "denied":
        return _make_profile(
            RoleName.CLEANUP,
            "cleanup_micro_corrective",
            "cleanup denied/stop",
            0.08,
            0.16,
            forced_clamp=True,
            notes=["cleanup denied or stop"],
        )

    if packet.readiness.value == "guarded":
        amount = 0.20 * intensity_scale * tone_scale
        return _make_profile(
            RoleName.CLEANUP,
            "cleanup_guarded_safe",
            "cleanup guarded",
            _clamp(amount, 0.16, 0.28),
            0.34,
            notes=["body protection active"],
        )

    aggressive_ok = True

    if anchor.fragility != RiskLevel.LOW:
        aggressive_ok = False
        notes.append("anchor fragility blocks aggressive cleanup")

    if bridge.glue_risk != RiskLevel.LOW:
        aggressive_ok = False
        notes.append("bridge glue tendency blocks aggressive cleanup")

    if _has(derived.center_body_support_proxy) and derived.center_body_support_proxy < 0.52:
        aggressive_ok = False
        notes.append("center-body support not strong enough for aggressive cleanup")

    if _has(derived.body_to_mid_handoff_proxy) and derived.body_to_mid_handoff_proxy < 0.50:
        aggressive_ok = False
        notes.append("handoff support not strong enough for aggressive cleanup")

    if packet.buildup_risk == RiskLevel.HIGH and aggressive_ok:
        amount = 0.38 * intensity_scale * tone_scale
        cap = 0.54
        return _make_profile(
            RoleName.CLEANUP,
            "cleanup_focused_dense",
            "cleanup safe and dense",
            _clamp(amount, 0.32, 0.46),
            cap,
            notes=notes,
        )

    if packet.buildup_risk in (RiskLevel.MEDIUM, RiskLevel.HIGH):
        amount = 0.22 * intensity_scale * tone_scale
        return _make_profile(
            RoleName.CLEANUP,
            "cleanup_guarded_safe",
            "cleanup safe but stack-limited",
            _clamp(amount, 0.18, 0.30),
            0.36,
            notes=notes if notes else ["cleanup held conservative"],
        )

    return _make_profile(
        RoleName.CLEANUP,
        "cleanup_micro_corrective",
        "cleanup only minor",
        0.10,
        0.18,
        notes=["no strong cleanup demand"],
    )


def select_guard_profile(
    schema: SmartMasterAnalysis,
    tone: str,
    intensity: str,
    cleanup_profile: SelectedRoleProfile,
) -> SelectedRoleProfile:
    packet = schema.guard
    derived = schema.derived
    notes: list[str] = []

    if packet.stop:
        return _make_profile(
            RoleName.GUARD,
            "guard_transition_support_safe",
            "guard stop fallback",
            0.18,
            0.28,
            forced_clamp=True,
            notes=["guard stop"],
        )

    if packet.transition_state.value == "thinning":
        return _make_profile(
            RoleName.GUARD,
            "guard_transition_support_safe",
            "guard thinning fallback",
            0.20,
            0.32,
            notes=["transition thinning"],
        )

    if packet.transition_state.value == "weak":
        return _make_profile(
            RoleName.GUARD,
            "guard_transition_support_safe",
            "guard weak transition",
            0.18,
            0.30,
            notes=["transition weak"],
        )

    if packet.shape == UpperBodyShape.BOXY:
        if cleanup_profile.profile_name == "cleanup_focused_dense":
            return _make_profile(
                RoleName.GUARD,
                "guard_boxiness_controlled",
                "guard boxy after dense cleanup",
                0.24,
                0.36,
                notes=["boxiness retained after meaningful cleanup"],
            )

        if _has(derived.body_to_mid_handoff_proxy) and derived.body_to_mid_handoff_proxy < 0.52:
            return _make_profile(
                RoleName.GUARD,
                "guard_transition_support_safe",
                "guard boxy but handoff fragile",
                0.18,
                0.30,
                notes=["prefer support over subtractive box control"],
            )

        return _make_profile(
            RoleName.GUARD,
            "guard_boxiness_controlled",
            "guard boxy",
            0.20,
            0.32,
            notes=["moderate boxiness control"],
        )

    return _make_profile(
        RoleName.GUARD,
        "guard_hold_safe",
        "guard stable",
        0.16,
        0.28,
    )


def select_projection_profile(
    schema: SmartMasterAnalysis,
    tone: str,
    intensity: str,
    cleanup_profile: SelectedRoleProfile,
    guard_profile: SelectedRoleProfile,
) -> SelectedRoleProfile:
    packet = schema.projection
    derived = schema.derived
    notes: list[str] = []

    tone_scale = _tone_projection_scale(tone)
    intensity_scale = _intensity_scale(intensity)

    if packet.stop or packet.readiness.value == "denied":
        return _make_profile(
            RoleName.PROJECTION,
            "projection_clamp_safe",
            "projection denied/stop",
            0.10,
            0.18,
            forced_clamp=True,
            notes=["projection denied or stop"],
        )

    if packet.state.value == "overpushed":
        return _make_profile(
            RoleName.PROJECTION,
            "projection_clamp_safe",
            "projection overpushed",
            0.12,
            0.22,
            forced_clamp=True,
            notes=["projection already too forward"],
        )

    if packet.readiness.value == "guarded":
        amount = 0.18 * intensity_scale * tone_scale
        return _make_profile(
            RoleName.PROJECTION,
            "projection_mild_safe",
            "projection guarded",
            _clamp(amount, 0.14, 0.24),
            0.30,
            notes=["projection guarded by risk layer"],
        )

    dense_projection_allowed = True

    if cleanup_profile.profile_name == "cleanup_focused_dense":
        dense_projection_allowed = False
        notes.append("dense cleanup blocks dense projection in same pass")

    if guard_profile.profile_name == "guard_boxiness_controlled":
        dense_projection_allowed = False
        notes.append("active boxiness control blocks dense projection in same pass")

    if packet.state == ProjectionState.UNDERPROJECTED:
        dense_projection_allowed = False
        notes.append("underprojected state prefers mild projection first")

    if _has(derived.top_push_safety_proxy) and derived.top_push_safety_proxy < 0.66:
        dense_projection_allowed = False
        notes.append("top push safety not strong enough for dense projection")

    if _has(derived.body_to_mid_handoff_proxy) and derived.body_to_mid_handoff_proxy < 0.60:
        dense_projection_allowed = False
        notes.append("handoff not strong enough for dense projection")

    if packet.harshness_risk != RiskLevel.LOW or packet.sibilance_risk != RiskLevel.LOW:
        dense_projection_allowed = False
        notes.append("top risks block dense projection")

    if dense_projection_allowed:
        amount = 0.34 * intensity_scale * tone_scale
        return _make_profile(
            RoleName.PROJECTION,
            "projection_controlled_dense",
            "projection ready and stack-safe",
            _clamp(amount, 0.28, 0.42),
            0.50,
            notes=notes,
        )

    amount = 0.18 * intensity_scale * tone_scale
    return _make_profile(
        RoleName.PROJECTION,
        "projection_mild_safe",
        "projection held conservative",
        _clamp(amount, 0.15, 0.24),
        0.30,
        notes=notes if notes else ["projection kept mild by stack logic"],
    )


def select_spark_profile(
    schema: SmartMasterAnalysis,
    tone: str,
    intensity: str,
    cleanup_profile: SelectedRoleProfile,
    guard_profile: SelectedRoleProfile,
    projection_profile: SelectedRoleProfile,
) -> SelectedRoleProfile:
    packet = schema.projection
    derived = schema.derived
    notes: list[str] = []

    if packet.readiness.value != "ready":
        return _make_profile(
            RoleName.SPARK,
            "finish_spark_off",
            "projection not ready",
            0.0,
            0.0,
            enabled=False,
            notes=["spark blocked: projection not ready"],
        )

    if packet.state == ProjectionState.UNDERPROJECTED:
        return _make_profile(
            RoleName.SPARK,
            "finish_spark_off",
            "projection still underprojected",
            0.0,
            0.0,
            enabled=False,
            notes=["spark blocked: finish cannot replace projection"],
        )

    if packet.harshness_risk == RiskLevel.HIGH or packet.sibilance_risk == RiskLevel.HIGH:
        return _make_profile(
            RoleName.SPARK,
            "finish_spark_micro_safe",
            "top risk guarded",
            0.06,
            0.14,
            notes=["spark minimized by top risk"],
        )

    if cleanup_profile.profile_name == "cleanup_focused_dense":
        return _make_profile(
            RoleName.SPARK,
            "finish_spark_off",
            "dense cleanup pass blocks spark",
            0.0,
            0.0,
            enabled=False,
            notes=["spark blocked by aggressive cleanup stack"],
        )

    if guard_profile.profile_name == "guard_boxiness_controlled":
        return _make_profile(
            RoleName.SPARK,
            "finish_spark_micro_safe",
            "active guard keeps spark micro",
            0.08,
            0.16,
            notes=["spark reduced while upper-body shape still managed"],
        )

    if projection_profile.profile_name != "projection_controlled_dense":
        return _make_profile(
            RoleName.SPARK,
            "finish_spark_micro_safe",
            "projection not dense enough for excited spark",
            0.08,
            0.16,
            notes=["spark kept micro until projection fully established"],
        )

    if _has(derived.top_push_safety_proxy) and derived.top_push_safety_proxy < 0.72:
        return _make_profile(
            RoleName.SPARK,
            "finish_spark_micro_safe",
            "top push safety moderate",
            0.08,
            0.16,
            notes=["spark kept micro by top push safety"],
        )

    return _make_profile(
        RoleName.SPARK,
        "finish_spark_controlled_excited",
        "spark allowed",
        0.18,
        0.28,
        notes=["excited spark only after safe projection stack"],
    )


def select_sm_profiles(schema: SmartMasterAnalysis, tone: str, intensity: str) -> RoleProfileSelection:
    anchor = select_anchor_profile(schema, tone, intensity)
    bridge = select_bridge_profile(schema, tone, intensity)
    cleanup = select_cleanup_profile(schema, tone, intensity)
    guard = select_guard_profile(schema, tone, intensity, cleanup)
    projection = select_projection_profile(schema, tone, intensity, cleanup, guard)
    spark = select_spark_profile(schema, tone, intensity, cleanup, guard, projection)

    return RoleProfileSelection(
        anchor=anchor,
        bridge=bridge,
        cleanup=cleanup,
        guard=guard,
        projection=projection,
        spark=spark,
    )

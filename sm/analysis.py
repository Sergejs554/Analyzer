# sm/analysis.py

from .contracts import (
    AnalysisMetrics,
    AnchorPacket,
    BridgePacket,
    CleanupPacket,
    GuardPacket,
    ProjectionPacket,
    SmartMasterAnalysis,
)
from .enums import (
    AnchorState,
    BridgeState,
    CleanupReadiness,
    UpperBodyShape,
    TransitionState,
    ProjectionReadiness,
    ProjectionState,
    RiskLevel,
)
from .metrics import collect_sm_metrics


def build_anchor_packet(metrics: AnalysisMetrics) -> AnchorPacket:
    return AnchorPacket(
        state=AnchorState.BALANCED,
        foundation_present=True,
        fragility=RiskLevel.LOW,
        notes=["stub anchor packet"],
    )


def build_bridge_packet(metrics: AnalysisMetrics) -> BridgePacket:
    return BridgePacket(
        state=BridgeState.BALANCED,
        gap_risk=RiskLevel.LOW,
        glue_risk=RiskLevel.LOW,
        notes=["stub bridge packet"],
    )


def build_cleanup_packet(metrics: AnalysisMetrics) -> CleanupPacket:
    return CleanupPacket(
        readiness=CleanupReadiness.SAFE,
        body_protection_need=RiskLevel.LOW,
        buildup_risk=RiskLevel.MEDIUM,
        notes=["stub cleanup packet"],
    )


def build_guard_packet(metrics: AnalysisMetrics) -> GuardPacket:
    return GuardPacket(
        shape=UpperBodyShape.NATURAL,
        transition_state=TransitionState.STABLE,
        thinning_risk=RiskLevel.LOW,
        notes=["stub guard packet"],
    )


def build_projection_packet(metrics: AnalysisMetrics) -> ProjectionPacket:
    return ProjectionPacket(
        readiness=ProjectionReadiness.READY,
        state=ProjectionState.UNDERPROJECTED,
        harshness_risk=RiskLevel.LOW,
        sibilance_risk=RiskLevel.LOW,
        punch_safety=RiskLevel.LOW,
        notes=["stub projection packet"],
    )


def analyze_sm_input(input_path: str) -> SmartMasterAnalysis:
    metrics = collect_sm_metrics(input_path)

    anchor = build_anchor_packet(metrics)
    bridge = build_bridge_packet(metrics)
    cleanup = build_cleanup_packet(metrics)
    guard = build_guard_packet(metrics)
    projection = build_projection_packet(metrics)

    return SmartMasterAnalysis(
        metrics=metrics,
        anchor=anchor,
        bridge=bridge,
        cleanup=cleanup,
        guard=guard,
        projection=projection,
        global_flags={},
    )

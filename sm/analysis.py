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


def _has(v):
    return v is not None


def _lt(v, x):
    return v is not None and v < x


def _le(v, x):
    return v is not None and v <= x


def _gt(v, x):
    return v is not None and v > x


def _ge(v, x):
    return v is not None and v >= x


def _count(*conds) -> int:
    return sum(1 for c in conds if c)


def _max_risk(*risks: RiskLevel) -> RiskLevel:
    if any(r == RiskLevel.HIGH for r in risks):
        return RiskLevel.HIGH
    if any(r == RiskLevel.MEDIUM for r in risks):
        return RiskLevel.MEDIUM
    return RiskLevel.LOW


def _risk_from_high_bad(v, medium_at, high_at) -> RiskLevel:
    if v is None:
        return RiskLevel.LOW
    if v >= high_at:
        return RiskLevel.HIGH
    if v >= medium_at:
        return RiskLevel.MEDIUM
    return RiskLevel.LOW


def _risk_from_low_bad(v, medium_at, high_at) -> RiskLevel:
    if v is None:
        return RiskLevel.LOW
    if v <= high_at:
        return RiskLevel.HIGH
    if v <= medium_at:
        return RiskLevel.MEDIUM
    return RiskLevel.LOW


def _infer_anchor_state(metrics: AnalysisMetrics, notes: list[str]) -> AnchorState:
    body = metrics.body_150_400_db
    low_body = metrics.low_body_150_300_db
    lowmid = metrics.lowmid_120_300_db
    buildup = metrics.lowmid_buildup_200_400_db
    mud = metrics.mud_200_500_db

    deficient_votes = _count(
        _lt(body, 31.0),
        _lt(low_body, 30.8),
        _lt(lowmid, 31.0),
    )

    excessive_votes = _count(
        _gt(body, 35.4),
        _gt(low_body, 34.8),
        _gt(lowmid, 34.7),
        _gt(buildup, 34.8),
        _gt(mud, 34.8),
    )

    if deficient_votes >= 2:
        notes.append("anchor deficient: body/low-body below support range")
        return AnchorState.DEFICIENT

    if excessive_votes >= 3:
        notes.append("anchor excessive: body cluster too heavy")
        return AnchorState.EXCESSIVE

    notes.append("anchor balanced")
    return AnchorState.BALANCED


def _infer_foundation_present(metrics: AnalysisMetrics, notes: list[str]) -> bool:
    foundation = metrics.low_foundation_50_100_db
    bass = metrics.bass_60_120_db
    sub_to_body = metrics.sub_to_body_db

    present_votes = _count(
        _ge(foundation, 41.0),
        _ge(bass, 39.0),
        _le(sub_to_body, 13.0) if _has(sub_to_body) else False,
    )

    missing_votes = _count(
        _lt(foundation, 39.5),
        _lt(bass, 37.8),
    )

    if missing_votes >= 2:
        notes.append("foundation missing: 50-120 support weak")
        return False

    if present_votes >= 2:
        notes.append("foundation present")
        return True

    notes.append("foundation assumed present")
    return True


def _infer_anchor_fragility(metrics: AnalysisMetrics, state: AnchorState) -> RiskLevel:
    body = metrics.body_150_400_db
    low_body = metrics.low_body_150_300_db
    punch = metrics.punch_proxy
    crest = metrics.crest_db

    low_body_risk = _risk_from_low_bad(low_body, 31.0, 30.0)
    body_risk = _risk_from_low_bad(body, 31.2, 29.8)
    punch_risk = _risk_from_low_bad(punch, 13.4, 12.8)
    crest_risk = _risk_from_low_bad(crest, 12.8, 12.0)

    risk = _max_risk(low_body_risk, body_risk, punch_risk, crest_risk)

    if state == AnchorState.DEFICIENT and risk == RiskLevel.MEDIUM:
        return RiskLevel.HIGH

    return risk


def build_anchor_packet(metrics: AnalysisMetrics) -> AnchorPacket:
    notes: list[str] = []

    state = _infer_anchor_state(metrics, notes)
    foundation_present = _infer_foundation_present(metrics, notes)
    fragility = _infer_anchor_fragility(metrics, state)

    stop = _count(
        _lt(metrics.body_150_400_db, 29.3),
        _lt(metrics.low_body_150_300_db, 29.0),
    ) >= 1

    warning = (
        state != AnchorState.BALANCED
        or fragility != RiskLevel.LOW
        or not foundation_present
    )

    if stop:
        notes.append("anchor stop: body collapsed too far")

    return AnchorPacket(
        state=state,
        foundation_present=foundation_present,
        fragility=fragility,
        stop=stop,
        warning=warning,
        notes=notes,
    )


def build_bridge_packet(metrics: AnalysisMetrics) -> BridgePacket:
    notes: list[str] = []

    bass_to_body = metrics.bass_to_body_db
    foundation_ratio = metrics.low_foundation_ratio_db
    sub_to_body = metrics.sub_to_body_db
    low_body = metrics.low_body_150_300_db
    lowmid = metrics.lowmid_120_300_db
    mud = metrics.mud_200_500_db
    buildup = metrics.lowmid_buildup_200_400_db

    gap_risk = _max_risk(
        _risk_from_high_bad(bass_to_body, 9.8, 10.8),
        _risk_from_high_bad(sub_to_body, 12.1, 13.0),
        _risk_from_high_bad(foundation_ratio, 12.6, 13.0),
        _risk_from_low_bad(low_body, 31.0, 30.0),
    )

    glue_risk = _max_risk(
        _risk_from_low_bad(bass_to_body, 7.7, 7.1),
        _risk_from_high_bad(mud, 33.1, 34.6),
        _risk_from_high_bad(buildup, 33.5, 35.0),
        _risk_from_high_bad(lowmid, 34.0, 35.0),
    )

    broken_votes = _count(
        gap_risk == RiskLevel.HIGH,
        _gt(bass_to_body, 10.8),
        _gt(sub_to_body, 13.0),
        _lt(low_body, 30.6),
    )

    glue_votes = _count(
        glue_risk == RiskLevel.HIGH,
        _lt(bass_to_body, 7.2),
        _gt(mud, 34.6),
        _gt(buildup, 35.0),
    )

    if broken_votes >= 2:
        state = BridgeState.BROKEN
        notes.append("bridge broken: low detached from body")
    elif glue_votes >= 2:
        state = BridgeState.OVERGLUED
        notes.append("bridge overglued: low/body too sticky")
    else:
        state = BridgeState.BALANCED
        notes.append("bridge balanced")

    stop = (
        (state == BridgeState.BROKEN and _lt(low_body, 29.8))
        or (state == BridgeState.OVERGLUED and _gt(mud, 35.2))
    )

    warning = state != BridgeState.BALANCED or gap_risk != RiskLevel.LOW or glue_risk != RiskLevel.LOW

    if stop:
        notes.append("bridge stop triggered")

    return BridgePacket(
        state=state,
        gap_risk=gap_risk,
        glue_risk=glue_risk,
        stop=stop,
        warning=warning,
        notes=notes,
    )


def build_cleanup_packet(metrics: AnalysisMetrics) -> CleanupPacket:
    notes: list[str] = []

    body = metrics.body_150_400_db
    low_body = metrics.low_body_150_300_db
    buildup = metrics.lowmid_buildup_200_400_db
    mud = metrics.mud_200_500_db
    mud_to_body = metrics.mud_to_body_db
    buildup_ratio = metrics.lowmid_buildup_ratio_db

    buildup_risk = _max_risk(
        _risk_from_high_bad(buildup, 32.8, 34.8),
        _risk_from_high_bad(mud, 32.8, 34.8),
        _risk_from_high_bad(buildup_ratio, 16.5, 18.0),
        _risk_from_high_bad(mud_to_body, -0.15, 0.20),
    )

    body_protection_need = _max_risk(
        _risk_from_low_bad(body, 31.2, 29.8),
        _risk_from_low_bad(low_body, 31.0, 29.8),
    )

    deny_votes = _count(
        _lt(body, 29.8),
        _lt(low_body, 29.8),
    )

    guarded_votes = _count(
        body_protection_need == RiskLevel.HIGH,
        _lt(body, 31.0),
        _lt(low_body, 30.8),
        _gt(buildup_ratio, 16.0),
    )

    if deny_votes >= 1:
        readiness = CleanupReadiness.DENIED
        notes.append("cleanup denied: body too fragile")
    elif guarded_votes >= 2:
        readiness = CleanupReadiness.GUARDED
        notes.append("cleanup guarded: body protection needed")
    else:
        readiness = CleanupReadiness.SAFE
        notes.append("cleanup safe")

    stop = (
        readiness == CleanupReadiness.DENIED
        and buildup_risk == RiskLevel.LOW
    )
    warning = readiness != CleanupReadiness.SAFE or buildup_risk != RiskLevel.LOW

    if stop:
        notes.append("cleanup stop: no safe meaningful cleanup headroom")

    return CleanupPacket(
        readiness=readiness,
        body_protection_need=body_protection_need,
        buildup_risk=buildup_risk,
        stop=stop,
        warning=warning,
        notes=notes,
    )


def build_guard_packet(metrics: AnalysisMetrics) -> GuardPacket:
    notes: list[str] = []

    mud_to_body = metrics.mud_to_body_db
    buildup_ratio = metrics.lowmid_buildup_ratio_db
    lowmid = metrics.lowmid_120_300_db
    mid = metrics.mid_1k_2k_db
    presence_to_body = metrics.presence_to_body_db

    boxy_votes = _count(
        _gt(mud_to_body, -0.10),
        _gt(buildup_ratio, 17.0),
        _gt(lowmid, 34.4),
    )

    if boxy_votes >= 2:
        shape = UpperBodyShape.BOXY
        notes.append("upper body boxy")
    else:
        shape = UpperBodyShape.NATURAL
        notes.append("upper body natural")

    thinning_votes = _count(
        _lt(lowmid, 30.8),
        _lt(mid, 18.3),
        _gt(presence_to_body, -14.7),
    )

    weak_votes = _count(
        _lt(lowmid, 31.8),
        _lt(mid, 18.9),
        _gt(presence_to_body, -15.5),
    )

    if thinning_votes >= 2:
        transition_state = TransitionState.THINNING
        notes.append("transition thinning")
    elif weak_votes >= 2:
        transition_state = TransitionState.WEAK
        notes.append("transition weak")
    else:
        transition_state = TransitionState.STABLE
        notes.append("transition stable")

    thinning_risk = _max_risk(
        _risk_from_low_bad(lowmid, 31.8, 30.8),
        _risk_from_low_bad(mid, 18.9, 18.2),
        _risk_from_high_bad(presence_to_body, -15.5, -14.6),
    )

    stop = transition_state == TransitionState.THINNING and thinning_risk == RiskLevel.HIGH
    warning = shape != UpperBodyShape.NATURAL or transition_state != TransitionState.STABLE

    if stop:
        notes.append("guard stop triggered")

    return GuardPacket(
        shape=shape,
        transition_state=transition_state,
        thinning_risk=thinning_risk,
        stop=stop,
        warning=warning,
        notes=notes,
    )


def build_projection_packet(metrics: AnalysisMetrics) -> ProjectionPacket:
    notes: list[str] = []

    mid = metrics.mid_1k_2k_db
    presence = metrics.presence_2k_5k_db
    presence_to_body = metrics.presence_to_body_db
    harsh = metrics.harsh_2p5k_6k_db
    harshness_index = metrics.harshness_index
    harsh_to_mid = metrics.harsh_to_mid_db
    sibilance = metrics.sibilance_5k_9k_db
    sibilance_index = metrics.sibilance_index
    punch = metrics.punch_proxy
    crest = metrics.crest_db
    body = metrics.body_150_400_db

    harshness_risk = _max_risk(
        _risk_from_high_bad(harsh, 16.8, 17.5),
        _risk_from_high_bad(harshness_index, -12.8, -11.4),
        _risk_from_high_bad(harsh_to_mid, -1.2, -0.3),
    )

    sibilance_risk = _max_risk(
        _risk_from_high_bad(sibilance, 16.6, 17.6),
        _risk_from_high_bad(sibilance_index, 2.2, 4.0),
    )

    punch_safety = _max_risk(
        _risk_from_low_bad(punch, 13.4, 12.8),
        _risk_from_low_bad(crest, 12.8, 12.1),
    )

    underprojected_votes = _count(
        _lt(mid, 18.8),
        _lt(presence, 15.9),
        _lt(presence_to_body, -16.2),
    )

    overpushed_votes = _count(
        harshness_risk == RiskLevel.HIGH,
        sibilance_risk == RiskLevel.HIGH,
        _gt(presence_to_body, -13.2),
        _gt(harsh_to_mid, -0.4),
    )

    if overpushed_votes >= 2:
        state = ProjectionState.OVERPUSHED
        notes.append("projection overpushed")
    elif underprojected_votes >= 2:
        state = ProjectionState.UNDERPROJECTED
        notes.append("projection underprojected")
    else:
        state = ProjectionState.BALANCED
        notes.append("projection balanced")

    deny_votes = _count(
        harshness_risk == RiskLevel.HIGH and sibilance_risk == RiskLevel.HIGH,
        _lt(body, 30.0) and _gt(presence_to_body, -15.0),
    )

    guarded_votes = _count(
        harshness_risk != RiskLevel.LOW,
        sibilance_risk != RiskLevel.LOW,
        punch_safety != RiskLevel.LOW,
        _lt(body, 31.0),
    )

    if deny_votes >= 1:
        readiness = ProjectionReadiness.DENIED
        notes.append("projection denied: top risk too high")
    elif guarded_votes >= 2:
        readiness = ProjectionReadiness.GUARDED
        notes.append("projection guarded")
    else:
        readiness = ProjectionReadiness.READY
        notes.append("projection ready")

    stop = readiness == ProjectionReadiness.DENIED and state == ProjectionState.OVERPUSHED
    warning = readiness != ProjectionReadiness.READY or state != ProjectionState.BALANCED

    if stop:
        notes.append("projection stop triggered")

    return ProjectionPacket(
        readiness=readiness,
        state=state,
        harshness_risk=harshness_risk,
        sibilance_risk=sibilance_risk,
        punch_safety=punch_safety,
        stop=stop,
        warning=warning,
        notes=notes,
    )


def analyze_sm_input(input_path: str) -> SmartMasterAnalysis:
    metrics = collect_sm_metrics(input_path)

    anchor = build_anchor_packet(metrics)
    bridge = build_bridge_packet(metrics)
    cleanup = build_cleanup_packet(metrics)
    guard = build_guard_packet(metrics)
    projection = build_projection_packet(metrics)

    dense_behavior = (
        anchor.state != AnchorState.DEFICIENT
        and anchor.foundation_present
        and anchor.fragility != RiskLevel.HIGH
        and cleanup.readiness != CleanupReadiness.DENIED
    )

    thin_behavior = (
        anchor.state == AnchorState.DEFICIENT
        or not anchor.foundation_present
        or bridge.state == BridgeState.BROKEN
        or anchor.fragility == RiskLevel.HIGH
        or cleanup.readiness in (CleanupReadiness.GUARDED, CleanupReadiness.DENIED)
    )

    top_risk = (
        projection.harshness_risk == RiskLevel.HIGH
        or projection.sibilance_risk == RiskLevel.HIGH
    )

    punch_fragile = projection.punch_safety in (RiskLevel.MEDIUM, RiskLevel.HIGH)

    return SmartMasterAnalysis(
        metrics=metrics,
        anchor=anchor,
        bridge=bridge,
        cleanup=cleanup,
        guard=guard,
        projection=projection,
        global_flags={
            "dense_behavior_candidate": dense_behavior,
            "thin_behavior_candidate": thin_behavior,
            "top_risk_candidate": top_risk,
            "punch_fragile_candidate": punch_fragile,
        },
    )

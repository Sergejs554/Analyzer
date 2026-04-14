# sm/contracts.py

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List

from .enums import (
    AnchorState,
    BridgeState,
    CleanupReadiness,
    UpperBodyShape,
    TransitionState,
    ProjectionReadiness,
    ProjectionState,
    RiskLevel,
    RoleName,
)


@dataclass
class AnalysisMetrics:
    # Body / support core
    body_150_400_db: Optional[float] = None
    low_body_150_300_db: Optional[float] = None
    lowmid_120_300_db: Optional[float] = None

    # Buildup / mud
    lowmid_buildup_200_400_db: Optional[float] = None
    mud_200_500_db: Optional[float] = None
    mud_to_body_db: Optional[float] = None
    lowmid_buildup_ratio_db: Optional[float] = None

    # Bass/body connection
    bass_to_body_db: Optional[float] = None
    low_foundation_ratio_db: Optional[float] = None
    sub_to_body_db: Optional[float] = None
    low_foundation_50_100_db: Optional[float] = None
    bass_60_120_db: Optional[float] = None

    # Mid / projection handoff
    mid_1k_2k_db: Optional[float] = None
    presence_2k_5k_db: Optional[float] = None
    presence_to_body_db: Optional[float] = None

    # Harsh / sibilance
    harsh_2p5k_6k_db: Optional[float] = None
    harshness_index: Optional[float] = None
    harsh_to_mid_db: Optional[float] = None
    sibilance_5k_9k_db: Optional[float] = None
    sibilance_index: Optional[float] = None

    # Air / top contour
    air_8k_12k_db: Optional[float] = None
    air_8k_16k_db: Optional[float] = None
    air16_to_body_db: Optional[float] = None
    air_ratio_db: Optional[float] = None
    tilt_indicator_db: Optional[float] = None

    # Dynamics / punch / delivery
    crest_db: Optional[float] = None
    punch_proxy: Optional[float] = None
    plr_proxy_db: Optional[float] = None
    integrated_lufs: Optional[float] = None
    true_peak_dbtp: Optional[float] = None

    # Stress / context
    near_clip_ratio: Optional[float] = None
    limiter_stress_proxy: Optional[float] = None
    transient_index: Optional[float] = None
    momentary_to_integrated_gap_db: Optional[float] = None
    short_term_to_integrated_gap_db: Optional[float] = None

    # Useful extras
    rms_dbfs: Optional[float] = None
    sample_peak_dbfs: Optional[float] = None
    lra_ebu: Optional[float] = None


@dataclass
class DerivedIndicators:
    center_body_support_proxy: Optional[float] = None
    body_to_mid_handoff_proxy: Optional[float] = None
    top_push_safety_proxy: Optional[float] = None


@dataclass
class AnchorPacket:
    state: AnchorState
    foundation_present: bool
    fragility: RiskLevel
    stop: bool = False
    warning: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass
class BridgePacket:
    state: BridgeState
    gap_risk: RiskLevel
    glue_risk: RiskLevel
    stop: bool = False
    warning: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass
class CleanupPacket:
    readiness: CleanupReadiness
    body_protection_need: RiskLevel
    buildup_risk: RiskLevel
    stop: bool = False
    warning: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass
class GuardPacket:
    shape: UpperBodyShape
    transition_state: TransitionState
    thinning_risk: RiskLevel
    stop: bool = False
    warning: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass
class ProjectionPacket:
    readiness: ProjectionReadiness
    state: ProjectionState
    harshness_risk: RiskLevel
    sibilance_risk: RiskLevel
    punch_safety: RiskLevel
    stop: bool = False
    warning: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass
class SmartMasterAnalysis:
    metrics: AnalysisMetrics
    derived: DerivedIndicators
    anchor: AnchorPacket
    bridge: BridgePacket
    cleanup: CleanupPacket
    guard: GuardPacket
    projection: ProjectionPacket
    global_flags: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SelectedRoleProfile:
    role: RoleName
    profile_name: str
    reason: str
    amount: float
    cap: float
    enabled: bool = True
    forced_clamp: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass
class RoleProfileSelection:
    anchor: SelectedRoleProfile
    bridge: SelectedRoleProfile
    cleanup: SelectedRoleProfile
    guard: SelectedRoleProfile
    projection: SelectedRoleProfile
    spark: SelectedRoleProfile


@dataclass
class RoleExecutionPlan:
    role: RoleName
    enabled: bool

    profile_name: str
    role_rank: str
    energy_class: str

    requested_amount: float
    requested_cap: float

    execution_amount: float
    execution_cap: float
    dynamic_scale: float

    target_band_mode: str
    protection_mode: str

    allowed_primitives: List[str]
    forbidden_primitives: List[str]

    interaction_tags: List[str]
    notes: List[str] = field(default_factory=list)


@dataclass
class SmartMasterExecutionBlueprint:
    anchor: RoleExecutionPlan
    bridge: RoleExecutionPlan
    cleanup: RoleExecutionPlan
    guard: RoleExecutionPlan
    projection: RoleExecutionPlan
    spark: RoleExecutionPlan

    primary_correction_lane: str
    secondary_support_lane: str
    primary_benefit_lane: str

    assembly_order: List[str]
    global_notes: List[str] = field(default_factory=list)


@dataclass
class RouterContext:
    analysis: SmartMasterAnalysis
    selection: RoleProfileSelection

    primary_correction_lane: str
    secondary_support_lane: str
    primary_benefit_lane: str

    top_safe: bool
    top_guarded: bool
    top_fragile: bool

    body_fragile: bool
    body_weak: bool
    foundation_missing: bool

    bridge_broken: bool
    bridge_gluey: bool
    bridge_gap_risky: bool

    cleanup_heavy_needed: bool
    cleanup_guarded: bool

    boxy_active: bool
    transition_fragile: bool
    underprojected: bool
    overpushed: bool

    dirty_dense_candidate: bool
    dense_good_candidate: bool
    thin_candidate: bool


@dataclass
class SmartMasterDebugBundle:
    analysis: SmartMasterAnalysis
    selection: RoleProfileSelection
    router: SmartMasterExecutionBlueprint
    dsp: Dict[str, Any]
    render_plan: Dict[str, Any]

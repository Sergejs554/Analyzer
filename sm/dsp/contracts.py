# sm/dsp/contracts.py

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..enums import RoleName


# ------------------------------------------------------------
# Primitive-level contracts
# ------------------------------------------------------------

@dataclass
class DSPPrimitiveSpec:
    primitive_name: str
    primitive_class: str

    # Execution topology
    path_type: str               # inplace / parallel / finish / delivery
    band_scope: str              # low_body / bridge / cleanup_zone / transition / presence / top / full
    channel_scope: str           # stereo / mid / side / mid_side_split
    detector_mode: str           # none / rms / peak / envelope / band_envelope
    phase_policy: str            # minimum / natural / linear / mixed

    # Safety / permissions
    legal_roles: List[RoleName] = field(default_factory=list)
    safety_tags: List[str] = field(default_factory=list)

    # Execution envelope
    min_amount: float = 0.0
    default_amount: float = 0.0
    max_amount: float = 1.0

    min_blend: float = 0.0
    default_blend: float = 0.0
    max_blend: float = 1.0

    min_gain_db: float = -12.0
    default_gain_db: float = 0.0
    max_gain_db: float = 12.0

    min_freq_hz: float = 20.0
    max_freq_hz: float = 20000.0

    min_q: float = 0.1
    max_q: float = 8.0

    min_attack_ms: float = 0.1
    max_attack_ms: float = 500.0
    min_release_ms: float = 1.0
    max_release_ms: float = 5000.0

    notes: List[str] = field(default_factory=list)


@dataclass
class DSPPrimitiveParams:
    freq_hz: Optional[float] = None
    q: Optional[float] = None
    gain_db: Optional[float] = None
    blend: Optional[float] = None

    attack_ms: Optional[float] = None
    release_ms: Optional[float] = None
    threshold_db: Optional[float] = None
    ratio: Optional[float] = None

    drive: Optional[float] = None
    width_amount: Optional[float] = None
    tilt_db_per_oct: Optional[float] = None

    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DSPPrimitiveInstance:
    primitive_name: str
    primitive_class: str
    enabled: bool

    role: RoleName
    stage_name: str
    stack_kind: str              # corrective_core / support_parallel / projection_contour / projection_assist / finish / delivery

    path_type: str
    target_zone: str
    channel_scope: str
    detector_mode: str
    phase_policy: str

    requested_amount: float
    execution_amount: float
    execution_cap: float
    dynamic_scale: float

    params: DSPPrimitiveParams = field(default_factory=DSPPrimitiveParams)

    safety_tags: List[str] = field(default_factory=list)
    active_clamps: List[str] = field(default_factory=list)
    blocked_reasons: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


# ------------------------------------------------------------
# Role-level DSP contracts
# ------------------------------------------------------------

@dataclass
class RoleDSPStack:
    role: RoleName
    enabled: bool

    stack_name: str
    stack_kind: str              # corrective_core / support_parallel / projection_contour / projection_assist / finish / delivery
    path_type: str               # inplace / parallel / finish / delivery

    tap_point: str               # prepared_input / post_cleanup / post_guard / post_support / post_projection ...
    output_node: str
    recombine_target: Optional[str] = None

    role_rank: str = "off"       # primary / support / restrained / off
    target_band_mode: str = "off"
    protection_mode: str = "off"

    requested_amount: float = 0.0
    requested_cap: float = 0.0

    execution_amount: float = 0.0
    execution_cap: float = 0.0
    dynamic_scale: float = 0.0

    allowed_primitive_names: List[str] = field(default_factory=list)
    forbidden_primitive_names: List[str] = field(default_factory=list)

    primitive_instances: List[DSPPrimitiveInstance] = field(default_factory=list)

    safety_tags: List[str] = field(default_factory=list)
    active_clamps: List[str] = field(default_factory=list)
    blocked_actions: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


# ------------------------------------------------------------
# Clamp / graph / stage contracts
# ------------------------------------------------------------

@dataclass
class DSPActiveClamp:
    clamp_name: str
    severity: str                # low / medium / high / hard

    source: str                  # router / graph / delivery / role_interaction
    reason: str

    target_roles: List[RoleName] = field(default_factory=list)
    target_primitives: List[str] = field(default_factory=list)

    actions: Dict[str, Any] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


@dataclass
class DSPRecombinePlan:
    recombine_name: str
    recombine_mode: str          # sum / weighted_sum / guarded_sum / finish_sum
    source_nodes: List[str] = field(default_factory=list)
    target_node: str = ""

    gain_db: float = 0.0
    blend: float = 1.0

    safety_tags: List[str] = field(default_factory=list)
    active_clamps: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


@dataclass
class DSPStagePlan:
    stage_name: str              # cleanup_core / guard_core / support_assembly / projection_assembly / finish_assembly / delivery_protect
    stage_kind: str              # corrective / support / projection / finish / delivery

    input_node: str
    output_node: str

    role_order: List[RoleName] = field(default_factory=list)
    role_stacks: List[RoleDSPStack] = field(default_factory=list)

    recombine_plans: List[DSPRecombinePlan] = field(default_factory=list)
    active_clamps: List[str] = field(default_factory=list)
    safety_tags: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


# ------------------------------------------------------------
# Final execution blueprint
# ------------------------------------------------------------

@dataclass
class DSPExecutionBlueprint:
    blueprint_name: str = "sm_dsp_execution_v1"

    prepared_input_node: str = "prepared_input"
    final_output_node: str = "final_output"

    cleanup_stack: Optional[RoleDSPStack] = None
    guard_stack: Optional[RoleDSPStack] = None
    anchor_parallel_stack: Optional[RoleDSPStack] = None
    bridge_parallel_stack: Optional[RoleDSPStack] = None
    projection_contour_stack: Optional[RoleDSPStack] = None
    projection_assist_stack: Optional[RoleDSPStack] = None
    spark_stack: Optional[RoleDSPStack] = None
    delivery_stack: Optional[RoleDSPStack] = None

    stage_plans: List[DSPStagePlan] = field(default_factory=list)
    recombine_plans: List[DSPRecombinePlan] = field(default_factory=list)

    support_recombine_gain_db: float = 0.0
    projection_assist_blend: float = 0.0
    spark_blend: float = 0.0

    active_clamps: List[DSPActiveClamp] = field(default_factory=list)
    blocked_actions: List[str] = field(default_factory=list)
    safety_notes: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

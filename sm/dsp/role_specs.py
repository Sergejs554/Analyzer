# sm/dsp/role_specs.py

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from ..enums import RoleName
from .primitives import PRIMITIVE_REGISTRY, has_primitive


ALL_PRIMITIVE_NAMES = set(PRIMITIVE_REGISTRY.keys())


# ------------------------------------------------------------
# Role mode / stack templates
# ------------------------------------------------------------

@dataclass
class RoleStackTemplate:
    stack_name: str
    stack_kind: str                  # corrective_core / support_parallel / projection_contour / projection_assist / finish / delivery
    path_type: str                   # inplace / parallel / finish / delivery

    default_tap_point: str
    output_node: str
    recombine_target: Optional[str] = None
    recombine_mode: str = "none"     # none / guarded_sum / assist_sum / finish_sum / sum

    preferred_order: int = 0

    allowed_primitive_names: List[str] = field(default_factory=list)
    required_safety_tags: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


@dataclass
class RoleModeSpec:
    role: RoleName
    target_band_mode: str

    role_rank_bias: str              # primary / support / restrained / off
    stack_templates: List[RoleStackTemplate] = field(default_factory=list)

    forbidden_primitive_names: List[str] = field(default_factory=list)
    required_safety_tags: List[str] = field(default_factory=list)
    blocked_by_default_clamps: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


def _stack(
    stack_name: str,
    stack_kind: str,
    path_type: str,
    default_tap_point: str,
    output_node: str,
    *,
    recombine_target: Optional[str] = None,
    recombine_mode: str = "none",
    preferred_order: int = 0,
    allowed_primitive_names: Optional[List[str]] = None,
    required_safety_tags: Optional[List[str]] = None,
    notes: Optional[List[str]] = None,
) -> RoleStackTemplate:
    return RoleStackTemplate(
        stack_name=stack_name,
        stack_kind=stack_kind,
        path_type=path_type,
        default_tap_point=default_tap_point,
        output_node=output_node,
        recombine_target=recombine_target,
        recombine_mode=recombine_mode,
        preferred_order=preferred_order,
        allowed_primitive_names=allowed_primitive_names or [],
        required_safety_tags=required_safety_tags or [],
        notes=notes or [],
    )


def _forbidden_except(allowed: List[str]) -> List[str]:
    allowed_set = set(allowed)
    return sorted(name for name in ALL_PRIMITIVE_NAMES if name not in allowed_set)


def _mode(
    role: RoleName,
    target_band_mode: str,
    role_rank_bias: str,
    stack_templates: List[RoleStackTemplate],
    *,
    required_safety_tags: Optional[List[str]] = None,
    blocked_by_default_clamps: Optional[List[str]] = None,
    notes: Optional[List[str]] = None,
) -> RoleModeSpec:
    allowed: List[str] = []
    for template in stack_templates:
        allowed.extend(template.allowed_primitive_names)

    return RoleModeSpec(
        role=role,
        target_band_mode=target_band_mode,
        role_rank_bias=role_rank_bias,
        stack_templates=stack_templates,
        forbidden_primitive_names=_forbidden_except(allowed),
        required_safety_tags=required_safety_tags or [],
        blocked_by_default_clamps=blocked_by_default_clamps or [],
        notes=notes or [],
    )


# ------------------------------------------------------------
# Node naming
# ------------------------------------------------------------

NODE_PREPARED_INPUT = "prepared_input"
NODE_CLEANUP_OUT = "cleanup_core_out"
NODE_GUARD_OUT = "guard_core_out"

NODE_ANCHOR_OUT = "anchor_parallel_out"
NODE_BRIDGE_OUT = "bridge_parallel_out"
NODE_SUPPORT_BUS = "support_parallel_bus"
NODE_SUPPORT_OUT = "support_stage_out"

NODE_PROJECTION_CONTOUR_OUT = "projection_contour_out"
NODE_PROJECTION_ASSIST_OUT = "projection_assist_out"
NODE_PROJECTION_OUT = "projection_stage_out"

NODE_SPARK_OUT = "spark_finish_out"
NODE_FINISH_OUT = "finish_stage_out"


# ------------------------------------------------------------
# Role mode registry
# ------------------------------------------------------------

ROLE_MODE_SPECS: Dict[Tuple[RoleName, str], RoleModeSpec] = {
    # --------------------------------------------------------
    # Anchor
    # --------------------------------------------------------
    (RoleName.ANCHOR, "body_restore"): _mode(
        role=RoleName.ANCHOR,
        target_band_mode="body_restore",
        role_rank_bias="support",
        stack_templates=[
            _stack(
                stack_name="anchor_parallel_stack",
                stack_kind="support_parallel",
                path_type="parallel",
                default_tap_point=NODE_GUARD_OUT,
                output_node=NODE_ANCHOR_OUT,
                recombine_target=NODE_SUPPORT_BUS,
                recombine_mode="guarded_sum",
                preferred_order=10,
                allowed_primitive_names=[
                    "controlled_bell_boost",
                    "dynamic_body_support_boost",
                    "restrained_parallel_fill",
                ],
                required_safety_tags=[
                    "body_sensitive",
                    "bridge_sensitive",
                ],
                notes=[
                    "Anchor restore must rebuild body without returning mud.",
                ],
            ),
        ],
        required_safety_tags=["body_sensitive", "bridge_sensitive"],
        blocked_by_default_clamps=[
            "body_fragile",
            "delivery_overbudget",
        ],
        notes=[
            "Body restore is supportive, not tonal repaint.",
        ],
    ),
    (RoleName.ANCHOR, "body_hold"): _mode(
        role=RoleName.ANCHOR,
        target_band_mode="body_hold",
        role_rank_bias="restrained",
        stack_templates=[
            _stack(
                stack_name="anchor_parallel_stack",
                stack_kind="support_parallel",
                path_type="parallel",
                default_tap_point=NODE_GUARD_OUT,
                output_node=NODE_ANCHOR_OUT,
                recombine_target=NODE_SUPPORT_BUS,
                recombine_mode="guarded_sum",
                preferred_order=10,
                allowed_primitive_names=[
                    "dynamic_body_support_boost",
                    "restrained_parallel_fill",
                ],
                required_safety_tags=[
                    "body_sensitive",
                ],
                notes=[
                    "Hold mode preserves body mass with lower support weight.",
                ],
            ),
        ],
        required_safety_tags=["body_sensitive"],
        blocked_by_default_clamps=[
            "body_fragile",
            "delivery_overbudget",
        ],
        notes=[
            "Body hold should feel like retained mass, not audible EQ.",
        ],
    ),
    (RoleName.ANCHOR, "body_restrain"): _mode(
        role=RoleName.ANCHOR,
        target_band_mode="body_restrain",
        role_rank_bias="restrained",
        stack_templates=[
            _stack(
                stack_name="anchor_parallel_stack",
                stack_kind="support_parallel",
                path_type="parallel",
                default_tap_point=NODE_GUARD_OUT,
                output_node=NODE_ANCHOR_OUT,
                recombine_target=NODE_SUPPORT_BUS,
                recombine_mode="guarded_sum",
                preferred_order=10,
                allowed_primitive_names=[
                    "restrained_parallel_fill",
                ],
                required_safety_tags=[
                    "body_sensitive",
                    "delivery_sensitive",
                ],
                notes=[
                    "Anchor restrain in V1 means support suppression, not subtractive cutting.",
                ],
            ),
        ],
        required_safety_tags=["body_sensitive", "delivery_sensitive"],
        blocked_by_default_clamps=[
            "body_fragile",
        ],
        notes=[
            "This mode exists to avoid over-support, not to carve body away.",
        ],
    ),
    (RoleName.ANCHOR, "off"): _mode(
        role=RoleName.ANCHOR,
        target_band_mode="off",
        role_rank_bias="off",
        stack_templates=[],
        notes=["Anchor off."],
    ),

    # --------------------------------------------------------
    # Bridge
    # --------------------------------------------------------
    (RoleName.BRIDGE, "bridge_restore"): _mode(
        role=RoleName.BRIDGE,
        target_band_mode="bridge_restore",
        role_rank_bias="support",
        stack_templates=[
            _stack(
                stack_name="bridge_parallel_stack",
                stack_kind="support_parallel",
                path_type="parallel",
                default_tap_point=NODE_GUARD_OUT,
                output_node=NODE_BRIDGE_OUT,
                recombine_target=NODE_SUPPORT_BUS,
                recombine_mode="guarded_sum",
                preferred_order=20,
                allowed_primitive_names=[
                    "restrained_parallel_handoff_support",
                    "transient_safe_support_compression",
                ],
                required_safety_tags=[
                    "bridge_sensitive",
                    "body_sensitive",
                ],
                notes=[
                    "Bridge restore should reconnect handoff, not widen the low-mid blob.",
                ],
            ),
        ],
        required_safety_tags=["bridge_sensitive", "body_sensitive"],
        blocked_by_default_clamps=[
            "bridge_gluey",
            "delivery_overbudget",
        ],
        notes=[
            "Bridge restore is structural support, never warmth-hype.",
        ],
    ),
    (RoleName.BRIDGE, "bridge_hold"): _mode(
        role=RoleName.BRIDGE,
        target_band_mode="bridge_hold",
        role_rank_bias="restrained",
        stack_templates=[
            _stack(
                stack_name="bridge_parallel_stack",
                stack_kind="support_parallel",
                path_type="parallel",
                default_tap_point=NODE_GUARD_OUT,
                output_node=NODE_BRIDGE_OUT,
                recombine_target=NODE_SUPPORT_BUS,
                recombine_mode="guarded_sum",
                preferred_order=20,
                allowed_primitive_names=[
                    "restrained_parallel_handoff_support",
                ],
                required_safety_tags=[
                    "bridge_sensitive",
                    "body_sensitive",
                ],
                notes=[
                    "Bridge hold keeps continuity without adding obvious glue.",
                ],
            ),
        ],
        required_safety_tags=["bridge_sensitive", "body_sensitive"],
        blocked_by_default_clamps=[
            "bridge_gluey",
            "delivery_overbudget",
        ],
        notes=[
            "Hold mode is subtle continuity retention.",
        ],
    ),
    (RoleName.BRIDGE, "bridge_restrain"): _mode(
        role=RoleName.BRIDGE,
        target_band_mode="bridge_restrain",
        role_rank_bias="restrained",
        stack_templates=[
            _stack(
                stack_name="bridge_parallel_stack",
                stack_kind="support_parallel",
                path_type="parallel",
                default_tap_point=NODE_GUARD_OUT,
                output_node=NODE_BRIDGE_OUT,
                recombine_target=NODE_SUPPORT_BUS,
                recombine_mode="guarded_sum",
                preferred_order=20,
                allowed_primitive_names=[
                    "transient_safe_support_compression",
                ],
                required_safety_tags=[
                    "bridge_sensitive",
                    "delivery_sensitive",
                ],
                notes=[
                    "Bridge restrain in V1 is support suppression and glue calming, not subtractive EQ.",
                ],
            ),
        ],
        required_safety_tags=["bridge_sensitive", "delivery_sensitive"],
        blocked_by_default_clamps=[],
        notes=[
            "Restrain mode prevents over-glue rather than cutting bridge body directly.",
        ],
    ),
    (RoleName.BRIDGE, "off"): _mode(
        role=RoleName.BRIDGE,
        target_band_mode="off",
        role_rank_bias="off",
        stack_templates=[],
        notes=["Bridge off."],
    ),

    # --------------------------------------------------------
    # Cleanup
    # --------------------------------------------------------
    (RoleName.CLEANUP, "cleanup_dense"): _mode(
        role=RoleName.CLEANUP,
        target_band_mode="cleanup_dense",
        role_rank_bias="primary",
        stack_templates=[
            _stack(
                stack_name="cleanup_core_stack",
                stack_kind="corrective_core",
                path_type="inplace",
                default_tap_point=NODE_PREPARED_INPUT,
                output_node=NODE_CLEANUP_OUT,
                preferred_order=10,
                allowed_primitive_names=[
                    "dynamic_bell_cut",
                    "dynamic_wide_cut",
                    "restrained_static_cut",
                    "dynamic_tilt_down",
                    "local_antiharsh_control",
                ],
                required_safety_tags=[
                    "body_sensitive",
                    "bridge_sensitive",
                    "top_sensitive",
                ],
                notes=[
                    "Dense cleanup is the main corrective lane.",
                    "Should separate buildup from useful body, not scoop blindly.",
                ],
            ),
        ],
        required_safety_tags=["body_sensitive", "bridge_sensitive", "top_sensitive"],
        blocked_by_default_clamps=[
            "body_fragile",
            "bridge_broken",
            "thin_candidate",
        ],
        notes=[
            "Dense cleanup only when the track truly carries removable buildup.",
        ],
    ),
    (RoleName.CLEANUP, "cleanup_guarded"): _mode(
        role=RoleName.CLEANUP,
        target_band_mode="cleanup_guarded",
        role_rank_bias="support",
        stack_templates=[
            _stack(
                stack_name="cleanup_core_stack",
                stack_kind="corrective_core",
                path_type="inplace",
                default_tap_point=NODE_PREPARED_INPUT,
                output_node=NODE_CLEANUP_OUT,
                preferred_order=10,
                allowed_primitive_names=[
                    "dynamic_bell_cut",
                    "restrained_static_cut",
                    "local_antiharsh_control",
                ],
                required_safety_tags=[
                    "body_sensitive",
                    "bridge_sensitive",
                ],
                notes=[
                    "Guarded cleanup preserves more body and bridge continuity.",
                ],
            ),
        ],
        required_safety_tags=["body_sensitive", "bridge_sensitive"],
        blocked_by_default_clamps=[],
        notes=[
            "Use when correction is needed but body or bridge are sensitive.",
        ],
    ),
    (RoleName.CLEANUP, "cleanup_micro"): _mode(
        role=RoleName.CLEANUP,
        target_band_mode="cleanup_micro",
        role_rank_bias="restrained",
        stack_templates=[
            _stack(
                stack_name="cleanup_core_stack",
                stack_kind="corrective_core",
                path_type="inplace",
                default_tap_point=NODE_PREPARED_INPUT,
                output_node=NODE_CLEANUP_OUT,
                preferred_order=10,
                allowed_primitive_names=[
                    "dynamic_bell_cut",
                    "restrained_static_cut",
                ],
                required_safety_tags=[
                    "body_sensitive",
                ],
                notes=[
                    "Micro cleanup is for touch-up only.",
                ],
            ),
        ],
        required_safety_tags=["body_sensitive"],
        blocked_by_default_clamps=[],
        notes=[
            "Should not become the main tonal shaper.",
        ],
    ),
    (RoleName.CLEANUP, "off"): _mode(
        role=RoleName.CLEANUP,
        target_band_mode="off",
        role_rank_bias="off",
        stack_templates=[],
        notes=["Cleanup off."],
    ),

    # --------------------------------------------------------
    # Guard
    # --------------------------------------------------------
    (RoleName.GUARD, "guard_boxiness"): _mode(
        role=RoleName.GUARD,
        target_band_mode="guard_boxiness",
        role_rank_bias="support",
        stack_templates=[
            _stack(
                stack_name="guard_core_stack",
                stack_kind="corrective_core",
                path_type="inplace",
                default_tap_point=NODE_CLEANUP_OUT,
                output_node=NODE_GUARD_OUT,
                preferred_order=20,
                allowed_primitive_names=[
                    "dynamic_bell_cut",
                    "restrained_static_cut",
                ],
                required_safety_tags=[
                    "body_sensitive",
                    "bridge_sensitive",
                ],
                notes=[
                    "Guard boxiness fixes congestion after cleanup without hollowing the body.",
                ],
            ),
        ],
        required_safety_tags=["body_sensitive", "bridge_sensitive"],
        blocked_by_default_clamps=[],
        notes=[
            "Guard is structural protection, never decorative EQ.",
        ],
    ),
    (RoleName.GUARD, "guard_transition_support"): _mode(
        role=RoleName.GUARD,
        target_band_mode="guard_transition_support",
        role_rank_bias="support",
        stack_templates=[
            _stack(
                stack_name="guard_core_stack",
                stack_kind="corrective_core",
                path_type="inplace",
                default_tap_point=NODE_CLEANUP_OUT,
                output_node=NODE_GUARD_OUT,
                preferred_order=20,
                allowed_primitive_names=[
                    "dynamic_bell_cut",
                    "restrained_static_cut",
                ],
                required_safety_tags=[
                    "body_sensitive",
                    "bridge_sensitive",
                ],
                notes=[
                    "In V1 transition support is encoded as milder guard behavior, not additive lift.",
                ],
            ),
        ],
        required_safety_tags=["body_sensitive", "bridge_sensitive"],
        blocked_by_default_clamps=[],
        notes=[
            "Transition support should preserve continuity, not brighten the upper body.",
        ],
    ),
    (RoleName.GUARD, "guard_hold"): _mode(
        role=RoleName.GUARD,
        target_band_mode="guard_hold",
        role_rank_bias="restrained",
        stack_templates=[
            _stack(
                stack_name="guard_core_stack",
                stack_kind="corrective_core",
                path_type="inplace",
                default_tap_point=NODE_CLEANUP_OUT,
                output_node=NODE_GUARD_OUT,
                preferred_order=20,
                allowed_primitive_names=[
                    "restrained_static_cut",
                ],
                required_safety_tags=[
                    "body_sensitive",
                ],
                notes=[
                    "Guard hold is minimal form protection.",
                ],
            ),
        ],
        required_safety_tags=["body_sensitive"],
        blocked_by_default_clamps=[],
        notes=[
            "Hold mode should barely move unless the structure needs it.",
        ],
    ),
    (RoleName.GUARD, "off"): _mode(
        role=RoleName.GUARD,
        target_band_mode="off",
        role_rank_bias="off",
        stack_templates=[],
        notes=["Guard off."],
    ),

    # --------------------------------------------------------
    # Projection
    # --------------------------------------------------------
    (RoleName.PROJECTION, "projection_dense"): _mode(
        role=RoleName.PROJECTION,
        target_band_mode="projection_dense",
        role_rank_bias="primary",
        stack_templates=[
            _stack(
                stack_name="projection_contour_stack",
                stack_kind="projection_contour",
                path_type="inplace",
                default_tap_point=NODE_SUPPORT_OUT,
                output_node=NODE_PROJECTION_CONTOUR_OUT,
                preferred_order=30,
                allowed_primitive_names=[
                    "broad_presence_contour",
                    "dynamic_presence_lift",
                    "projection_local_deharsh",
                ],
                required_safety_tags=[
                    "top_sensitive",
                    "body_sensitive",
                ],
                notes=[
                    "Contour builds real studio-forward center image.",
                ],
            ),
            _stack(
                stack_name="projection_assist_stack",
                stack_kind="projection_assist",
                path_type="parallel",
                default_tap_point=NODE_PROJECTION_CONTOUR_OUT,
                output_node=NODE_PROJECTION_ASSIST_OUT,
                recombine_target=NODE_PROJECTION_OUT,
                recombine_mode="assist_sum",
                preferred_order=40,
                allowed_primitive_names=[
                    "band_limited_soft_saturation",
                    "controlled_harmonic_density",
                ],
                required_safety_tags=[
                    "top_sensitive",
                    "delivery_sensitive",
                ],
                notes=[
                    "Assist adds density, never fake brightness.",
                ],
            ),
        ],
        required_safety_tags=["top_sensitive", "delivery_sensitive"],
        blocked_by_default_clamps=[
            "cleanup_primary_active",
            "guard_active_boxiness",
            "top_fragile",
        ],
        notes=[
            "Dense projection only when body, bridge, and top safety already allow it.",
        ],
    ),
    (RoleName.PROJECTION, "projection_mild"): _mode(
        role=RoleName.PROJECTION,
        target_band_mode="projection_mild",
        role_rank_bias="restrained",
        stack_templates=[
            _stack(
                stack_name="projection_contour_stack",
                stack_kind="projection_contour",
                path_type="inplace",
                default_tap_point=NODE_SUPPORT_OUT,
                output_node=NODE_PROJECTION_CONTOUR_OUT,
                preferred_order=30,
                allowed_primitive_names=[
                    "broad_presence_contour",
                    "dynamic_presence_lift",
                    "projection_local_deharsh",
                ],
                required_safety_tags=[
                    "top_sensitive",
                ],
                notes=[
                    "Mild projection keeps forwardness controlled.",
                ],
            ),
            _stack(
                stack_name="projection_assist_stack",
                stack_kind="projection_assist",
                path_type="parallel",
                default_tap_point=NODE_PROJECTION_CONTOUR_OUT,
                output_node=NODE_PROJECTION_ASSIST_OUT,
                recombine_target=NODE_PROJECTION_OUT,
                recombine_mode="assist_sum",
                preferred_order=40,
                allowed_primitive_names=[
                    "controlled_harmonic_density",
                ],
                required_safety_tags=[
                    "top_sensitive",
                    "delivery_sensitive",
                ],
                notes=[
                    "Assist stays conservative in mild mode.",
                ],
            ),
        ],
        required_safety_tags=["top_sensitive", "delivery_sensitive"],
        blocked_by_default_clamps=[
            "top_fragile",
        ],
        notes=[
            "This is the normal protected projection mode for sensitive material.",
        ],
    ),
    (RoleName.PROJECTION, "projection_clamp"): _mode(
        role=RoleName.PROJECTION,
        target_band_mode="projection_clamp",
        role_rank_bias="restrained",
        stack_templates=[
            _stack(
                stack_name="projection_contour_stack",
                stack_kind="projection_contour",
                path_type="inplace",
                default_tap_point=NODE_SUPPORT_OUT,
                output_node=NODE_PROJECTION_CONTOUR_OUT,
                preferred_order=30,
                allowed_primitive_names=[
                    "projection_local_deharsh",
                ],
                required_safety_tags=[
                    "top_sensitive",
                ],
                notes=[
                    "Clamp mode is safety-led and contour-minimal.",
                ],
            ),
        ],
        required_safety_tags=["top_sensitive"],
        blocked_by_default_clamps=[],
        notes=[
            "Projection clamp is a safety mode, not a benefit mode.",
        ],
    ),
    (RoleName.PROJECTION, "off"): _mode(
        role=RoleName.PROJECTION,
        target_band_mode="off",
        role_rank_bias="off",
        stack_templates=[],
        notes=["Projection off."],
    ),

    # --------------------------------------------------------
    # Spark
    # --------------------------------------------------------
    (RoleName.SPARK, "spark_excited"): _mode(
        role=RoleName.SPARK,
        target_band_mode="spark_excited",
        role_rank_bias="support",
        stack_templates=[
            _stack(
                stack_name="spark_finish_stack",
                stack_kind="finish",
                path_type="finish",
                default_tap_point=NODE_PROJECTION_OUT,
                output_node=NODE_SPARK_OUT,
                recombine_target=NODE_FINISH_OUT,
                recombine_mode="finish_sum",
                preferred_order=50,
                allowed_primitive_names=[
                    "micro_air_shelf",
                    "micro_top_texture",
                    "protected_high_side_polish",
                    "micro_width_high_only",
                    "local_desibilance_control",
                ],
                required_safety_tags=[
                    "top_sensitive",
                    "width_sensitive",
                    "finish_sensitive",
                    "delivery_sensitive",
                ],
                notes=[
                    "Excited spark is still micro by law.",
                    "Width remains high-only and protected.",
                ],
            ),
        ],
        required_safety_tags=["top_sensitive", "width_sensitive", "finish_sensitive", "delivery_sensitive"],
        blocked_by_default_clamps=[
            "underprojected",
            "top_fragile",
            "mono_risk",
        ],
        notes=[
            "Spark excited is allowed only when projection is already real and safe.",
        ],
    ),
    (RoleName.SPARK, "spark_micro"): _mode(
        role=RoleName.SPARK,
        target_band_mode="spark_micro",
        role_rank_bias="restrained",
        stack_templates=[
            _stack(
                stack_name="spark_finish_stack",
                stack_kind="finish",
                path_type="finish",
                default_tap_point=NODE_PROJECTION_OUT,
                output_node=NODE_SPARK_OUT,
                recombine_target=NODE_FINISH_OUT,
                recombine_mode="finish_sum",
                preferred_order=50,
                allowed_primitive_names=[
                    "micro_air_shelf",
                    "micro_top_texture",
                    "protected_high_side_polish",
                    "local_desibilance_control",
                ],
                required_safety_tags=[
                    "top_sensitive",
                    "finish_sensitive",
                    "delivery_sensitive",
                ],
                notes=[
                    "Micro spark is finish polish only.",
                ],
            ),
        ],
        required_safety_tags=["top_sensitive", "finish_sensitive", "delivery_sensitive"],
        blocked_by_default_clamps=[
            "top_fragile",
        ],
        notes=[
            "No width by default in micro spark mode.",
        ],
    ),
    (RoleName.SPARK, "spark_off"): _mode(
        role=RoleName.SPARK,
        target_band_mode="spark_off",
        role_rank_bias="off",
        stack_templates=[],
        notes=["Spark off."],
    ),
    (RoleName.SPARK, "off"): _mode(
        role=RoleName.SPARK,
        target_band_mode="off",
        role_rank_bias="off",
        stack_templates=[],
        notes=["Spark off alias."],
    ),
}


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def get_role_mode_spec(role: RoleName, target_band_mode: str) -> RoleModeSpec:
    key = (role, target_band_mode)
    if key not in ROLE_MODE_SPECS:
        off_key = (role, "off")
        if off_key in ROLE_MODE_SPECS:
            return ROLE_MODE_SPECS[off_key]
        raise KeyError(f"Unknown role mode spec: role={role} target_band_mode={target_band_mode}")
    return ROLE_MODE_SPECS[key]


def list_role_mode_specs(role: Optional[RoleName] = None) -> List[RoleModeSpec]:
    if role is None:
        return list(ROLE_MODE_SPECS.values())
    return [spec for (r, _), spec in ROLE_MODE_SPECS.items() if r == role]


def list_stack_templates(role: RoleName, target_band_mode: str) -> List[RoleStackTemplate]:
    return get_role_mode_spec(role, target_band_mode).stack_templates


def list_allowed_primitives(role: RoleName, target_band_mode: str) -> List[str]:
    spec = get_role_mode_spec(role, target_band_mode)
    allowed: List[str] = []
    for stack in spec.stack_templates:
        allowed.extend(stack.allowed_primitive_names)
    seen = set()
    out: List[str] = []
    for name in allowed:
        if name not in seen:
            seen.add(name)
            out.append(name)
    return out


def list_forbidden_primitives(role: RoleName, target_band_mode: str) -> List[str]:
    return get_role_mode_spec(role, target_band_mode).forbidden_primitive_names


def get_primary_stack_template(role: RoleName, target_band_mode: str) -> Optional[RoleStackTemplate]:
    templates = list_stack_templates(role, target_band_mode)
    if not templates:
        return None
    return sorted(templates, key=lambda x: x.preferred_order)[0]


def validate_role_specs() -> None:
    for (role, mode), spec in ROLE_MODE_SPECS.items():
        if spec.role != role:
            raise ValueError(f"RoleModeSpec role mismatch for key {(role, mode)}")

        if spec.target_band_mode != mode:
            raise ValueError(f"RoleModeSpec target_band_mode mismatch for key {(role, mode)}")

        for template in spec.stack_templates:
            for primitive_name in template.allowed_primitive_names:
                if not has_primitive(primitive_name):
                    raise ValueError(
                        f"Unknown primitive '{primitive_name}' in role={role} mode={mode} stack={template.stack_name}"
                    )

                primitive_spec = PRIMITIVE_REGISTRY[primitive_name]
                if role not in primitive_spec.legal_roles:
                    raise ValueError(
                        f"Primitive '{primitive_name}' is not legal for role={role} "
                        f"(role mode={mode}, stack={template.stack_name})"
                    )

                if primitive_spec.path_type != template.path_type:
                    raise ValueError(
                        f"Path type mismatch for primitive '{primitive_name}' in role={role} mode={mode}: "
                        f"primitive path_type={primitive_spec.path_type}, stack path_type={template.path_type}"
                    )


validate_role_specs()

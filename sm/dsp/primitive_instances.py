from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Dict, List, Optional

from ..contracts import SmartMasterAnalysis
from .contracts import DSPExecutionBlueprint, RoleDSPStack
from .primitives import PRIMITIVE_REGISTRY


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _lerp(a: float, b: float, t: float) -> float:
    t = _clamp(t, 0.0, 1.0)
    return a + (b - a) * t


def _uniq(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        if not value:
            continue
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _safe(v: Optional[float], fallback: float) -> float:
    return fallback if v is None else float(v)


def _safe_db(v: Optional[float], fallback: float = 0.0) -> float:
    return fallback if v is None else float(v)


def _norm_stack_amount(stack: RoleDSPStack) -> float:
    if stack.execution_cap <= 1e-9:
        return 0.0
    return _clamp(stack.execution_amount / stack.execution_cap, 0.0, 1.0)


def _stack_intensity(stack: RoleDSPStack) -> float:
    amount_norm = _norm_stack_amount(stack)
    return _clamp((amount_norm * 0.62) + (stack.dynamic_scale * 0.38), 0.0, 1.0)


def _role_key(stack: RoleDSPStack) -> str:
    role = stack.role
    if hasattr(role, "value"):
        return str(role.value).strip().lower()
    return str(role).strip().lower()


def _metric_presence_center(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics
    presence_to_body = _safe(m.presence_to_body_db, -16.0)
    harsh_to_mid = _safe(m.harsh_to_mid_db, -6.0)

    if presence_to_body < -18.5:
        return 2200.0
    if harsh_to_mid > -4.5:
        return 2600.0
    return 2400.0


def _metric_cleanup_center(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics
    mud = _safe(m.mud_200_500_db, 34.0)
    buildup = _safe(m.lowmid_buildup_200_400_db, 34.0)
    low_body = _safe(m.low_body_150_300_db, 32.0)

    if buildup - low_body > 1.2:
        return 250.0
    if mud >= 37.0:
        return 300.0
    return 270.0


def _metric_guard_center(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics
    lowmid = _safe(m.lowmid_120_300_db, 33.0)
    mud = _safe(m.mud_200_500_db, 34.0)

    if mud - lowmid > 2.0:
        return 420.0
    if mud > 36.0:
        return 360.0
    return 320.0


def _metric_anchor_center(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics
    bass_to_body = _safe(m.bass_to_body_db, 6.0)

    if bass_to_body < 3.5:
        return 170.0
    if bass_to_body < 6.0:
        return 190.0
    return 210.0


def _metric_bridge_center(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics
    sub_to_body = _safe(m.sub_to_body_db, 4.5)
    bass_to_body = _safe(m.bass_to_body_db, 5.0)

    if sub_to_body < 2.5:
        return 120.0
    if bass_to_body < 4.0:
        return 145.0
    return 160.0


def _metric_harsh_center(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics
    harsh = _safe(m.harsh_2p5k_6k_db, 16.0)
    sibilance = _safe(m.sibilance_5k_9k_db, 15.5)

    if harsh > 18.0:
        return 4300.0
    if sibilance > 16.5:
        return 5200.0
    return 4700.0


def _metric_sibilance_center(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics
    sibilance = _safe(m.sibilance_5k_9k_db, 15.5)
    if sibilance > 16.5:
        return 7100.0
    if sibilance > 15.5:
        return 6600.0
    return 6100.0


def _metric_air_center(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics
    air_ratio = _safe(m.air_ratio_db, -18.0)
    if air_ratio < -23.0:
        return 11000.0
    if air_ratio < -19.0:
        return 10250.0
    return 9600.0


def _delivery_hot_score(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics

    true_peak_dbtp = _safe(m.true_peak_dbtp, -1.0)
    integrated_lufs = _safe(m.integrated_lufs, -12.0)
    limiter_stress_proxy = _safe(m.limiter_stress_proxy, 0.0)
    near_clip_ratio = _safe(m.near_clip_ratio, 0.0)

    tp_hot = _clamp((true_peak_dbtp + 0.15) / 1.60, 0.0, 1.0)
    loud_hot = _clamp((integrated_lufs + 8.40) / 2.20, 0.0, 1.0)
    stress_hot = _clamp((limiter_stress_proxy - 0.88) / 0.18, 0.0, 1.0)
    clip_hot = _clamp(near_clip_ratio / 0.0040, 0.0, 1.0)

    return _clamp(
        (tp_hot * 0.42)
        + (loud_hot * 0.20)
        + (stress_hot * 0.23)
        + (clip_hot * 0.15),
        0.0,
        1.0,
    )


def _delivery_quiet_score(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics

    true_peak_dbtp = _safe(m.true_peak_dbtp, -1.0)
    integrated_lufs = _safe(m.integrated_lufs, -12.0)
    limiter_stress_proxy = _safe(m.limiter_stress_proxy, 0.0)
    near_clip_ratio = _safe(m.near_clip_ratio, 0.0)

    quiet_lufs = _clamp((-10.60 - integrated_lufs) / 3.40, 0.0, 1.0)
    tp_room = _clamp((-1.40 - true_peak_dbtp) / 1.80, 0.0, 1.0)
    stress_room = _clamp((0.90 - limiter_stress_proxy) / 0.22, 0.0, 1.0)
    clip_room = _clamp((0.0015 - near_clip_ratio) / 0.0015, 0.0, 1.0)

    return _clamp(
        (quiet_lufs * 0.45)
        + (tp_room * 0.25)
        + (stress_room * 0.20)
        + (clip_room * 0.10),
        0.0,
        1.0,
    )


def _delivery_punch_safety(analysis: SmartMasterAnalysis) -> float:
    m = analysis.metrics

    crest_db = _safe(m.crest_db, 10.0)
    punch_proxy = _safe(m.punch_proxy, 10.0)
    lra_ebu = _safe(m.lra_ebu, 3.0)

    crest_score = _clamp((crest_db - 8.8) / 3.2, 0.0, 1.0)
    punch_score = _clamp((punch_proxy - 9.8) / 3.0, 0.0, 1.0)
    lra_score = _clamp((lra_ebu - 1.8) / 3.0, 0.0, 1.0)

    return _clamp(
        (crest_score * 0.45)
        + (punch_score * 0.45)
        + (lra_score * 0.10),
        0.0,
        1.0,
    )


def _spec_attr(primitive_name: str, attr: str, fallback: Any) -> Any:
    spec = PRIMITIVE_REGISTRY[primitive_name]
    return getattr(spec, attr, fallback)


@dataclass
class _PrimitiveBuildContext:
    analysis: SmartMasterAnalysis
    stack: RoleDSPStack
    order_index: int
    amount_norm: float
    activity: float

    @property
    def role_key(self) -> str:
        return _role_key(self.stack)

    @property
    def is_cleanup(self) -> bool:
        return self.role_key == "cleanup"

    @property
    def is_guard(self) -> bool:
        return self.role_key == "guard"

    @property
    def is_anchor(self) -> bool:
        return self.role_key == "anchor"

    @property
    def is_bridge(self) -> bool:
        return self.role_key == "bridge"

    @property
    def is_projection(self) -> bool:
        return self.role_key == "projection"

    @property
    def is_spark(self) -> bool:
        return self.role_key == "spark"

    @property
    def is_delivery(self) -> bool:
        return self.role_key == "delivery"


def _base_instance(
    ctx: _PrimitiveBuildContext,
    primitive_name: str,
    *,
    enabled: bool = True,
    channel_mode: Optional[str] = None,
    gain_db: Optional[float] = None,
    freq_hz: Optional[float] = None,
    q: Optional[float] = None,
    attack_ms: Optional[float] = None,
    release_ms: Optional[float] = None,
    mix: Optional[float] = None,
    drive_db: Optional[float] = None,
    ratio: Optional[float] = None,
    threshold_db: Optional[float] = None,
    tilt_db: Optional[float] = None,
    pivot_hz: Optional[float] = None,
    side_gain_db: Optional[float] = None,
    width_amount: Optional[float] = None,
    low_cut_hz: Optional[float] = None,
    high_cut_hz: Optional[float] = None,
    notes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    spec = PRIMITIVE_REGISTRY[primitive_name]

    return {
        "instance_name": f"{ctx.stack.stack_name}__{ctx.order_index:02d}__{primitive_name}",
        "primitive_name": primitive_name,
        "primitive_class": spec.primitive_class,
        "enabled": bool(enabled and ctx.stack.enabled),
        "role": ctx.stack.role.value if hasattr(ctx.stack.role, "value") else str(ctx.stack.role),
        "stack_name": ctx.stack.stack_name,
        "stack_kind": ctx.stack.stack_kind,
        "path_type": ctx.stack.path_type,
        "target_band_mode": ctx.stack.target_band_mode,
        "protection_mode": ctx.stack.protection_mode,
        "order_index": ctx.order_index,
        "amount_norm": round(ctx.amount_norm, 6),
        "activity": round(ctx.activity, 6),
        "dynamic_scale": round(ctx.stack.dynamic_scale, 6),
        "channel_scope": spec.channel_scope,
        "channel_mode": channel_mode or spec.channel_scope,
        "band_scope": spec.band_scope,
        "detector_mode": spec.detector_mode,
        "phase_policy": spec.phase_policy,
        "safety_tags": _uniq(list(spec.safety_tags or []) + list(ctx.stack.safety_tags or [])),
        "params": {
            "gain_db": gain_db,
            "freq_hz": freq_hz,
            "q": q,
            "attack_ms": attack_ms,
            "release_ms": release_ms,
            "mix": mix,
            "drive_db": drive_db,
            "ratio": ratio,
            "threshold_db": threshold_db,
            "tilt_db": tilt_db,
            "pivot_hz": pivot_hz,
            "side_gain_db": side_gain_db,
            "width_amount": width_amount,
            "low_cut_hz": low_cut_hz,
            "high_cut_hz": high_cut_hz,
        },
        "notes": notes or [],
    }


def _build_controlled_bell_boost(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_anchor_center(ctx.analysis)
    gain = _lerp(0.26, 1.05, ctx.activity)
    q = _lerp(0.70, 1.12, ctx.activity)
    return _base_instance(
        ctx,
        "controlled_bell_boost",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        notes=["Controlled body-support bell boost with audible foundation intent."],
    )


def _build_dynamic_body_support_boost(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_anchor_center(ctx.analysis)
    gain = _lerp(0.32, 1.28, ctx.activity)
    q = _lerp(0.82, 1.50, ctx.activity)
    attack = _lerp(14.0, 28.0, 1.0 - ctx.activity)
    release = _lerp(90.0, 220.0, ctx.activity)
    return _base_instance(
        ctx,
        "dynamic_body_support_boost",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Dynamic body support boost to make body audibly present, not barely protected."],
    )


def _build_restrained_parallel_fill(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_anchor_center(ctx.analysis)
    gain = _lerp(0.22, 0.82, ctx.activity)
    mix = _lerp(0.055, 0.18, ctx.activity)
    q = _lerp(0.68, 1.00, ctx.activity)
    return _base_instance(
        ctx,
        "restrained_parallel_fill",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        mix=round(mix, 4),
        notes=["Parallel body fill with musical audibility; still protected from mud rebuild."],
    )


def _build_restrained_parallel_handoff_support(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_bridge_center(ctx.analysis)
    gain = _lerp(0.20, 0.66, ctx.activity)
    mix = _lerp(0.045, 0.14, ctx.activity)
    q = _lerp(0.72, 1.08, ctx.activity)
    return _base_instance(
        ctx,
        "restrained_parallel_handoff_support",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        mix=round(mix, 4),
        notes=["Bridge/handoff support to make bass-to-body continuity audible."],
    )


def _build_transient_safe_support_compression(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    ratio = _lerp(1.25, 2.05, ctx.activity)
    threshold = _lerp(-25.0, -14.0, ctx.activity)
    attack = _lerp(18.0, 35.0, 1.0 - ctx.activity)
    release = _lerp(85.0, 190.0, ctx.activity)
    mix = _lerp(0.10, 0.26, ctx.activity)
    return _base_instance(
        ctx,
        "transient_safe_support_compression",
        ratio=round(ratio, 4),
        threshold_db=round(threshold, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        mix=round(mix, 4),
        notes=["Support compression for weight and continuity without flattening punch."],
    )


def _build_dynamic_bell_cut(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    if ctx.is_cleanup:
        freq = _metric_cleanup_center(ctx.analysis)
        gain = -_lerp(0.42, 1.72, ctx.activity)
        q = _lerp(1.00, 2.00, ctx.activity)
    elif ctx.is_guard:
        freq = _metric_guard_center(ctx.analysis)
        gain = -_lerp(0.30, 1.10, ctx.activity)
        q = _lerp(1.15, 2.10, ctx.activity)
    elif ctx.is_anchor:
        freq = _lerp(210.0, 300.0, ctx.activity)
        gain = -_lerp(0.12, 0.48, ctx.activity)
        q = _lerp(0.90, 1.45, ctx.activity)
    elif ctx.is_bridge:
        freq = _lerp(120.0, 190.0, ctx.activity)
        gain = -_lerp(0.12, 0.45, ctx.activity)
        q = _lerp(0.85, 1.40, ctx.activity)
    else:
        freq = 280.0
        gain = -_lerp(0.22, 0.80, ctx.activity)
        q = _lerp(1.00, 1.70, ctx.activity)

    attack = _lerp(6.0, 18.0, 1.0 - ctx.activity)
    release = _lerp(55.0, 180.0, ctx.activity)

    return _base_instance(
        ctx,
        "dynamic_bell_cut",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Dynamic cut separates buildup from body without turning cleanup into sterilization."],
    )


def _build_dynamic_wide_cut(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_cleanup_center(ctx.analysis)
    gain = -_lerp(0.25, 1.05, ctx.activity)
    q = _lerp(0.48, 0.82, ctx.activity)
    attack = _lerp(10.0, 20.0, 1.0 - ctx.activity)
    release = _lerp(90.0, 220.0, ctx.activity)
    return _base_instance(
        ctx,
        "dynamic_wide_cut",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Distributed low-mid decongestion with body-aware restraint."],
    )


def _build_restrained_static_cut(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    if ctx.is_cleanup:
        freq = _metric_cleanup_center(ctx.analysis)
    elif ctx.is_guard:
        freq = _metric_guard_center(ctx.analysis)
    elif ctx.is_anchor:
        freq = 260.0
    elif ctx.is_bridge:
        freq = 160.0
    else:
        freq = 320.0

    gain = -_lerp(0.12, 0.55, ctx.activity)
    q = _lerp(0.90, 1.60, ctx.activity)

    return _base_instance(
        ctx,
        "restrained_static_cut",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        notes=["Restrained static cut; never the main source of polish magic."],
    )


def _build_dynamic_tilt_down(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    pivot = _lerp(900.0, 1400.0, ctx.activity)
    tilt = -_lerp(0.15, 0.72, ctx.activity)
    attack = _lerp(10.0, 24.0, 1.0 - ctx.activity)
    release = _lerp(90.0, 200.0, ctx.activity)
    return _base_instance(
        ctx,
        "dynamic_tilt_down",
        tilt_db=round(tilt, 4),
        pivot_hz=round(pivot, 2),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Dynamic calming tilt, reduced so it does not eat projection by default."],
    )


def _build_local_antiharsh_control(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_harsh_center(ctx.analysis)
    gain = -_lerp(0.35, 1.15, ctx.activity)
    q = _lerp(1.40, 2.80, ctx.activity)
    attack = _lerp(1.5, 8.0, 1.0 - ctx.activity)
    release = _lerp(35.0, 120.0, ctx.activity)
    return _base_instance(
        ctx,
        "local_antiharsh_control",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Local anti-harsh restraint without muting musical forwardness."],
    )


def _build_broad_presence_contour(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_presence_center(ctx.analysis)
    gain = _lerp(0.45, 1.65, ctx.activity)
    q = _lerp(0.42, 0.82, ctx.activity)
    return _base_instance(
        ctx,
        "broad_presence_contour",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        notes=["Audible center-forward contour for record-like projection."],
    )


def _build_dynamic_presence_lift(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_presence_center(ctx.analysis)
    gain = _lerp(0.35, 1.35, ctx.activity)
    q = _lerp(0.78, 1.35, ctx.activity)
    attack = _lerp(4.0, 13.0, 1.0 - ctx.activity)
    release = _lerp(40.0, 115.0, ctx.activity)
    return _base_instance(
        ctx,
        "dynamic_presence_lift",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Dynamic presence lift for audible projection without static harshness."],
    )


def _build_projection_local_deharsh(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_harsh_center(ctx.analysis)
    gain = -_lerp(0.25, 0.90, ctx.activity)
    q = _lerp(1.60, 3.00, ctx.activity)
    attack = _lerp(1.0, 6.0, 1.0 - ctx.activity)
    release = _lerp(35.0, 115.0, ctx.activity)
    return _base_instance(
        ctx,
        "projection_local_deharsh",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Local de-harsh protection bound directly to stronger projection."],
    )


def _build_band_limited_soft_saturation(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    drive = _lerp(0.45, 2.35, ctx.activity)
    mix = _lerp(0.06, 0.22, ctx.activity)
    return _base_instance(
        ctx,
        "band_limited_soft_saturation",
        drive_db=round(drive, 4),
        mix=round(mix, 4),
        low_cut_hz=1300.0,
        high_cut_hz=6200.0,
        notes=["Band-limited saturation for audible projection density and record-like push."],
    )


def _build_controlled_harmonic_density(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    drive = _lerp(0.38, 1.85, ctx.activity)
    mix = _lerp(0.06, 0.18, ctx.activity)
    return _base_instance(
        ctx,
        "controlled_harmonic_density",
        drive_db=round(drive, 4),
        mix=round(mix, 4),
        low_cut_hz=1200.0,
        high_cut_hz=5600.0,
        notes=["Controlled harmonic density for premium center energy without cheap top gloss."],
    )


def _build_micro_air_shelf(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_air_center(ctx.analysis)
    gain = _lerp(0.12, 0.70, ctx.activity)
    return _base_instance(
        ctx,
        "micro_air_shelf",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=0.45,
        notes=["Audible but controlled air polish for finished feel."],
    )


def _build_micro_top_texture(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    drive = _lerp(0.12, 0.95, ctx.activity)
    mix = _lerp(0.025, 0.095, ctx.activity)
    return _base_instance(
        ctx,
        "micro_top_texture",
        drive_db=round(drive, 4),
        mix=round(mix, 4),
        low_cut_hz=8000.0,
        high_cut_hz=16000.0,
        notes=["Top texture micro-layer for premium spark, not harsh brightness."],
    )


def _build_protected_high_side_polish(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_air_center(ctx.analysis)
    gain = _lerp(0.10, 0.42, ctx.activity)
    mix = _lerp(0.025, 0.085, ctx.activity)
    return _base_instance(
        ctx,
        "protected_high_side_polish",
        channel_mode="side",
        side_gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        mix=round(mix, 4),
        notes=["Protected high-side polish for premium width only when allowed."],
    )


def _build_micro_width_high_only(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    width = _lerp(0.025, 0.095, ctx.activity)
    mix = _lerp(0.025, 0.075, ctx.activity)
    return _base_instance(
        ctx,
        "micro_width_high_only",
        channel_mode="side",
        width_amount=round(width, 4),
        mix=round(mix, 4),
        low_cut_hz=7000.0,
        high_cut_hz=16000.0,
        notes=["High-only width micro-layer for finish size, never replacing body or projection."],
    )


def _build_local_desibilance_control(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_sibilance_center(ctx.analysis)
    gain = -_lerp(0.22, 0.82, ctx.activity)
    q = _lerp(1.20, 2.40, ctx.activity)
    attack = _lerp(0.7, 4.5, 1.0 - ctx.activity)
    release = _lerp(25.0, 95.0, ctx.activity)
    return _base_instance(
        ctx,
        "local_desibilance_control",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Finish-local de-sibilance so spark does not become squeak."],
    )


def _build_output_gain_trim(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    hot_score = _delivery_hot_score(ctx.analysis)
    quiet_score = _delivery_quiet_score(ctx.analysis)
    punch_safety = _delivery_punch_safety(ctx.analysis)

    if hot_score >= 0.22:
        gain_trim = -(
            0.015
            + (0.26 * hot_score)
            + (0.04 * (1.0 - punch_safety) * hot_score)
        )
    else:
        lift_room = _clamp((0.28 - hot_score) / 0.28, 0.0, 1.0)
        gain_trim = (
            (0.18 + (0.92 * quiet_score) + (0.26 * punch_safety * quiet_score))
            * quiet_score
            * lift_room
        )

    gain_trim = _clamp(gain_trim, -0.30, 1.10)

    note = (
        "Delta-aware trim before limiter."
        if gain_trim <= 0.0
        else "Delta-aware musical lift before limiter."
    )

    return _base_instance(
        ctx,
        "output_gain_trim",
        gain_db=round(gain_trim, 4),
        notes=[
            note,
            "Delivery is allowed to add mastered feel when headroom and punch allow it.",
        ],
    )


def _build_true_peak_limiter(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    m = ctx.analysis.metrics

    limiter_stress_proxy = _safe(m.limiter_stress_proxy, 0.0)
    near_clip_ratio = _safe(m.near_clip_ratio, 0.0)

    hot_score = _delivery_hot_score(ctx.analysis)
    quiet_score = _delivery_quiet_score(ctx.analysis)
    punch_safety = _delivery_punch_safety(ctx.analysis)

    safety_headroom_score = _clamp((0.96 - limiter_stress_proxy) / 0.24, 0.0, 1.0)
    codec_room_score = _clamp((0.0035 - near_clip_ratio) / 0.0035, 0.0, 1.0)

    drive_score = (
        (quiet_score * 0.40)
        + (punch_safety * 0.28)
        + (safety_headroom_score * 0.18)
        + (codec_room_score * 0.14)
    )

    drive_penalty = hot_score * (0.58 + (0.18 * (1.0 - punch_safety)))

    desired_drive_db = _clamp(
        0.28 + (1.85 * drive_score) - (0.58 * drive_penalty),
        0.20,
        1.95,
    )

    ceiling_db = -1.00
    threshold_db = ceiling_db - desired_drive_db

    attack_ms = _clamp(
        0.24 + (0.16 * hot_score) - (0.08 * punch_safety),
        0.16,
        0.48,
    )

    release_ms = _clamp(
        54.0
        + (28.0 * hot_score)
        + (18.0 * (1.0 - punch_safety))
        - (16.0 * quiet_score),
        40.0,
        96.0,
    )

    return _base_instance(
        ctx,
        "true_peak_limiter",
        gain_db=round(ceiling_db, 4),
        threshold_db=round(threshold_db, 4),
        attack_ms=round(attack_ms, 3),
        release_ms=round(release_ms, 3),
        mix=1.0,
        notes=[
            "Terminal true-peak limiter.",
            "Limiter drive is allowed to create controlled mastered feel when safe.",
        ],
    )


def _generic_instance(ctx: _PrimitiveBuildContext, primitive_name: str) -> Dict[str, Any]:
    min_gain = _spec_attr(primitive_name, "min_gain_db", -0.5)
    default_gain = _spec_attr(primitive_name, "default_gain_db", 0.0)
    max_gain = _spec_attr(primitive_name, "max_gain_db", 0.5)

    min_freq = _spec_attr(primitive_name, "min_freq_hz", 120.0)
    max_freq = _spec_attr(primitive_name, "max_freq_hz", 5000.0)

    min_q = _spec_attr(primitive_name, "min_q", 0.7)
    max_q = _spec_attr(primitive_name, "max_q", 1.8)

    min_attack = _spec_attr(primitive_name, "min_attack_ms", 3.0)
    max_attack = _spec_attr(primitive_name, "max_attack_ms", 20.0)

    min_release = _spec_attr(primitive_name, "min_release_ms", 40.0)
    max_release = _spec_attr(primitive_name, "max_release_ms", 140.0)

    freq = _lerp(min_freq, max_freq, 0.5)
    q = _lerp(min_q, max_q, ctx.activity)
    attack = _lerp(min_attack, max_attack, 1.0 - ctx.activity)
    release = _lerp(min_release, max_release, ctx.activity)

    if default_gain >= 0.0:
        gain = _lerp(max(0.0, min_gain), max_gain, ctx.activity)
    else:
        gain = _lerp(min_gain, min(0.0, max_gain), ctx.activity)

    return _base_instance(
        ctx,
        primitive_name,
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Generic parameter fallback instance."],
    )


_PRIMITIVE_BUILDERS = {
    "controlled_bell_boost": _build_controlled_bell_boost,
    "dynamic_body_support_boost": _build_dynamic_body_support_boost,
    "restrained_parallel_fill": _build_restrained_parallel_fill,
    "restrained_parallel_handoff_support": _build_restrained_parallel_handoff_support,
    "transient_safe_support_compression": _build_transient_safe_support_compression,
    "dynamic_bell_cut": _build_dynamic_bell_cut,
    "dynamic_wide_cut": _build_dynamic_wide_cut,
    "restrained_static_cut": _build_restrained_static_cut,
    "dynamic_tilt_down": _build_dynamic_tilt_down,
    "local_antiharsh_control": _build_local_antiharsh_control,
    "broad_presence_contour": _build_broad_presence_contour,
    "dynamic_presence_lift": _build_dynamic_presence_lift,
    "projection_local_deharsh": _build_projection_local_deharsh,
    "band_limited_soft_saturation": _build_band_limited_soft_saturation,
    "controlled_harmonic_density": _build_controlled_harmonic_density,
    "micro_air_shelf": _build_micro_air_shelf,
    "micro_top_texture": _build_micro_top_texture,
    "protected_high_side_polish": _build_protected_high_side_polish,
    "micro_width_high_only": _build_micro_width_high_only,
    "local_desibilance_control": _build_local_desibilance_control,
    "output_gain_trim": _build_output_gain_trim,
    "true_peak_limiter": _build_true_peak_limiter,
}


def build_stack_primitive_instances(
    analysis: SmartMasterAnalysis,
    stack: Optional[RoleDSPStack],
) -> List[Dict[str, Any]]:
    if stack is None or not stack.enabled:
        return []

    amount_norm = _norm_stack_amount(stack)
    activity = _stack_intensity(stack)

    instances: List[Dict[str, Any]] = []
    for idx, primitive_name in enumerate(stack.allowed_primitive_names or []):
        ctx = _PrimitiveBuildContext(
            analysis=analysis,
            stack=stack,
            order_index=idx,
            amount_norm=amount_norm,
            activity=activity,
        )
        builder = _PRIMITIVE_BUILDERS.get(primitive_name)
        if builder is None:
            instances.append(_generic_instance(ctx, primitive_name))
        else:
            instances.append(builder(ctx))
    return instances


def _attach_instances_to_stack(
    analysis: SmartMasterAnalysis,
    stack: Optional[RoleDSPStack],
) -> Optional[RoleDSPStack]:
    if stack is None:
        return None
    instances = build_stack_primitive_instances(analysis, stack)
    return replace(
        stack,
        primitive_instances=instances,
        notes=_uniq(
            list(stack.notes or [])
            + [f"primitive_instance_count={len(instances)}"]
        ),
    )


def attach_primitive_instances_to_blueprint(
    blueprint: DSPExecutionBlueprint,
    analysis: SmartMasterAnalysis,
) -> DSPExecutionBlueprint:
    cleanup_stack = _attach_instances_to_stack(analysis, blueprint.cleanup_stack)
    guard_stack = _attach_instances_to_stack(analysis, blueprint.guard_stack)
    anchor_parallel_stack = _attach_instances_to_stack(analysis, blueprint.anchor_parallel_stack)
    bridge_parallel_stack = _attach_instances_to_stack(analysis, blueprint.bridge_parallel_stack)
    projection_contour_stack = _attach_instances_to_stack(analysis, blueprint.projection_contour_stack)
    projection_assist_stack = _attach_instances_to_stack(analysis, blueprint.projection_assist_stack)
    spark_stack = _attach_instances_to_stack(analysis, blueprint.spark_stack)
    delivery_stack = _attach_instances_to_stack(analysis, blueprint.delivery_stack)

    total_instances = 0
    for stack in [
        cleanup_stack,
        guard_stack,
        anchor_parallel_stack,
        bridge_parallel_stack,
        projection_contour_stack,
        projection_assist_stack,
        spark_stack,
        delivery_stack,
    ]:
        if stack is None:
            continue
        total_instances += len(stack.primitive_instances or [])

    return replace(
        blueprint,
        cleanup_stack=cleanup_stack,
        guard_stack=guard_stack,
        anchor_parallel_stack=anchor_parallel_stack,
        bridge_parallel_stack=bridge_parallel_stack,
        projection_contour_stack=projection_contour_stack,
        projection_assist_stack=projection_assist_stack,
        spark_stack=spark_stack,
        delivery_stack=delivery_stack,
        notes=_uniq(
            list(blueprint.notes or [])
            + [
                "primitive_instances_attached",
                "music_first_primitive_gain_map_v2",
                f"primitive_instance_total={total_instances}",
            ]
        ),
    )

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any, Dict, List, Optional

from ..contracts import SmartMasterAnalysis
from .contracts import DSPExecutionBlueprint, RoleDSPStack
from .primitives import PRIMITIVE_REGISTRY


def _read(obj: Any, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


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
    if v is None:
        return float(fallback)
    try:
        x = float(v)
    except Exception:
        return float(fallback)
    if not math.isfinite(x):
        return float(fallback)
    return x


def _metric(analysis: SmartMasterAnalysis, name: str, fallback: float) -> float:
    metrics = _read(analysis, "metrics", {}) or {}
    return _safe(_read(metrics, name, fallback), fallback)


def _section_value(
    analysis: SmartMasterAnalysis,
    section_name: str,
    key: str,
    fallback: Any = None,
) -> Any:
    section = _read(analysis, section_name, {}) or {}
    return _read(section, key, fallback)


def _flag_bool(
    analysis: SmartMasterAnalysis,
    key: str,
    fallback: bool = False,
) -> bool:
    flags = _read(analysis, "global_flags", {}) or {}
    return bool(_read(flags, key, fallback))


def _value(v: Any) -> str:
    if hasattr(v, "value"):
        return str(v.value).strip().lower()
    return str(v).strip().lower()


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


def _buildup_need_score(analysis: SmartMasterAnalysis) -> float:
    lowmid_buildup_ratio = _metric(analysis, "lowmid_buildup_ratio_db", 12.0)
    mud_to_body = _metric(analysis, "mud_to_body_db", -1.0)
    mud = _metric(analysis, "mud_200_500_db", 34.0)
    buildup = _metric(analysis, "lowmid_buildup_200_400_db", 34.0)
    body = _metric(analysis, "body_150_400_db", 35.0)

    ratio_score = _clamp((lowmid_buildup_ratio - 11.0) / 9.0, 0.0, 1.0)
    mud_relation_score = _clamp((mud_to_body + 1.20) / 2.40, 0.0, 1.0)
    absolute_mud_score = _clamp((mud - 34.5) / 4.5, 0.0, 1.0)
    cluster_score = _clamp((buildup - body + 0.45) / 2.00, 0.0, 1.0)

    cleanup_risk = _value(_section_value(analysis, "cleanup", "buildup_risk", "low"))
    cleanup_bonus = 0.14 if cleanup_risk == "high" else 0.06 if cleanup_risk == "medium" else 0.0

    return _clamp(
        (ratio_score * 0.38)
        + (mud_relation_score * 0.22)
        + (absolute_mud_score * 0.25)
        + (cluster_score * 0.15)
        + cleanup_bonus,
        0.0,
        1.0,
    )


def _body_protection_score(analysis: SmartMasterAnalysis) -> float:
    body = _metric(analysis, "body_150_400_db", 35.0)
    low_body = _metric(analysis, "low_body_150_300_db", 34.0)
    lowmid = _metric(analysis, "lowmid_120_300_db", 34.0)
    punch = _metric(analysis, "punch_proxy", 10.5)
    crest = _metric(analysis, "crest_db", 10.5)

    body_abs_fragile = _clamp((34.2 - body) / 4.0, 0.0, 1.0)
    low_body_fragile = _clamp((33.8 - low_body) / 4.0, 0.0, 1.0)
    lowmid_fragile = _clamp((33.5 - lowmid) / 4.0, 0.0, 1.0)
    punch_fragile = _clamp((10.0 - punch) / 2.0, 0.0, 1.0)
    crest_fragile = _clamp((9.4 - crest) / 2.0, 0.0, 1.0)

    thin_bonus = 0.22 if _flag_bool(analysis, "thin_behavior_candidate", False) else 0.0
    punch_flag_bonus = 0.14 if _flag_bool(analysis, "punch_fragile_candidate", False) else 0.0

    anchor_fragility = _value(_section_value(analysis, "anchor", "fragility", "low"))
    anchor_bonus = 0.18 if anchor_fragility == "high" else 0.08 if anchor_fragility == "medium" else 0.0

    excessive_body_relief = _clamp((body - 37.2) / 2.8, 0.0, 1.0) * 0.30

    return _clamp(
        (body_abs_fragile * 0.26)
        + (low_body_fragile * 0.24)
        + (lowmid_fragile * 0.14)
        + (punch_fragile * 0.12)
        + (crest_fragile * 0.08)
        + thin_bonus
        + punch_flag_bonus
        + anchor_bonus
        - excessive_body_relief,
        0.0,
        1.0,
    )


def _bridge_protection_score(analysis: SmartMasterAnalysis) -> float:
    bass_to_body = _metric(analysis, "bass_to_body_db", 5.0)
    sub_to_body = _metric(analysis, "sub_to_body_db", 4.0)
    low_foundation_ratio = _metric(analysis, "low_foundation_ratio_db", 4.0)

    detached_bass = _clamp((bass_to_body - 6.0) / 4.0, 0.0, 1.0)
    weak_sub_handoff = _clamp((2.2 - sub_to_body) / 3.2, 0.0, 1.0)
    weak_foundation = _clamp((2.4 - low_foundation_ratio) / 3.0, 0.0, 1.0)

    bridge_state = _value(_section_value(analysis, "bridge", "state", "ok"))
    bridge_stop = bool(_section_value(analysis, "bridge", "stop", False))

    overglue_guard = 0.22 if bridge_state == "overglued" else 0.0
    stop_guard = 0.16 if bridge_stop else 0.0

    return _clamp(
        (detached_bass * 0.30)
        + (weak_sub_handoff * 0.25)
        + (weak_foundation * 0.14)
        + overglue_guard
        + stop_guard,
        0.0,
        1.0,
    )


def _guard_need_score(analysis: SmartMasterAnalysis) -> float:
    mud = _metric(analysis, "mud_200_500_db", 34.0)
    mud_to_body = _metric(analysis, "mud_to_body_db", -1.0)
    lowmid_buildup_ratio = _metric(analysis, "lowmid_buildup_ratio_db", 12.0)

    mud_score = _clamp((mud - 35.2) / 3.8, 0.0, 1.0)
    relation_score = _clamp((mud_to_body + 0.65) / 1.80, 0.0, 1.0)
    ratio_score = _clamp((lowmid_buildup_ratio - 13.0) / 8.0, 0.0, 1.0)

    guard_shape = _value(_section_value(analysis, "guard", "shape", "stable"))
    shape_bonus = 0.22 if guard_shape == "boxy" else 0.0

    return _clamp(
        (mud_score * 0.38)
        + (relation_score * 0.24)
        + (ratio_score * 0.16)
        + shape_bonus,
        0.0,
        1.0,
    )


def _top_risk_score(analysis: SmartMasterAnalysis) -> float:
    harshness_index = _metric(analysis, "harshness_index", -10.0)
    sibilance_index = _metric(analysis, "sibilance_index", -7.0)
    harsh_to_mid = _metric(analysis, "harsh_to_mid_db", -7.0)
    harsh_band = _metric(analysis, "harsh_2p5k_6k_db", 16.0)
    sibilance_band = _metric(analysis, "sibilance_5k_9k_db", 15.0)

    harsh_score = _clamp((harshness_index + 12.0) / 6.0, 0.0, 1.0)
    sibilance_score = _clamp((sibilance_index + 7.0) / 5.0, 0.0, 1.0)
    harsh_to_mid_score = _clamp((harsh_to_mid + 7.0) / 4.0, 0.0, 1.0)
    harsh_band_score = _clamp((harsh_band - 16.0) / 5.0, 0.0, 1.0)
    sib_band_score = _clamp((sibilance_band - 15.0) / 4.0, 0.0, 1.0)

    projection_harsh = _value(_section_value(analysis, "projection", "harshness_risk", "low"))
    projection_sib = _value(_section_value(analysis, "projection", "sibilance_risk", "low"))

    projection_bonus = 0.20 if projection_harsh == "high" else 0.10 if projection_harsh == "medium" else 0.0
    sib_bonus = 0.12 if projection_sib == "high" else 0.06 if projection_sib == "medium" else 0.0
    flag_bonus = 0.10 if _flag_bool(analysis, "top_risk_candidate", False) else 0.0

    return _clamp(
        (harsh_score * 0.22)
        + (sibilance_score * 0.18)
        + (harsh_to_mid_score * 0.18)
        + (harsh_band_score * 0.14)
        + (sib_band_score * 0.10)
        + projection_bonus
        + sib_bonus
        + flag_bonus,
        0.0,
        1.0,
    )


def _projection_need_score(analysis: SmartMasterAnalysis) -> float:
    presence_to_body = _metric(analysis, "presence_to_body_db", -15.0)
    mid_1k_2k = _metric(analysis, "mid_1k_2k_db", 27.0)
    body_handoff_proxy = _safe(_section_value(analysis, "derived", "body_to_mid_handoff_proxy", 0.7), 0.7)

    presence_gap = _clamp((-12.5 - presence_to_body) / 8.0, 0.0, 1.0)
    mid_weak = _clamp((27.0 - mid_1k_2k) / 5.0, 0.0, 1.0)
    handoff_bonus = _clamp((body_handoff_proxy - 0.70) / 0.35, 0.0, 1.0) * 0.12

    state = _value(_section_value(analysis, "projection", "readiness", "balanced"))
    readiness_bonus = 0.10 if state in {"guarded", "ready", "balanced"} else 0.0

    return _clamp(
        (presence_gap * 0.62)
        + (mid_weak * 0.18)
        + handoff_bonus
        + readiness_bonus,
        0.0,
        1.0,
    )


def _air_need_score(analysis: SmartMasterAnalysis) -> float:
    air_ratio = _metric(analysis, "air_ratio_db", -20.0)
    air16_to_body = _metric(analysis, "air16_to_body_db", -22.0)

    air_gap = _clamp((-18.5 - air_ratio) / 8.0, 0.0, 1.0)
    air16_gap = _clamp((-20.5 - air16_to_body) / 8.0, 0.0, 1.0)

    return _clamp((air_gap * 0.62) + (air16_gap * 0.38), 0.0, 1.0)


def _metric_presence_center(analysis: SmartMasterAnalysis) -> float:
    presence_to_body = _metric(analysis, "presence_to_body_db", -16.0)
    harsh_to_mid = _metric(analysis, "harsh_to_mid_db", -6.0)
    top_risk = _top_risk_score(analysis)

    if top_risk > 0.62:
        return 2150.0 if presence_to_body < -17.0 else 2300.0

    if presence_to_body < -18.5:
        return 2200.0

    if harsh_to_mid > -4.5:
        return 2550.0

    return 2400.0


def _metric_cleanup_center(analysis: SmartMasterAnalysis) -> float:
    mud = _metric(analysis, "mud_200_500_db", 34.0)
    buildup = _metric(analysis, "lowmid_buildup_200_400_db", 34.0)
    low_body = _metric(analysis, "low_body_150_300_db", 32.0)
    mud_to_body = _metric(analysis, "mud_to_body_db", -1.0)

    body_protect = _body_protection_score(analysis)
    bridge_protect = _bridge_protection_score(analysis)
    buildup_need = _buildup_need_score(analysis)

    if body_protect > 0.66:
        return 340.0

    if bridge_protect > 0.62:
        return 325.0

    if mud_to_body > 0.35:
        return 340.0

    if mud >= 37.0 and buildup_need > 0.55:
        return 315.0

    if buildup - low_body > 1.2 and body_protect < 0.45:
        return 280.0

    if mud >= 36.0:
        return 300.0

    return 285.0


def _metric_guard_center(analysis: SmartMasterAnalysis) -> float:
    mud = _metric(analysis, "mud_200_500_db", 34.0)
    mud_to_body = _metric(analysis, "mud_to_body_db", -1.0)
    guard_need = _guard_need_score(analysis)
    body_protect = _body_protection_score(analysis)

    if body_protect > 0.66:
        return 390.0

    if mud_to_body > 0.35:
        return 420.0

    if mud > 37.0 and guard_need > 0.55:
        return 390.0

    if mud > 36.0:
        return 360.0

    return 330.0


def _metric_anchor_center(analysis: SmartMasterAnalysis) -> float:
    bass_to_body = _metric(analysis, "bass_to_body_db", 6.0)
    anchor_state = _value(_section_value(analysis, "anchor", "state", "ok"))

    if anchor_state == "excessive":
        return 165.0

    if bass_to_body < 3.5:
        return 170.0

    if bass_to_body < 6.0:
        return 185.0

    return 200.0


def _metric_bridge_center(analysis: SmartMasterAnalysis) -> float:
    sub_to_body = _metric(analysis, "sub_to_body_db", 4.5)
    bass_to_body = _metric(analysis, "bass_to_body_db", 5.0)
    bridge_state = _value(_section_value(analysis, "bridge", "state", "ok"))

    if bridge_state == "overglued":
        return 118.0

    if sub_to_body < 2.5:
        return 120.0

    if bass_to_body < 4.0:
        return 140.0

    return 155.0


def _metric_harsh_center(analysis: SmartMasterAnalysis) -> float:
    harsh = _metric(analysis, "harsh_2p5k_6k_db", 16.0)
    sibilance = _metric(analysis, "sibilance_5k_9k_db", 15.5)
    harsh_to_mid = _metric(analysis, "harsh_to_mid_db", -7.0)

    if sibilance > 16.8:
        return 5200.0

    if harsh > 18.0 or harsh_to_mid > -5.2:
        return 4300.0

    return 4700.0


def _metric_sibilance_center(analysis: SmartMasterAnalysis) -> float:
    sibilance = _metric(analysis, "sibilance_5k_9k_db", 15.5)
    if sibilance > 16.5:
        return 7100.0
    if sibilance > 15.5:
        return 6600.0
    return 6100.0


def _metric_air_center(analysis: SmartMasterAnalysis) -> float:
    air_ratio = _metric(analysis, "air_ratio_db", -18.0)
    top_risk = _top_risk_score(analysis)

    if top_risk > 0.62:
        return 11800.0

    if air_ratio < -23.0:
        return 11000.0

    if air_ratio < -19.0:
        return 10250.0

    return 9600.0


def _delivery_hot_score(analysis: SmartMasterAnalysis) -> float:
    true_peak_dbtp = _metric(analysis, "true_peak_dbtp", -1.0)
    integrated_lufs = _metric(analysis, "integrated_lufs", -12.0)
    limiter_stress_proxy = _metric(analysis, "limiter_stress_proxy", 0.0)
    near_clip_ratio = _metric(analysis, "near_clip_ratio", 0.0)

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
    true_peak_dbtp = _metric(analysis, "true_peak_dbtp", -1.0)
    integrated_lufs = _metric(analysis, "integrated_lufs", -12.0)
    limiter_stress_proxy = _metric(analysis, "limiter_stress_proxy", 0.0)
    near_clip_ratio = _metric(analysis, "near_clip_ratio", 0.0)

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
    crest_db = _metric(analysis, "crest_db", 10.0)
    punch_proxy = _metric(analysis, "punch_proxy", 10.0)
    lra_ebu = _metric(analysis, "lra_ebu", 3.0)

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
    body_protect = _body_protection_score(ctx.analysis)
    buildup_need = _buildup_need_score(ctx.analysis)

    gain = _lerp(0.30, 1.18, ctx.activity)
    gain *= _lerp(1.08, 0.88, buildup_need)
    gain *= _lerp(0.92, 1.14, body_protect)
    gain = _clamp(gain, 0.22, 1.20)

    q = _lerp(0.68, 1.08, ctx.activity)

    return _base_instance(
        ctx,
        "controlled_bell_boost",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        notes=["Controlled body-support bell boost with body-protection and mud-aware scaling."],
    )


def _build_dynamic_body_support_boost(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_anchor_center(ctx.analysis)
    body_protect = _body_protection_score(ctx.analysis)
    buildup_need = _buildup_need_score(ctx.analysis)

    gain = _lerp(0.38, 1.42, ctx.activity)
    gain *= _lerp(1.04, 0.88, buildup_need)
    gain *= _lerp(0.94, 1.18, body_protect)
    gain = _clamp(gain, 0.30, 1.45)

    q = _lerp(0.78, 1.38, ctx.activity)
    attack = _lerp(14.0, 30.0, 1.0 - ctx.activity)
    release = _lerp(95.0, 240.0, ctx.activity)

    return _base_instance(
        ctx,
        "dynamic_body_support_boost",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Dynamic body support boost: preserves useful body without rebuilding mud."],
    )


def _build_restrained_parallel_fill(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_anchor_center(ctx.analysis)
    body_protect = _body_protection_score(ctx.analysis)
    buildup_need = _buildup_need_score(ctx.analysis)

    gain = _lerp(0.34, 1.12, ctx.activity)
    gain *= _lerp(1.06, 0.86, buildup_need)
    gain *= _lerp(0.96, 1.16, body_protect)
    gain = _clamp(gain, 0.24, 1.14)

    mix = _lerp(0.070, 0.215, ctx.activity)
    mix *= _lerp(1.05, 0.82, buildup_need)
    mix *= _lerp(0.95, 1.12, body_protect)
    mix = _clamp(mix, 0.055, 0.215)

    q = _lerp(0.66, 1.02, ctx.activity)

    return _base_instance(
        ctx,
        "restrained_parallel_fill",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        mix=round(mix, 4),
        notes=["Parallel body anchor: audible support, restrained when buildup/mud risk is high."],
    )


def _build_restrained_parallel_handoff_support(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_bridge_center(ctx.analysis)
    bridge_protect = _bridge_protection_score(ctx.analysis)
    buildup_need = _buildup_need_score(ctx.analysis)

    gain = _lerp(0.28, 0.82, ctx.activity)
    gain *= _lerp(1.04, 0.90, buildup_need)
    gain *= _lerp(0.96, 1.08, bridge_protect)
    gain = _clamp(gain, 0.22, 0.84)

    mix = _lerp(0.060, 0.175, ctx.activity)
    mix *= _lerp(1.03, 0.86, buildup_need)
    mix *= _lerp(0.96, 1.10, bridge_protect)
    mix = _clamp(mix, 0.048, 0.175)

    q = _lerp(0.70, 1.08, ctx.activity)

    return _base_instance(
        ctx,
        "restrained_parallel_handoff_support",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        mix=round(mix, 4),
        notes=["Bass-to-body bridge floor: keeps handoff alive without overglue."],
    )


def _build_transient_safe_support_compression(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    bridge_protect = _bridge_protection_score(ctx.analysis)
    punch_safety = _delivery_punch_safety(ctx.analysis)

    ratio = _lerp(1.18, 1.82, ctx.activity)
    ratio *= _lerp(0.92, 1.04, bridge_protect)
    ratio *= _lerp(0.88, 1.00, punch_safety)
    ratio = _clamp(ratio, 1.12, 1.90)

    threshold = _lerp(-24.0, -15.5, ctx.activity)
    attack = _lerp(22.0, 42.0, 1.0 - ctx.activity)
    release = _lerp(90.0, 210.0, ctx.activity)

    mix = _lerp(0.08, 0.22, ctx.activity)
    mix *= _lerp(0.85, 1.00, punch_safety)
    mix = _clamp(mix, 0.06, 0.22)

    return _base_instance(
        ctx,
        "transient_safe_support_compression",
        ratio=round(ratio, 4),
        threshold_db=round(threshold, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        mix=round(mix, 4),
        notes=["Support compression for continuity with punch-safe moderation."],
    )


def _build_dynamic_bell_cut(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    buildup_need = _buildup_need_score(ctx.analysis)
    body_protect = _body_protection_score(ctx.analysis)
    bridge_protect = _bridge_protection_score(ctx.analysis)
    guard_need = _guard_need_score(ctx.analysis)

    if ctx.is_cleanup:
        freq = _metric_cleanup_center(ctx.analysis)

        depth = _lerp(0.58, 2.25, ctx.activity)
        depth *= _lerp(0.92, 1.24, buildup_need)
        depth *= _lerp(1.00, 0.70, body_protect)
        depth *= _lerp(1.00, 0.90, bridge_protect)
        depth = _clamp(depth, 0.45, 2.35)

        q = _lerp(1.08, 2.05, ctx.activity)
        q *= _lerp(0.96, 1.10, buildup_need)
        q = _clamp(q, 0.95, 2.25)

        attack = _lerp(7.0, 16.0, 1.0 - ctx.activity)
        release = _lerp(75.0, 210.0, ctx.activity)

    elif ctx.is_guard:
        freq = _metric_guard_center(ctx.analysis)

        depth = _lerp(0.24, 0.98, ctx.activity)
        depth *= _lerp(0.88, 1.18, guard_need)
        depth *= _lerp(1.00, 0.74, body_protect)
        depth = _clamp(depth, 0.18, 1.05)

        q = _lerp(1.05, 1.80, ctx.activity)
        q *= _lerp(0.95, 1.08, guard_need)
        q = _clamp(q, 0.95, 1.95)

        attack = _lerp(8.0, 18.0, 1.0 - ctx.activity)
        release = _lerp(85.0, 220.0, ctx.activity)

    elif ctx.is_anchor:
        freq = _lerp(210.0, 300.0, ctx.activity)
        depth = _lerp(0.10, 0.42, ctx.activity)
        depth *= _lerp(1.00, 0.82, body_protect)
        depth = _clamp(depth, 0.06, 0.45)

        q = _lerp(0.85, 1.35, ctx.activity)
        attack = _lerp(10.0, 20.0, 1.0 - ctx.activity)
        release = _lerp(80.0, 170.0, ctx.activity)

    elif ctx.is_bridge:
        freq = _lerp(125.0, 190.0, ctx.activity)
        depth = _lerp(0.08, 0.38, ctx.activity)
        depth *= _lerp(1.00, 0.82, bridge_protect)
        depth = _clamp(depth, 0.05, 0.40)

        q = _lerp(0.82, 1.35, ctx.activity)
        attack = _lerp(10.0, 20.0, 1.0 - ctx.activity)
        release = _lerp(80.0, 170.0, ctx.activity)

    else:
        freq = 300.0
        depth = _lerp(0.24, 0.88, ctx.activity)
        q = _lerp(1.00, 1.70, ctx.activity)
        attack = _lerp(8.0, 18.0, 1.0 - ctx.activity)
        release = _lerp(70.0, 180.0, ctx.activity)

    return _base_instance(
        ctx,
        "dynamic_bell_cut",
        gain_db=round(-depth, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Dynamic cleanup separates buildup/mud from useful body with body and bridge protection."],
    )


def _build_dynamic_wide_cut(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_cleanup_center(ctx.analysis)
    buildup_need = _buildup_need_score(ctx.analysis)
    body_protect = _body_protection_score(ctx.analysis)

    depth = _lerp(0.28, 1.32, ctx.activity)
    depth *= _lerp(0.92, 1.16, buildup_need)
    depth *= _lerp(1.00, 0.68, body_protect)
    depth = _clamp(depth, 0.22, 1.38)

    q = _lerp(0.45, 0.82, ctx.activity)
    attack = _lerp(12.0, 22.0, 1.0 - ctx.activity)
    release = _lerp(110.0, 260.0, ctx.activity)

    return _base_instance(
        ctx,
        "dynamic_wide_cut",
        gain_db=round(-depth, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Wide low-mid cleanup for distributed buildup, reduced on body-fragile material."],
    )


def _build_restrained_static_cut(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    if ctx.is_cleanup:
        freq = _metric_cleanup_center(ctx.analysis)
    elif ctx.is_guard:
        freq = _metric_guard_center(ctx.analysis)
    elif ctx.is_anchor:
        freq = 250.0
    elif ctx.is_bridge:
        freq = 155.0
    else:
        freq = 330.0

    body_protect = _body_protection_score(ctx.analysis)

    depth = _lerp(0.10, 0.50, ctx.activity)
    depth *= _lerp(1.00, 0.70, body_protect)
    depth = _clamp(depth, 0.06, 0.52)

    q = _lerp(0.88, 1.55, ctx.activity)

    return _base_instance(
        ctx,
        "restrained_static_cut",
        gain_db=round(-depth, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        notes=["Restrained static shaping only; never allowed to become body removal."],
    )


def _build_dynamic_tilt_down(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    top_risk = _top_risk_score(ctx.analysis)
    projection_need = _projection_need_score(ctx.analysis)

    pivot = _lerp(950.0, 1450.0, ctx.activity)
    tilt = _lerp(0.12, 0.62, ctx.activity)
    tilt *= _lerp(0.80, 1.16, top_risk)
    tilt *= _lerp(1.00, 0.72, projection_need)
    tilt = _clamp(tilt, 0.08, 0.66)

    attack = _lerp(10.0, 24.0, 1.0 - ctx.activity)
    release = _lerp(90.0, 210.0, ctx.activity)

    return _base_instance(
        ctx,
        "dynamic_tilt_down",
        tilt_db=round(-tilt, 4),
        pivot_hz=round(pivot, 2),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Top calming tilt; reduced when projection is needed so center does not disappear."],
    )


def _build_local_antiharsh_control(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_harsh_center(ctx.analysis)
    top_risk = _top_risk_score(ctx.analysis)

    depth = _lerp(0.34, 1.22, ctx.activity)
    depth *= _lerp(0.82, 1.28, top_risk)
    depth = _clamp(depth, 0.28, 1.38)

    q = _lerp(1.35, 2.90, ctx.activity)
    attack = _lerp(1.2, 7.5, 1.0 - ctx.activity)
    release = _lerp(35.0, 130.0, ctx.activity)

    return _base_instance(
        ctx,
        "local_antiharsh_control",
        gain_db=round(-depth, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Local anti-harsh restraint: protects projection without muting musical forwardness."],
    )


def _build_broad_presence_contour(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_presence_center(ctx.analysis)
    projection_need = _projection_need_score(ctx.analysis)
    top_risk = _top_risk_score(ctx.analysis)
    body_protect = _body_protection_score(ctx.analysis)

    gain = _lerp(0.58, 1.92, ctx.activity)
    gain *= _lerp(0.96, 1.24, projection_need)
    gain *= _lerp(1.00, 0.78, top_risk)
    gain *= _lerp(1.00, 0.92, body_protect)
    gain = _clamp(gain, 0.38, 1.95)

    q = _lerp(0.40, 0.78, ctx.activity)
    q *= _lerp(1.00, 0.92, top_risk)
    q = _clamp(q, 0.38, 0.82)

    return _base_instance(
        ctx,
        "broad_presence_contour",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        notes=["Body-linked center-forward contour for real mastered projection, not cheap brightness."],
    )


def _build_dynamic_presence_lift(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_presence_center(ctx.analysis)
    projection_need = _projection_need_score(ctx.analysis)
    top_risk = _top_risk_score(ctx.analysis)

    gain = _lerp(0.42, 1.55, ctx.activity)
    gain *= _lerp(0.96, 1.22, projection_need)
    gain *= _lerp(1.00, 0.76, top_risk)
    gain = _clamp(gain, 0.28, 1.55)

    q = _lerp(0.74, 1.30, ctx.activity)
    attack = _lerp(4.0, 13.0, 1.0 - ctx.activity)
    release = _lerp(45.0, 125.0, ctx.activity)

    return _base_instance(
        ctx,
        "dynamic_presence_lift",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Dynamic presence lift: audible center push with top-risk scaling."],
    )


def _build_projection_local_deharsh(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_harsh_center(ctx.analysis)
    top_risk = _top_risk_score(ctx.analysis)
    projection_need = _projection_need_score(ctx.analysis)

    depth = _lerp(0.30, 1.12, ctx.activity)
    depth *= _lerp(0.90, 1.32, top_risk)
    depth *= _lerp(0.92, 1.08, projection_need)
    depth = _clamp(depth, 0.24, 1.28)

    q = _lerp(1.55, 3.00, ctx.activity)
    attack = _lerp(0.9, 5.8, 1.0 - ctx.activity)
    release = _lerp(38.0, 120.0, ctx.activity)

    return _base_instance(
        ctx,
        "projection_local_deharsh",
        gain_db=round(-depth, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Projection-bound deharsh: allows forward center without harshness purchase."],
    )


def _build_band_limited_soft_saturation(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    projection_need = _projection_need_score(ctx.analysis)
    top_risk = _top_risk_score(ctx.analysis)

    drive = _lerp(0.52, 2.55, ctx.activity)
    drive *= _lerp(0.96, 1.18, projection_need)
    drive *= _lerp(1.00, 0.76, top_risk)
    drive = _clamp(drive, 0.35, 2.45)

    mix = _lerp(0.075, 0.235, ctx.activity)
    mix *= _lerp(0.96, 1.14, projection_need)
    mix *= _lerp(1.00, 0.72, top_risk)
    mix = _clamp(mix, 0.050, 0.225)

    low_cut = _lerp(1200.0, 1500.0, top_risk)
    high_cut = _lerp(6400.0, 5000.0, top_risk)

    return _base_instance(
        ctx,
        "band_limited_soft_saturation",
        drive_db=round(drive, 4),
        mix=round(mix, 4),
        low_cut_hz=round(low_cut, 2),
        high_cut_hz=round(high_cut, 2),
        notes=["Projection density saturation: center push, narrowed upward when top risk is high."],
    )


def _build_controlled_harmonic_density(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    projection_need = _projection_need_score(ctx.analysis)
    top_risk = _top_risk_score(ctx.analysis)

    drive = _lerp(0.42, 2.05, ctx.activity)
    drive *= _lerp(0.98, 1.14, projection_need)
    drive *= _lerp(1.00, 0.78, top_risk)
    drive = _clamp(drive, 0.32, 1.95)

    mix = _lerp(0.070, 0.200, ctx.activity)
    mix *= _lerp(0.98, 1.12, projection_need)
    mix *= _lerp(1.00, 0.74, top_risk)
    mix = _clamp(mix, 0.048, 0.190)

    low_cut = _lerp(1150.0, 1450.0, top_risk)
    high_cut = _lerp(5700.0, 4700.0, top_risk)

    return _base_instance(
        ctx,
        "controlled_harmonic_density",
        drive_db=round(drive, 4),
        mix=round(mix, 4),
        low_cut_hz=round(low_cut, 2),
        high_cut_hz=round(high_cut, 2),
        notes=["Controlled harmonic density for premium forwardness without upper-mid glass."],
    )


def _build_micro_air_shelf(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_air_center(ctx.analysis)
    air_need = _air_need_score(ctx.analysis)
    top_risk = _top_risk_score(ctx.analysis)

    gain = _lerp(0.16, 0.82, ctx.activity)
    gain *= _lerp(0.94, 1.20, air_need)
    gain *= _lerp(1.00, 0.68, top_risk)
    gain = _clamp(gain, 0.08, 0.78)

    return _base_instance(
        ctx,
        "micro_air_shelf",
        gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        q=0.45,
        notes=["Audible premium air, scaled by top risk so it does not become glass."],
    )


def _build_micro_top_texture(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    air_need = _air_need_score(ctx.analysis)
    top_risk = _top_risk_score(ctx.analysis)

    drive = _lerp(0.16, 1.05, ctx.activity)
    drive *= _lerp(0.96, 1.15, air_need)
    drive *= _lerp(1.00, 0.72, top_risk)
    drive = _clamp(drive, 0.10, 1.00)

    mix = _lerp(0.030, 0.105, ctx.activity)
    mix *= _lerp(0.96, 1.12, air_need)
    mix *= _lerp(1.00, 0.70, top_risk)
    mix = _clamp(mix, 0.020, 0.095)

    high_cut = _lerp(16000.0, 14500.0, top_risk)
    low_cut = _lerp(7800.0, 9000.0, top_risk)

    return _base_instance(
        ctx,
        "micro_top_texture",
        drive_db=round(drive, 4),
        mix=round(mix, 4),
        low_cut_hz=round(low_cut, 2),
        high_cut_hz=round(high_cut, 2),
        notes=["Top texture micro-layer: finish polish without sibilant brightness."],
    )


def _build_protected_high_side_polish(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_air_center(ctx.analysis)
    air_need = _air_need_score(ctx.analysis)
    top_risk = _top_risk_score(ctx.analysis)

    gain = _lerp(0.12, 0.46, ctx.activity)
    gain *= _lerp(0.96, 1.12, air_need)
    gain *= _lerp(1.00, 0.62, top_risk)
    gain = _clamp(gain, 0.06, 0.42)

    mix = _lerp(0.030, 0.090, ctx.activity)
    mix *= _lerp(0.96, 1.10, air_need)
    mix *= _lerp(1.00, 0.60, top_risk)
    mix = _clamp(mix, 0.018, 0.082)

    return _base_instance(
        ctx,
        "protected_high_side_polish",
        channel_mode="side",
        side_gain_db=round(gain, 4),
        freq_hz=round(freq, 2),
        mix=round(mix, 4),
        notes=["Protected high-side polish: width only in upper air, center stays intact."],
    )


def _build_micro_width_high_only(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    top_risk = _top_risk_score(ctx.analysis)
    air_need = _air_need_score(ctx.analysis)

    width = _lerp(0.030, 0.105, ctx.activity)
    width *= _lerp(0.96, 1.08, air_need)
    width *= _lerp(1.00, 0.58, top_risk)
    width = _clamp(width, 0.016, 0.095)

    mix = _lerp(0.030, 0.080, ctx.activity)
    mix *= _lerp(1.00, 0.62, top_risk)
    mix = _clamp(mix, 0.018, 0.074)

    low_cut = _lerp(7200.0, 8800.0, top_risk)
    high_cut = 16000.0

    return _base_instance(
        ctx,
        "micro_width_high_only",
        channel_mode="side",
        width_amount=round(width, 4),
        mix=round(mix, 4),
        low_cut_hz=round(low_cut, 2),
        high_cut_hz=round(high_cut, 2),
        notes=["High-only width micro-layer: finish size without breaking vocal/center."],
    )


def _build_local_desibilance_control(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    freq = _metric_sibilance_center(ctx.analysis)
    top_risk = _top_risk_score(ctx.analysis)

    depth = _lerp(0.24, 0.90, ctx.activity)
    depth *= _lerp(0.88, 1.28, top_risk)
    depth = _clamp(depth, 0.18, 1.02)

    q = _lerp(1.20, 2.50, ctx.activity)
    attack = _lerp(0.7, 4.5, 1.0 - ctx.activity)
    release = _lerp(26.0, 100.0, ctx.activity)

    return _base_instance(
        ctx,
        "local_desibilance_control",
        gain_db=round(-depth, 4),
        freq_hz=round(freq, 2),
        q=round(q, 4),
        attack_ms=round(attack, 3),
        release_ms=round(release, 3),
        notes=["Finish-local de-sibilance: spark remains expensive, not squeaky."],
    )


def _build_output_gain_trim(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    hot_score = _delivery_hot_score(ctx.analysis)
    quiet_score = _delivery_quiet_score(ctx.analysis)
    punch_safety = _delivery_punch_safety(ctx.analysis)

    if hot_score >= 0.22:
        gain_trim = -(
            0.005
            + (0.145 * hot_score)
            + (0.030 * (1.0 - punch_safety) * hot_score)
        )
    else:
        lift_room = _clamp((0.30 - hot_score) / 0.30, 0.0, 1.0)
        gain_trim = (
            (0.14 + (0.72 * quiet_score) + (0.22 * punch_safety * quiet_score))
            * quiet_score
            * lift_room
        )

    gain_trim = _clamp(gain_trim, -0.18, 0.82)

    note = (
        "Delta-aware safety trim before limiter."
        if gain_trim <= 0.0
        else "Delta-aware controlled lift before limiter."
    )

    return _base_instance(
        ctx,
        "output_gain_trim",
        gain_db=round(gain_trim, 4),
        notes=[
            note,
            "Delivery protects true peak and codec headroom without remastering tone.",
        ],
    )


def _build_true_peak_limiter(ctx: _PrimitiveBuildContext) -> Dict[str, Any]:
    limiter_stress_proxy = _metric(ctx.analysis, "limiter_stress_proxy", 0.0)
    near_clip_ratio = _metric(ctx.analysis, "near_clip_ratio", 0.0)

    hot_score = _delivery_hot_score(ctx.analysis)
    quiet_score = _delivery_quiet_score(ctx.analysis)
    punch_safety = _delivery_punch_safety(ctx.analysis)

    safety_headroom_score = _clamp((0.96 - limiter_stress_proxy) / 0.24, 0.0, 1.0)
    codec_room_score = _clamp((0.0035 - near_clip_ratio) / 0.0035, 0.0, 1.0)

    drive_score = (
        (quiet_score * 0.38)
        + (punch_safety * 0.28)
        + (safety_headroom_score * 0.20)
        + (codec_room_score * 0.14)
    )

    drive_penalty = hot_score * (0.66 + (0.20 * (1.0 - punch_safety)))

    desired_drive_db = _clamp(
        0.24 + (1.62 * drive_score) - (0.54 * drive_penalty),
        0.16,
        1.65,
    )

    ceiling_db = -1.00
    threshold_db = ceiling_db - desired_drive_db

    attack_ms = _clamp(
        0.22 + (0.16 * hot_score) - (0.06 * punch_safety),
        0.16,
        0.46,
    )

    release_ms = _clamp(
        54.0
        + (30.0 * hot_score)
        + (18.0 * (1.0 - punch_safety))
        - (14.0 * quiet_score),
        42.0,
        98.0,
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
            "Peak safety only: preserves loudness and punch unless real peak danger exists.",
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
                "music_first_primitive_gain_map_v3",
                "buildup_body_bridge_projection_scores_enabled",
                "delivery_trim_reduced_to_peak_safety",
                f"primitive_instance_total={total_instances}",
            ]
        ),
    )

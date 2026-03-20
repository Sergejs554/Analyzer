# ---------------------------
# POLISH / ENHANCE BRANCH
# branch-only donor
# Mixea V1 = cleanup -> body -> center -> harsh/sib control -> micro-finish -> punch keeper
# ---------------------------

# --- shared scaling ---
_MX_TONE_BODY_MUL = float(os.getenv("MX_TONE_BODY_MUL", "1.00"))
_MX_TONE_MID_MUL = float(os.getenv("MX_TONE_MID_MUL", "1.00"))
_MX_TONE_HARSH_MUL = float(os.getenv("MX_TONE_HARSH_MUL", "1.00"))
_MX_TONE_SIB_MUL = float(os.getenv("MX_TONE_SIB_MUL", "1.00"))
_MX_TONE_FINISH_MUL = float(os.getenv("MX_TONE_FINISH_MUL", "1.00"))

_MX_INTENSITY_CORE = float(os.getenv("MX_INTENSITY_CORE", "1.00"))
_MX_INTENSITY_FINISH = float(os.getenv("MX_INTENSITY_FINISH", "1.00"))
_MX_INTENSITY_DYNAMIC = float(os.getenv("MX_INTENSITY_DYNAMIC", "1.00"))

# --- structural cleanup ---
_MX_CLEAN_F1 = float(os.getenv("MX_CLEAN_F1", "280"))
_MX_CLEAN_G1 = float(os.getenv("MX_CLEAN_G1", "-1.35"))
_MX_CLEAN_W1 = float(os.getenv("MX_CLEAN_W1", "1.10"))

_MX_CLEAN_F2 = float(os.getenv("MX_CLEAN_F2", "430"))
_MX_CLEAN_G2 = float(os.getenv("MX_CLEAN_G2", "-0.70"))
_MX_CLEAN_W2 = float(os.getenv("MX_CLEAN_W2", "1.00"))

_MX_CLEAN_F3 = float(os.getenv("MX_CLEAN_F3", "670"))
_MX_CLEAN_G3 = float(os.getenv("MX_CLEAN_G3", "-0.28"))
_MX_CLEAN_W3 = float(os.getenv("MX_CLEAN_W3", "1.00"))

# --- body anchor ---
_MX_BODY_F1 = float(os.getenv("MX_BODY_F1", "205"))
_MX_BODY_G1 = float(os.getenv("MX_BODY_G1", "1.45"))
_MX_BODY_W1 = float(os.getenv("MX_BODY_W1", "1.00"))

_MX_BODY_F2 = float(os.getenv("MX_BODY_F2", "305"))
_MX_BODY_G2 = float(os.getenv("MX_BODY_G2", "0.70"))
_MX_BODY_W2 = float(os.getenv("MX_BODY_W2", "1.00"))

_MX_BODY_GUARD_F = float(os.getenv("MX_BODY_GUARD_F", "390"))
_MX_BODY_GUARD_G = float(os.getenv("MX_BODY_GUARD_G", "-0.45"))
_MX_BODY_GUARD_W = float(os.getenv("MX_BODY_GUARD_W", "1.15"))

# --- mid-anchor projection ---
_MX_PROJ_F1 = float(os.getenv("MX_PROJ_F1", "1280"))
_MX_PROJ_G1 = float(os.getenv("MX_PROJ_G1", "1.02"))
_MX_PROJ_W1 = float(os.getenv("MX_PROJ_W1", "0.95"))

_MX_PROJ_F2 = float(os.getenv("MX_PROJ_F2", "2050"))
_MX_PROJ_G2 = float(os.getenv("MX_PROJ_G2", "0.42"))
_MX_PROJ_W2 = float(os.getenv("MX_PROJ_W2", "1.00"))

_MX_PROJ_F3 = float(os.getenv("MX_PROJ_F3", "3500"))
_MX_PROJ_G3 = float(os.getenv("MX_PROJ_G3", "-0.32"))
_MX_PROJ_W3 = float(os.getenv("MX_PROJ_W3", "1.15"))

# --- harsh control ---
_MX_HARSH_F1 = float(os.getenv("MX_HARSH_F1", "3200"))
_MX_HARSH_G1 = float(os.getenv("MX_HARSH_G1", "-0.92"))
_MX_HARSH_W1 = float(os.getenv("MX_HARSH_W1", "1.20"))

_MX_HARSH_F2 = float(os.getenv("MX_HARSH_F2", "5200"))
_MX_HARSH_G2 = float(os.getenv("MX_HARSH_G2", "-0.55"))
_MX_HARSH_W2 = float(os.getenv("MX_HARSH_W2", "1.25"))

# --- sibilance control ---
_MX_SIB_F1 = float(os.getenv("MX_SIB_F1", "7300"))
_MX_SIB_G1 = float(os.getenv("MX_SIB_G1", "-0.72"))
_MX_SIB_W1 = float(os.getenv("MX_SIB_W1", "1.55"))

# --- micro-finish ---
_MX_EDGE_ON = (os.getenv("MX_EDGE_ON", "1").strip() == "1")
_MX_EDGE_HP = float(os.getenv("MX_EDGE_HP", "3300"))
_MX_EDGE_LP = float(os.getenv("MX_EDGE_LP", "8200"))
_MX_EDGE_DRIVE = float(os.getenv("MX_EDGE_DRIVE", "0.85"))
_MX_EDGE_MIX = float(os.getenv("MX_EDGE_MIX", "0.065"))

_MX_EDGE_POST_F = float(os.getenv("MX_EDGE_POST_F", "5200"))
_MX_EDGE_POST_G = float(os.getenv("MX_EDGE_POST_G", "-0.20"))
_MX_EDGE_POST_W = float(os.getenv("MX_EDGE_POST_W", "1.20"))

_MX_AIR_ON = (os.getenv("MX_AIR_ON", "1").strip() == "1")
_MX_AIR_F = float(os.getenv("MX_AIR_F", "11800"))
_MX_AIR_G = float(os.getenv("MX_AIR_G", "0.42"))

# --- punch keeper / output ---
_MX_PUNCH_ON = (os.getenv("MX_PUNCH_ON", "1").strip() == "1")
_MX_PUNCH_THRESHOLD_DB = float(os.getenv("MX_PUNCH_THRESHOLD_DB", "-24"))
_MX_PUNCH_RATIO = float(os.getenv("MX_PUNCH_RATIO", "1.16"))
_MX_PUNCH_ATTACK_MS = float(os.getenv("MX_PUNCH_ATTACK_MS", "16"))
_MX_PUNCH_RELEASE_MS = float(os.getenv("MX_PUNCH_RELEASE_MS", "115"))
_MX_PUNCH_KNEE_DB = float(os.getenv("MX_PUNCH_KNEE_DB", "1.5"))
_MX_PUNCH_MAKEUP_DB = float(os.getenv("MX_PUNCH_MAKEUP_DB", "0.0"))

_MX_TRIM_DB = float(os.getenv("MX_TRIM_DB", "-0.70"))


def _render_polish_branch(in_path: str, tone: str, intensity: str, fmt: str, td: str) -> tuple[str, str]:
    tone = _normalize_tone(tone)
    intensity = _normalize_intensity(intensity)
    fmt = _normalize_format(fmt)

    # --- shared tone scalers ---
    tone_body_mul = {
        "warm": 1.10,
        "balanced": 1.00,
        "bright": 0.93,
    }[tone] * _MX_TONE_BODY_MUL

    tone_mid_mul = {
        "warm": 0.96,
        "balanced": 1.00,
        "bright": 1.06,
    }[tone] * _MX_TONE_MID_MUL

    tone_harsh_mul = {
        "warm": 1.06,
        "balanced": 1.00,
        "bright": 1.10,
    }[tone] * _MX_TONE_HARSH_MUL

    tone_sib_mul = {
        "warm": 1.04,
        "balanced": 1.00,
        "bright": 1.10,
    }[tone] * _MX_TONE_SIB_MUL

    tone_finish_mul = {
        "warm": 0.90,
        "balanced": 1.00,
        "bright": 1.08,
    }[tone] * _MX_TONE_FINISH_MUL

    # --- shared intensity scalers ---
    intensity_core = {
        "low": 0.88,
        "balanced": 1.00,
        "high": 1.12,
    }[intensity] * _MX_INTENSITY_CORE

    intensity_finish = {
        "low": 0.82,
        "balanced": 1.00,
        "high": 1.12,
    }[intensity] * _MX_INTENSITY_FINISH

    intensity_dynamic = {
        "low": 0.92,
        "balanced": 1.00,
        "high": 1.08,
    }[intensity] * _MX_INTENSITY_DYNAMIC

    # ---------------------------
    # Section 1-5: serial tonal core
    # cleanup -> body -> center -> harsh -> sib
    # ---------------------------

    clean_f1 = _clamp(_MX_CLEAN_F1, 220.0, 340.0)
    clean_g1 = _clamp(_MX_CLEAN_G1 * intensity_core, -2.5, -0.2)
    clean_w1 = _clamp(_MX_CLEAN_W1, 0.4, 2.5)

    clean_f2 = _clamp(_MX_CLEAN_F2, 360.0, 520.0)
    clean_g2 = _clamp(_MX_CLEAN_G2 * intensity_core, -1.5, 0.0)
    clean_w2 = _clamp(_MX_CLEAN_W2, 0.4, 2.5)

    clean_f3 = _clamp(_MX_CLEAN_F3, 560.0, 820.0)
    clean_g3 = _clamp(_MX_CLEAN_G3 * intensity_core, -1.2, 0.0)
    clean_w3 = _clamp(_MX_CLEAN_W3, 0.4, 2.5)

    body_f1 = _clamp(_MX_BODY_F1, 160.0, 250.0)
    body_g1 = _clamp(_MX_BODY_G1 * tone_body_mul * intensity_core, 0.0, 3.0)
    body_w1 = _clamp(_MX_BODY_W1, 0.4, 2.5)

    body_f2 = _clamp(_MX_BODY_F2, 240.0, 340.0)
    body_g2 = _clamp(_MX_BODY_G2 * tone_body_mul * intensity_core, 0.0, 2.0)
    body_w2 = _clamp(_MX_BODY_W2, 0.4, 2.5)

    body_guard_f = _clamp(_MX_BODY_GUARD_F, 280.0, 420.0)
    body_guard_g = _clamp(_MX_BODY_GUARD_G, -1.2, 0.0)
    body_guard_w = _clamp(_MX_BODY_GUARD_W, 0.4, 2.5)

    proj_f1 = _clamp(_MX_PROJ_F1, 1050.0, 1550.0)
    proj_g1 = _clamp(_MX_PROJ_G1 * tone_mid_mul * intensity_core, 0.0, 2.0)
    proj_w1 = _clamp(_MX_PROJ_W1, 0.4, 2.5)

    proj_f2 = _clamp(_MX_PROJ_F2, 1650.0, 2350.0)
    proj_g2 = _clamp(_MX_PROJ_G2 * tone_mid_mul * intensity_core, -0.2, 1.2)
    proj_w2 = _clamp(_MX_PROJ_W2, 0.4, 2.5)

    proj_f3 = _clamp(_MX_PROJ_F3, 3000.0, 4500.0)
    proj_g3 = _clamp(_MX_PROJ_G3 * tone_harsh_mul, -1.5, 0.5)
    proj_w3 = _clamp(_MX_PROJ_W3, 0.4, 2.5)

    harsh_f1 = _clamp(_MX_HARSH_F1, 2800.0, 4200.0)
    harsh_g1 = _clamp(_MX_HARSH_G1 * tone_harsh_mul * intensity_core, -2.5, 0.0)
    harsh_w1 = _clamp(_MX_HARSH_W1, 0.4, 3.0)

    harsh_f2 = _clamp(_MX_HARSH_F2, 4300.0, 6200.0)
    harsh_g2 = _clamp(_MX_HARSH_G2 * tone_harsh_mul * intensity_core, -2.0, 0.0)
    harsh_w2 = _clamp(_MX_HARSH_W2, 0.4, 3.0)

    sib_f1 = _clamp(_MX_SIB_F1, 6200.0, 8200.0)
    sib_g1 = _clamp(_MX_SIB_G1 * tone_sib_mul * intensity_core, -2.2, 0.0)
    sib_w1 = _clamp(_MX_SIB_W1, 0.5, 4.0)

    serial_parts = [
        # Structural cleanup
        f"equalizer=f={clean_f1}:t=q:w={clean_w1}:g={clean_g1}",
        f"equalizer=f={clean_f2}:t=q:w={clean_w2}:g={clean_g2}",
        f"equalizer=f={clean_f3}:t=q:w={clean_w3}:g={clean_g3}",

        # Body anchor
        f"equalizer=f={body_f1}:t=q:w={body_w1}:g={body_g1}",
        f"equalizer=f={body_f2}:t=q:w={body_w2}:g={body_g2}",
        f"equalizer=f={body_guard_f}:t=q:w={body_guard_w}:g={body_guard_g}",

        # Mid-anchor projection
        f"equalizer=f={proj_f1}:t=q:w={proj_w1}:g={proj_g1}",
        f"equalizer=f={proj_f2}:t=q:w={proj_w2}:g={proj_g2}",
        f"equalizer=f={proj_f3}:t=q:w={proj_w3}:g={proj_g3}",

        # Harsh control
        f"equalizer=f={harsh_f1}:t=q:w={harsh_w1}:g={harsh_g1}",
        f"equalizer=f={harsh_f2}:t=q:w={harsh_w2}:g={harsh_g2}",

        # Sibilance control
        f"equalizer=f={sib_f1}:t=q:w={sib_w1}:g={sib_g1}",
    ]

    # ---------------------------
    # Section 6: micro-finish
    # ---------------------------

    edge_on = _MX_EDGE_ON
    edge_hp = _clamp(_MX_EDGE_HP, 2800.0, 4200.0)
    edge_lp = _clamp(_MX_EDGE_LP, 6500.0, 9500.0)
    if edge_lp <= edge_hp + 800.0:
        edge_lp = edge_hp + 800.0

    edge_drive = _clamp(_MX_EDGE_DRIVE * tone_finish_mul * intensity_finish, 0.0, 3.0)
    edge_mix = _clamp(_MX_EDGE_MIX * tone_finish_mul * intensity_finish, 0.0, 0.20)

    edge_post_f = _clamp(_MX_EDGE_POST_F, 4200.0, 6500.0)
    edge_post_g = _clamp(_MX_EDGE_POST_G, -1.2, 0.5)
    edge_post_w = _clamp(_MX_EDGE_POST_W, 0.4, 3.0)

    air_on = _MX_AIR_ON
    air_f = _clamp(_MX_AIR_F, 10500.0, 14500.0)
    air_g = _clamp(_MX_AIR_G * tone_finish_mul * intensity_finish, 0.0, 1.2)

    # ---------------------------
    # Section 7: punch keeper + output trim
    # ---------------------------

    punch_on = _MX_PUNCH_ON
    punch_thr = _clamp(_MX_PUNCH_THRESHOLD_DB / intensity_dynamic, -36.0, -12.0)
    punch_ratio = _clamp(_MX_PUNCH_RATIO * intensity_dynamic, 1.0, 1.5)
    punch_att = _clamp(_MX_PUNCH_ATTACK_MS / max(intensity_dynamic, 0.6), 4.0, 60.0)
    punch_rel = _clamp(_MX_PUNCH_RELEASE_MS * intensity_dynamic, 40.0, 260.0)
    punch_knee = _clamp(_MX_PUNCH_KNEE_DB, 0.0, 6.0)
    punch_makeup = _clamp(_MX_PUNCH_MAKEUP_DB, -1.0, 1.0)

    trim_db = _clamp(_MX_TRIM_DB, -6.0, 2.0)

    # build filter graph
    parts = []
    parts.append(f"[0:a]{','.join(serial_parts)}[mx_core]")

    if edge_on and edge_mix > 0.0:
        parts.append("[mx_core]asplit=2[mx_edge_dry][mx_edge_wet]")
        parts.append(
            f"[mx_edge_wet]"
            f"{_os_softclip_chain(drive_db=edge_drive, hp=edge_hp, lp=edge_lp, post_gain_db=0.0)},"
            f"equalizer=f={edge_post_f}:t=q:w={edge_post_w}:g={edge_post_g},"
            f"volume={edge_mix}"
            f"[mx_edge_proc]"
        )
        parts.append("[mx_edge_dry][mx_edge_proc]amix=inputs=2:normalize=0[mx_after_edge]")
    else:
        parts.append("[mx_core]anull[mx_after_edge]")

    if air_on and air_g > 0.0:
        parts.append(f"[mx_after_edge]highshelf=f={air_f}:g={air_g}[mx_after_air]")
    else:
        parts.append("[mx_after_edge]anull[mx_after_air]")

    if punch_on:
        parts.append(
            f"[mx_after_air]"
            f"acompressor=threshold={punch_thr}dB:"
            f"ratio={punch_ratio}:"
            f"attack={punch_att}:"
            f"release={punch_rel}:"
            f"knee={punch_knee}dB:"
            f"makeup={punch_makeup}dB:"
            f"mix=1"
            f"[mx_after_punch]"
        )
    else:
        parts.append("[mx_after_air]anull[mx_after_punch]")

    if abs(trim_db) > 1e-9:
        parts.append(f"[mx_after_punch]volume={trim_db}dB[out]")
    else:
        parts.append("[mx_after_punch]anull[out]")

    fc = ";".join(parts)

    out_args, out_name, _mime = _out_args(fmt)
    out_name = f"polish_{out_name}"
    out_path = os.path.join(td, out_name)

    cmd = (
        f'ffmpeg -y -hide_banner -i {shlex.quote(in_path)} '
        f'-filter_complex "{fc}" -map "[out]" '
        f'{out_args} {shlex.quote(out_path)}'
    )
    _run(cmd)
    return out_path, out_name

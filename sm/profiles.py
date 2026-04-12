# sm/profiles.py

PROFILE_DEFAULTS = {
    "anchor_hold_safe": {
        "amount": 0.18,
        "cap": 0.30,
        "dynamic_scale": 0.20,
        "target_band_mode": "body_hold",
        "protection_mode": "body_strict",
    },
    "anchor_restore_controlled": {
        "amount": 0.34,
        "cap": 0.48,
        "dynamic_scale": 0.50,
        "target_band_mode": "body_restore",
        "protection_mode": "body_guarded",
    },
    "anchor_restrain_upper_body": {
        "amount": 0.20,
        "cap": 0.34,
        "dynamic_scale": 0.35,
        "target_band_mode": "upper_body_restrain",
        "protection_mode": "anti_hole",
    },

    "bridge_hold_safe": {
        "amount": 0.16,
        "cap": 0.28,
        "dynamic_scale": 0.24,
        "target_band_mode": "bridge_hold",
        "protection_mode": "bridge_strict",
    },
    "bridge_restore_controlled": {
        "amount": 0.30,
        "cap": 0.44,
        "dynamic_scale": 0.52,
        "target_band_mode": "bridge_restore",
        "protection_mode": "anti_glue",
    },
    "bridge_restrain_glue": {
        "amount": 0.22,
        "cap": 0.36,
        "dynamic_scale": 0.36,
        "target_band_mode": "bridge_restrain",
        "protection_mode": "bridge_keep",
    },

    "cleanup_guarded_safe": {
        "amount": 0.20,
        "cap": 0.34,
        "dynamic_scale": 0.56,
        "target_band_mode": "cleanup_guarded",
        "protection_mode": "body_bridge_strict",
    },
    "cleanup_focused_dense": {
        "amount": 0.42,
        "cap": 0.60,
        "dynamic_scale": 0.58,
        "target_band_mode": "cleanup_dense",
        "protection_mode": "body_bridge_guarded",
    },
    "cleanup_micro_corrective": {
        "amount": 0.12,
        "cap": 0.22,
        "dynamic_scale": 0.22,
        "target_band_mode": "cleanup_micro",
        "protection_mode": "body_max",
    },

    "guard_hold_safe": {
        "amount": 0.16,
        "cap": 0.28,
        "dynamic_scale": 0.24,
        "target_band_mode": "guard_hold",
        "protection_mode": "transition_keep",
    },
    "guard_boxiness_controlled": {
        "amount": 0.24,
        "cap": 0.38,
        "dynamic_scale": 0.34,
        "target_band_mode": "guard_boxiness",
        "protection_mode": "anti_hole",
    },
    "guard_transition_support_safe": {
        "amount": 0.20,
        "cap": 0.32,
        "dynamic_scale": 0.34,
        "target_band_mode": "guard_transition_support",
        "protection_mode": "transition_strict",
    },

    "projection_mild_safe": {
        "amount": 0.18,
        "cap": 0.30,
        "dynamic_scale": 0.30,
        "target_band_mode": "projection_mild",
        "protection_mode": "top_strict",
    },
    "projection_controlled_dense": {
        "amount": 0.38,
        "cap": 0.54,
        "dynamic_scale": 0.46,
        "target_band_mode": "projection_dense",
        "protection_mode": "top_guarded",
    },
    "projection_clamp_safe": {
        "amount": 0.12,
        "cap": 0.22,
        "dynamic_scale": 0.20,
        "target_band_mode": "projection_clamp",
        "protection_mode": "top_max",
    },

    "finish_spark_micro_safe": {
        "amount": 0.10,
        "cap": 0.18,
        "dynamic_scale": 0.16,
        "target_band_mode": "spark_micro",
        "protection_mode": "sibilance_max",
    },
    "finish_spark_controlled_excited": {
        "amount": 0.22,
        "cap": 0.34,
        "dynamic_scale": 0.24,
        "target_band_mode": "spark_controlled",
        "protection_mode": "spark_guarded",
    },
    "finish_spark_off": {
        "amount": 0.0,
        "cap": 0.0,
        "dynamic_scale": 0.0,
        "target_band_mode": "off",
        "protection_mode": "off",
    },
}

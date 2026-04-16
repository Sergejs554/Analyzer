    # --------------------------------------------------------
    # Delivery
    # --------------------------------------------------------
    (RoleName.DELIVERY, "fullband_delivery"): _mode(
        role=RoleName.DELIVERY,
        target_band_mode="fullband_delivery",
        role_rank_bias="primary",
        stack_templates=[
            _stack(
                stack_name="delivery_final_stack",
                stack_kind="delivery",
                path_type="delivery",
                default_tap_point=NODE_FINISH_OUT,
                output_node=NODE_DELIVERY_OUT,
                recombine_target="final_output",
                recombine_mode="sum",
                preferred_order=60,
                allowed_primitive_names=[
                    "output_gain_trim",
                    "safety_ceiling_trim",
                    "true_peak_limiter",
                ],
                required_safety_tags=[
                    "delivery_sensitive",
                ],
                notes=[
                    "Delivery is last-mile protection only.",
                    "It trims level and true peak without repainting polish voicing.",
                    "Its job is safe release, not new tone.",
                ],
            ),
        ],
        required_safety_tags=[
            "delivery_sensitive",
        ],
        blocked_by_default_clamps=[],
        notes=[
            "Fullband delivery is the final protection lane of the polish main branch.",
            "It must solve true-peak and release safety without buying safety through tonal damage.",
        ],
    ),
    (RoleName.DELIVERY, "off"): _mode(
        role=RoleName.DELIVERY,
        target_band_mode="off",
        role_rank_bias="off",
        stack_templates=[],
        notes=[
            "Delivery off.",
        ],
    ),

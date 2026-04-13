def build_router_context(
    analysis: SmartMasterAnalysis,
    selection: RoleProfileSelection,
) -> RouterContext:
    m = analysis.metrics
    d = analysis.derived
    a = analysis.anchor
    b = analysis.bridge
    c = analysis.cleanup
    g = analysis.guard
    p = analysis.projection

    primary_correction_lane = _collect_lane_from_selection(selection, "correction") or _fallback_primary_correction_lane(selection)
    secondary_support_lane = _collect_lane_from_selection(selection, "support") or _fallback_support_lane(selection)
    primary_benefit_lane = _collect_lane_from_selection(selection, "benefit") or _fallback_benefit_lane(selection)

    top_safe = (
        p.harshness_risk == RiskLevel.LOW
        and p.sibilance_risk == RiskLevel.LOW
        and (_has(d.top_push_safety_proxy) is False or d.top_push_safety_proxy >= 0.60)
    )
    top_guarded = (
        (not top_safe)
        and (_has(d.top_push_safety_proxy) is False or d.top_push_safety_proxy >= 0.42)
    )
    top_fragile = (not top_safe) and (not top_guarded)

    foundation_missing = not a.foundation_present
    body_fragile = _risk_ge(a.fragility, RiskLevel.MEDIUM)
    body_weak = (
        foundation_missing
        or (_has(d.center_body_support_proxy) and d.center_body_support_proxy < 0.44)
        or (_has(m.low_body_150_300_db) and m.low_body_150_300_db < 31.0)
    )

    bridge_broken = _role_key(b.state) == "broken"
    bridge_gluey = _role_key(b.state) == "overglued" or _risk_ge(b.glue_risk, RiskLevel.MEDIUM)
    bridge_gap_risky = _risk_ge(b.gap_risk, RiskLevel.MEDIUM)

    cleanup_heavy_needed = (
        _role_key(c.readiness) == "safe"
        and c.buildup_risk == RiskLevel.HIGH
    )
    cleanup_guarded = _role_key(c.readiness) == "guarded"

    boxy_active = _role_key(g.shape) == "boxy"
    transition_fragile = _role_key(g.transition_state) in {"weak", "thinning"}

    underprojected = _role_key(p.state) == "underprojected"
    overpushed = _role_key(p.state) == "overpushed"

    dense_good_candidate = (
        not body_fragile
        and not body_weak
        and not bridge_broken
        and not boxy_active
        and not transition_fragile
        and (_has(d.center_body_support_proxy) is False or d.center_body_support_proxy >= 0.54)
        and (_has(d.body_to_mid_handoff_proxy) is False or d.body_to_mid_handoff_proxy >= 0.54)
        and (_has(m.mud_to_body_db) is False or m.mud_to_body_db < 0.10)
    )

    dirty_dense_candidate = (
        cleanup_heavy_needed
        and (boxy_active or (_has(m.mud_to_body_db) and m.mud_to_body_db >= -0.05))
        and (bridge_gluey or (_has(m.lowmid_buildup_ratio_db) and m.lowmid_buildup_ratio_db >= 17.2))
    )

    thin_candidate = body_weak or bridge_broken or transition_fragile

    return RouterContext(
        analysis=analysis,
        selection=selection,
        primary_correction_lane=primary_correction_lane,
        secondary_support_lane=secondary_support_lane,
        primary_benefit_lane=primary_benefit_lane,
        top_safe=top_safe,
        top_guarded=top_guarded,
        top_fragile=top_fragile,
        body_fragile=body_fragile,
        body_weak=body_weak,
        foundation_missing=foundation_missing,
        bridge_broken=bridge_broken,
        bridge_gluey=bridge_gluey,
        bridge_gap_risky=bridge_gap_risky,
        cleanup_heavy_needed=cleanup_heavy_needed,
        cleanup_guarded=cleanup_guarded,
        boxy_active=boxy_active,
        transition_fragile=transition_fragile,
        underprojected=underprojected,
        overpushed=overpushed,
        dirty_dense_candidate=dirty_dense_candidate,
        dense_good_candidate=dense_good_candidate,
        thin_candidate=thin_candidate,
    )

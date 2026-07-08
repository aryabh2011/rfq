import pytest

from src.execution.rfq_anchor_engine import MinimalistPennyingEngine, _round_down_to_cents


def test_single_leg_produces_two_sided_quote():
    engine = MinimalistPennyingEngine(fee_buffer=0.02)
    # independent = 0.60, yes_bid = 0.60-0.02 = 0.58, no_bid = 0.40-0.02 = 0.38
    quote = engine.calculate_anchored_quote([0.60])
    assert quote == (0.58, 0.38)


def test_two_leg_combo_sums_below_one():
    engine = MinimalistPennyingEngine(fee_buffer=0.02)
    # independent = 0.42, yes_bid = 0.40, no_bid = 0.58-0.02 = 0.56
    yes_bid, no_bid = engine.calculate_anchored_quote([0.60, 0.70])
    assert (yes_bid, no_bid) == (0.40, 0.56)
    assert yes_bid + no_bid <= 1.0


def test_declines_a_side_when_margin_swallows_it():
    engine = MinimalistPennyingEngine(fee_buffer=0.05)
    # independent = 0.99*0.99 = 0.9801; no_bid_raw = 0.0199 - 0.05 < 0 -> decline that side
    yes_bid, no_bid = engine.calculate_anchored_quote([0.99, 0.99])
    assert yes_bid == 0.93
    assert no_bid == 0.0


def test_returns_none_when_both_sides_would_be_declined():
    engine = MinimalistPennyingEngine(fee_buffer=0.495)
    # independent = 0.5; yes_bid_raw = 0.005, no_bid_raw = 0.005 -- both below the min tick
    assert engine.calculate_anchored_quote([0.5]) is None


def test_fee_buffer_scales_with_leg_count():
    engine = MinimalistPennyingEngine(fee_buffer=0.02, fee_buffer_per_extra_leg=0.01, base_legs=2)
    yes_bid, no_bid = engine.calculate_anchored_quote([0.5, 0.6, 0.7])
    # independent ~= 0.21, 1 extra leg -> effective fee_buffer = 0.03
    assert yes_bid == pytest.approx(0.18, abs=0.005)
    assert no_bid == pytest.approx(0.76, abs=0.005)


def test_fee_buffer_does_not_scale_by_default():
    engine = MinimalistPennyingEngine(fee_buffer=0.02)
    yes_bid, no_bid = engine.calculate_anchored_quote([0.5, 0.6, 0.7])
    assert yes_bid == pytest.approx(0.19, abs=0.005)
    assert no_bid == pytest.approx(0.77, abs=0.005)


def test_rejects_out_of_bounds_inputs():
    engine = MinimalistPennyingEngine()
    assert engine.calculate_anchored_quote([]) is None
    assert engine.calculate_anchored_quote([1.5]) is None
    assert engine.calculate_anchored_quote([0.0]) is None


def test_engine_rejects_invalid_construction_params():
    with pytest.raises(ValueError):
        MinimalistPennyingEngine(fee_buffer_per_extra_leg=-0.01)
    with pytest.raises(ValueError):
        MinimalistPennyingEngine(base_legs=0)
    with pytest.raises(ValueError):
        MinimalistPennyingEngine(fee_buffer=1.0)


def test_round_down_to_cents_never_rounds_up_past_a_half_cent_boundary():
    # 0.41 + 0.005 == 0.415 exactly in float; a naive round(x, 2) can round this UP to 0.42,
    # which would mean bidding a cent more than intended -- overpaying. Rounding down is safe.
    value = 0.41 + 0.005
    assert _round_down_to_cents(value) == 0.41


def test_round_down_to_cents_does_not_undershoot_a_clean_value():
    assert _round_down_to_cents(0.40) == 0.40
    assert _round_down_to_cents(0.56) == 0.56

from src.execution.rfq_penny_engine import MinimalistPennyingEngine


def test_yes_pennies_when_room_available():
    engine = MinimalistPennyingEngine(fee_buffer=0.02)
    # independent=0.42, floor=0.44, ceiling=min(0.6,0.7)=0.6
    quote = engine.calculate_safe_quote([0.60, 0.70], best_competitor_quote=0.50, side="yes")
    assert quote == 0.49


def test_yes_falls_back_to_floor_when_squeezed():
    engine = MinimalistPennyingEngine(fee_buffer=0.02)
    # independent=0.42, floor=0.44
    quote = engine.calculate_safe_quote([0.60, 0.70], best_competitor_quote=0.43, side="yes")
    assert quote == 0.44


def test_yes_rejects_when_floor_breaches_ceiling():
    engine = MinimalistPennyingEngine(fee_buffer=0.3)
    # independent=0.25, floor=0.55, ceiling=min(0.5,0.5)=0.5 -> floor >= ceiling
    quote = engine.calculate_safe_quote([0.5, 0.5], best_competitor_quote=0.45, side="yes")
    assert quote is None


def test_no_pennies_when_room_available():
    engine = MinimalistPennyingEngine(fee_buffer=0.02)
    # independent=0.42, no_baseline=0.58, floor=max(0.60, 1-0.6=0.40)=0.60
    quote = engine.calculate_safe_quote([0.60, 0.70], best_competitor_quote=0.70, side="no")
    assert quote == 0.69


def test_no_falls_back_to_floor_when_squeezed():
    engine = MinimalistPennyingEngine(fee_buffer=0.02)
    quote = engine.calculate_safe_quote([0.60, 0.70], best_competitor_quote=0.60, side="no")
    assert quote == 0.60


def test_rejects_out_of_bounds_inputs():
    engine = MinimalistPennyingEngine()
    assert engine.calculate_safe_quote([], best_competitor_quote=0.5, side="yes") is None
    assert engine.calculate_safe_quote([1.5], best_competitor_quote=0.5, side="yes") is None
    assert engine.calculate_safe_quote([0.5], best_competitor_quote=1.5, side="yes") is None

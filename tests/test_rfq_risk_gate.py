import pytest

from src.execution.rfq_risk_gate import RFQInventoryManager


@pytest.fixture
def gate():
    return RFQInventoryManager(
        get_bankroll=lambda: 10_000.0,
        max_exposure_pct_per_contract=0.01,
        max_exposure_pct_per_prefix=0.03,
    )


async def test_reserve_within_cap_succeeds(gate):
    # liability = (1-0.5)*100 = 50, cap = 10000*0.01 = 100
    assert await gate.try_reserve("TICKER-A", price=0.5, contracts=100) is True


async def test_reserve_rejects_over_contract_cap(gate):
    # liability = (1-0.5)*1000 = 500 > cap 100
    assert await gate.try_reserve("TICKER-B", price=0.5, contracts=1000) is False


async def test_prefix_concentration_cap_blocks_correlated_tickers():
    gate = RFQInventoryManager(
        get_bankroll=lambda: 10_000.0,
        max_exposure_pct_per_contract=0.02,
        max_exposure_pct_per_prefix=0.03,
    )
    # per-contract cap = 200, per-prefix cap = 300; both tickers share prefix "EVENT-A"
    assert await gate.try_reserve("EVENT-A-YES", price=0.5, contracts=300) is True  # liability=150
    assert await gate.try_reserve("EVENT-A-NO", price=0.5, contracts=350) is False  # would total 325 > 300


async def test_release_frees_capacity(gate):
    assert await gate.try_reserve("TICKER-C", price=0.5, contracts=100) is True
    gate.release("TICKER-C", price=0.5, contracts=100)
    assert await gate.try_reserve("TICKER-C", price=0.5, contracts=100) is True

import pytest

from src.execution.rfq_risk_gate import RFQInventoryManager, contract_liability, legs_share_event


def test_contract_liability_is_buyer_side_not_seller_side():
    # the bot buys YES/NO from the requester; worst case is losing the full amount paid,
    # not "1 - price" (that would be seller risk, which isn't this bot's role).
    assert contract_liability(price=0.40, contracts=10) == 4.0


def test_legs_share_event_detects_same_game_combo():
    # both legs strip to prefix "KXNFLGAME-24DEC25KCMIA" -> SGP, should be flagged
    assert legs_share_event(["KXNFLGAME-24DEC25KCMIA-KC", "KXNFLGAME-24DEC25KCMIA-OVER45"]) is True


def test_legs_share_event_allows_cross_game_combo():
    # different games entirely -> MGP, should not be flagged
    assert legs_share_event(["KXNFLGAME-24DEC25KCMIA-KC", "KXNFLGAME-24DEC25DALPHI-DAL"]) is False


def test_legs_share_event_single_leg_is_never_flagged():
    assert legs_share_event(["KXNFLGAME-24DEC25KCMIA-KC"]) is False


def test_legs_share_event_flags_if_any_pair_matches():
    # 3 legs, two of which share an event -> still correlated, should be flagged
    assert legs_share_event([
        "KXNFLGAME-24DEC25KCMIA-KC",
        "KXNFLGAME-24DEC25KCMIA-OVER45",
        "KXNFLGAME-24DEC25DALPHI-DAL",
    ]) is True


@pytest.fixture
def gate():
    return RFQInventoryManager(
        get_bankroll=lambda: 10_000.0,
        max_exposure_pct_per_contract=0.01,
        max_exposure_pct_per_prefix=0.03,
    )


async def test_reserve_within_cap_succeeds(gate):
    # cap = 10000*0.01 = 100
    assert await gate.try_reserve("TICKER-A", liability=50.0) is True


async def test_reserve_rejects_over_contract_cap(gate):
    assert await gate.try_reserve("TICKER-B", liability=500.0) is False


async def test_prefix_concentration_cap_blocks_correlated_tickers():
    gate = RFQInventoryManager(
        get_bankroll=lambda: 10_000.0,
        max_exposure_pct_per_contract=0.02,
        max_exposure_pct_per_prefix=0.03,
    )
    # per-contract cap = 200, per-prefix cap = 300; both tickers share prefix "EVENT-A"
    assert await gate.try_reserve("EVENT-A-YES", liability=150.0) is True
    assert await gate.try_reserve("EVENT-A-NO", liability=175.0) is False  # 150 + 175 = 325 > 300


async def test_release_frees_capacity(gate):
    assert await gate.try_reserve("TICKER-C", liability=50.0) is True
    gate.release("TICKER-C", liability=50.0)
    assert await gate.try_reserve("TICKER-C", liability=50.0) is True


async def test_seed_reflects_real_liability_and_is_respected_by_new_reservations():
    gate = RFQInventoryManager(
        get_bankroll=lambda: 10_000.0,
        max_exposure_pct_per_contract=0.01,
        max_exposure_pct_per_prefix=0.03,
    )
    # cap per contract = 100, per prefix = 300
    gate.seed({"EVENT-A-YES": 80.0})
    # 80 (seeded) + 30 (new) = 110 > 100 cap -> rejected, proving the seed is actually enforced
    assert await gate.try_reserve("EVENT-A-YES", liability=30.0) is False
    # a different ticker under the same prefix should also see the seeded prefix liability
    assert await gate.try_reserve("EVENT-A-NO", liability=220.0) is False  # 80 + 220 > 300 cap


def test_seed_overwrites_rather_than_accumulates():
    gate = RFQInventoryManager(get_bankroll=lambda: 10_000.0)
    gate.seed({"TICKER-A": 50.0})
    gate.seed({"TICKER-A": 10.0})  # a second reconciliation should replace, not add to, the first
    assert gate._liability_by_ticker["TICKER-A"] == 10.0


async def test_notional_floor_rescues_a_tiny_bankroll():
    # at a $52.48 bankroll, 1%/3% caps round to $0.52/$1.57 -- smaller than almost any real
    # contract's liability. The floor should let a realistic quote through anyway.
    gate = RFQInventoryManager(
        get_bankroll=lambda: 52.48,
        max_exposure_pct_per_contract=0.01,
        max_exposure_pct_per_prefix=0.03,
        min_notional_cap_per_contract_usd=2.00,
        min_notional_cap_per_prefix_usd=5.00,
    )
    assert await gate.try_reserve("EVENT-A-YES", liability=2.00) is True  # fits the $2 floor


async def test_notional_floor_still_enforces_a_real_cap():
    gate = RFQInventoryManager(
        get_bankroll=lambda: 52.48,
        min_notional_cap_per_contract_usd=2.00,
        min_notional_cap_per_prefix_usd=5.00,
    )
    assert await gate.try_reserve("EVENT-A-YES", liability=50.0) is False  # $50 > $2 floor


async def test_notional_floor_yields_to_percentage_once_bankroll_grows():
    # once bankroll * pct exceeds the floor, the percentage takes back over automatically.
    gate = RFQInventoryManager(
        get_bankroll=lambda: 100_000.0,
        max_exposure_pct_per_contract=0.01,  # 1% of 100k = $1000, well above the $2 floor
        min_notional_cap_per_contract_usd=2.00,
        min_notional_cap_per_prefix_usd=5.00,
    )
    assert await gate.try_reserve("EVENT-A-YES", liability=50.0) is True  # $50 < $1000 cap


def test_rejects_invalid_notional_floor_construction():
    with pytest.raises(ValueError):
        RFQInventoryManager(get_bankroll=lambda: 1.0, min_notional_cap_per_contract_usd=-1.0)
    with pytest.raises(ValueError):
        RFQInventoryManager(
            get_bankroll=lambda: 1.0,
            min_notional_cap_per_contract_usd=10.0,
            min_notional_cap_per_prefix_usd=5.0,
        )

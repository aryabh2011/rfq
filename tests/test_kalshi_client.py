from src.kalshi_client import (
    KalshiWebSocketClient,
    OrderbookLevel,
    OrderbookSnapshot,
    QuoteAccepted,
    QuoteExecuted,
    RFQBroadcast,
    _parse_levels,
    compute_vwmid,
    extract_position_liabilities,
)


def _book(yes: list[tuple[float, int]], no: list[tuple[float, int]]) -> OrderbookSnapshot:
    return OrderbookSnapshot(
        ticker="TEST",
        yes_levels=[OrderbookLevel(price=p, size=s) for p, s in yes],
        no_levels=[OrderbookLevel(price=p, size=s) for p, s in no],
    )


def test_vwmid_balances_between_bid_and_implied_ask():
    # yes bid 0.40 (size 100), no bid 0.55 (size 100) -> implied yes ask 0.45
    book = _book(yes=[(0.40, 100)], no=[(0.55, 100)])
    assert compute_vwmid(book) == 0.425  # equal sizes -> simple midpoint


def test_vwmid_leans_toward_thinner_side():
    # yes bid 0.40 (size 300) weighted by no_size=100; no bid 0.55 (size 100) weighted by yes_size=300
    # microprice = (0.40*100 + 0.45*300) / 400 = 0.4375
    book = _book(yes=[(0.40, 300)], no=[(0.55, 100)])
    assert compute_vwmid(book) == 0.4375


def test_vwmid_returns_none_on_crossed_book():
    # yes bid 0.60, no bid 0.50 -> implied yes ask = 0.50 <= yes bid 0.60
    book = _book(yes=[(0.60, 50)], no=[(0.50, 50)])
    assert compute_vwmid(book) is None


def test_vwmid_returns_none_on_empty_side():
    assert compute_vwmid(_book(yes=[], no=[(0.5, 10)])) is None
    assert compute_vwmid(_book(yes=[(0.5, 10)], no=[])) is None


def test_vwmid_picks_best_of_multiple_levels():
    book = _book(yes=[(0.30, 10), (0.40, 20)], no=[(0.50, 10), (0.55, 20)])
    # best yes bid = 0.40 (size 20), best no bid = 0.55 (size 20) -> implied ask 0.45, equal sizes -> midpoint
    assert compute_vwmid(book) == 0.425


def test_parse_message_rfq_created_with_mve_legs():
    # real shape observed against Kalshi's demo API: a combo RFQ carries its legs directly
    message = {
        "type": "rfq_created",
        "sid": 1,
        "seq": 1,
        "msg": {
            "id": "rfq-abc123",
            "creator_id": "some-id",
            "market_ticker": "KXMVESPORTSMULTIGAMEEXTENDED-S1-X",
            "target_cost_dollars": "0.5000",
            "created_ts": "2026-07-08T00:00:00Z",
            "mve_collection_ticker": "KXMVESPORTSMULTIGAMEEXTENDED-R",
            "mve_selected_legs": [
                {"event_ticker": "KXMLBGAME-A", "market_ticker": "KXMLBGAME-A-TEAM1", "side": "yes"},
                {"event_ticker": "KXMLBGAME-B", "market_ticker": "KXMLBGAME-B-TEAM2", "side": "yes"},
            ],
        },
    }
    event = KalshiWebSocketClient._parse_message(message)
    assert event == RFQBroadcast(
        rfq_id="rfq-abc123",
        market_ticker="KXMVESPORTSMULTIGAMEEXTENDED-S1-X",
        leg_tickers=["KXMLBGAME-A-TEAM1", "KXMLBGAME-B-TEAM2"],
        contracts=None,
        target_cost_dollars=0.5,
    )


def test_parse_message_rfq_created_plain_market_uses_itself_as_the_only_leg():
    # a non-combo RFQ has no mve_selected_legs -- the market itself is the sole "leg"
    message = {
        "type": "rfq_created",
        "msg": {"id": "rfq-xyz", "market_ticker": "FED-23DEC-T3.00", "contracts_fp": "100.00"},
    }
    event = KalshiWebSocketClient._parse_message(message)
    assert event == RFQBroadcast(
        rfq_id="rfq-xyz",
        market_ticker="FED-23DEC-T3.00",
        leg_tickers=["FED-23DEC-T3.00"],
        contracts=100.0,
        target_cost_dollars=None,
    )


def test_parse_message_ignores_unactionable_types():
    assert KalshiWebSocketClient._parse_message({"type": "subscribed", "msg": {}}) is None
    assert KalshiWebSocketClient._parse_message({"type": "quote_created", "msg": {}}) is None
    assert KalshiWebSocketClient._parse_message({"type": "rfq_deleted", "msg": {}}) is None


def test_parse_message_rfq_created_drops_missing_size_fields():
    message = {"type": "rfq_created", "msg": {"id": "rfq-abc", "market_ticker": "FED-23DEC-T3.00"}}
    assert KalshiWebSocketClient._parse_message(message) is None


def test_parse_message_rfq_created_drops_non_positive_size():
    message = {
        "type": "rfq_created",
        "msg": {"id": "rfq-abc", "market_ticker": "FED-23DEC-T3.00", "contracts_fp": "0.00"},
    }
    assert KalshiWebSocketClient._parse_message(message) is None


def test_parse_message_quote_accepted():
    message = {
        "type": "quote_accepted",
        "msg": {"quote_id": "q1", "rfq_id": "rfq1", "accepted_side": "yes", "contracts_accepted_fp": "12.50"},
    }
    assert KalshiWebSocketClient._parse_message(message) == QuoteAccepted(
        quote_id="q1", rfq_id="rfq1", accepted_side="yes", contracts_accepted=12.5,
    )


def test_parse_message_quote_executed():
    message = {
        "type": "quote_executed",
        "msg": {"quote_id": "q1", "rfq_id": "rfq1", "order_id": "order1"},
    }
    assert KalshiWebSocketClient._parse_message(message) == QuoteExecuted(
        quote_id="q1", rfq_id="rfq1", order_id="order1",
    )


def test_parse_levels_parses_dollar_and_fixed_point_strings():
    levels = _parse_levels([["0.4200", "13.00"], ["0.3800", "5.50"]])
    assert levels == [OrderbookLevel(price=0.42, size=13.0), OrderbookLevel(price=0.38, size=5.5)]


def test_parse_levels_skips_unparseable_entries():
    levels = _parse_levels([["0.4200", "13.00"], ["not-a-number", "5.00"], None])
    assert levels == [OrderbookLevel(price=0.42, size=13.0)]


def test_parse_levels_handles_missing_or_empty():
    assert _parse_levels(None) == []
    assert _parse_levels([]) == []


def test_vwmid_rejects_thin_top_of_book():
    # sizes below the default min_size=5 -> untradeable, even though the book isn't crossed/empty
    book = _book(yes=[(0.40, 3)], no=[(0.55, 3)])
    assert compute_vwmid(book) is None
    assert compute_vwmid(book, min_size=1) == 0.425  # explicit lower min_size allows it through


def test_extract_position_liabilities_parses_market_exposure():
    positions = [
        {"ticker": "EVENT-A-YES", "market_exposure": 4000},  # $40.00
        {"ticker": "EVENT-B-NO", "market_exposure": 250},    # $2.50
    ]
    liabilities, fully_parsed = extract_position_liabilities(positions)
    assert fully_parsed is True
    assert liabilities == {"EVENT-A-YES": 40.0, "EVENT-B-NO": 2.5}


def test_extract_position_liabilities_flags_unparseable_records():
    positions = [
        {"ticker": "EVENT-A-YES", "market_exposure": 4000},
        {"ticker": "EVENT-B-NO"},  # missing market_exposure
    ]
    liabilities, fully_parsed = extract_position_liabilities(positions)
    assert fully_parsed is False
    assert liabilities == {"EVENT-A-YES": 40.0}  # the parseable record is still kept


def test_extract_position_liabilities_sums_duplicate_tickers():
    positions = [
        {"ticker": "EVENT-A-YES", "market_exposure": 1000},
        {"ticker": "EVENT-A-YES", "market_exposure": 500},
    ]
    liabilities, fully_parsed = extract_position_liabilities(positions)
    assert fully_parsed is True
    assert liabilities == {"EVENT-A-YES": 15.0}

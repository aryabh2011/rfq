from unittest.mock import AsyncMock, MagicMock

from src.kalshi_client import KalshiHttpClient


def _make_client(kalshi_config, **kwargs) -> KalshiHttpClient:
    return KalshiHttpClient(kalshi_config, max_read_requests_per_second=100.0, max_write_requests_per_second=100.0, **kwargs)


def _mock_response(status_code: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.raise_for_status = MagicMock()
    return response


async def test_get_orderbook_acquires_from_read_bucket_not_write(kalshi_config):
    client = _make_client(kalshi_config)
    client._read_rate_limiter.acquire = AsyncMock()
    client._write_rate_limiter.acquire = AsyncMock()
    response = _mock_response()
    response.json = MagicMock(return_value={"orderbook_fp": {}})
    client._client.get = AsyncMock(return_value=response)

    await client.get_orderbook("SOME-TICKER")

    client._read_rate_limiter.acquire.assert_awaited_once()
    client._write_rate_limiter.acquire.assert_not_awaited()


async def test_create_quote_acquires_from_write_bucket_not_read(kalshi_config):
    client = _make_client(kalshi_config)
    client._read_rate_limiter.acquire = AsyncMock()
    client._write_rate_limiter.acquire = AsyncMock()
    response = _mock_response(201)
    response.json = MagicMock(return_value={"id": "quote-1"})
    client._client.post = AsyncMock(return_value=response)

    quote_id = await client.create_quote("rfq-1", 0.4, 0.55, 10.0, 10.0)

    assert quote_id == "quote-1"
    client._write_rate_limiter.acquire.assert_awaited_once()
    client._read_rate_limiter.acquire.assert_not_awaited()


async def test_confirm_quote_acquires_from_write_bucket(kalshi_config):
    client = _make_client(kalshi_config)
    client._read_rate_limiter.acquire = AsyncMock()
    client._write_rate_limiter.acquire = AsyncMock()
    client._client.put = AsyncMock(return_value=_mock_response(204))

    await client.confirm_quote("rfq-1", "quote-1")

    client._write_rate_limiter.acquire.assert_awaited_once()
    client._read_rate_limiter.acquire.assert_not_awaited()


async def test_upgrade_api_usage_level_posts_to_correct_path(kalshi_config):
    client = _make_client(kalshi_config)
    client._client.post = AsyncMock(return_value=_mock_response(201))

    await client.upgrade_api_usage_level()

    args, _ = client._client.post.call_args
    assert args[0] == "/trade-api/v2/account/api_usage_level/upgrade"


async def test_429_response_is_logged_distinctly(kalshi_config, caplog):
    client = _make_client(kalshi_config)
    client._client.get = AsyncMock(return_value=_mock_response(429))

    response = await client._get("/some/path", headers={})

    assert response.status_code == 429
    assert any("Rate limited by Kalshi" in record.message for record in caplog.records)

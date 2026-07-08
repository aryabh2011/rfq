import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from src.config import KalshiConfig


@pytest.fixture
def kalshi_config(tmp_path):
    """A throwaway, self-generated key so tests never depend on real project credentials --
    KalshiHttpClient loads a real key file on construction regardless of whether any network
    call is ever made."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_path = tmp_path / "test_key.pem"
    key_path.write_bytes(pem)
    return KalshiConfig(
        demo_mode=True,
        api_key_id="test-key-id",
        private_key_path=str(key_path),
        rest_host="https://example.invalid",
        rest_path_prefix="/trade-api/v2",
        ws_host="wss://example.invalid",
        ws_path="/trade-api/ws/v2",
    )

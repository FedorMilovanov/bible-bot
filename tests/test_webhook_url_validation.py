import pytest

from web_api import telegram_transport


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com",
        "https://example.com/path",
        "https://user@example.com",
        "https://user:pass@example.com",
        "https://example.com/?x=1",
        "https://example.com/#fragment",
        "https://example.com:4443",
        "https://example.com:not-a-port",
    ],
)
def test_webhook_base_url_rejects_non_origin_or_unsupported_urls(monkeypatch, url):
    monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", url)

    with pytest.raises(telegram_transport.TransportConfigurationError):
        telegram_transport.telegram_webhook_base_url()


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com",
        "https://example.com/",
        "https://example.com:443",
        "https://example.com:80",
        "https://example.com:88",
        "https://example.com:8443/",
    ],
)
def test_webhook_base_url_accepts_telegram_supported_https_origins(monkeypatch, url):
    monkeypatch.setenv("TELEGRAM_WEBHOOK_BASE_URL", url)

    assert telegram_transport.telegram_webhook_base_url() == url.rstrip("/")
    assert telegram_transport.telegram_webhook_url().endswith("/telegram/webhook")

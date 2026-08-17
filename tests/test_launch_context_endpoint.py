import hashlib
import hmac
import json
import time
from urllib.parse import parse_qsl, urlencode

import web_api
from web_api import auth


def signed_init_data(
    token: str,
    *,
    user_id: int = 987654321,
    start_param: str = "v1_site_app__home",
    query_id: str = "launch-query",
    auth_date: int | None = None,
) -> str:
    data = {
        "auth_date": str(auth_date if auth_date is not None else int(time.time())),
        "query_id": query_id,
        "start_param": start_param,
        "user": json.dumps(
            {"id": user_id, "first_name": "Launch Test"},
            separators=(",", ":"),
        ),
    }
    check = "\n".join(f"{key}={value}" for key, value in sorted(data.items()))
    secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    data["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return urlencode(data)


def test_verified_payload_exposes_signed_start_param_and_query_id(monkeypatch):
    token = "123456:TEST_TOKEN"
    monkeypatch.setenv("BOT_TOKEN", token)
    payload = auth.verify_init_data_payload(signed_init_data(token))

    assert payload is not None
    assert payload.user["id"] == 987654321
    assert payload.start_param == "v1_site_app__home"
    assert payload.query_id == "launch-query"


def test_tampered_start_param_and_expired_init_data_are_rejected(monkeypatch):
    token = "123456:TEST_TOKEN"
    monkeypatch.setenv("BOT_TOKEN", token)

    signed = signed_init_data(token)
    pairs = dict(parse_qsl(signed, keep_blank_values=True))
    pairs["start_param"] = "v1_tg_pin__home"
    assert auth.verify_init_data_payload(urlencode(pairs)) is None

    expired = signed_init_data(token, auth_date=int(time.time()) - 25 * 60 * 60)
    assert auth.verify_init_data_payload(expired) is None


def test_launch_endpoint_ignores_client_source_and_uses_signed_start_param(
    monkeypatch,
):
    token = "123456:TEST_TOKEN"
    monkeypatch.setenv("BOT_TOKEN", token)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setattr(web_api, "persist_launch_attribution", lambda **_: True)

    app = web_api.create_app()
    client = app.test_client()
    response = client.get(
        "/api/launch-context?source=vk_pin&destination=chapter5",
        headers={"X-Telegram-Init-Data": signed_init_data(token)},
    )

    assert response.status_code == 200
    assert response.get_json() == {
        "kind": "v1",
        "source": "site_app",
        "destination": "home",
        "return_context": {
            "kind": "site",
            "label": "Вернуться на сайт",
            "url": "https://gospod-bog.ru/app/",
        },
        "attribution_persisted": True,
    }


def test_debug_identity_cannot_be_launch_attribution_authority(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("ALLOW_DEBUG_AUTH", "true")
    monkeypatch.setenv("BOT_TOKEN", "123456:TEST_TOKEN")
    app = web_api.create_app()
    client = app.test_client()

    response = client.get(
        "/api/launch-context",
        headers={"X-Debug-User-Id": "123"},
    )
    assert response.status_code == 401


def test_attribution_persistence_failure_does_not_break_signed_context(
    monkeypatch,
):
    token = "123456:TEST_TOKEN"
    monkeypatch.setenv("BOT_TOKEN", token)

    def fail_persistence(**_kwargs):
        raise RuntimeError("simulated telemetry outage")

    monkeypatch.setattr(web_api, "persist_launch_attribution", fail_persistence)
    app = web_api.create_app()
    client = app.test_client()
    response = client.get(
        "/api/launch-context",
        headers={"X-Telegram-Init-Data": signed_init_data(token)},
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["kind"] == "v1"
    assert body["source"] == "site_app"
    assert body["destination"] == "home"
    assert body["attribution_persisted"] is False

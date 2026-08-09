import hashlib
import hmac
import json
import time
from urllib.parse import urlencode

from web_api import auth


def signed_init_data(token: str, user_id):
    data = {
        "auth_date": str(int(time.time())),
        "query_id": "schema-test",
        "user": json.dumps({"id": user_id, "first_name": "Test"}, separators=(",", ":")),
    }
    check = "\n".join(f"{key}={value}" for key, value in sorted(data.items()))
    secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    data["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return urlencode(data)


def test_signed_positive_integer_user_id_is_accepted(monkeypatch):
    token = "123456:TEST_TOKEN"
    monkeypatch.setenv("BOT_TOKEN", token)

    user = auth.verify_init_data(signed_init_data(token, 9876543210))

    assert user["id"] == 9876543210


def test_signed_non_integer_or_non_positive_user_ids_are_rejected(monkeypatch):
    token = "123456:TEST_TOKEN"
    monkeypatch.setenv("BOT_TOKEN", token)

    for invalid_id in ("123", True, False, 0, -1, None):
        assert auth.verify_init_data(signed_init_data(token, invalid_id)) is None

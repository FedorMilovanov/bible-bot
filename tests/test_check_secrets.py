from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_secrets.py"
SPEC = spec_from_file_location("check_secrets_module", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
CHECK_SECRETS = module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK_SECRETS)


def _mongo_uri(*, username: str = "real_user", password: str = "real_password") -> str:
    return (
        "MONGO_URL="
        + "mongodb+srv://"
        + username
        + ":"
        + password
        + "@cluster.mongodb.net/?retryWrites=true&w=majority"
    )


def _telegram_token() -> str:
    return "BOT_TOKEN=" + "1234567890:" + ("A" * 35)


def _example_mongo_placeholder() -> str:
    return (
        "MONGO_URL="
        + "mongodb+srv://"
        + "user:password@cluster0.example.mongodb.net/"
        + "?retryWrites=true&w=majority"
    )


def test_only_exact_env_example_mongo_placeholder_is_allowlisted():
    label = "MongoDB URI with embedded credentials"
    placeholder = _example_mongo_placeholder()

    assert CHECK_SECRETS.PATTERNS[label].search(placeholder)
    assert CHECK_SECRETS.is_safe_placeholder(".env.example", label, placeholder)
    assert not CHECK_SECRETS.is_safe_placeholder(
        ".env.example",
        label,
        _mongo_uri(),
    )
    assert not CHECK_SECRETS.is_safe_placeholder(
        "docs/example.md",
        label,
        placeholder,
    )


def test_env_example_real_mongo_credentials_are_detected():
    line = _mongo_uri()
    label = "MongoDB URI with embedded credentials"

    assert CHECK_SECRETS.PATTERNS[label].search(line)
    assert not CHECK_SECRETS.is_safe_placeholder(".env.example", label, line)


def test_env_example_telegram_token_is_detected():
    line = _telegram_token()
    label = "Telegram bot token"

    assert CHECK_SECRETS.PATTERNS[label].search(line)
    assert not CHECK_SECRETS.is_safe_placeholder(".env.example", label, line)

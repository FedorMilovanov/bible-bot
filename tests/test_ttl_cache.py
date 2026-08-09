import web_api.ttl_cache as ttl_cache
from web_api.ttl_cache import TTLValueCache


def test_ttl_cache_reuses_value_until_expiry(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(ttl_cache.time, "monotonic", lambda: now[0])
    cache = TTLValueCache[int](ttl_seconds=2)
    calls = []

    def load():
        calls.append(True)
        return len(calls)

    assert cache.get(load) == 1
    assert cache.get(load) == 1
    assert len(calls) == 1

    now[0] = 102.1
    assert cache.get(load) == 2
    assert len(calls) == 2


def test_ttl_cache_refreshes_when_loader_function_changes(monkeypatch):
    monkeypatch.setattr(ttl_cache.time, "monotonic", lambda: 100.0)
    cache = TTLValueCache[int](ttl_seconds=60)

    def first():
        return 1

    def second():
        return 2

    assert cache.get(first) == 1
    assert cache.get(second) == 2


def test_ttl_cache_clear_forces_refresh(monkeypatch):
    monkeypatch.setattr(ttl_cache.time, "monotonic", lambda: 100.0)
    cache = TTLValueCache[int](ttl_seconds=60)
    calls = []

    def load():
        calls.append(True)
        return len(calls)

    assert cache.get(load) == 1
    cache.clear()
    assert cache.get(load) == 2

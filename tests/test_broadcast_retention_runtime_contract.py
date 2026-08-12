import broadcast_index_safety as safety
import scripts.check_retention_indexes as preflight


def test_broadcast_retention_preflight_matches_strict_runtime_contract():
    specs = {spec[0]: spec for spec in preflight.EXPECTED}

    assert specs["broadcasts"][2:] == (
        safety._BROADCAST_TTL[0],
        safety._BROADCAST_TTL[1],
        safety._BROADCAST_TTL[2],
        None,
    )
    assert specs["broadcast_deliveries"][2:] == (
        safety._DELIVERY_TTL[0],
        safety._DELIVERY_TTL[1],
        safety._DELIVERY_TTL[2],
        None,
    )
    assert preflight._RUNTIME_BOOTSTRAP_MISSING_OK == frozenset(
        {"broadcasts", "broadcast_deliveries"}
    )

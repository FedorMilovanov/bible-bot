from pathlib import Path


BOT = (Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8")


def async_function(name: str) -> str:
    marker = f"async def {name}"
    start = BOT.index(marker)
    next_async = BOT.find("\nasync def ", start + len(marker))
    return BOT[start:] if next_async == -1 else BOT[start:next_async]


def test_battle_outbox_migration_is_all_or_nothing():
    migration_markers = (
        "from legacy_delivery_drain import",
        "drain_pending_deliveries(",
        "deliver_battle_recipient_once(",
    )
    if not any(marker in BOT for marker in migration_markers):
        # The controller still uses the historical direct battle-result sender.
        # Once outbox delivery migration begins, all invariants below are required.
        return

    finish = async_function("finish_battle_for_user")
    show = async_function("show_battle_results")

    assert "drain_pending_deliveries(" in BOT
    assert "await show_battle_results(bot, final_battle)" not in finish

    # The historical helper sends both recipients in one ephemeral loop. Keeping
    # that path active beside the durable per-recipient outbox recreates the
    # crash window that the delivery leases are meant to eliminate.
    assert 'for uid in (battle["creator_id"], battle["opponent_id"])' not in show

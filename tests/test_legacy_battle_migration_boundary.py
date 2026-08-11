from pathlib import Path


BOT = (Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8")


def async_function(name: str) -> str:
    marker = f"async def {name}"
    start = BOT.index(marker)
    next_async = BOT.find("\nasync def ", start + len(marker))
    return BOT[start:] if next_async == -1 else BOT[start:next_async]


def _assert_before(source: str, first: str, later: str) -> None:
    assert first in source
    assert later in source
    assert source.index(first) < source.index(later)


def test_battle_outbox_migration_is_all_or_nothing():
    migration_markers = (
        "from legacy_delivery_drain import",
        "drain_pending_deliveries(",
        "deliver_battle_recipient_once(",
        "BATTLE_DELIVERY_PROTOCOL_OUTBOX",
    )
    if not any(marker in BOT for marker in migration_markers):
        # The controller still uses the historical direct battle-result sender.
        # Once outbox delivery migration begins, all invariants below are required.
        return

    finish = async_function("finish_battle_for_user")
    show = async_function("show_battle_results")
    compact_finish = "".join(finish.split())

    assert "drain_pending_deliveries(" in BOT
    assert "BATTLE_DELIVERY_PROTOCOL_OUTBOX" in BOT
    assert "claim_final_battle(" in finish
    assert (
        "delivery_protocol=BATTLE_DELIVERY_PROTOCOL_OUTBOX"
        in compact_finish
    )
    assert "await show_battle_results(bot, final_battle)" not in finish

    # The historical helper sends both recipients in one ephemeral loop. Keeping
    # that path active beside the durable per-recipient outbox recreates the
    # crash window that the delivery leases are meant to eliminate.
    assert 'for uid in (battle["creator_id"], battle["opponent_id"])' not in show


def test_battle_question_progress_migration_is_all_or_nothing():
    migration_markers = (
        "from legacy_battle_progress import",
        "ensure_battle_progress(",
        "mark_battle_question_sent(",
        "record_battle_answer_once(",
        "completed_battle_result_inputs(",
        "from legacy_battle_callback_protocol import",
        "build_battle_answer_callback(",
        "parse_battle_answer_callback(",
        "from legacy_battle_session import",
        "create_durable_battle(",
    )
    if not any(marker in BOT for marker in migration_markers):
        # The controller is intentionally still on RAM-only PvP question
        # progress. Once durable migration starts, the whole question pipeline
        # must move together so old callbacks cannot reset/double-advance it.
        return

    menu = async_function("show_battle_menu")
    create = async_function("create_battle")
    join = async_function("join_battle")
    start = async_function("start_battle_questions")
    send = async_function("send_battle_question")
    answer = async_function("battle_answer")
    finish = async_function("finish_battle_for_user")
    cleanup = async_function("cleanup_old_battles_job")

    for required in (
        "create_durable_battle(",
        "get_waiting_durable_battles(",
        "claim_durable_battle_opponent(",
        "ensure_battle_progress(",
        "mark_battle_question_sent(",
        "record_battle_answer_once(",
        "completed_battle_result_inputs(",
        "build_battle_answer_callback(",
        "parse_battle_answer_callback(",
        "resolve_battle_option(",
        "cleanup_stale_waiting_battles(",
    ):
        assert required in BOT

    # New progress cannot be activated for ambiguous in-flight legacy battles.
    # Creation marks durable-v1 atomically; discovery and join only admit that
    # version, so old/unversioned waiting battles remain on their old contract.
    assert "create_durable_battle(" in create
    assert "create_battle_doc(" not in create
    assert "get_waiting_durable_battles(" in menu
    assert "get_waiting_battles(" not in menu
    assert "claim_durable_battle_opponent(" in join
    assert "claim_battle_opponent(" not in join

    # A repeated/stale Start callback must recover the durable participant
    # progress instead of rebuilding a zeroed RAM attempt first.
    _assert_before(start, "ensure_battle_progress(", "user_data[user_id]")

    # Buttons bind semantic option identity, not the current shuffled display
    # slot. The historical ba_<index> protocol and parser disappear together.
    assert 'callback_data=f"ba_{i}"' not in send
    assert 'query.data.replace("ba_", "")' not in answer
    assert 'pattern=r"^ba_\\d+$"' not in BOT
    assert "build_battle_answer_callback(" in send
    assert "parse_battle_answer_callback(" in answer
    assert "resolve_battle_option(" in answer

    # Timer authority is durable and is established only as part of the
    # successful question-delivery path, never by setting RAM time first.
    assert "mark_battle_question_sent(" in send
    assert 'data["question_sent_at"]     = time.time()' not in send

    # Mongo is the progress authority. Feedback/UI follows the answer CAS, and
    # controller arithmetic cannot double-count a replay or skip a question.
    _assert_before(answer, "record_battle_answer_once(", "await query.answer(")
    for ram_mutation in (
        'data["correct_answers"] +=',
        'data["battle_points"] =',
        'data["current_question"] +=',
    ):
        assert ram_mutation not in answer

    # Final participant result comes from the durable question ledger rather
    # than RAM counters/timing, then enters the existing idempotent result CAS.
    _assert_before(finish, "completed_battle_result_inputs(", "record_battle_result(")

    # Age-based cleanup may not delete a waiting battle once either durable
    # result evidence or per-question recovery state exists.
    assert "cleanup_stale_waiting_battles(" in cleanup
    assert "db_cleanup_stale_battles(" not in cleanup

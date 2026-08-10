from pathlib import Path

path = Path("bot.py")
data = path.read_bytes()
eol = b"\r\n" if b"\r\n" in data else b"\n"


def enc(text: str) -> bytes:
    return text.replace("\n", eol.decode()).encode("utf-8")


def replace_once(old: str, new: str, label: str) -> None:
    global data
    old_b = enc(old)
    count = data.count(old_b)
    if count != 1:
        raise SystemExit(f"{label}: expected one exact match, got {count}")
    data = data.replace(old_b, enc(new), 1)


def replace_region(start: str, end: str, replacement: str, label: str) -> None:
    global data
    start_b = enc(start)
    end_b = enc(end)
    if data.count(start_b) != 1 or data.count(end_b) != 1:
        raise SystemExit(f"{label}: region markers are not unique")
    start_i = data.index(start_b)
    end_i = data.index(end_b, start_i)
    data = data[:start_i] + enc(replacement) + data[end_i:]


def replace_once_in_region(start: str, end: str, old: str, new: str, label: str) -> None:
    global data
    start_b = enc(start)
    end_b = enc(end)
    if data.count(start_b) != 1 or data.count(end_b) != 1:
        raise SystemExit(f"{label}: region markers are not unique")
    start_i = data.index(start_b)
    end_i = data.index(end_b, start_i)
    region = data[start_i:end_i]
    old_b = enc(old)
    count = region.count(old_b)
    if count != 1:
        raise SystemExit(f"{label}: expected one match inside region, got {count}")
    region = region.replace(old_b, enc(new), 1)
    data = data[:start_i] + region + data[end_i:]


replace_once(
    '''from battle_integrity import (
    BattleStoreUnavailable, battle_role_for_user,
    claim_battle_opponent, delete_battle_for_participant,
)
''',
    '''from battle_integrity import (
    BattleStoreUnavailable, battle_role_for_user,
    claim_battle_opponent, claim_final_battle,
    delete_battle_for_participant, record_battle_result,
)
from session_integrity import (
    QuizSessionStoreUnavailable, cancel_owned_quiz_session,
    get_owned_quiz_session,
)
''',
    "integrity imports",
)

replace_region(
    "async def review_errors_handler(update: Update, context):\n",
    "\n\n# ═══════════════════════════════════════════════\n# ВОССТАНОВЛЕНИЕ СЕССИИ ПОСЛЕ РЕСТАРТА\n",
    '''async def review_errors_handler(update: Update, context):
    """Показывает только собственные ошибки пользователя."""
    query = update.callback_query
    user_id = query.from_user.id
    data_cb = query.data or ""

    if data_cb.startswith("review_errors_"):
        parts = data_cb.split("_")
        if len(parts) != 4:
            await query.answer("Некорректная кнопка.", show_alert=True)
            return
        try:
            target_id = int(parts[2])
            index = int(parts[3])
        except (TypeError, ValueError):
            await query.answer("Некорректная кнопка.", show_alert=True)
            return
        if target_id != user_id:
            await query.answer("Нет доступа к чужому разбору ошибок.", show_alert=True)
            return
    elif data_cb.startswith("review_nav_"):
        suffix = data_cb.replace("review_nav_", "")
        if suffix == "noop":
            await query.answer()
            return
        try:
            index = int(suffix)
        except (TypeError, ValueError):
            await query.answer("Некорректная кнопка.", show_alert=True)
            return
        target_id = user_id
    else:
        await query.answer()
        return

    await query.answer()
    if target_id not in user_data:
        await query.edit_message_text("⚠️ Данные устарели. Начни новый тест.")
        return

    wrong = user_data[user_id].get("wrong_answers", [])
    if not wrong:
        await query.edit_message_text("✅ Ошибок нет!")
        return

    index = max(0, min(index, len(wrong) - 1))
    text, keyboard = _build_error_page(wrong, index)

    try:
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")
    except Exception as exc:
        if "not modified" not in str(exc).lower():
            raise''',
    "review errors owner check",
)

replace_once_in_region(
    "async def resume_session_handler(update: Update, context):\n",
    "\n\nasync def restart_session_handler(update: Update, context):\n",
    '''    db_session = get_quiz_session(session_id)
    if not db_session or db_session.get("status") != "in_progress":
        await query.edit_message_text("⚠️ Сессия не найдена или уже завершена.")
        return
''',
    '''    try:
        db_session = get_owned_quiz_session(session_id, user_id)
    except QuizSessionStoreUnavailable:
        await query.edit_message_text("⚠️ База сессий временно недоступна. Попробуй позже.")
        return
    if not db_session or db_session.get("status") != "in_progress":
        await query.edit_message_text("⚠️ Сессия не найдена, уже завершена или принадлежит другому пользователю.")
        return
''',
    "resume owner scope",
)

replace_once_in_region(
    "async def restart_session_handler(update: Update, context):\n",
    "\n\nasync def cancel_session_handler(update: Update, context):\n",
    '''    db_session = get_quiz_session(session_id)
    cancel_quiz_session(session_id)

    if not db_session:
        await query.edit_message_text("⚠️ Сессия не найдена.")
        return
''',
    '''    try:
        db_session = cancel_owned_quiz_session(session_id, user_id)
    except QuizSessionStoreUnavailable:
        await query.edit_message_text("⚠️ База сессий временно недоступна. Попробуй позже.")
        return

    if not db_session:
        await query.edit_message_text("⚠️ Сессия не найдена, уже завершена или принадлежит другому пользователю.")
        return
''',
    "restart owner scope",
)

replace_region(
    "async def cancel_session_handler(update: Update, context):\n",
    "\n\n# ═══════════════════════════════════════════════\n# РЕЖИМ БИТВЫ — MongoDB-backed (задание 1.2)\n",
    '''async def cancel_session_handler(update: Update, context):
    query = update.callback_query
    await query.answer()
    session_id = query.data.replace("cancel_session_", "")
    user_id = query.from_user.id

    try:
        cancelled = cancel_owned_quiz_session(session_id, user_id)
    except QuizSessionStoreUnavailable:
        await query.edit_message_text("⚠️ База сессий временно недоступна. Попробуй позже.")
        return
    if not cancelled:
        await query.edit_message_text("⚠️ Сессия не найдена, уже завершена или принадлежит другому пользователю.")
        return

    local = user_data.get(user_id)
    if local and str(local.get("session_id")) == str(session_id):
        user_data.pop(user_id, None)
    await query.edit_message_text("❌ Тест отменён.", reply_markup=_main_keyboard())''',
    "cancel session owner scope",
)

replace_region(
    "async def battle_answer(update: Update, context):\n",
    "\n\nasync def finish_battle_for_user(bot, chat_id: int, user_id: int):\n",
    '''async def battle_answer(update: Update, context):
    """Обрабатывает ответ битвы и безопасно переживает stale/retry callbacks."""
    query = update.callback_query
    user_id = query.from_user.id

    if user_id not in user_data or not user_data[user_id].get("is_battle"):
        await query.answer("Эта кнопка битвы уже устарела.")
        return

    data = user_data[user_id]
    chat_id = data.get("battle_chat_id") or query.message.chat_id

    if data.get("current_question", 0) >= len(data.get("questions", [])):
        pending = bool(data.get("battle_result_pending"))
        await query.answer(
            "Повторяю сохранение результата…" if pending else "Этот ответ уже обработан."
        )
        if pending:
            await finish_battle_for_user(context.bot, chat_id, user_id)
        return

    if data.get("processing_answer"):
        await query.answer("Ответ уже обрабатывается.")
        return
    data["processing_answer"] = True

    try:
        try:
            idx = int(query.data.replace("ba_", ""))
        except (TypeError, ValueError):
            await query.answer("Некорректный ответ.", show_alert=True)
            return
        current_options = data.get("current_options", [])
        if idx < 0 or idx >= len(current_options):
            await query.answer("Некорректный ответ.", show_alert=True)
            return

        q_num = data["current_question"]
        if q_num < 0 or q_num >= len(data["questions"]):
            await query.answer("Этот вопрос уже закрыт.")
            return
        q = data["questions"][q_num]
        user_answer = current_options[idx]
        correct_text = data.get("current_correct_text") or q["options"][q["correct"]]

        sent_at = data.get("question_sent_at", time.time())
        elapsed = min(time.time() - sent_at, 7.0)

        if user_answer == correct_text:
            data["correct_answers"] += 1
            speed_bonus = round((7.0 - elapsed) / 7.0 * 7)
            points = 10 + speed_bonus
            data["battle_points"] = data.get("battle_points", 0) + points
            await query.answer(f"✅ +{points} очков (⚡{speed_bonus} бонус)", show_alert=False)
        else:
            await query.answer(f"❌ Верно: {correct_text}", show_alert=True)

        data["current_question"] += 1
    finally:
        data["processing_answer"] = False

    if data["current_question"] < len(data["questions"]):
        await send_battle_question(context.bot, chat_id, user_id)
    else:
        await finish_battle_for_user(context.bot, chat_id, user_id)''',
    "stale battle answers",
)

replace_region(
    "async def finish_battle_for_user(bot, chat_id: int, user_id: int):\n",
    "\n\nasync def cancel_battle(update: Update, context):\n",
    '''async def _retire_battle_message(bot, chat_id: int, data: dict):
    message_id = data.get("battle_message_id")
    if not message_id:
        return
    try:
        await bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=None,
        )
    except Exception:
        pass


async def finish_battle_for_user(bot, chat_id: int, user_id: int):
    """Идемпотентно сохраняет игрока и ровно один раз выдаёт общий итог битвы."""
    data = user_data.get(user_id)
    if not data or not data.get("is_battle"):
        return

    battle_id = data["battle_id"]
    role = data.get("role")
    correct_answers = int(data.get("correct_answers", 0))
    time_taken = max(0.0, time.time() - data.get("start_time", time.time()))
    battle_points = int(data.get("battle_points", 0))
    data["battle_result_pending"] = True

    try:
        battle = record_battle_result(
            battle_id,
            user_id,
            role,
            score=correct_answers,
            time_seconds=time_taken,
            points=battle_points,
        )
    except BattleStoreUnavailable:
        await bot.send_message(
            chat_id=chat_id,
            text="⚠️ Не удалось сохранить результат битвы. Нажми последнюю кнопку ещё раз через несколько секунд.",
        )
        return

    if battle is None:
        # Другой concurrent finisher may already have atomically claimed and
        # removed the completed battle. Its shared result delivery covers us.
        await _retire_battle_message(bot, chat_id, data)
        user_data.pop(user_id, None)
        return

    final_battle = None
    if battle.get("creator_finished") and battle.get("opponent_finished"):
        try:
            final_battle = claim_final_battle(battle_id)
        except BattleStoreUnavailable:
            await bot.send_message(
                chat_id=chat_id,
                text="⚠️ Итог битвы сохранён частично. Нажми последнюю кнопку ещё раз для безопасного повтора.",
            )
            return

    await _retire_battle_message(bot, chat_id, data)
    data["battle_result_pending"] = False
    user_data.pop(user_id, None)

    if final_battle is not None:
        await show_battle_results(bot, final_battle)
        return
    if battle.get("creator_finished") and battle.get("opponent_finished"):
        # Another finisher claimed the completed battle and will deliver the
        # shared result to both participants.
        return

    await bot.send_message(
        chat_id=chat_id,
        text=(
            f"✅ *Ты закончил!*\\n\\n"
            f"📊 Твой результат: {correct_answers}/10\\n"
            f"⏱ Время: {format_time(time_taken)}\\n\\n"
            "⏳ Ожидание соперника..."
        ),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main")]]),
    )


async def show_battle_results(bot, battle: dict):
    """Отправляет уже атомарно claimed итог битвы обоим участникам."""
    creator_points = battle.get("creator_points", 0)
    opponent_points = battle.get("opponent_points", 0)

    if creator_points > opponent_points:
        winner, winner_name = "creator", battle["creator_name"]
    elif opponent_points > creator_points:
        winner, winner_name = "opponent", battle.get("opponent_name", "Соперник")
    else:
        winner, winner_name = "draw", None

    text = "⚔️ *РЕЗУЛЬТАТЫ БИТВЫ*\\n\\n"
    text += f"🏆 *Победитель: {winner_name}!*\\n\\n" if winner != "draw" else "🤝 *НИЧЬЯ!*\\n\\n"
    text += (
        f"👤 *{battle['creator_name']}*\\n"
        f"   ✅ {battle['creator_score']}/10 • ⚡ {creator_points} очков"
        f" • ⏱ {format_time(battle['creator_time'])}\\n\\n"
    )
    text += (
        f"👤 *{battle.get('opponent_name', 'Соперник')}*\\n"
        f"   ✅ {battle['opponent_score']}/10 • ⚡ {opponent_points} очков"
        f" • ⏱ {format_time(battle['opponent_time'])}\\n\\n"
    )
    text += "💎 *+5 баллов* победителю!\\n" if winner != "draw" else "💎 *+2 балла* каждому!\\n"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Новая битва", callback_data="battle_menu")],
        [InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main")],
    ])

    for uid in (battle["creator_id"], battle["opponent_id"]):
        try:
            await bot.send_message(
                chat_id=uid,
                text=text,
                reply_markup=keyboard,
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning("Battle result delivery to %s failed: %s", uid, exc)''',
    "battle finalization",
)

replace_once_in_region(
    "async def cancel_battle(update: Update, context):\n",
    "\n# ═══════════════════════════════════════════════\n# INLINE MODE — Вызов на дуэль (задание 4.1)\n",
    '''    user_id = query.from_user.id

    battle = get_battle(battle_id)
''',
    '''    user_id = query.from_user.id

    local = user_data.get(user_id)
    if (
        local
        and local.get("is_battle")
        and local.get("battle_id") == battle_id
        and local.get("battle_result_pending")
    ):
        await query.answer("Результат битвы ещё синхронизируется; отмена временно недоступна.", show_alert=True)
        return

    battle = get_battle(battle_id)
''',
    "prevent cancel during pending result",
)

required = [
    b"get_owned_quiz_session(session_id, user_id)",
    b"cancel_owned_quiz_session(session_id, user_id)",
    b"target_id != user_id",
    b"record_battle_result(",
    b"claim_final_battle(battle_id)",
    b"battle_result_pending",
]
for marker in required:
    if marker not in data:
        raise SystemExit(f"missing integrity marker after patch: {marker!r}")

for forbidden in (
    b"await show_battle_results(bot, battle_id)",
    b"update_battle_stats(battle[\"creator_id\"]",
):
    if forbidden in data:
        raise SystemExit(f"legacy unsafe battle finalization remains: {forbidden!r}")

path.write_bytes(data)

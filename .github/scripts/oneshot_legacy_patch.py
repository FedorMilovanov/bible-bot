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


def replace_region(start: str, end: str, replacement: str, label: str) -> None:
    global data
    start_b = enc(start)
    end_b = enc(end)
    if data.count(start_b) != 1 or data.count(end_b) != 1:
        raise SystemExit(f"{label}: region markers are not unique")
    start_i = data.index(start_b)
    end_i = data.index(end_b, start_i)
    data = data[:start_i] + enc(replacement) + data[end_i:]


replace_once(
    "from questions import get_pool_by_key, BATTLE_POOL\n",
    "from questions import get_pool_by_key, BATTLE_POOL\n"
    "from battle_integrity import (\n"
    "    BattleStoreUnavailable, battle_role_for_user,\n"
    "    claim_battle_opponent, delete_battle_for_participant,\n"
    ")\n",
    "battle helper import",
)

replace_once_in_region(
    "async def random_command(update: Update, context):\n",
    "\n\nasync def admin_command(update: Update, context):\n",
    '        "linguistics_ch1", "linguistics_ch1_2", "linguistics_ch1_3",\n'
    '        "intro1", "intro2", "intro3",\n',
    '        "linguistics_ch1", "linguistics_ch1_2", "linguistics_ch1_3",\n'
    '        "nero", "geography",\n'
    '        "intro1", "intro2", "intro3",\n',
    "random all-theme pool",
)

replace_region(
    "async def join_battle(update: Update, context):\n",
    "\n\nasync def start_battle_questions(update: Update, context):\n",
    '''async def join_battle(update: Update, context):
    query = update.callback_query
    battle_id = query.data.replace("join_battle_", "")
    user_id = query.from_user.id
    user_name = query.from_user.first_name

    battle = get_battle(battle_id)
    if not battle or battle.get("status") != "waiting":
        await query.answer("Битва не найдена или уже началась.", show_alert=True)
        return
    if battle.get("creator_id") == user_id:
        await query.answer("Нельзя присоединиться к своей битве!", show_alert=True)
        return

    try:
        battle = claim_battle_opponent(battle_id, user_id, user_name)
    except BattleStoreUnavailable:
        await query.answer("База битв временно недоступна. Попробуй ещё раз.", show_alert=True)
        return
    if not battle:
        await query.answer("Эту битву уже занял другой игрок.", show_alert=True)
        return

    await query.answer()
    await query.edit_message_text(
        f"⚔️ *БИТВА НАЧАЛАСЬ!*\\n\\n"
        f"👤 Ты vs 👤 {battle['creator_name']}\\n\\n"
        "📝 10 вопросов\\n⏱ Время учитывается!\\nНажми «Начать»",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Начать отвечать", callback_data=f"start_battle_{battle_id}_opponent")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="battle_menu")],
        ]),
        parse_mode="Markdown",
    )''',
    "join battle",
)

replace_region(
    "async def start_battle_questions(update: Update, context):\n",
    "\n\nasync def send_battle_question(bot, chat_id: int, user_id: int):\n",
    '''async def start_battle_questions(update: Update, context):
    query = update.callback_query
    data_parts = query.data.replace("start_battle_", "").rsplit("_", 1)
    if len(data_parts) != 2 or data_parts[1] not in {"creator", "opponent"}:
        await query.answer("Некорректная кнопка битвы.", show_alert=True)
        return

    battle_id, requested_role = data_parts
    user_id = query.from_user.id
    battle = get_battle(battle_id)
    if not battle:
        await query.answer("Битва не найдена.", show_alert=True)
        return

    persisted_role = battle_role_for_user(battle, user_id)
    if persisted_role is None or persisted_role != requested_role:
        await query.answer("Эта кнопка принадлежит другому участнику.", show_alert=True)
        return

    await query.answer()
    user_data[user_id] = _create_session_data(
        user_id=user_id,
        session_id=battle_id,
        questions=battle["questions"],
        level_name="⚔️ PvP Битва",
        chat_id=query.message.chat_id,
        battle_id=battle_id,
        role=persisted_role,
        correct_answers=0,
        start_time=time.time(),
        last_activity=time.time(),
        is_battle=True,
        battle_points=0,
        battle_chat_id=query.message.chat_id,
        battle_role=persisted_role,
    )

    await query.edit_message_text("⚔️ *БИТВА: Вопрос 1/10*\\n\\nНачинаем! 🍀", parse_mode="Markdown")
    await send_battle_question(context.bot, query.message.chat_id, user_id)
    return BATTLE_ANSWERING''',
    "start battle",
)

replace_region(
    "async def cancel_battle(update: Update, context):\n",
    "\n\n# ═══════════════════════════════════════════════\n# INLINE MODE — Вызов на дуэль (задание 4.1)\n",
    '''async def cancel_battle(update: Update, context):
    query = update.callback_query
    battle_id = query.data.replace("cancel_battle_", "")
    user_id = query.from_user.id

    battle = get_battle(battle_id)
    if not battle:
        await query.answer("Битва уже завершена или удалена.", show_alert=True)
        return
    if battle_role_for_user(battle, user_id) is None:
        await query.answer("Ты не участник этой битвы.", show_alert=True)
        return

    try:
        deleted = delete_battle_for_participant(battle_id, user_id)
    except BattleStoreUnavailable:
        await query.answer("База битв временно недоступна.", show_alert=True)
        return
    if not deleted:
        await query.answer("Битва уже завершена или изменена.", show_alert=True)
        return

    user_data.pop(user_id, None)
    await query.answer()
    await query.edit_message_text(
        "❌ Битва отменена.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="battle_menu")]]),
    )''',
    "cancel battle",
)

if data.count(enc('"nero", "geography",\n')) != 1:
    raise SystemExit("random pool did not receive the historical themes exactly once")
for required in (
    b"claim_battle_opponent(",
    b"battle_role_for_user(",
    b"delete_battle_for_participant(",
):
    if required not in data:
        raise SystemExit(f"missing required battle integration: {required!r}")

path.write_bytes(data)

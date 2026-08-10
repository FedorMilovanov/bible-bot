from pathlib import Path

path = Path("bot.py")
data = path.read_bytes()
eol = b"\r\n" if b"\r\n" in data else b"\n"


def enc(text: str) -> bytes:
    return text.replace("\n", eol.decode()).encode("utf-8")


def replace_region(start: str, end: str, replacement: str, label: str) -> None:
    global data
    start_b = enc(start)
    end_b = enc(end)
    if data.count(start_b) != 1 or data.count(end_b) != 1:
        raise SystemExit(f"{label}: region markers are not unique")
    start_i = data.index(start_b)
    end_i = data.index(end_b, start_i)
    data = data[:start_i] + enc(replacement) + data[end_i:]


replace_region(
    "async def random_all_start_handler(update: Update, context):\n",
    "\n\nasync def timed_mode_handler(update: Update, context):\n",
    r'''async def random_all_start_handler(update: Update, context):
    """Случайный режим: 10 вопросов из канонического random_all пула."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    _touch(user_id)

    all_questions = get_pool_by_key("random_all")
    if not all_questions:
        await query.edit_message_text("⚠️ Вопросы не найдены.")
        return

    questions = random.sample(all_questions, min(10, len(all_questions)))
    level_name = "🎲 Случайный режим (все темы)"

    cancel_active_quiz_session(user_id)
    question_ids = [get_qid(q) for q in questions]
    session_id = create_quiz_session(
        user_id=user_id,
        mode="level",
        question_ids=question_ids,
        questions_data=questions,
        level_key="random_all",
        level_name=level_name,
        time_limit=None,
        chat_id=query.message.chat_id,
    )

    user_data[user_id] = _create_session_data(
        user_id=user_id,
        session_id=session_id,
        questions=questions,
        level_name=level_name,
        chat_id=query.message.chat_id,
        level_key="random_all",
        correct_answers=0,
        start_time=time.time(),
        last_activity=time.time(),
        is_battle=False,
        battle_points=0,
        username=update.effective_user.username,
        first_name=update.effective_user.first_name,
        quiz_mode="relaxed",
        score_multiplier=1.0,
        quiz_time_limit=None,
    )

    await query.edit_message_text(
        f"*{level_name}*\n\n📝 Вопросов: {len(questions)} · 🧘 Без таймера\nНачинаем!",
        parse_mode="Markdown",
    )
    await send_question(context.bot, user_id, time_limit=None)''',
    "random_all callback",
)

replace_region(
    "async def random_command(update: Update, context):\n",
    "\n\nasync def admin_command(update: Update, context):\n",
    r'''async def random_command(update: Update, context):
    """Команда /random — запускает тот же канонический random_all режим."""
    user_id = update.effective_user.id
    _touch(user_id)

    all_questions = get_pool_by_key("random_all")
    if not all_questions:
        await update.message.reply_text("⚠️ Вопросы не найдены.", reply_markup=_main_keyboard())
        return

    questions = random.sample(all_questions, min(10, len(all_questions)))
    level_name = "🎲 Случайный режим (все темы)"
    cancel_active_quiz_session(user_id)

    question_ids = [get_qid(q) for q in questions]
    session_id = create_quiz_session(
        user_id=user_id,
        mode="level",
        question_ids=question_ids,
        questions_data=questions,
        level_key="random_all",
        level_name=level_name,
        time_limit=None,
        chat_id=update.effective_chat.id,
    )

    user_data[user_id] = _create_session_data(
        user_id=user_id,
        session_id=session_id,
        questions=questions,
        level_name=level_name,
        chat_id=update.effective_chat.id,
        level_key="random_all",
        correct_answers=0,
        start_time=time.time(),
        last_activity=time.time(),
        is_battle=False,
        battle_points=0,
        username=update.effective_user.username,
        first_name=update.effective_user.first_name,
        quiz_mode="relaxed",
        score_multiplier=1.0,
        quiz_time_limit=None,
    )

    await update.message.reply_text(
        f"🎲 *Случайный тест*\n{len(questions)} вопросов из всех тем\n\nНачинаем!",
        parse_mode="Markdown",
    )
    await send_question(context.bot, user_id, time_limit=None)''',
    "random command",
)

for marker in (
    b'all_questions = get_pool_by_key("random_all")',
    b'await send_question(context.bot, user_id, time_limit=None)',
):
    if data.count(marker) < 2:
        raise SystemExit(f"expected both random entry paths to use {marker!r}")

for forbidden in (
    b'context.user_data["session_id"] = str(session_id)',
    b'await send_question(update, context, questions, 0, user_id',
):
    if forbidden in data:
        raise SystemExit(f"obsolete random path remains: {forbidden!r}")

path.write_bytes(data)

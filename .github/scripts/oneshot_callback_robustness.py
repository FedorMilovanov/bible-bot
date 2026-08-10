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
    "async def report_inaccuracy_handler(update: Update, context):\n",
    "\n\nasync def _handle_question_timeout(bot, user_id: int, q_num_at_send: int, timeout_seconds: int):\n",
    '''async def report_inaccuracy_handler(update: Update, context):
    """Отправляет админу именно тот вопрос, чья кнопка «Неточность?» была нажата."""
    query = update.callback_query
    user = update.effective_user
    user_id = user.id
    data = user_data.get(user_id, {})

    try:
        q_num = int((query.data or "").replace("report_inaccuracy_", "", 1))
    except (TypeError, ValueError):
        await query.answer("Некорректная кнопка.", show_alert=True)
        return

    q_list = data.get("questions", [])
    if q_num < 0 or q_num >= len(q_list):
        await query.answer("Этот вопрос уже недоступен.", show_alert=True)
        return

    await query.answer("✅ Принято, отправляю автору.", show_alert=False)

    q = q_list[q_num]
    level_name = data.get("level_name", "—")
    username = f"@{user.username}" if user.username else f"{user.first_name} (id: {user_id})"
    q_text = q.get("question", "—")
    options = q.get("options", [])
    correct_idx = q.get("correct", 0)
    correct_ans = options[correct_idx] if isinstance(correct_idx, int) and 0 <= correct_idx < len(options) else "—"
    options_str = "\n".join(f"  {i + 1}. {opt}" for i, opt in enumerate(options))
    msg = (
        "⚠️ СООБЩЕНИЕ О НЕТОЧНОСТИ\n\n"
        f"👤 От: {username}\n"
        f"📚 Тест: {level_name}\n"
        f"❓ Вопрос {q_num + 1}: {q_text}\n\n"
        f"📋 Варианты:\n{options_str}\n\n"
        f"✅ Правильный ответ в базе: {correct_ans}"
    )

    try:
        await context.bot.send_message(chat_id=ADMIN_USER_ID, text=msg)
    except Exception as exc:
        logger.warning("report_inaccuracy: не удалось отправить сообщение админу: %s", exc)
        try:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="⚠️ Не удалось доставить сообщение автору. Попробуй ещё раз позже.",
            )
        except Exception:
            pass''',
    "question inaccuracy reporting",
)

replace_region(
    "async def retry_errors(update: Update, context):\n",
    "\n\n# ═══════════════════════════════════════════════\n# ПАГИНАЦИЯ РАЗБОРА ОШИБОК\n",
    '''async def retry_errors(update: Update, context):
    query = update.callback_query
    user_id = query.from_user.id

    try:
        target_id = int((query.data or "").replace("retry_errors_", "", 1))
    except (TypeError, ValueError):
        await query.answer("⚠️ Некорректная кнопка.", show_alert=True)
        return ConversationHandler.END

    if target_id != user_id:
        await query.answer("⚠️ Это не ваша сессия.", show_alert=True)
        return ConversationHandler.END

    if target_id not in user_data:
        await query.answer()
        await query.edit_message_text("⚠️ Данные сессии устарели. Начни новый тест.")
        return ConversationHandler.END

    prev_data = user_data[target_id]
    answered = prev_data.get("answered_questions", [])
    wrong_questions = [
        item["question_obj"] for item in answered
        if _is_wrong(item)
    ]

    if not wrong_questions:
        await query.answer("Ошибок нет!", show_alert=True)
        return ConversationHandler.END

    await query.answer()
    user_data[user_id] = _create_session_data(
        user_id=user_id,
        session_id=None,
        questions=wrong_questions,
        level_name=f"🔁 Повторение ошибок ({prev_data['level_name']})",
        chat_id=query.message.chat_id,
        level_key=prev_data["level_key"],
        correct_answers=0,
        start_time=time.time(),
        last_activity=time.time(),
        is_battle=False,
        battle_points=0,
        is_retry=True,
        username=query.from_user.username,
        first_name=query.from_user.first_name,
    )

    await query.edit_message_text(
        f"🔁 *ПОВТОРЕНИЕ ОШИБОК*\n\nВопросов: {len(wrong_questions)}\nПоехали! 💪",
        parse_mode="Markdown",
    )
    await send_question(context.bot, user_id)
    return ANSWERING''',
    "retry errors callback",
)

replace_region(
    "async def review_test_handler(update: Update, context):\n",
    "\n\nasync def review_errors_handler(update: Update, context):\n",
    '''async def review_test_handler(update: Update, context):
    """Листание вопросов теста с правильными ответами после завершения."""
    query = update.callback_query
    user_id = query.from_user.id
    data = user_data.get(user_id, {})

    try:
        q_index = int((query.data or "").rsplit("_", 1)[-1])
    except (TypeError, ValueError):
        await query.answer("Некорректная кнопка.", show_alert=True)
        return

    answered = data.get("answered_questions", [])
    if not answered or q_index < 0 or q_index >= len(answered):
        await query.answer()
        await query.edit_message_text("❌ Данные теста не найдены. Пройди тест заново.")
        return

    await query.answer()
    total = len(answered)
    answer_data = answered[q_index]
    q = answer_data.get("question_obj", {})
    user_answer = answer_data.get("user_answer", "—")
    correct_answer = _correct_text(q)
    is_correct = user_answer == correct_answer
    status = "✅" if is_correct else "❌"

    text = (
        f"📖 *Просмотр теста* ({q_index + 1}/{total})\n\n"
        f"*Вопрос:*\n{q.get('question', '—')}\n\n"
        "*Варианты:*\n"
    )
    for i, opt in enumerate(q.get("options", [])):
        if i == q.get("correct"):
            marker = "✅"
        elif opt == user_answer and not is_correct:
            marker = "❌"
        else:
            marker = "⬜"
        arrow = " ← твой ответ" if opt == user_answer and not is_correct else ""
        text += f"{marker} {i + 1}. {opt}{arrow}\n"

    text += f"\n*Твой ответ:* {user_answer} {status}"
    explanation = q.get("explanation") or q.get("fun_fact")
    if explanation:
        text += f"\n\n💡 *Пояснение:*\n_{explanation}_"

    nav_row = []
    if q_index > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Пред.", callback_data=f"review_test_{q_index - 1}"))
    nav_row.append(InlineKeyboardButton(f"{q_index + 1}/{total}", callback_data="noop"))
    if q_index < total - 1:
        nav_row.append(InlineKeyboardButton("➡️ След.", callback_data=f"review_test_{q_index + 1}"))

    buttons = [nav_row, [InlineKeyboardButton("🏠 В меню", callback_data="back_to_main")]]
    await query.edit_message_text(
        text=text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )''',
    "review test callback",
)

replace_region(
    "async def report_start(update: Update, context):\n",
    "\n\nasync def report_receive_text(update: Update, context):\n",
    '''async def report_start(update: Update, context):
    query = update.callback_query
    report_type = (query.data or "").replace("report_start_", "", 1)
    if report_type == "bug_direct":
        report_type = "bug"
    if report_type not in REPORT_TYPE_LABELS:
        await query.answer("Некорректный тип сообщения.", show_alert=True)
        return ConversationHandler.END

    user_id = query.from_user.id
    if not can_submit_report(user_id):
        remaining = seconds_until_next_report(user_id)
        await query.answer(f"⏳ Слишком часто. Попробуй через {remaining} сек.", show_alert=True)
        return ConversationHandler.END

    await query.answer()
    report_drafts[user_id] = {"type": report_type, "text": None, "photo_file_id": None}
    label = REPORT_TYPE_LABELS[report_type]
    await safe_edit(query, f"{label}\n\n✏️ Напиши своё сообщение.\n\nДля отмены: /cancelreport")
    return REPORT_TEXT''',
    "report start callback",
)

for required in (
    b'q_num = int((query.data or "").replace("report_inaccuracy_", "", 1))',
    b"report_type not in REPORT_TYPE_LABELS",
    b"q_index < 0 or q_index >= len(answered)",
    b"target_id != user_id",
):
    if required not in data:
        raise SystemExit(f"missing callback robustness marker: {required!r}")

path.write_bytes(data)

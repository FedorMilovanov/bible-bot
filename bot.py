from keep_alive import keep_alive
keep_alive()

from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ConversationHandler, CallbackQueryHandler
import json
import time
import os
import re
from datetime import datetime
from pymongo import MongoClient

# --- НАСТРОЙКА БАЗЫ ДАННЫХ ---
MONGO_URL = os.getenv('MONGO_URL') 

if MONGO_URL:
    try:
        cluster = MongoClient(MONGO_URL)
        db = cluster["bible_bot_db"]
        collection = db["leaderboard"]
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        collection = None
else:
    print("⚠️ ВНИМАНИЕ: Не задана переменная MONGO_URL. Статистика не будет сохраняться!")
    collection = None

# Состояния разговора
CHOOSING_LEVEL, ANSWERING = range(2)

# ЛЁГКИЙ УРОВЕНЬ
easy_questions = [
    {
        "question": "Кто написал Первое послание Петра?",
        "options": ["Апостол Павел", "Апостол Петр", "Апостол Иоанн", "Апостол Иаков"],
        "correct": 1,
        "explanation": "Автором послания является апостол Петр, один из ближайших учеников Иисуса Христа."
    },
    {
        "question": "В каком году примерно было написано послание?",
        "options": ["30-33 гг.", "50-55 гг.", "62-63 гг.", "70-75 гг."],
        "correct": 2,
        "explanation": "Послание было написано около 62-63 гг. н.э."
    },
    {
        "question": "Где находился Петр, когда писал послание?",
        "options": ["В Иерусалиме", "В Риме", "В Антиохии", "В Ефесе"],
        "correct": 1,
        "explanation": "Петр находился в Риме, когда писал послание."
    },
    {
        "question": "Как Петр символически назвал Рим в послании?",
        "options": ["Египет", "Вавилон", "Содом", "Ниневия"],
        "correct": 1,
        "explanation": "Петр назвал Рим 'Вавилоном' (1 Пет. 5:13)."
    },
    {
        "question": "Кому было адресовано послание?",
        "options": ["Римским христианам", "Иерусалимской церкви", "Христианам Малой Азии", "Всем язычникам"],
        "correct": 2,
        "explanation": "Послание было адресовано христианам, рассеянным в провинциях Малой Азии."
    },
    {
        "question": "Какова главная цель послания?",
        "options": ["Осудить лжеучителей", "Укрепить в страданиях", "Объяснить доктрины", "Собрать пожертвования"],
        "correct": 1,
        "explanation": "Главная цель — укрепить верующих в страданиях."
    },
    {
        "question": "Через что Бог возродил нас к живому упованию? (1 Пет. 1:3)",
        "options": ["Через крещение", "Через воскресение Христа", "Через веру", "Через Слово Божье"],
        "correct": 1,
        "explanation": "Бог возродил нас 'воскресением Иисуса Христа из мёртвых'."
    },
    {
        "question": "Какое наследие ожидает верующих? (1 Пет. 1:4)",
        "options": ["Земное богатство", "Нетленное, чистое, неувядаемое", "Долгая жизнь", "Власть над народами"],
        "correct": 1,
        "explanation": "Верующих ожидает 'наследство нетленное, чистое, неувядаемое'."
    },
    {
        "question": "Как верующие сохраняются ко спасению? (1 Пет. 1:5)",
        "options": ["Перенося верно страдания", "Силою Божией посредством их веры", "Через Причастие", "Добрыми делами"],
        "correct": 1,
        "explanation": "Верующие 'силою Божиею через веру соблюдаемы ко спасению'."
    },
    {
        "question": "К чему призывает Петр верующих? (1 Пет. 1:15-16)",
        "options": ["К богатству", "К святости", "К терпению", "К знанию"],
        "correct": 1,
        "explanation": "'Будьте святы, потому что Я свят' (1 Пет. 1:15-16)."
    }
]

# СРЕДНИЙ УРОВЕНЬ
medium_questions = [
    {
        "question": "Почему Петр назвал Рим 'Вавилоном'?",
        "options": ["Это метафора идолопоклонства, центра языческой власти", "Рим был основан вавилонянами", "Петр ошибся", "Он на самом деле сидел в Вавилоне"],
        "correct": 0,
        "explanation": "Вавилон — это кодовое имя для Рима, символ языческой власти."
    },
    {
        "question": "При каком императоре начались массовые гонения на христиан?",
        "options": ["Клавдий", "Нерон", "Домициан", "Траян"],
        "correct": 1,
        "explanation": "Массовые гонения начались при императоре Нероне в 64 г. н.э."
    },
    {
        "question": "Что означает греческое слово 'πρόγνωσις' (предведение)?",
        "options": ["Простое предвидение", "Избрание по заветной любви", "Знание будущего", "Божья мудрость"],
        "correct": 1,
        "explanation": "Греческое 'πρόγνωσις' означает избрание по заветной любви."
    },
    {
        "question": "Кто исследовал пророчества о спасении? (1 Пет. 1:10)",
        "options": ["Апостолы", "Пророки", "Ангелы", "Первосвященники"],
        "correct": 1,
        "explanation": "'К сему-то спасению относились изыскания и исследования пророков'."
    },
    {
        "question": "Что указывал Дух Христов в пророках? (1 Пет. 1:11)",
        "options": ["Только страдания Христа", "Только славу Христа", "Страдания и последующую славу", "Конец мира"],
        "correct": 2,
        "explanation": "Дух Христов 'предвозвещал Христовы страдания и последующую славу'."
    },
    {
        "question": "Что означает 'препоясать чресла ума'? (1 Пет. 1:13)",
        "options": ["Молиться усерднее", "Быть собранным духовно, бодрствовать", "Изучать Писание", "Поститься"],
        "correct": 1,
        "explanation": "Это образ готовности к действию, как при Исходе из Египта."
    },
    {
        "question": "С чем сравнивается испытание веры? (1 Пет. 1:7)",
        "options": ["С очищением серебра", "С очищением золота огнем", "С огранкой алмаза", "С закаливанием стали"],
        "correct": 1,
        "explanation": "Вера испытывается как золото огнём."
    },
    {
        "question": "Откуда взята цитата 'будьте святы, потому что Я свят'?",
        "options": ["Исход", "Левит", "Второзаконие", "Псалмы"],
        "correct": 1,
        "explanation": "Цитата взята из книги Левит."
    },
    {
        "question": "Что является целью веры? (1 Пет. 1:9)",
        "options": ["Богатство", "Здоровье", "Спасение душ", "Мудрость"],
        "correct": 2,
        "explanation": "'Достигая наконец верою вашею спасения душ'."
    },
    {
        "question": "Кто желает проникнуть в тайну спасения? (1 Пет. 1:12)",
        "options": ["Демоны", "Ангелы", "Люди", "Пророки"],
        "correct": 1,
        "explanation": "'Во что желают проникнуть Ангелы'."
    }
]

# СЛОЖНЫЙ УРОВЕНЬ
hard_questions = [
    {
        "question": "Какие три действия Троицы описаны в 1 Пет. 1:2?",
        "options": [
            "Творение, искупление, освящение",
            "Предведение Отца, освящение Духа, окропление кровью Христа",
            "Избрание, призвание, прославление",
            "Вера, надежда, любовь"
        ],
        "correct": 1,
        "explanation": "Спасение — троичное дело: Отец избирает, Дух освящает, Сын искупает."
    },
    {
        "question": "Что означает греческое слово 'παρακύψαι' в 1 Пет. 1:12?",
        "options": ["Страх и трепет", "Благоговейное любопытство", "Глубокое исследование", "Недоумение"],
        "correct": 1,
        "explanation": "Греческое 'παρακύψαι' означает 'наклониться, чтобы заглянуть'."
    },
    {
        "question": "Почему Петр использовал образ 'препоясания чресел'?",
        "options": ["Обычная одежда того времени", "Образ воинской готовности", "Образ готовности к Исходу из Египта", "Символ скромности"],
        "correct": 2,
        "explanation": "Образ отсылает к Исходу (Исх. 12:11)."
    },
    {
        "question": "Что Петр НЕ упомянул как характеристику небесного наследия?",
        "options": ["Нетленное", "Чистое", "Неувядаемое", "Безгрешное"],
        "correct": 3,
        "explanation": "Петр описывает наследие как 'нетленное, чистое, неувядаемое'. 'Безгрешное' не упоминается."
    },
    {
        "question": "Какое слово из Сираха 2:5 перекликается с учением о страданиях?",
        "options": ["Золото испытывается в огне", "Терпение рождает опытность", "Страдание очищает душу", "Вера без дел мертва"],
        "correct": 0,
        "explanation": "В Сир. 2:5: 'Золото испытывается в огне'."
    },
    {
        "question": "В каком контексте Петр говорит о 'живой надежде'?",
        "options": ["Надежда на улучшение жизни", "Надежда основана на воскресшем Христе", "Надежда на избавление от страданий", "Надежда на второе пришествие"],
        "correct": 1,
        "explanation": "'Живая надежда' основана на воскресении Христа."
    },
    {
        "question": "Как связаны страдания Христа и Его слава в пророчествах?",
        "options": ["Слава без страданий", "Страдания без славы", "Сначала страдания, потом слава", "Слава заменяет страдания"],
        "correct": 2,
        "explanation": "Пророчества показывают: сначала страдания, затем прославление."
    },
    {
        "question": "Какая привилегия верующих подчёркнута в 1 Пет. 1:12?",
        "options": ["Они умнее пророков", "Они живут в эпоху исполнения пророчеств", "Они не нуждаются в пророчествах", "Они знают больше ангелов"],
        "correct": 1,
        "explanation": "Верующие живут в эпоху исполнения пророчеств."
    },
    {
        "question": "Что означает 'не сообразуйтесь с прежними похотями'?",
        "options": ["Полное безгрешие", "Отделение от греховных моделей прошлого", "Аскетический образ жизни", "Уход от мира"],
        "correct": 1,
        "explanation": "Призыв отделиться от греховных моделей прошлой жизни."
    },
    {
        "question": "Почему Бог сохраняет верующих 'силою Божией через веру'?",
        "options": ["Вера — человеческое достижение", "Сохранение — дар Божий, а не наша заслуга", "Бог помогает тем, кто помогает себе", "Вера заменяет Божью силу"],
        "correct": 1,
        "explanation": "Мы сохраняемы ко спасению силою Божией, а не нашей."
    }
]

# Хранилище данных пользователей
user_data = {}

# --- ФУНКЦИИ БАЗЫ ДАННЫХ ---

def add_to_leaderboard(user_id, username, first_name, level_key, score, total, time_seconds):
    if collection is None:
        return

    points_per_question = {"easy": 1, "medium": 2, "hard": 3}
    earned_points = score * points_per_question[level_key]
    user_id_str = str(user_id)
    
    try:
        entry = collection.find_one({"_id": user_id_str})
        
        if entry:
            new_total = entry.get("total_points", 0) + earned_points
            new_attempts = entry.get(f"{level_key}_attempts", 0) + 1
            new_best_score = max(entry.get(f"{level_key}_best_score", 0), score)
            current_best_time = entry.get(f"{level_key}_best_time", float('inf'))
            new_best_time = min(current_best_time, time_seconds)
            
            collection.update_one(
                {"_id": user_id_str},
                {
                    "$set": {
                        "total_points": new_total,
                        f"{level_key}_attempts": new_attempts,
                        f"{level_key}_best_score": new_best_score,
                        f"{level_key}_best_time": new_best_time,
                        "last_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "first_name": first_name,
                        "username": username
                    }
                }
            )
        else:
            new_entry = {
                "_id": user_id_str,
                "username": username or "Без username",
                "first_name": first_name or "Пользователь",
                "total_points": earned_points,
                "easy_attempts": 1 if level_key == "easy" else 0,
                "medium_attempts": 1 if level_key == "medium" else 0,
                "hard_attempts": 1 if level_key == "hard" else 0,
                "easy_best_score": score if level_key == "easy" else 0,
                "medium_best_score": score if level_key == "medium" else 0,
                "hard_best_score": score if level_key == "hard" else 0,
                "easy_best_time": time_seconds if level_key == "easy" else float('inf'),
                "medium_best_time": time_seconds if level_key == "medium" else float('inf'),
                "hard_best_time": time_seconds if level_key == "hard" else float('inf'),
                "last_date": datetime.now().strftime("%Y-%m-%d %H:%M")
            }
            collection.insert_one(new_entry)
    except Exception as e:
        print(f"Ошибка записи в БД: {e}")

def get_user_position(user_id):
    if collection is None:
        return None, None
    user_id_str = str(user_id)
    try:
        entry = collection.find_one({"_id": user_id_str})
        if not entry:
            return None, None
        my_points = entry.get("total_points", 0)
        count_better = collection.count_documents({"total_points": {"$gt": my_points}})
        return count_better + 1, entry
    except Exception:
        return None, None

def get_leaderboard_page(page_number):
    if collection is None:
        return []
    try:
        skip_amount = page_number * 10
        return list(collection.find().sort("total_points", -1).skip(skip_amount).limit(10))
    except Exception:
        return []

def get_total_users():
    if collection is None:
        return 0
    try:
        return collection.count_documents({})
    except Exception:
        return 0

def format_time(seconds):
    if seconds == float('inf'):
        return "—"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes > 0:
        return f"{minutes}м {secs}с"
    return f"{secs}с"

# Команда /start
async def start(update: Update, context):
    keyboard = [
        [InlineKeyboardButton("📖 О боте", callback_data='about')],
        [InlineKeyboardButton("🎯 Начать тест", callback_data='start_test')],
        [InlineKeyboardButton("🏆 Таблица лидеров", callback_data='leaderboard')],
        [InlineKeyboardButton("📊 Моя статистика", callback_data='my_stats')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        '📖 *БИБЛЕЙСКИЙ ТЕСТ-БОТ*\n\n'
        '*Тема:* 1 Петра 1:1-16\n'
        '*Материал:* Введение и комментарий\n\n'
        'Выбери действие:',
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

# Обработка кнопок главного меню
async def button_handler(update: Update, context):
    query = update.callback_query
    await query.answer()
    
    if query.data.startswith('leaderboard_page_'):
        page = int(query.data.split('_')[2])
        await show_general_leaderboard(query, page)
        return

    if query.data == 'about':
        await query.edit_message_text(
            '📚 *О БОТЕ*\n\n'
            'Этот бот поможет проверить знания по Первому посланию Петра.\n\n'
            '*Уровни сложности:*\n'
            '🟢 Лёгкий — 1 балл за вопрос\n'
            '🟡 Средний — 2 балла за вопрос\n'
            '🔴 Сложный — 3 балла за вопрос\n\n'
            'Используй /test чтобы начать!',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data='back_to_main')]]),
            parse_mode='Markdown'
        )
    
    elif query.data == 'start_test':
        await choose_level(update, context, is_callback=True)
    
    elif query.data == 'leaderboard':
        await show_general_leaderboard(query, 0)
    
    elif query.data == 'my_stats':
        await show_my_stats(query)

# Показ таблицы лидеров
async def show_general_leaderboard(query, page=0):
    users = get_leaderboard_page(page)
    total_users = get_total_users()
    
    if not users:
        text = '🏆 *ТАБЛИЦА ЛИДЕРОВ*\n\nПока никто не проходил тесты.\nБудь первым! 🚀'
    else:
        text = f'🏆 *ТАБЛИЦА ЛИДЕРОВ* (Стр. {page + 1})\n\n'
        start_rank = (page * 10) + 1
        
        for i, entry in enumerate(users, start_rank):
            medal = ""
            if i == 1: medal = "🥇"
            elif i == 2: medal = "🥈"
            elif i == 3: medal = "🥉"
            
            name = entry.get('first_name', 'Unknown')
            if len(name) > 15:
                name = name[:15] + "..."
            
            text += f'{medal} *{i}.* {name}\n'
            text += f'   💎 {entry.get("total_points", 0)} баллов\n\n'
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f'leaderboard_page_{page-1}'))
    if (page + 1) * 10 < total_users:
        nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f'leaderboard_page_{page+1}'))
    
    keyboard = []
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton("🎯 Начать тест", callback_data='start_test')])
    keyboard.append([InlineKeyboardButton("⬅️ В меню", callback_data='back_to_main')])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

# Моя статистика
async def show_my_stats(query):
    user_id = query.from_user.id
    position, entry = get_user_position(user_id)
    
    if not entry:
        text = '📊 *МОЯ СТАТИСТИКА*\n\nВы ещё не проходили тесты.\nИспользуйте /test чтобы начать!'
    else:
        text = '📊 *МОЯ СТАТИСТИКА*\n\n'
        text += f'🏅 Позиция: *#{position}*\n'
        text += f'💎 Всего баллов: *{entry.get("total_points", 0)}*\n\n'
        text += f'🟢 Лёгкий: {entry.get("easy_attempts", 0)} раз(а)\n'
        text += f'🟡 Средний: {entry.get("medium_attempts", 0)} раз(а)\n'
        text += f'🔴 Сложный: {entry.get("hard_attempts", 0)} раз(а)\n'
    
    keyboard = [
        [InlineKeyboardButton("🎯 Начать тест", callback_data='start_test')],
        [InlineKeyboardButton("⬅️ Назад", callback_data='back_to_main')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

# Возврат в главное меню
async def back_to_main(update: Update, context):
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [InlineKeyboardButton("📖 О боте", callback_data='about')],
        [InlineKeyboardButton("🎯 Начать тест", callback_data='start_test')],
        [InlineKeyboardButton("🏆 Таблица лидеров", callback_data='leaderboard')],
        [InlineKeyboardButton("📊 Моя статистика", callback_data='my_stats')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        '📖 *БИБЛЕЙСКИЙ ТЕСТ-БОТ*\n\nВыбери действие:',
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

# Выбор уровня сложности
async def choose_level(update, context, is_callback=False):
    keyboard = [
        [InlineKeyboardButton("🟢 Лёгкий (1 балл)", callback_data='level_easy')],
        [InlineKeyboardButton("🟡 Средний (2 балла)", callback_data='level_medium')],
        [InlineKeyboardButton("🔴 Сложный (3 балла)", callback_data='level_hard')],
        [InlineKeyboardButton("⬅️ Назад", callback_data='back_to_main')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = '🎯 *ВЫБЕРИ УРОВЕНЬ СЛОЖНОСТИ*\n\n🟢 Лёгкий — 1 балл\n🟡 Средний — 2 балла\n🔴 Сложный — 3 балла'
    
    if is_callback and hasattr(update, 'callback_query'):
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

# Начало теста
async def level_selected(update: Update, context):
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    
    if query.data == 'back_to_main':
        await back_to_main(update, context)
        return ConversationHandler.END
    
    if query.data == 'level_easy':
        questions = easy_questions
        level_name = "🟢 Лёгкий"
        level_key = "easy"
    elif query.data == 'level_medium':
        questions = medium_questions
        level_name = "🟡 Средний"
        level_key = "medium"
    else:
        questions = hard_questions
        level_name = "🔴 Сложный"
        level_key = "hard"
    
    user_data[user_id] = {
        "questions": questions,
        "level_name": level_name,
        "level_key": level_key,
        "current_question": 0,
        "correct_answers": 0,
        "wrong_answers": [],
        "start_time": time.time()
    }
    
    await query.edit_message_text(
        f'*{level_name} уровень*\n\nНачинаем тест! 📝\nЗасекаем время... ⏱',
        parse_mode='Markdown'
    )
    await send_question(query.message, user_id)
    return ANSWERING

# Отправка вопроса
async def send_question(message, user_id):
    data = user_data[user_id]
    q_num = data["current_question"]
    
    if q_num >= len(data["questions"]):
        await show_results(message, user_id)
        return ConversationHandler.END
    
    q = data["questions"][q_num]
    keyboard = [[option] for option in q["options"]]
    
    await message.reply_text(
        f'*Вопрос {q_num + 1}/10*\n\n{q["question"]}',
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True),
        parse_mode='Markdown'
    )

# Обработка ответа
async def answer(update: Update, context):
    user_id = update.effective_user.id
    
    if user_id not in user_data:
        await update.message.reply_text("Используй /test чтобы начать тест")
        return ConversationHandler.END
    
    data = user_data[user_id]
    q_num = data["current_question"]
    q = data["questions"][q_num]
    
    user_answer = update.message.text
    
    try:
        answer_index = q["options"].index(user_answer)
        if answer_index == q["correct"]:
            data["correct_answers"] += 1
        else:
            data["wrong_answers"].append({
                "question": q["question"],
                "your_answer": user_answer,
                "correct_answer": q["options"][q["correct"]],
                "explanation": q["explanation"]
            })
        await update.message.reply_text("✓ Принято", reply_markup=ReplyKeyboardRemove())
    except ValueError:
        await update.message.reply_text("Выбери один из вариантов")
        return ANSWERING
    
    data["current_question"] += 1
    
    if data["current_question"] < len(data["questions"]):
        await send_question(update.message, user_id)
        return ANSWERING
    else:
        await show_results(update.message, user_id)
        return ConversationHandler.END

# Показ результатов
async def show_results(message, user_id):
    data = user_data[user_id]
    score = data["correct_answers"]
    total = len(data["questions"])
    percentage = (score / total) * 100
    
    time_taken = time.time() - data["start_time"]
    user = message.from_user
    
    add_to_leaderboard(user_id, user.username, user.first_name, data["level_key"], score, total, time_taken)
    
    position, entry = get_user_position(user_id)
    points_per_question = {"easy": 1, "medium": 2, "hard": 3}
    earned_points = score * points_per_question[data["level_key"]]
    
    if percentage >= 90:
        grade = "Отлично! 🌟"
    elif percentage >= 70:
        grade = "Хорошо! 👍"
    elif percentage >= 50:
        grade = "Удовлетворительно 📖"
    else:
        grade = "Нужно повторить 📚"
    
    result_text = f'🏆 *РЕЗУЛЬТАТЫ*\n\n'
    result_text += f'*Уровень:* {data["level_name"]}\n'
    result_text += f'*Правильно:* {score}/{total}\n'
    result_text += f'*Баллы:* +{earned_points} 💎\n'
    result_text += f'*Время:* {format_time(time_taken)}\n'
    result_text += f'*Позиция:* #{position}\n'
    result_text += f'*Оценка:* {grade}\n\n'
    
    if data["wrong_answers"]:
        result_text += '❌ *ОШИБКИ:*\n\n'
        for i, wrong in enumerate(data["wrong_answers"], 1):
            result_text += f'*{i}. {wrong["question"]}*\n'
            result_text += f'✅ {wrong["correct_answer"]}\n\n'
    
    keyboard = [
        [InlineKeyboardButton("🔄 Ещё раз", callback_data='start_test')],
        [InlineKeyboardButton("🏆 Лидеры", callback_data='leaderboard')],
        [InlineKeyboardButton("⬅️ Меню", callback_data='back_to_main')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await message.reply_text(result_text, reply_markup=reply_markup, parse_mode='Markdown')

# Команда /test
async def test_command(update: Update, context):
    await choose_level(update, context, is_callback=False)
    return CHOOSING_LEVEL

# Команда /leaderboard
async def leaderboard_command(update: Update, context):
    await show_general_leaderboard(update.message, 0)

# Отмена
async def cancel(update: Update, context):
    await update.message.reply_text('❌ Тест отменён.')
    return ConversationHandler.END

# Главная функция
def main():
    app = Application.builder().token("8134773553:AAF4DWLR7DBDolkigso_ZgXd4Ml_90YaaK8").build()
    
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('test', test_command),
            CallbackQueryHandler(level_selected, pattern='^level_')
        ],
        states={
            CHOOSING_LEVEL: [CallbackQueryHandler(level_selected)],
            ANSWERING: [MessageHandler(filters.TEXT & ~filters.COMMAND, answer)]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CallbackQueryHandler(back_to_main, pattern='^back_to_main$'),
            CallbackQueryHandler(button_handler, pattern='^(about|start_test|leaderboard|my_stats)$'),
            CallbackQueryHandler(button_handler, pattern=r'^leaderboard_page_\d+$')
        ],
        allow_reentry=True
    )
    
    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("leaderboard", leaderboard_command))
    app.add_handler(CallbackQueryHandler(button_handler, pattern=r'^(about|start_test|leaderboard|my_stats|leaderboard_page_\d+)$'))
    app.add_handler(CallbackQueryHandler(back_to_main, pattern='^back_to_main$'))
    
    print('🤖 Бот запущен!')
    app.run_polling()

if __name__ == '__main__':
    main()

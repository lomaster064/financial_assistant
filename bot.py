import sqlite3
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command, StateFilter
from aiogram.types import BufferedInputFile, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
import calendar
import os
from dotenv import load_dotenv
from aiogram.types import BufferedInputFile

from functools import wraps

def restrict_access(handler):
    @wraps(handler)
    async def wrapper(message: types.Message, *args, **kwargs):
        user_id = message.from_user.id

        # Исключаем кнопку "Оплатить подписку" из проверки
        if message.text == "Оплатить подписку":
            return await handler(message, *args, **kwargs)

        # Проверяем, есть ли пользователь в списке разрешённых
        cursor.execute('SELECT has_access, subscription_end FROM allowed_users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()

        if result is None:
            has_access = 0
            subscription_end = None
        else:
            has_access, subscription_end = result

        # Проверяем, активна ли подписка
        if subscription_end:
            try:
                subscription_end_date = datetime.strptime(subscription_end, '%Y-%m-%d')
                if subscription_end_date < datetime.now():
                    has_access = 0
                    # Обновляем статус, если подписка истекла
                    cursor.execute('UPDATE allowed_users SET has_access = 0 WHERE user_id = ?', (user_id,))
                    conn.commit()
            except ValueError:
                has_access = 0
                cursor.execute('UPDATE allowed_users SET has_access = 0, subscription_end = NULL WHERE user_id = ?', (user_id,))
                conn.commit()

        if has_access or user_id in ADMINS:
            return await handler(message, *args, **kwargs)
        else:
            await message.reply("У вас нет доступа к этому боту. Нажмите на кнопку 'Оплатить подписку' ниже или напишите администратору: @YourAdminUsername")
            return
    return wrapper

def restrict_access_callback(handler):
    @wraps(handler)
    async def wrapper(callback: types.CallbackQuery, *args, **kwargs):
        user_id = callback.from_user.id

        # Проверяем, есть ли пользователь в списке разрешённых
        cursor.execute('SELECT has_access, subscription_end FROM allowed_users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()

        if result is None:
            has_access = 0
            subscription_end = None
        else:
            has_access, subscription_end = result

        # Проверяем, активна ли подписка
        if subscription_end:
            try:
                subscription_end_date = datetime.strptime(subscription_end, '%Y-%m-%d')
                if subscription_end_date < datetime.now():
                    has_access = 0
                    # Обновляем статус, если подписка истекла
                    cursor.execute('UPDATE allowed_users SET has_access = 0 WHERE user_id = ?', (user_id,))
                    conn.commit()
            except ValueError:
                has_access = 0
                cursor.execute('UPDATE allowed_users SET has_access = 0, subscription_end = NULL WHERE user_id = ?', (user_id,))
                conn.commit()

        if has_access or user_id in ADMINS:
            return await handler(callback, *args, **kwargs)
        else:
            await callback.message.reply("У вас нет доступа к этому боту. Вы можете купить подписку с помощью команды /subscribe или написать администратору: @YourAdminUsername")
            await callback.answer()
            return
    return wrapper

load_dotenv()

# Токен вашего бота
API_TOKEN = os.getenv("TELEGRAM_API")
# Список администраторов (замените на свои user_id)
ADMINS = [1776467286] 

# Инициализация бота
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Подключение к базе данных SQLite
conn = sqlite3.connect('finance.db', check_same_thread=False)
cursor = conn.cursor()

# Создание таблицы (если она еще не создана)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        type TEXT,
        amount REAL,
        category TEXT,
        date TEXT
    )
''')

# Создание таблицы для целей
cursor.execute('''
    CREATE TABLE IF NOT EXISTS goals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        goal_name TEXT,
        target_amount REAL,
        current_amount REAL DEFAULT 0
    )
''')
conn.commit()


# Создание таблицы для разрешённых пользователей
cursor.execute('''
    CREATE TABLE IF NOT EXISTS allowed_users (
        user_id INTEGER PRIMARY KEY,
        has_access BOOLEAN DEFAULT 0,
        subscription_end TEXT DEFAULT NULL
    )
''')
conn.commit()

# Добавление столбцов first_name и last_name, если их нет
try:
    cursor.execute('ALTER TABLE allowed_users ADD COLUMN first_name TEXT')
except sqlite3.OperationalError:
    pass  # Столбец уже существует

try:
    cursor.execute('ALTER TABLE allowed_users ADD COLUMN last_name TEXT')
except sqlite3.OperationalError:
    pass  # Столбец уже существует

conn.commit()

# Добавляем столбец comment, если его еще нет
try:
    cursor.execute('ALTER TABLE transactions ADD COLUMN comment TEXT')
except sqlite3.OperationalError:
    # Столбец уже существует, ничего не делаем
    pass
conn.commit()

# Определение состояний FSM
class FinanceForm(StatesGroup):
    amount = State()
    category = State()
    date = State()
    comment = State()

class DeleteTransaction(StatesGroup):
    select_period = State()
    select_transaction = State()

class EditTransaction(StatesGroup):
    select_period = State()
    select_transaction = State()
    select_field = State()
    edit_amount = State()
    edit_category = State()
    edit_date = State()
    edit_comment = State()

class ReportForm(StatesGroup):
    period = State()
    start_date = State()
    end_date = State()
    report_type = State()
    category = State()

class ChartForm(StatesGroup):
    period = State()
    start_date = State()
    end_date = State()
    chart_type = State()
    diagram_type = State()

class ChartForm(StatesGroup):
    period = State()
    start_date = State()
    end_date = State()
    chart_type = State()
    diagram_type = State()
    interval = State()  # Новое состояние для выбора интервала разбиения

# Новые классы состояний для целей
class GoalForm(StatesGroup):
    name = State()
    target = State()
    contribute = State()


# Интервалы для разбиения данных
INTERVALS = {
    "day": "По дням",
    "week": "По неделям",
    "month": "По месяцам",
    "none": "Без разбиения"
}

# Основная клавиатура с объединёнными кнопками
main_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Транзакции")],
        [KeyboardButton(text="Вывод данных")],
        [KeyboardButton(text="Управление целями")],
        [KeyboardButton(text="Оплатить подписку")]
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)

# Клавиатура для пользователей без подписки
no_subscription_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Оплатить подписку")]
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)

# Клавиатура для администратора
admin_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Транзакции")],
        [KeyboardButton(text="Вывод данных")],
        [KeyboardButton(text="Управление целями")],
        [KeyboardButton(text="Управление пользователями")]
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)

# Подменю для "Транзакции"
transactions_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Добавить доход"), KeyboardButton(text="Добавить расход")],
        [KeyboardButton(text="Удалить транзакцию"), KeyboardButton(text="Изменить транзакцию")],
        [KeyboardButton(text="Назад")]
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)

# Подменю для "Вывод данных"
data_output_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Отчет"), KeyboardButton(text="Визуализация")],
        [KeyboardButton(text="Назад")]
    ],
    resize_keyboard=True,
    one_time_keyboard=False
)

# Клавиатура для пропуска комментария
skip_comment_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Пропустить")]
    ],
    resize_keyboard=True,
    one_time_keyboard=True
)

# Категории для доходов и расходов
INCOME_CATEGORIES = ["Зарплата Илья", "Зарплата Мария", "Подарок", "Инвестиции", "Другое"]
EXPENSE_CATEGORIES = ["Кредиты", "Кредитная карта", "Страхование", "Подарок", "Сигареты", "Связь", "Еда", "Транспорт", "Жилье", "Развлечения", "Маркетплейсы", "Путешествия", "Другое"]

# Периоды для отчетов и диаграмм
PERIODS = {
    "day": ("День", timedelta(days=1)),
    "week": ("Неделя", timedelta(weeks=1)),
    "month": ("Месяц", timedelta(days=30)),
    "quarter": ("Квартал", timedelta(days=90)),
    "year": ("Год", timedelta(days=365)),
    "custom": ("Ручной выбор", None)
}

# Типы отчетов/диаграмм
REPORT_TYPES = {
    "income": "Доходы",
    "expense": "Расходы",
    "balance": "Разница"
}

# Типы диаграмм
DIAGRAM_TYPES = {
    "pie": "Круговая диаграмма",
    "bar": "Столбчатая гистограмма"
}

# Функция для создания календаря
def get_calendar(year=None, month=None, prefix=""):
    now = datetime.now()
    year = year or now.year
    month = month or now.month

    first_day = datetime(year, month, 1)
    _, days_in_month = calendar.monthrange(year, month)

    keyboard = []
    month_names = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
    keyboard.append([InlineKeyboardButton(text=f"{month_names[month - 1]} {year}", callback_data="ignore")])

    days_of_week = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    keyboard.append([InlineKeyboardButton(text=day, callback_data="ignore") for day in days_of_week])

    current_day = 1
    week = []
    for _ in range(first_day.weekday()):
        week.append(InlineKeyboardButton(text=" ", callback_data="ignore"))

    while current_day <= days_in_month:
        week.append(InlineKeyboardButton(text=str(current_day), callback_data=f"{prefix}date_{year}_{month}_{current_day}"))
        if len(week) == 7:
            keyboard.append(week)
            week = []
        current_day += 1

    if week:
        while len(week) < 7:
            week.append(InlineKeyboardButton(text=" ", callback_data="ignore"))
        keyboard.append(week)

    keyboard.append([
        InlineKeyboardButton(text="<<", callback_data=f"{prefix}prev_{year}_{month}"),
        InlineKeyboardButton(text=">>", callback_data=f"{prefix}next_{year}_{month}")
    ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)

@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    user_id = message.from_user.id
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name

    # Проверяем, есть ли пользователь в списке разрешённых
    cursor.execute('SELECT has_access, subscription_end, first_name, last_name FROM allowed_users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()

    if result is None:
        # Если пользователя нет в базе, добавляем его с has_access = 0 и сохраняем имя/фамилию
        cursor.execute('INSERT INTO allowed_users (user_id, has_access, first_name, last_name) VALUES (?, 0, ?, ?)',
                       (user_id, first_name, last_name))
        conn.commit()
        has_access = 0
        subscription_end = None
    else:
        has_access, subscription_end, _, _ = result
        # Обновляем имя и фамилию, если они изменились
        cursor.execute('UPDATE allowed_users SET first_name = ?, last_name = ? WHERE user_id = ?',
                       (first_name, last_name, user_id))
        conn.commit()

    # Проверяем, активна ли подписка
    if subscription_end:
        try:
            subscription_end_date = datetime.strptime(subscription_end, '%Y-%m-%d')
            if subscription_end_date < datetime.now():
                has_access = 0
                # Обновляем статус, если подписка истекла
                cursor.execute('UPDATE allowed_users SET has_access = 0 WHERE user_id = ?', (user_id,))
                conn.commit()
        except ValueError:
            has_access = 0
            cursor.execute('UPDATE allowed_users SET has_access = 0, subscription_end = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Показываем меню в зависимости от статуса пользователя
    if has_access or user_id in ADMINS:
        keyboard = admin_keyboard if user_id in ADMINS else main_keyboard
        await message.reply("Привет! Я бот для учета финансов. Выберите действие:", reply_markup=keyboard)
    else:
        keyboard = no_subscription_keyboard
        await message.reply(f"У вас нет доступа к основным функциям бота.\n\n"
                            f"Вы можете купить подписку за {SUBSCRIPTION_PRICE} RUB на {SUBSCRIPTION_DURATION_DAYS} дней, нажав на кнопку ниже.\n"
                            f"Или напишите администратору: @YourAdminUsername", reply_markup=keyboard)


# Стоимость подписки и длительность (в днях)
SUBSCRIPTION_PRICE = 100  # Цена в рублях (или другой валюте)
SUBSCRIPTION_DURATION_DAYS = 30  # Длительность подписки в днях

@dp.message(Command("subscribe"))
async def subscribe(message: types.Message):
    user_id = message.from_user.id

    # Проверяем, есть ли уже активная подписка
    cursor.execute('SELECT subscription_end FROM allowed_users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()

    if result and result[0]:
        subscription_end = datetime.strptime(result[0], '%Y-%m-%d')
        if subscription_end > datetime.now():
            await message.reply(f"У вас уже есть активная подписка до {result[0]}. Вы можете продлить её после окончания.")
            return
        else:
            # Подписка истекла, но мы можем продлить от её окончания
            description = f"Продление подписки на {SUBSCRIPTION_DURATION_DAYS} дней (с {result[0]})"
    else:
        # Подписки нет, начинаем с текущей даты
        description = f"Доступ к боту на {SUBSCRIPTION_DURATION_DAYS} дней"

    # Создаём инвойс для оплаты
    prices = [types.LabeledPrice(label="Подписка на бота", amount=SUBSCRIPTION_PRICE * 100)]  # Telegram принимает сумму в копейках
    await bot.send_invoice(
        chat_id=user_id,
        title="Подписка на финансовый бот",
        description=description,
        payload="subscription_payment",
        provider_token="YOUR_PROVIDER_TOKEN",  # Замените на ваш токен провайдера (например, YooMoney)
        currency="RUB",
        prices=prices,
        start_parameter="subscribe",
        need_name=False,
        need_phone_number=False,
        need_email=False,
        need_shipping_address=False,
        is_flexible=False
    )

@dp.message(lambda message: message.text == "Оплатить подписку")
async def subscribe_button(message: types.Message):
    await subscribe(message)

@dp.message(Command("status"))
async def check_status(message: types.Message):
    user_id = message.from_user.id

    cursor.execute('SELECT has_access, subscription_end FROM allowed_users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()

    if result is None or not result[0]:
        await message.reply("У вас нет активной подписки. Нажмите на кнопку 'Оплатить подписку' ниже.",
                            reply_markup=no_subscription_keyboard)
        return

    has_access, subscription_end = result
    subscription_end_date = datetime.strptime(subscription_end, '%Y-%m-%d')
    if subscription_end_date < datetime.now():
        await message.reply("Ваша подписка истекла. Нажмите на кнопку 'Оплатить подписку' ниже, чтобы продлить её.",
                            reply_markup=no_subscription_keyboard)
    else:
        keyboard = admin_keyboard if user_id in ADMINS else main_keyboard
        await message.reply(f"Ваша подписка активна до {subscription_end}.", reply_markup=keyboard)


@dp.message(Command("manage_users"))
async def manage_users(message: types.Message):
    user_id = message.from_user.id

    # Проверяем, является ли пользователь администратором
    if user_id not in ADMINS:
        await message.reply("У вас нет прав для выполнения этой команды.")
        return

    # Получаем список всех пользователей
    cursor.execute('SELECT user_id, has_access, subscription_end, first_name, last_name FROM allowed_users')
    users = cursor.fetchall()

    if not users:
        await message.reply("В базе данных пока нет пользователей.")
        return

    # Создаём клавиатуру с пользователями
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for user in users:
        user_id, has_access, subscription_end, first_name, last_name = user
        status = "✅ Активен" if has_access else "❌ Неактивен"
        if subscription_end:
            status += f" (до {subscription_end})"
        # Формируем имя для отображения
        name = first_name if first_name else "Не указано"
        if last_name:
            name += f" {last_name}"
        display_text = f"{name} (ID: {user_id}) | {status}"
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=display_text, callback_data=f"manage_user_{user_id}")
        ])

    await message.reply("Список пользователей (выберите для управления):", reply_markup=keyboard)

# Предоставление доступа
@dp.callback_query(lambda c: c.data.startswith("grant_access_"))
async def grant_access(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    if user_id not in ADMINS:
        await callback.message.edit_text("У вас нет прав для выполнения этой команды.")
        await callback.answer()
        return

    target_user_id = int(callback.data.split("_")[2])

    # Обновляем статус пользователя
    cursor.execute('UPDATE allowed_users SET has_access = 1 WHERE user_id = ?', (target_user_id,))
    conn.commit()

    # Обновляем информацию о пользователе
    cursor.execute('SELECT has_access, subscription_end FROM allowed_users WHERE user_id = ?', (target_user_id,))
    user = cursor.fetchone()
    has_access, subscription_end = user
    status = "✅ Активен" if has_access else "❌ Неактивен"
    if subscription_end:
        status += f"\nПодписка до: {subscription_end}"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Предоставить доступ", callback_data=f"grant_access_{target_user_id}")],
        [InlineKeyboardButton(text="Отозвать доступ", callback_data=f"revoke_access_{target_user_id}")],
        [InlineKeyboardButton(text="Назад к списку", callback_data="back_to_users")]
    ])

    await callback.message.edit_text(f"Управление пользователем ID: {target_user_id}\nСтатус: {status}\n\nДоступ предоставлен!", reply_markup=keyboard)
    await callback.answer()

    # Уведомляем пользователя
    try:
        await bot.send_message(target_user_id, "Администратор предоставил вам доступ к боту! Напишите /start, чтобы начать.")
    except Exception as e:
        await bot.send_message(user_id, f"Не удалось уведомить пользователя {target_user_id}: {e}")

# Отзыв доступа
@dp.callback_query(lambda c: c.data.startswith("revoke_access_"))
async def revoke_access(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    if user_id not in ADMINS:
        await callback.message.edit_text("У вас нет прав для выполнения этой команды.")
        await callback.answer()
        return

    target_user_id = int(callback.data.split("_")[2])

    # Обновляем статус пользователя
    cursor.execute('UPDATE allowed_users SET has_access = 0 WHERE user_id = ?', (target_user_id,))
    conn.commit()

    # Обновляем информацию о пользователе
    cursor.execute('SELECT has_access, subscription_end FROM allowed_users WHERE user_id = ?', (target_user_id,))
    user = cursor.fetchone()
    has_access, subscription_end = user
    status = "✅ Активен" if has_access else "❌ Неактивен"
    if subscription_end:
        status += f"\nПодписка до: {subscription_end}"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Предоставить доступ", callback_data=f"grant_access_{target_user_id}")],
        [InlineKeyboardButton(text="Отозвать доступ", callback_data=f"revoke_access_{target_user_id}")],
        [InlineKeyboardButton(text="Назад к списку", callback_data="back_to_users")]
    ])

    await callback.message.edit_text(f"Управление пользователем ID: {target_user_id}\nСтатус: {status}\n\nДоступ отозван!", reply_markup=keyboard)
    await callback.answer()

    # Уведомляем пользователя
    try:
        await bot.send_message(target_user_id, "Администратор отозвал ваш доступ к боту. Для возобновления доступа обратитесь к администратору или купите подписку с помощью команды /subscribe.")
    except Exception as e:
        await bot.send_message(user_id, f"Не удалось уведомить пользователя {target_user_id}: {e}")

# Возврат к списку пользователей
@dp.callback_query(lambda c: c.data == "back_to_users")
async def back_to_users(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    if user_id not in ADMINS:
        await callback.message.edit_text("У вас нет прав для выполнения этой команды.")
        await callback.answer()
        return

    # Получаем список всех пользователей
    cursor.execute('SELECT user_id, has_access, subscription_end, first_name, last_name FROM allowed_users')
    users = cursor.fetchall()

    if not users:
        await callback.message.edit_text("В базе данных пока нет пользователей.")
        await callback.answer()
        return

    # Создаём клавиатуру с пользователями
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for user in users:
        user_id, has_access, subscription_end, first_name, last_name = user
        status = "✅ Активен" if has_access else "❌ Неактивен"
        if subscription_end:
            status += f" (до {subscription_end})"
        # Формируем имя для отображения
        name = first_name if first_name else "Не указано"
        if last_name:
            name += f" {last_name}"
        display_text = f"{name} (ID: {user_id}) | {status}"
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=display_text, callback_data=f"manage_user_{user_id}")
        ])

    await callback.message.edit_text("Список пользователей (выберите для управления):", reply_markup=keyboard)
    await callback.answer()

# Предоставление доступа
@dp.callback_query(lambda c: c.data.startswith("grant_access_"))
async def grant_access(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    if user_id not in ADMINS:
        await callback.message.edit_text("У вас нет прав для выполнения этой команды.")
        await callback.answer()
        return

    target_user_id = int(callback.data.split("_")[2])

    # Обновляем статус пользователя
    cursor.execute('UPDATE allowed_users SET has_access = 1 WHERE user_id = ?', (target_user_id,))
    conn.commit()

    # Обновляем информацию о пользователе
    cursor.execute('SELECT has_access, subscription_end, first_name, last_name FROM allowed_users WHERE user_id = ?', (target_user_id,))
    user = cursor.fetchone()
    has_access, subscription_end, first_name, last_name = user
    status = "✅ Активен" if has_access else "❌ Неактивен"
    if subscription_end:
        status += f"\nПодписка до: {subscription_end}"

    # Формируем имя для отображения
    name = first_name if first_name else "Не указано"
    if last_name:
        name += f" {last_name}"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Предоставить доступ", callback_data=f"grant_access_{target_user_id}")],
        [InlineKeyboardButton(text="Отозвать доступ", callback_data=f"revoke_access_{target_user_id}")],
        [InlineKeyboardButton(text="Назад к списку", callback_data="back_to_users")]
    ])

    await callback.message.edit_text(f"Управление пользователем: {name} (ID: {target_user_id})\nСтатус: {status}\n\nДоступ предоставлен!", reply_markup=keyboard)
    await callback.answer()

    # Уведомляем пользователя
    try:
        await bot.send_message(target_user_id, "Администратор предоставил вам доступ к боту! Напишите /start, чтобы начать.")
    except Exception as e:
        await bot.send_message(user_id, f"Не удалось уведомить пользователя {target_user_id}: {e}")

# Отзыв доступа
@dp.callback_query(lambda c: c.data.startswith("revoke_access_"))
async def revoke_access(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    if user_id not in ADMINS:
        await callback.message.edit_text("У вас нет прав для выполнения этой команды.")
        await callback.answer()
        return

    target_user_id = int(callback.data.split("_")[2])

    # Обновляем статус пользователя
    cursor.execute('UPDATE allowed_users SET has_access = 0 WHERE user_id = ?', (target_user_id,))
    conn.commit()

    # Обновляем информацию о пользователе
    cursor.execute('SELECT has_access, subscription_end, first_name, last_name FROM allowed_users WHERE user_id = ?', (target_user_id,))
    user = cursor.fetchone()
    has_access, subscription_end, first_name, last_name = user
    status = "✅ Активен" if has_access else "❌ Неактивен"
    if subscription_end:
        status += f"\nПодписка до: {subscription_end}"

    # Формируем имя для отображения
    name = first_name if first_name else "Не указано"
    if last_name:
        name += f" {last_name}"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Предоставить доступ", callback_data=f"grant_access_{target_user_id}")],
        [InlineKeyboardButton(text="Отозвать доступ", callback_data=f"revoke_access_{target_user_id}")],
        [InlineKeyboardButton(text="Назад к списку", callback_data="back_to_users")]
    ])

    await callback.message.edit_text(f"Управление пользователем: {name} (ID: {target_user_id})\nСтатус: {status}\n\nДоступ отозван!", reply_markup=keyboard)
    await callback.answer()

    # Уведомляем пользователя
    try:
        await bot.send_message(target_user_id, "Администратор отозвал ваш доступ к боту. Для возобновления доступа обратитесь к администратору или купите подписку с помощью команды /subscribe.")
    except Exception as e:
        await bot.send_message(user_id, f"Не удалось уведомить пользователя {target_user_id}: {e}")


# Возврат к списку пользователей
@dp.callback_query(lambda c: c.data == "back_to_users")
async def back_to_users(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    if user_id not in ADMINS:
        await callback.message.edit_text("У вас нет прав для выполнения этой команды.")
        await callback.answer()
        return

    # Получаем список всех пользователей
    cursor.execute('SELECT user_id, has_access, subscription_end FROM allowed_users')
    users = cursor.fetchall()

    if not users:
        await callback.message.edit_text("В базе данных пока нет пользователей.")
        await callback.answer()
        return

    # Создаём клавиатуру с пользователями
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for user in users:
        user_id, has_access, subscription_end = user
        status = "✅ Активен" if has_access else "❌ Неактивен"
        if subscription_end:
            status += f" (до {subscription_end})"
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(text=f"ID: {user_id} | {status}", callback_data=f"manage_user_{user_id}")
        ])

    await callback.message.edit_text("Список пользователей (выберите для управления):", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("manage_user_"))
async def manage_user(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    # Проверяем, является ли пользователь администратором
    if user_id not in ADMINS:
        await callback.message.edit_text("У вас нет прав для выполнения этой команды.")
        await callback.answer()
        return

    # Получаем user_id выбранного пользователя
    target_user_id = int(callback.data.split("_")[2])

    # Получаем информацию о пользователе
    cursor.execute('SELECT has_access, subscription_end, first_name, last_name FROM allowed_users WHERE user_id = ?', (target_user_id,))
    user = cursor.fetchone()

    if not user:
        await callback.message.edit_text("Пользователь не найден.")
        await callback.answer()
        return

    has_access, subscription_end, first_name, last_name = user
    status = "✅ Активен" if has_access else "❌ Неактивен"
    if subscription_end:
        status += f"\nПодписка до: {subscription_end}"

    # Формируем имя для отображения
    name = first_name if first_name else "Не указано"
    if last_name:
        name += f" {last_name}"

    # Создаём клавиатуру для управления пользователем
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Предоставить доступ", callback_data=f"grant_access_{target_user_id}")],
        [InlineKeyboardButton(text="Отозвать доступ", callback_data=f"revoke_access_{target_user_id}")],
        [InlineKeyboardButton(text="Назад к списку", callback_data="back_to_users")]
    ])

    await callback.message.edit_text(f"Управление пользователем: {name} (ID: {target_user_id})\nСтатус: {status}", reply_markup=keyboard)
    await callback.answer()

# Обработка pre_checkout_query (подтверждение перед оплатой)
@dp.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: types.PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

# Обработка успешного платежа
@dp.message(lambda message: message.successful_payment is not None)
async def process_successful_payment(message: types.Message):
    user_id = message.from_user.id
    payment = message.successful_payment

    if payment.invoice_payload == "subscription_payment":
        # Проверяем текущую дату окончания подписки
        cursor.execute('SELECT subscription_end FROM allowed_users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()

        # Если есть предыдущая подписка, продлеваем от её окончания, иначе от текущей даты
        if result and result[0]:
            try:
                current_subscription_end = datetime.strptime(result[0], '%Y-%m-%d')
                # Если текущая подписка ещё активна, продлеваем от её окончания
                if current_subscription_end > datetime.now():
                    subscription_end = current_subscription_end + timedelta(days=SUBSCRIPTION_DURATION_DAYS)
                else:
                    # Если подписка истекла, начинаем с текущей даты
                    subscription_end = datetime.now() + timedelta(days=SUBSCRIPTION_DURATION_DAYS)
            except ValueError:
                # Если формат даты неверный, начинаем с текущей даты
                subscription_end = datetime.now() + timedelta(days=SUBSCRIPTION_DURATION_DAYS)
        else:
            # Если подписки нет, начинаем с текущей даты
            subscription_end = datetime.now() + timedelta(days=SUBSCRIPTION_DURATION_DAYS)

        subscription_end_str = subscription_end.strftime('%Y-%m-%d')

        # Обновляем статус пользователя
        cursor.execute('UPDATE allowed_users SET has_access = 1, subscription_end = ? WHERE user_id = ?',
                       (subscription_end_str, user_id))
        conn.commit()

        # Показываем меню в зависимости от того, админ ли пользователь
        keyboard = admin_keyboard if user_id in ADMINS else main_keyboard
        await message.reply(f"Оплата прошла успешно! Ваш доступ активирован до {subscription_end_str}. Выберите действие:", reply_markup=keyboard)

# Обработчик для кнопки "Транзакции"
@dp.message(lambda message: message.text == "Транзакции")
@restrict_access
async def transactions_menu(message: types.Message):
    await message.reply("Выберите действие с транзакциями:", reply_markup=transactions_keyboard)

# Обработчик для кнопки "Вывод данных"
@dp.message(lambda message: message.text == "Вывод данных")
@restrict_access
async def data_output_menu(message: types.Message):
    await message.reply("Выберите действие для вывода данных:", reply_markup=data_output_keyboard)

# Обработчик для кнопки "Назад"
@dp.message(lambda message: message.text == "Назад")
@restrict_access
async def go_back(message: types.Message):
    user_id = message.from_user.id

    # Проверяем, есть ли активная подписка
    cursor.execute('SELECT has_access, subscription_end FROM allowed_users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()

    if result is None:
        has_access = 0
        subscription_end = None
    else:
        has_access, subscription_end = result

    # Проверяем, активна ли подписка
    if subscription_end:
        try:
            subscription_end_date = datetime.strptime(subscription_end, '%Y-%m-%d')
            if subscription_end_date < datetime.now():
                has_access = 0
                cursor.execute('UPDATE allowed_users SET has_access = 0 WHERE user_id = ?', (user_id,))
                conn.commit()
        except ValueError:
            has_access = 0
            cursor.execute('UPDATE allowed_users SET has_access = 0, subscription_end = NULL WHERE user_id = ?', (user_id,))
            conn.commit()

    # Показываем меню в зависимости от статуса
    if has_access or user_id in ADMINS:
        keyboard = admin_keyboard if user_id in ADMINS else main_keyboard
        await message.reply("Вы вернулись в главное меню:", reply_markup=keyboard)
    else:
        keyboard = no_subscription_keyboard
        await message.reply(f"У вас нет доступа к основным функциям бота.\n\n"
                            f"Вы можете купить подписку за {SUBSCRIPTION_PRICE} RUB на {SUBSCRIPTION_DURATION_DAYS} дней, нажав на кнопку ниже.\n"
                            f"Или напишите администратору: @YourAdminUsername", reply_markup=keyboard)

@dp.message(lambda message: message.text == "Управление пользователями")
async def manage_users_button(message: types.Message):
    user_id = message.from_user.id

    # Проверяем, является ли пользователь администратором
    if user_id not in ADMINS:
        await message.reply("У вас нет прав для выполнения этой команды.")
        return

    # Вызываем функцию manage_users
    await manage_users(message)

# Добавление дохода
@dp.message(lambda message: message.text == "Добавить доход")
@restrict_access
async def income_start(message: types.Message, state: FSMContext):
    await state.update_data(type="income")
    await message.reply("Введите сумму дохода:")
    await state.set_state(FinanceForm.amount)

# Добавление расхода
@dp.message(lambda message: message.text == "Добавить расход")
@restrict_access
async def expense_start(message: types.Message, state: FSMContext):
    await state.update_data(type="expense")
    await message.reply("Введите сумму расхода:")
    await state.set_state(FinanceForm.amount)

# Обработка суммы
@dp.message(StateFilter(FinanceForm.amount))
async def process_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text)
        await state.update_data(amount=amount)

        data = await state.get_data()
        categories = INCOME_CATEGORIES if data["type"] == "income" else EXPENSE_CATEGORIES
        category_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=cat, callback_data=f"category_{cat}")] for cat in categories
        ])

        await message.reply("Выберите категорию:", reply_markup=category_keyboard)
        await state.set_state(FinanceForm.category)
    except ValueError:
        await message.reply("Пожалуйста, введите корректную сумму!")
        return

# Обработка выбора категории
@dp.callback_query(lambda c: c.data.startswith("category_"), StateFilter(FinanceForm.category))
async def process_category(callback: types.CallbackQuery, state: FSMContext):
    category = callback.data.split("_")[1]
    await state.update_data(category=category)
    await callback.message.edit_text("Выберите дату:", reply_markup=get_calendar())
    await state.set_state(FinanceForm.date)
    await callback.answer()

# Обработка выбора даты
@dp.callback_query(lambda c: c.data.startswith("date_"), StateFilter(FinanceForm.date))
async def process_date(callback: types.CallbackQuery, state: FSMContext):
    _, year, month, day = callback.data.split("_")
    date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    await state.update_data(date=date)
    await callback.message.edit_text("Введите комментарий к транзакции (или нажмите 'Пропустить'):", reply_markup=None)
    await bot.send_message(callback.from_user.id, "Комментарий:", reply_markup=skip_comment_keyboard)
    await state.set_state(FinanceForm.comment)
    await callback.answer()

# Обработка комментария
@dp.message(StateFilter(FinanceForm.comment))
async def process_comment(message: types.Message, state: FSMContext):
    comment = message.text if message.text != "Пропустить" else None
    data = await state.get_data()
    user_id = message.from_user.id
    transaction_type = "Доход" if data["type"] == "income" else "Расход"
    amount = data["amount"]
    category = data["category"]
    date = data["date"]

    cursor.execute('INSERT INTO transactions (user_id, type, amount, category, date, comment) VALUES (?, ?, ?, ?, ?, ?)',
                   (user_id, data["type"], amount, category, date, comment))
    conn.commit()

    comment_text = f", комментарий: {comment}" if comment else ""
    await message.reply(f"{transaction_type} {amount} в категории '{category}' за {date}{comment_text} сохранен!", reply_markup=transactions_keyboard)
    await state.clear()

# Обработка смены месяца
@dp.callback_query(lambda c: c.data.startswith(("prev_", "next_")))
async def change_month(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    # callback.data format: [prefix_]prev_2025_4 or [prefix_]next_2025_4
    # If there's a prefix, it will be the first part; otherwise, the first part is "prev" or "next"
    if parts[0] in ("prev", "next"):
        action = parts[0]
        year = int(parts[1])
        month = int(parts[2])
    else:
        action = parts[1]
        year = int(parts[2])
        month = int(parts[3])

    if action == "prev":
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    elif action == "next":
        month += 1
        if month > 12:
            month = 1
            year += 1

    # Determine the prefix (if any) to pass to get_calendar
    prefix = "" if parts[0] in ("prev", "next") else f"{parts[0]}_"
    await callback.message.edit_reply_markup(reply_markup=get_calendar(year, month, prefix=prefix))
    await callback.answer()

# Удаление транзакции: выбор периода
@dp.message(lambda message: message.text == "Удалить транзакцию")
@restrict_access
async def delete_transaction_start(message: types.Message, state: FSMContext):
    period_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=name, callback_data=f"delete_period_{period}")] for period, (name, _) in PERIODS.items()
    ])
    await message.reply("Выберите период для отображения транзакций:", reply_markup=period_keyboard)
    await state.set_state(DeleteTransaction.select_period)

# Обработка выбора периода для удаления
@dp.callback_query(lambda c: c.data.startswith("delete_period_"), StateFilter(DeleteTransaction.select_period))
async def process_delete_period(callback: types.CallbackQuery, state: FSMContext):
    period_key = callback.data.split("_")[2]
    period_name, delta = PERIODS[period_key]

    if period_key == "custom":
        await callback.message.edit_text("Выберите начальную дату периода:", reply_markup=get_calendar(prefix="delete_start_"))
        await state.set_state(DeleteTransaction.select_period)
        await state.update_data(period="custom", start_date=None, end_date=None)
        await callback.answer()
        return

    end_date = datetime.now()
    start_date = end_date - delta

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    user_id = callback.from_user.id
    cursor.execute('SELECT id, type, amount, category, date, comment FROM transactions WHERE user_id = ? AND date BETWEEN ? AND ? ORDER BY date DESC',
                   (user_id, start_date_str, end_date_str))
    transactions = cursor.fetchall()

    if not transactions:
        await callback.message.edit_text(f"За {period_name.lower()} нет транзакций для удаления.", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=main_keyboard)
        await state.clear()
        await callback.answer()
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for trans in transactions:
        trans_id, trans_type, amount, category, date, comment = trans
        display_type = "Доход" if trans_type == "income" else "Расход"
        comment_text = f", комм.: {comment}" if comment else ""
        button_text = f"{date} | {display_type} | {category} | {amount:,}{comment_text}"
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=button_text, callback_data=f"delete_{trans_id}")])

    await callback.message.edit_text(f"Транзакции за {period_name.lower()} ({start_date_str} - {end_date_str}):\nВыберите транзакцию для удаления:", reply_markup=keyboard)
    await state.set_state(DeleteTransaction.select_transaction)
    await callback.answer()

# Обработка выбора начальной даты для удаления
@dp.callback_query(lambda c: c.data.startswith("delete_start_date_"), StateFilter(DeleteTransaction.select_period))
async def delete_select_start_date(callback: types.CallbackQuery, state: FSMContext):
    _, year, month, day = callback.data.split("_")[2:]
    start_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    await state.update_data(start_date=start_date)
    await callback.message.edit_text("Выберите конечную дату периода:", reply_markup=get_calendar(prefix="delete_end_"))
    await state.set_state(DeleteTransaction.select_period)
    await callback.answer()

# Обработка выбора конечной даты для удаления
@dp.callback_query(lambda c: c.data.startswith("delete_end_date_"), StateFilter(DeleteTransaction.select_period))
async def delete_select_end_date(callback: types.CallbackQuery, state: FSMContext):
    _, year, month, day = callback.data.split("_")[2:]
    end_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    data = await state.get_data()
    start_date = data["start_date"]
    
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    if start_dt > end_dt:
        await callback.message.edit_text("Ошибка: начальная дата не может быть позже конечной. Попробуйте снова.", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Выберите период для удаления:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=name, callback_data=f"delete_period_{period}")] for period, (name, _) in PERIODS.items()
        ]))
        await state.set_state(DeleteTransaction.select_period)
        await callback.answer()
        return

    user_id = callback.from_user.id
    cursor.execute('SELECT id, type, amount, category, date, comment FROM transactions WHERE user_id = ? AND date BETWEEN ? AND ? ORDER BY date DESC',
                   (user_id, start_date, end_date))
    transactions = cursor.fetchall()

    if not transactions:
        await callback.message.edit_text(f"За выбранный период нет транзакций для удаления.", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=main_keyboard)
        await state.clear()
        await callback.answer()
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for trans in transactions:
        trans_id, trans_type, amount, category, date, comment = trans
        display_type = "Доход" if trans_type == "income" else "Расход"
        comment_text = f", комм.: {comment}" if comment else ""
        button_text = f"{date} | {display_type} | {category} | {amount:,}{comment_text}"
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=button_text, callback_data=f"delete_{trans_id}")])

    await callback.message.edit_text(f"Транзакции за период {start_date} - {end_date}:\nВыберите транзакцию для удаления:", reply_markup=keyboard)
    await state.set_state(DeleteTransaction.select_transaction)
    await callback.answer()

# Обработка смены месяца в календаре для начальной даты удаления
@dp.callback_query(lambda c: c.data.startswith(("delete_start_prev_", "delete_start_next_")), StateFilter(DeleteTransaction.select_period))
async def delete_change_start_month(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    action = parts[2]
    year = int(parts[3])
    month = int(parts[4])

    if action == "prev":
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    elif action == "next":
        month += 1
        if month > 12:
            month = 1
            year += 1

    await callback.message.edit_reply_markup(reply_markup=get_calendar(year, month, prefix="delete_start_"))
    await callback.answer()

# Обработка смены месяца в календаре для конечной даты удаления
@dp.callback_query(lambda c: c.data.startswith(("delete_end_prev_", "delete_end_next_")), StateFilter(DeleteTransaction.select_period))
async def delete_change_end_month(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    action = parts[2]
    year = int(parts[3])
    month = int(parts[4])

    if action == "prev":
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    elif action == "next":
        month += 1
        if month > 12:
            month = 1
            year += 1

    await callback.message.edit_reply_markup(reply_markup=get_calendar(year, month, prefix="delete_end_"))
    await callback.answer()

# Обработка выбора транзакции для удаления
@dp.callback_query(lambda c: c.data.startswith("delete_"), StateFilter(DeleteTransaction.select_transaction))
async def process_delete_transaction(callback: types.CallbackQuery, state: FSMContext):
    trans_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id

    cursor.execute('SELECT type, amount, category, date, comment FROM transactions WHERE id = ? AND user_id = ?', (trans_id, user_id))
    transaction = cursor.fetchone()

    if not transaction:
        await callback.message.edit_text("Транзакция не найдена или не принадлежит вам.", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=transactions_keyboard)
        await state.clear()
        await callback.answer()
        return

    trans_type, amount, category, date, comment = transaction
    display_type = "Доход" if trans_type == "income" else "Расход"
    comment_text = f", комментарий: {comment}" if comment else ""

    cursor.execute('DELETE FROM transactions WHERE id = ? AND user_id = ?', (trans_id, user_id))
    conn.commit()

    await callback.message.edit_text(f"{display_type} {amount:,} в категории '{category}' за {date}{comment_text} удален!", reply_markup=None)
    await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=transactions_keyboard)
    await state.clear()
    await callback.answer()

# Редактирование транзакции: выбор периода
@dp.message(lambda message: message.text == "Изменить транзакцию")
@restrict_access
async def edit_transaction_start(message: types.Message, state: FSMContext):
    period_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=name, callback_data=f"edit_period_{period}")] for period, (name, _) in PERIODS.items()
    ])
    await message.reply("Выберите период для отображения транзакций:", reply_markup=period_keyboard)
    await state.set_state(EditTransaction.select_period)

# Обработка выбора периода для редактирования
@dp.callback_query(lambda c: c.data.startswith("edit_period_"), StateFilter(EditTransaction.select_period))
async def process_edit_period(callback: types.CallbackQuery, state: FSMContext):
    period_key = callback.data.split("_")[2]
    period_name, delta = PERIODS[period_key]

    if period_key == "custom":
        await callback.message.edit_text("Выберите начальную дату периода:", reply_markup=get_calendar(prefix="edit_start_"))
        await state.set_state(EditTransaction.select_period)
        await state.update_data(period="custom", start_date=None, end_date=None)
        await callback.answer()
        return

    end_date = datetime.now()
    start_date = end_date - delta

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    user_id = callback.from_user.id
    cursor.execute('SELECT id, type, amount, category, date, comment FROM transactions WHERE user_id = ? AND date BETWEEN ? AND ? ORDER BY date DESC',
                   (user_id, start_date_str, end_date_str))
    transactions = cursor.fetchall()

    if not transactions:
        await callback.message.edit_text(f"За {period_name.lower()} нет транзакций для редактирования.", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=main_keyboard)
        await state.clear()
        await callback.answer()
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for trans in transactions:
        trans_id, trans_type, amount, category, date, comment = trans
        display_type = "Доход" if trans_type == "income" else "Расход"
        comment_text = f", комм.: {comment}" if comment else ""
        button_text = f"{date} | {display_type} | {category} | {amount:,}{comment_text}"
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=button_text, callback_data=f"edit_{trans_id}")])

    await callback.message.edit_text(f"Транзакции за {period_name.lower()} ({start_date_str} - {end_date_str}):\nВыберите транзакцию для редактирования:", reply_markup=keyboard)
    await state.set_state(EditTransaction.select_transaction)
    await callback.answer()

# Обработка выбора транзакции для редактирования
@dp.callback_query(lambda c: c.data.startswith("edit_"), StateFilter(EditTransaction.select_transaction))
async def process_edit_transaction(callback: types.CallbackQuery, state: FSMContext):
    trans_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id

    cursor.execute('SELECT type, amount, category, date, comment FROM transactions WHERE id = ? AND user_id = ?', (trans_id, user_id))
    transaction = cursor.fetchone()

    if not transaction:
        await callback.message.edit_text("Транзакция не найдена или не принадлежит вам.", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=main_keyboard)
        await state.clear()
        await callback.answer()
        return

    trans_type, amount, category, date, comment = transaction
    await state.update_data(trans_id=trans_id, trans_type=trans_type)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сумма", callback_data="edit_field_amount")],
        [InlineKeyboardButton(text="Категория", callback_data="edit_field_category")],
        [InlineKeyboardButton(text="Дата", callback_data="edit_field_date")],
        [InlineKeyboardButton(text="Комментарий", callback_data="edit_field_comment")]
    ])

    display_type = "Доход" if trans_type == "income" else "Расход"
    comment_text = f", комментарий: {comment}" if comment else ""
    await callback.message.edit_text(
        f"Транзакция для редактирования:\n{display_type} {amount:,} в категории '{category}' за {date}{comment_text}\n\nВыберите поле для редактирования:",
        reply_markup=keyboard
    )
    await state.set_state(EditTransaction.select_field)
    await callback.answer()

# Обработка выбора поля для редактирования
@dp.callback_query(lambda c: c.data.startswith("edit_field_"), StateFilter(EditTransaction.select_field))
async def process_edit_field(callback: types.CallbackQuery, state: FSMContext):
    field = callback.data.split("_")[2]
    data = await state.get_data()
    trans_id = data["trans_id"]

    if field == "amount":
        await callback.message.edit_text("Введите новую сумму:")
        await state.set_state(EditTransaction.edit_amount)
    elif field == "category":
        categories = INCOME_CATEGORIES if data["trans_type"] == "income" else EXPENSE_CATEGORIES
        category_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=cat, callback_data=f"edit_category_{cat}")] for cat in categories
        ])
        await callback.message.edit_text("Выберите новую категорию:", reply_markup=category_keyboard)
        await state.set_state(EditTransaction.edit_category)
    elif field == "date":
        await callback.message.edit_text("Выберите новую дату:", reply_markup=get_calendar(prefix="edit_date_"))
        await state.set_state(EditTransaction.edit_date)
    elif field == "comment":
        await callback.message.edit_text("Введите новый комментарий (или нажмите 'Пропустить'):", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Комментарий:", reply_markup=skip_comment_keyboard)
        await state.set_state(EditTransaction.edit_comment)
    
    await callback.answer()

# Обработка изменения суммы
@dp.message(StateFilter(EditTransaction.edit_amount))
async def process_edit_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text)
        data = await state.get_data()
        trans_id = data["trans_id"]
        user_id = message.from_user.id

        cursor.execute('UPDATE transactions SET amount = ? WHERE id = ? AND user_id = ?', (amount, trans_id, user_id))
        conn.commit()

        await message.reply(f"Сумма транзакции успешно изменена на {amount:,}!", reply_markup=transactions_keyboard)
        await state.clear()
    except ValueError:
        await message.reply("Пожалуйста, введите корректную сумму!")
        return

# Обработка изменения категории
@dp.callback_query(lambda c: c.data.startswith("edit_category_"), StateFilter(EditTransaction.edit_category))
async def process_edit_category(callback: types.CallbackQuery, state: FSMContext):
    category = callback.data.split("_")[2]
    data = await state.get_data()
    trans_id = data["trans_id"]
    user_id = callback.from_user.id

    cursor.execute('UPDATE transactions SET category = ? WHERE id = ? AND user_id = ?', (category, trans_id, user_id))
    conn.commit()

    await callback.message.edit_text(f"Категория транзакции успешно изменена на '{category}'!", reply_markup=None)
    await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=transactions_keyboard)
    await state.clear()
    await callback.answer()

# Обработка изменения даты
@dp.callback_query(lambda c: c.data.startswith("edit_date_"), StateFilter(EditTransaction.edit_date))
async def process_edit_date(callback: types.CallbackQuery, state: FSMContext):
    _, year, month, day = callback.data.split("_")[2:]
    date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    data = await state.get_data()
    trans_id = data["trans_id"]
    user_id = callback.from_user.id

    cursor.execute('UPDATE transactions SET date = ? WHERE id = ? AND user_id = ?', (date, trans_id, user_id))
    conn.commit()

    await callback.message.edit_text(f"Дата транзакции успешно изменена на {date}!", reply_markup=None)
    await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=transactions_keyboard)
    await state.clear()
    await callback.answer()

# Обработка изменения комментария
@dp.message(StateFilter(EditTransaction.edit_comment))
async def process_edit_comment(message: types.Message, state: FSMContext):
    comment = message.text if message.text != "Пропустить" else None
    data = await state.get_data()
    trans_id = data["trans_id"]
    user_id = message.from_user.id

    cursor.execute('UPDATE transactions SET comment = ? WHERE id = ? AND user_id = ?', (comment, trans_id, user_id))
    conn.commit()

    comment_text = f" на '{comment}'" if comment else " удален"
    await message.reply(f"Комментарий транзакции успешно изменен{comment_text}!", reply_markup=transactions_keyboard)
    await state.clear()

# Обработка смены месяца в календаре для редактирования даты
@dp.callback_query(lambda c: c.data.startswith(("edit_date_prev_", "edit_date_next_")), StateFilter(EditTransaction.edit_date))
async def edit_change_month(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    action = parts[2]
    year = int(parts[3])
    month = int(parts[4])

    if action == "prev":
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    elif action == "next":
        month += 1
        if month > 12:
            month = 1
            year += 1

    await callback.message.edit_reply_markup(reply_markup=get_calendar(year, month, prefix="edit_date_"))
    await callback.answer()

# Обработка выбора начальной даты для ручного периода редактирования
@dp.callback_query(lambda c: c.data.startswith("edit_start_date_"), StateFilter(EditTransaction.select_period))
async def edit_select_start_date(callback: types.CallbackQuery, state: FSMContext):
    _, year, month, day = callback.data.split("_")[2:]
    start_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    await state.update_data(start_date=start_date)
    await callback.message.edit_text("Выберите конечную дату периода:", reply_markup=get_calendar(prefix="edit_end_"))
    await state.set_state(EditTransaction.select_period)
    await callback.answer()

# Обработка выбора конечной даты для ручного периода редактирования
@dp.callback_query(lambda c: c.data.startswith("edit_end_date_"), StateFilter(EditTransaction.select_period))
async def edit_select_end_date(callback: types.CallbackQuery, state: FSMContext):
    _, year, month, day = callback.data.split("_")[2:]
    end_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    data = await state.get_data()
    start_date = data["start_date"]
    
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    if start_dt > end_dt:
        await callback.message.edit_text("Ошибка: начальная дата не может быть позже конечной. Попробуйте снова.", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Выберите период для редактирования:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=name, callback_data=f"edit_period_{period}")] for period, (name, _) in PERIODS.items()
        ]))
        await state.set_state(EditTransaction.select_period)
        await callback.answer()
        return

    user_id = callback.from_user.id
    cursor.execute('SELECT id, type, amount, category, date, comment FROM transactions WHERE user_id = ? AND date BETWEEN ? AND ? ORDER BY date DESC',
                   (user_id, start_date, end_date))
    transactions = cursor.fetchall()

    if not transactions:
        await callback.message.edit_text(f"За выбранный период нет транзакций для редактирования.", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=main_keyboard)
        await state.clear()
        await callback.answer()
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for trans in transactions:
        trans_id, trans_type, amount, category, date, comment = trans
        display_type = "Доход" if trans_type == "income" else "Расход"
        comment_text = f", комм.: {comment}" if comment else ""
        button_text = f"{date} | {display_type} | {category} | {amount:,}{comment_text}"
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=button_text, callback_data=f"edit_{trans_id}")])

    await callback.message.edit_text(f"Транзакции за период {start_date} - {end_date}:\nВыберите транзакцию для редактирования:", reply_markup=keyboard)
    await state.set_state(EditTransaction.select_transaction)
    await callback.answer()

# Обработка смены месяца в календаре для начальной даты редактирования
@dp.callback_query(lambda c: c.data.startswith(("edit_start_prev_", "edit_start_next_")), StateFilter(EditTransaction.select_period))
async def edit_change_start_month(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    action = parts[2]
    year = int(parts[3])
    month = int(parts[4])

    if action == "prev":
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    elif action == "next":
        month += 1
        if month > 12:
            month = 1
            year += 1

    await callback.message.edit_reply_markup(reply_markup=get_calendar(year, month, prefix="edit_start_"))
    await callback.answer()

# Обработка смены месяца в календаре для конечной даты редактирования
@dp.callback_query(lambda c: c.data.startswith(("edit_end_prev_", "edit_end_next_")), StateFilter(EditTransaction.select_period))
async def edit_change_end_month(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    action = parts[2]
    year = int(parts[3])
    month = int(parts[4])

    if action == "prev":
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    elif action == "next":
        month += 1
        if month > 12:
            month = 1
            year += 1

    await callback.message.edit_reply_markup(reply_markup=get_calendar(year, month, prefix="edit_end_"))
    await callback.answer()

# Отчет: выбор периода
@dp.message(lambda message: message.text == "Отчет")
@restrict_access
async def report_start(message: types.Message, state: FSMContext):
    period_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=name, callback_data=f"report_period_{period}")] for period, (name, _) in PERIODS.items()
    ])
    await message.reply("Выберите период для отчета:", reply_markup=period_keyboard)
    await state.set_state(ReportForm.period)

# Отчет: выбор начальной даты для ручного периода
@dp.callback_query(lambda c: c.data.startswith("report_period_"), StateFilter(ReportForm.period))
@restrict_access_callback
async def report_select_period(callback: types.CallbackQuery, state: FSMContext):
    period_key = callback.data.split("_")[2]
    await state.update_data(period=period_key)

    if period_key == "custom":
        await callback.message.edit_text("Выберите начальную дату периода:", reply_markup=get_calendar(prefix="report_start_"))
        await state.set_state(ReportForm.start_date)
    else:
        type_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=name, callback_data=f"report_type_{type_key}")] for type_key, name in REPORT_TYPES.items()
        ])
        await callback.message.edit_text(
            f"Вы выбрали период: {PERIODS[period_key][0].lower()}. Теперь выберите тип отчета:", reply_markup=type_keyboard)
        await state.set_state(ReportForm.report_type)
    await callback.answer()

# Отчет: обработка начальной даты
@dp.callback_query(lambda c: c.data.startswith("report_start_date_"), StateFilter(ReportForm.start_date))
async def report_select_start_date(callback: types.CallbackQuery, state: FSMContext):
    _, year, month, day = callback.data.split("_")[2:]
    start_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    await state.update_data(start_date=start_date)
    await callback.message.edit_text("Выберите конечную дату периода:", reply_markup=get_calendar(prefix="report_end_"))
    await state.set_state(ReportForm.end_date)
    await callback.answer()

# Отчет: обработка конечной даты
@dp.callback_query(lambda c: c.data.startswith("report_end_date_"), StateFilter(ReportForm.end_date))
async def report_select_end_date(callback: types.CallbackQuery, state: FSMContext):
    _, year, month, day = callback.data.split("_")[2:]
    end_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    await state.update_data(end_date=end_date)

    data = await state.get_data()
    start_date = data["start_date"]
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    if start_dt > end_dt:
        await callback.message.edit_text("Ошибка: начальная дата не может быть позже конечной. Попробуйте снова.", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Выберите период для отчета:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=name, callback_data=f"report_period_{period}")] for period, (name, _) in PERIODS.items()
        ]))
        await state.set_state(ReportForm.period)
        await callback.answer()
        return

    type_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=name, callback_data=f"report_type_{type_key}")] for type_key, name in REPORT_TYPES.items()
    ])
    await callback.message.edit_text(
        f"Вы выбрали период: {start_date} - {end_date}. Теперь выберите тип отчета:", reply_markup=type_keyboard)
    await state.set_state(ReportForm.report_type)
    await callback.answer()

# Отчет: смена месяца в календаре для начальной даты
@dp.callback_query(lambda c: c.data.startswith(("report_start_prev_", "report_start_next_")), StateFilter(ReportForm.start_date))
async def report_change_start_month(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    action = parts[2]
    year = int(parts[3])
    month = int(parts[4])

    if action == "prev":
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    elif action == "next":
        month += 1
        if month > 12:
            month = 1
            year += 1

    await callback.message.edit_reply_markup(reply_markup=get_calendar(year, month, prefix="report_start_"))
    await callback.answer()

# Отчет: смена месяца в календаре для конечной даты
@dp.callback_query(lambda c: c.data.startswith(("report_end_prev_", "report_end_next_")), StateFilter(ReportForm.end_date))
async def report_change_end_month(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    action = parts[2]
    year = int(parts[3])
    month = int(parts[4])

    if action == "prev":
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    elif action == "next":
        month += 1
        if month > 12:
            month = 1
            year += 1

    await callback.message.edit_reply_markup(reply_markup=get_calendar(year, month, prefix="report_end_"))
    await callback.answer()

# Отчет: выбор категории после типа
@dp.callback_query(lambda c: c.data.startswith("report_type_"), StateFilter(ReportForm.report_type))
async def report_select_category(callback: types.CallbackQuery, state: FSMContext):
    report_type = callback.data.split("_")[2]
    await state.update_data(report_type=report_type)

    # Определяем категории в зависимости от типа отчёта
    categories = INCOME_CATEGORIES if report_type == "income" else EXPENSE_CATEGORIES
    category_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=cat, callback_data=f"report_category_{cat}")] for cat in categories
    ])
    # Добавляем кнопку "Все категории"
    category_keyboard.inline_keyboard.append([InlineKeyboardButton(text="Все категории", callback_data="report_category_all")])

    data = await state.get_data()
    period_key = data["period"]
    if period_key == "custom":
        period_display = f"{data['start_date']} - {data['end_date']}"
    else:
        period_display = PERIODS[period_key][0].lower()
    await callback.message.edit_text(
        f"Вы выбрали период: {period_display}, тип отчета: {REPORT_TYPES[report_type]}. Теперь выберите категорию:",
        reply_markup=category_keyboard
    )
    await state.set_state(ReportForm.category)
    await callback.answer()

# Генерация детализированного отчета
@dp.callback_query(lambda c: c.data.startswith("report_category_"), StateFilter(ReportForm.category))
async def generate_report(callback: types.CallbackQuery, state: FSMContext):
    category_choice = callback.data.split("_")[2]  # "all" или название категории
    data = await state.get_data()
    period_key = data["period"]
    report_type = data["report_type"]

    if period_key == "custom":
        start_date_str = data["start_date"]
        end_date_str = data["end_date"]
        period_name = "выбранный период"
    else:
        period_name, delta = PERIODS[period_key]
        end_date = datetime.now()
        start_date = end_date - delta
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

    # Формируем запрос с учётом категории
    query = 'SELECT type, amount, category, date, comment FROM transactions WHERE user_id = ? AND date BETWEEN ? AND ?'
    params = [callback.from_user.id, start_date_str, end_date_str]

    if category_choice != "all":
        query += ' AND category = ?'
        params.append(category_choice)

    cursor.execute(query, params)
    transactions = cursor.fetchall()

    if not transactions:
        await callback.message.edit_text(f"За {period_name.lower()} нет данных.", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=data_output_keyboard)
        await state.clear()
        await callback.answer()
        return

    incomes = [(t[3], t[2], t[1], t[4]) for t in transactions if t[0] == "income"]
    expenses = [(t[3], t[2], t[1], t[4]) for t in transactions if t[0] == "expense"]

    incomes.sort(key=lambda x: x[0])
    expenses.sort(key=lambda x: x[0])

    total_income = sum(t[1] for t in transactions if t[0] == "income")
    total_expense = sum(t[1] for t in transactions if t[0] == "expense")

    # Добавляем информацию о категории в заголовок отчёта
    category_text = f" (категория: {category_choice})" if category_choice != "all" else " (все категории)"
    report_header = f"Отчет за {period_name.lower()} ({start_date_str} - {end_date_str}){category_text}:\n\n"

    # Формируем части отчета
    report_parts = []
    current_part = report_header

    if report_type == "income":
        current_part += f"Доходы: {total_income:,.2f}\n"
        if incomes:
            current_part += "Детализация доходов:\n"
            for date, category, amount, comment in incomes:
                comment_text = f", комм.: {comment}" if comment else ""
                transaction_line = f"{date} | {category} | {amount:,.2f}{comment_text}\n"
                # Проверяем, не превысит ли добавление строки лимит
                if len(current_part) + len(transaction_line) > 4000:  # Оставляем запас
                    report_parts.append(current_part)
                    current_part = "Детализация доходов (продолжение):\n"
                current_part += transaction_line
        else:
            current_part += "Нет доходов за этот период.\n"
    elif report_type == "expense":
        current_part += f"Расходы: {total_expense:,.2f}\n"
        if expenses:
            current_part += "Детализация расходов:\n"
            for date, category, amount, comment in expenses:
                comment_text = f", комм.: {comment}" if comment else ""
                transaction_line = f"{date} | {category} | {amount:,.2f}{comment_text}\n"
                if len(current_part) + len(transaction_line) > 4000:
                    report_parts.append(current_part)
                    current_part = "Детализация расходов (продолжение):\n"
                current_part += transaction_line
        else:
            current_part += "Нет расходов за этот период.\n"
    else:  # balance
        current_part += f"Доходы: {total_income:,.2f}\n"
        if incomes:
            current_part += "Детализация доходов:\n"
            for date, category, amount, comment in incomes:
                comment_text = f", комм.: {comment}" if comment else ""
                transaction_line = f"{date} | {category} | {amount:,.2f}{comment_text}\n"
                if len(current_part) + len(transaction_line) > 4000:
                    report_parts.append(current_part)
                    current_part = "Детализация доходов (продолжение):\n"
                current_part += transaction_line
        else:
            current_part += "Нет доходов за этот период.\n"
        current_part += f"\nРасходы: {total_expense:,.2f}\n"
        if expenses:
            current_part += "Детализация расходов:\n"
            for date, category, amount, comment in expenses:
                comment_text = f", комм.: {comment}" if comment else ""
                transaction_line = f"{date} | {category} | {amount:,.2f}{comment_text}\n"
                if len(current_part) + len(transaction_line) > 4000:
                    report_parts.append(current_part)
                    current_part = "Детализация расходов (продолжение):\n"
                current_part += transaction_line
        else:
            current_part += "Нет расходов за этот период.\n"
        current_part += f"\nБаланс: {(total_income - total_expense):,.2f}"

    # Добавляем последнюю часть отчета
    report_parts.append(current_part)

    # Удаляем исходное сообщение
    await callback.message.delete()

    # Отправляем все части отчета
    for i, part in enumerate(report_parts):
        if i == len(report_parts) - 1:
            # Последняя часть — добавляем клавиатуру
            await bot.send_message(callback.from_user.id, part, reply_markup=data_output_keyboard)
        else:
            await bot.send_message(callback.from_user.id, part)

    await state.clear()
    await callback.answer()

# Диаграмма: выбор периода
@dp.message(lambda message: message.text == "Визуализация")
@restrict_access
async def chart_start(message: types.Message, state: FSMContext):
    period_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=name, callback_data=f"chart_period_{period}")] for period, (name, _) in PERIODS.items()
    ])
    await message.reply("Выберите период для диаграммы:", reply_markup=period_keyboard)
    await state.set_state(ChartForm.period)

# Диаграмма: выбор начальной даты для ручного периода
@dp.callback_query(lambda c: c.data.startswith("chart_period_"), StateFilter(ChartForm.period))
async def chart_select_period(callback: types.CallbackQuery, state: FSMContext):
    period_key = callback.data.split("_")[2]
    await state.update_data(period=period_key)

    if period_key == "custom":
        await callback.message.edit_text("Выберите начальную дату периода:", reply_markup=get_calendar(prefix="chart_start_"))
        await state.set_state(ChartForm.start_date)
    else:
        # Создаем клавиатуру только с опциями доходов и расходов
        type_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=name, callback_data=f"chart_type_{type_key}")] 
            for type_key, name in REPORT_TYPES.items() 
            if type_key != "balance"  # Исключаем опцию "balance" (Разница)
        ])
        await callback.message.edit_text(
            f"Вы выбрали период: {PERIODS[period_key][0].lower()}. Теперь выберите тип диаграммы:", reply_markup=type_keyboard)
        await state.set_state(ChartForm.chart_type)
    await callback.answer()

# Диаграмма: обработка начальной даты
@dp.callback_query(lambda c: c.data.startswith("chart_start_date_"), StateFilter(ChartForm.start_date))
async def chart_select_start_date(callback: types.CallbackQuery, state: FSMContext):
    _, year, month, day = callback.data.split("_")[2:]
    start_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    await state.update_data(start_date=start_date)
    await callback.message.edit_text("Выберите конечную дату периода:", reply_markup=get_calendar(prefix="chart_end_"))
    await state.set_state(ChartForm.end_date)
    await callback.answer()

# Диаграмма: обработка конечной даты
@dp.callback_query(lambda c: c.data.startswith("chart_end_date_"), StateFilter(ChartForm.end_date))
async def chart_select_end_date(callback: types.CallbackQuery, state: FSMContext):
    _, year, month, day = callback.data.split("_")[2:]
    end_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    await state.update_data(end_date=end_date)

    data = await state.get_data()
    start_date = data["start_date"]
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    if start_dt > end_dt:
        await callback.message.edit_text("Ошибка: начальная дата не может быть позже конечной. Попробуйте снова.", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Выберите период для диаграммы:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=name, callback_data=f"chart_period_{period}")] for period, (name, _) in PERIODS.items()
        ]))
        await state.set_state(ChartForm.period)
        await callback.answer()
        return

    # Создаем клавиатуру только с опциями доходов и расходов
    type_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=name, callback_data=f"chart_type_{type_key}")] 
        for type_key, name in REPORT_TYPES.items() 
        if type_key != "balance"  # Исключаем опцию "balance" (Разница)
    ])
    await callback.message.edit_text(
        f"Вы выбрали период: {start_date} - {end_date}. Теперь выберите тип диаграммы:", reply_markup=type_keyboard)
    await state.set_state(ChartForm.chart_type)
    await callback.answer()

# Диаграмма: смена месяца в календаре для начальной даты
@dp.callback_query(lambda c: c.data.startswith(("chart_start_prev_", "chart_start_next_")), StateFilter(ChartForm.start_date))
async def chart_change_start_month(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    action = parts[2]
    year = int(parts[3])
    month = int(parts[4])

    if action == "prev":
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    elif action == "next":
        month += 1
        if month > 12:
            month = 1
            year += 1

    await callback.message.edit_reply_markup(reply_markup=get_calendar(year, month, prefix="chart_start_"))
    await callback.answer()

# Диаграмма: смена месяца в календаре для конечной даты
@dp.callback_query(lambda c: c.data.startswith(("chart_end_prev_", "chart_end_next_")), StateFilter(ChartForm.end_date))
async def chart_change_end_month(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    action = parts[2]
    year = int(parts[3])
    month = int(parts[4])

    if action == "prev":
        month -= 1
        if month < 1:
            month = 12
            year -= 1
    elif action == "next":
        month += 1
        if month > 12:
            month = 1
            year += 1

    await callback.message.edit_reply_markup(reply_markup=get_calendar(year, month, prefix="chart_end_"))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("chart_type_"), StateFilter(ChartForm.chart_type))
async def chart_select_interval(callback: types.CallbackQuery, state: FSMContext):
    chart_type = callback.data.split("_")[2]
    await state.update_data(chart_type=chart_type)
    
    # Создаем клавиатуру для выбора интервала разбиения
    interval_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=name, callback_data=f"interval_{interval_key}")] 
        for interval_key, name in INTERVALS.items()
    ])
    
    data = await state.get_data()
    period_key = data["period"]
    if period_key == "custom":
        period_display = f"{data['start_date']} - {data['end_date']}"
    else:
        period_display = PERIODS[period_key][0].lower()
    
    await callback.message.edit_text(
        f"Вы выбрали период: {period_display}, тип данных: {REPORT_TYPES[chart_type]}. Теперь выберите интервал разбиения:",
        reply_markup=interval_keyboard
    )
    await state.set_state(ChartForm.interval)
    await callback.answer()

# Диаграмма: выбор типа данных после периода
@dp.callback_query(lambda c: c.data.startswith("interval_"), StateFilter(ChartForm.interval))
async def chart_select_diagram_type(callback: types.CallbackQuery, state: FSMContext):
    interval = callback.data.split("_")[1]
    await state.update_data(interval=interval)
    
    # Если выбрано разбиение, то круговая диаграмма не имеет смысла, поэтому можно ограничить выбор
    diagram_types = DIAGRAM_TYPES.copy()
    if interval != "none":
        # Удаляем круговую диаграмму, если выбрано разбиение
        diagram_types.pop("pie", None)
    
    diagram_type_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=name, callback_data=f"diagram_type_{type_key}")] 
        for type_key, name in diagram_types.items()
    ])
    
    data = await state.get_data()
    period_key = data["period"]
    if period_key == "custom":
        period_display = f"{data['start_date']} - {data['end_date']}"
    else:
        period_display = PERIODS[period_key][0].lower()
    
    await callback.message.edit_text(
        f"Вы выбрали период: {period_display}, тип данных: {REPORT_TYPES[data['chart_type']]}, интервал: {INTERVALS[interval]}. Теперь выберите тип диаграммы:",
        reply_markup=diagram_type_keyboard
    )
    await state.set_state(ChartForm.diagram_type)
    await callback.answer()

# Генерация диаграммы (круговая или столбчатая)
@dp.callback_query(lambda c: c.data.startswith("diagram_type_"), StateFilter(ChartForm.diagram_type))
async def generate_chart(callback: types.CallbackQuery, state: FSMContext):
    diagram_type = callback.data.split("_")[2]  # "pie" или "bar"
    data = await state.get_data()
    period_key = data["period"]
    chart_type = data["chart_type"]
    interval = data["interval"]

    if period_key == "custom":
        start_date_str = data["start_date"]
        end_date_str = data["end_date"]
        period_name = "выбранный период"
    else:
        period_name, delta = PERIODS[period_key]
        end_date = datetime.now()
        start_date = end_date - delta
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

    cursor.execute('SELECT category, amount, type, date FROM transactions WHERE user_id = ? AND date BETWEEN ? AND ?',
                   (callback.from_user.id, start_date_str, end_date_str))
    transactions = cursor.fetchall()

    if not transactions:
        await callback.message.edit_text(f"За {period_name.lower()} нет данных.", reply_markup=None)
        await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=data_output_keyboard)
        await state.clear()
        await callback.answer()
        return

    # Функция для получения ключа периода
    def get_period_key(date_str, interval_type):
        date = datetime.strptime(date_str, '%Y-%m-%d')
        if interval_type == "day":
            return date.strftime('%Y-%m-%d')
        elif interval_type == "week":
            # Начало недели (понедельник)
            start_of_week = date - timedelta(days=date.weekday())
            return start_of_week.strftime('%Y-%m-%d')
        elif interval_type == "month":
            return date.strftime('%Y-%m')
        return None

    # Если выбрано разбиение по периодам
    if interval != "none" and diagram_type == "bar":
        # Группируем данные по периодам
        periods = {}
        for t in transactions:
            category, amount, t_type, date = t
            if (chart_type == "income" and t_type == "income") or (chart_type == "expense" and t_type == "expense"):
                period_key = get_period_key(date, interval)
                if period_key:
                    if period_key not in periods:
                        periods[period_key] = 0
                    periods[period_key] += amount

        if not periods:
            await callback.message.edit_text(f"За {period_name.lower()} нет данных для отображения.", reply_markup=None)
            await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=data_output_keyboard)
            await state.clear()
            await callback.answer()
            return

        # Сортируем периоды
        sorted_periods = dict(sorted(periods.items()))
        period_names = list(sorted_periods.keys())
        period_values = list(sorted_periods.values())

        # Общая сумма
        total_amount = sum(period_values)

        # Создаем столбчатую диаграмму
        fig, ax = plt.subplots(figsize=(12, 6))

        # Столбцы
        bars = ax.bar(period_names, period_values, color='skyblue', edgecolor='black')

        # Добавляем значения над столбцами
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2, height,
                f"{height:,.2f}",
                ha='center', va='bottom', fontsize=8
            )

        # Заголовок
        ax.set_title(
            f"{REPORT_TYPES[chart_type]} за {period_name.lower()} ({start_date_str} - {end_date_str})\nОбщая сумма: {total_amount:,.2f}",
            fontsize=14,
            loc='left',
            pad=20
        )

        # Настройка осей
        ax.set_ylabel("Сумма", fontsize=12)
        ax.set_xlabel("Периоды", fontsize=12)
        ax.tick_params(axis='x', rotation=45, labelsize=8)
        ax.tick_params(axis='y', labelsize=10)

        # Добавляем сетку для лучшей читаемости
        ax.grid(True, axis='y', linestyle='--', alpha=0.7)

        # Настройка отступов
        plt.tight_layout()

    else:
        # Оригинальная логика для отображения по категориям (без разбиения или для круговой диаграммы)
        categories = {}
        for t in transactions:
            category, amount, t_type, _ = t
            if (chart_type == "income" and t_type == "income") or (chart_type == "expense" and t_type == "expense"):
                categories[category] = categories.get(category, 0) + amount

        if not categories:
            await callback.message.edit_text(f"За {period_name.lower()} нет данных для отображения.", reply_markup=None)
            await bot.send_message(callback.from_user.id, "Выберите следующее действие:", reply_markup=main_keyboard)
            await state.clear()
            await callback.answer()
            return

        # Общая сумма
        total_amount = sum(categories.values())

        # Сортируем категории по убыванию суммы
        sorted_categories = dict(sorted(categories.items(), key=lambda x: x[1], reverse=True))
        category_names = list(sorted_categories.keys())
        category_values = list(sorted_categories.values())

        # Создаем фиксированные цвета для каждой категории
        def create_category_colors(categories):
            category_colors = {
                # Цвета для доходов
                "Зарплата Илья": (0.0, 0.8, 0.0),    # Яркий зеленый (ранее использовался для "Зарплата")
                "Зарплата Мария": (0.2, 0.6, 0.2),   # Темно-зеленый (новый цвет для отличия)
                "Подарок": (0.0, 0.0, 1.0),          # Чистый синий
                "Инвестиции": (0.8, 0.0, 0.8),       # Фиолетовый
                # Цвета для расходов
                "Кредиты": (1.0, 0.0, 0.0),          # Чистый красный
                "Кредитная карта": (0.9, 0.1, 0.1),  # Темно-красный
                "Страхование": (0.0, 0.8, 0.8),      # Бирюзовый
                "Связь": (1.0, 0.8, 0.0),           # Золотой
                "Еда": (1.0, 0.4, 0.0),             # Оранжевый
                "Транспорт": (0.5, 0.0, 1.0),       # Фиолетово-синий
                "Жилье": (0.0, 0.5, 1.0),           # Яркий голубой
                "Развлечения": (1.0, 0.0, 0.5),      # Малиновый
                "Маркетплейсы": (0.5, 1.0, 0.0),     # Ярко-лаймовый
                "Путешествия": (0.0, 1.0, 1.0),      # Циан
                "Сигареты": (0.7, 0.3, 0.0),        # Коричневый
                "Здоровье": (1.0, 0.6, 0.8),        # Розовый
                "Другое": (0.3, 0.3, 0.3)           # Темно-серый
            }
            
            return [category_colors.get(cat, (0.8, 0.2, 0.6)) for cat in categories]

        colors = create_category_colors(category_names)

        if diagram_type == "pie":
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6), gridspec_kw={'width_ratios': [2, 1]})

            explode = []
            for amount in category_values:
                if amount < 0.1 * total_amount:
                    explode.append(0.15)
                else:
                    explode.append(0.05)

            def autopct_func(pct):
                if pct > 5:
                    return f"{pct:.1f}%\n({int(pct / 100. * total_amount):,})"
                return ""

            wedges, texts, autotexts = ax1.pie(
                category_values,
                autopct=autopct_func,
                startangle=90,
                textprops={'fontsize': 7},
                explode=explode,
                colors=colors,
                pctdistance=0.65,
                wedgeprops={'edgecolor': 'white', 'linewidth': 1}
            )

            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontsize(6)

            ax1.set_title(
                f"{REPORT_TYPES[chart_type]} за {period_name.lower()} ({start_date_str} - {end_date_str})\nОбщая сумма: {total_amount:,.2f}",
                fontsize=14,
                loc='left',
                y=1.05,
                pad=10
            )

            ax2.axis('off')
            legend_labels = [f"{cat}: {amt:,.2f}" for cat, amt in sorted_categories.items()]
            ax2.legend(
                wedges,
                legend_labels,
                title="Категории",
                loc="center left",
                bbox_to_anchor=(0, 0.5),
                fontsize=10,
                title_fontsize=12,
                labelspacing=0.8
            )

            plt.tight_layout()
            plt.subplots_adjust(wspace=0.1)

        else:  # diagram_type == "bar"
            fig, ax = plt.subplots(figsize=(12, 6))

            bars = ax.bar(category_names, category_values, color=colors, edgecolor='black')

            for bar in bars:
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width() / 2, height,
                    f"{height:,.2f}",
                    ha='center', va='bottom', fontsize=8
                )

            ax.set_title(
                f"{REPORT_TYPES[chart_type]} за {period_name.lower()} ({start_date_str} - {end_date_str})\nОбщая сумма: {total_amount:,.2f}",
                fontsize=14,
                loc='left',
                pad=20
            )

            ax.set_ylabel("Сумма", fontsize=12)
            ax.set_xlabel("Категории", fontsize=12)
            ax.tick_params(axis='x', rotation=45, labelsize=10)
            ax.tick_params(axis='y', labelsize=10)

            ax.grid(True, axis='y', linestyle='--', alpha=0.7)

            plt.tight_layout()

    buffer = BytesIO()
    plt.savefig(buffer, format="png", bbox_inches="tight", dpi=150)
    buffer.seek(0)

    photo = BufferedInputFile(buffer.getvalue(), filename="chart.png")
    plt.close()

    await callback.message.delete()
    await bot.send_message(callback.from_user.id, "Диаграмма построена:")
    await bot.send_photo(callback.from_user.id, photo, caption=f"Диаграмма: {REPORT_TYPES[chart_type]} за {period_name.lower()}", reply_markup=main_keyboard)
    buffer.close()
    await state.clear()
    await callback.answer()

# Новая функция: Управление целями
@dp.message(lambda message: message.text == "Управление целями")
@restrict_access
async def goals_start(message: types.Message, state: FSMContext):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Создать цель", callback_data="goal_create")],
        [InlineKeyboardButton(text="Внести сумму", callback_data="goal_contribute")],
        [InlineKeyboardButton(text="Посмотреть прогресс", callback_data="goal_progress")]
    ])
    await message.reply("Выберите действие:", reply_markup=keyboard)

# Создание цели
@dp.callback_query(lambda c: c.data == "goal_create")
@restrict_access_callback
async def goal_create(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Введите название цели:")
    await state.set_state(GoalForm.name)
    await callback.answer()

@dp.message(StateFilter(GoalForm.name))
async def process_goal_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.reply("Введите целевую сумму:")
    await state.set_state(GoalForm.target)

@dp.message(StateFilter(GoalForm.target))
async def process_goal_target(message: types.Message, state: FSMContext):
    try:
        target = float(message.text)
        data = await state.get_data()
        user_id = message.from_user.id
        name = data["name"]

        cursor.execute('INSERT INTO goals (user_id, goal_name, target_amount) VALUES (?, ?, ?)',
                       (user_id, name, target))
        conn.commit()

        await message.reply(f"Цель '{name}' на сумму {target:,} создана!", reply_markup=main_keyboard)
        await state.clear()
    except ValueError:
        await message.reply("Пожалуйста, введите корректную сумму!")
        return

# Внесение суммы в цель
@dp.callback_query(lambda c: c.data == "goal_contribute")
async def goal_contribute(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    cursor.execute('SELECT id, goal_name FROM goals WHERE user_id = ?', (user_id,))
    goals = cursor.fetchall()

    if not goals:
        await callback.message.edit_text("У вас нет целей для внесения средств.", reply_markup=None)
        await bot.send_message(user_id, "Выберите действие:", reply_markup=main_keyboard)
        await callback.answer()
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=name, callback_data=f"contribute_{id}")] for id, name in goals
    ])
    await callback.message.edit_text("Выберите цель для внесения суммы:", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("contribute_"))
async def process_contribute_goal(callback: types.CallbackQuery, state: FSMContext):
    goal_id = int(callback.data.split("_")[1])
    await state.update_data(goal_id=goal_id)
    await callback.message.edit_text("Введите сумму для внесения:")
    await state.set_state(GoalForm.contribute)
    await callback.answer()

@dp.message(StateFilter(GoalForm.contribute))
async def process_contribute_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text)
        data = await state.get_data()
        user_id = message.from_user.id
        goal_id = data["goal_id"]

        cursor.execute('UPDATE goals SET current_amount = current_amount + ? WHERE id = ? AND user_id = ?', (amount, goal_id, user_id))
        conn.commit()

        cursor.execute('SELECT goal_name, target_amount, current_amount FROM goals WHERE id = ? AND user_id = ?', (goal_id, user_id))
        goal = cursor.fetchone()
        name, target, current = goal

        await message.reply(f"Сумма {amount:,} внесена в цель '{name}'. Прогресс: {current:,}/{target:,} ({current/target*100:.1f}%)", reply_markup=main_keyboard)
        await state.clear()
    except ValueError:
        await message.reply("Пожалуйста, введите корректную сумму!")
        return

# Прогресс целей
@dp.callback_query(lambda c: c.data == "goal_progress")
async def goal_progress(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    cursor.execute('SELECT goal_name, target_amount, current_amount FROM goals WHERE user_id = ?', (user_id,))
    goals = cursor.fetchall()

    if not goals:
        await callback.message.edit_text("У вас нет активных целей.", reply_markup=None)
        await bot.send_message(user_id, "Выберите действие:", reply_markup=main_keyboard)
        await callback.answer()
        return

    response = "Прогресс ваших целей:\n\n"
    for name, target, current in goals:
        percent = current / target * 100
        response += f"**{name}**: {current:,}/{target:,} ({percent:.1f}%)\n"

    await callback.message.edit_text(response, reply_markup=None)
    await bot.send_message(user_id, "Выберите действие:", reply_markup=main_keyboard)
    await callback.answer()

# Проверка подписок и отправка уведомлений
# Проверка подписок и отправка уведомлений
async def check_subscriptions():
    while True:
        cursor.execute('SELECT user_id, subscription_end FROM allowed_users WHERE has_access = 1')
        users = cursor.fetchall()

        for user_id, subscription_end in users:
            if subscription_end:
                try:
                    subscription_end_date = datetime.strptime(subscription_end, '%Y-%m-%d')
                    days_left = (subscription_end_date - datetime.now()).days
                    if days_left == 7:
                        try:
                            await bot.send_message(user_id, f"Напоминание: ваша подписка истекает через 7 дней ({subscription_end}). "
                                                            f"Продлите её, нажав на кнопку 'Оплатить подписку' в главном меню.")
                        except Exception as e:
                            print(f"Не удалось отправить уведомление за 7 дней пользователю {user_id}: {e}")
                    elif days_left == 1:
                        try:
                            await bot.send_message(user_id, f"Напоминание: ваша подписка истекает завтра ({subscription_end}). "
                                                            f"Продлите её, нажав на кнопку 'Оплатить подписку' в главном меню.")
                        except Exception as e:
                            print(f"Не удалось отправить уведомление за 1 день пользователю {user_id}: {e}")
                except ValueError as e:
                    print(f"Ошибка формата даты для пользователя {user_id}: {e}")

        # Проверяем раз в день (каждые 24 часа)
        await asyncio.sleep(24 * 60 * 60)

# Запуск проверки подписок
import asyncio
loop = asyncio.get_event_loop()
loop.create_task(check_subscriptions())

# Запуск бота
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
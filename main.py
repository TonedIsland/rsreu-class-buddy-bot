"""
Telegram Bot для расписания РГРТУ
ФИНАЛЬНАЯ ВЕРСИЯ С РАССЫЛКАМИ, КЛИКАБЕЛЬНЫМИ ID И ПРОВЕРКОЙ ВСЕХ СООБЩЕНИЙ
"""

import asyncio
import aiohttp
import aiosqlite
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from datetime import datetime, time, timedelta, date
import re
import logging
import json
from typing import Optional, Dict, List, Tuple, Any
import pytz
from urllib.parse import urlencode
import os
from dotenv import load_dotenv
from pathlib import Path
from aiohttp import web

# ==================== ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ====================
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

BOT_TOKEN = os.getenv('BOT_TOKEN')
BETA_TESTER_ID = int(os.getenv('BETA_TESTER_ID', '0'))
BROADCAST_MODE = os.getenv('BROADCAST_MODE', 'beta')
SPECIFIC_USER_ID = int(os.getenv('SPECIFIC_USER_ID', '123456789'))

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения! Проверь файл .env")

# ==================== СОСТОЯНИЯ FSM ====================
class Form(StatesGroup):
    waiting_for_group = State()

class BroadcastStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_media = State()

# ==================== НАСТРОЙКИ ====================
LOCAL_TIMEZONE = pytz.timezone('Europe/Moscow')
BASE_URL = "https://rasp.rsreu.ru"
SCHEDULE_URL = f"{BASE_URL}/schedule-frame/group"

# ==================== КАСТОМНЫЕ ЭМОДЗИ ====================
CUSTOM_EMOJI = {
    'search': {'id': '5190595516269865314', 'fallback': '🔍'},
    'error': {'id': '5019523782004441717', 'fallback': '❌'},
    'success': {'id': '5021905410089550576', 'fallback': '✅'},
    'welcome': {'id': '5195448447062251797', 'fallback': '👋'},
    'beta': {'id': '5206621104403129406', 'fallback': '🔬'},
    'faculty': {'id': '5204128352629169390', 'fallback': '🎓'},
    'group': {'id': '5253675142600490236', 'fallback': '👥'},
    'calendar': {'id': '5274055917766202507', 'fallback': '📅'},
    'reminder': {'id': '5382146496416196771', 'fallback': '⏰'},
    'stats': {'id': '5303026378415820622', 'fallback': '📊'},
    'test': {'id': '5240374792820890829', 'fallback': '🧪'},
    'broadcast': {'id': '5424818078833715060', 'fallback': '📢'},
    'commands': {'id': '6285014721582076161', 'fallback': '📚'},
    'dot': {'id': '5350751092936303896', 'fallback': '•'},
    'info': {'id': '6285014721582076161', 'fallback': 'ℹ️'},
    'target': {'id': '5424818078833715060', 'fallback': '🎯'},
    'list': {'id': '6285014721582076161', 'fallback': '📋'},
    'settings': {'id': '5206621104403129406', 'fallback': '⚙️'},
}

def emoji(name: str) -> str:
    """Возвращает HTML-тег для кастомного эмодзи"""
    if name in CUSTOM_EMOJI:
        e = CUSTOM_EMOJI[name]
        return f'<tg-emoji emoji-id="{e["id"]}">{e["fallback"]}</tg-emoji>'
    return ''

# ==================== НАСТРОЙКИ КЕШИРОВАНИЯ ====================
CACHE_TTL_HOURS = 6
MAX_REQUESTS_PER_MINUTE = 30

# ==================== НАСТРОЙКИ БЕТА-ТЕСТА ====================
BETA_MODE = True
# Эти значения будут перезаписаны из .env
BETA_TESTER_ID = BETA_TESTER_ID
BROADCAST_MODE = BROADCAST_MODE
SPECIFIC_USER_ID = SPECIFIC_USER_ID

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ====================
http_session: Optional[aiohttp.ClientSession] = None
request_timestamps: List[datetime] = []
all_groups_cache: Dict[str, Dict[str, str]] = {}
groups_loaded = False

# ==================== ИНИЦИАЛИЗАЦИЯ ====================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ==================== БАЗА ДАННЫХ ====================
async def init_db():
    async with aiosqlite.connect('users.db') as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                faculty_id TEXT NOT NULL,
                faculty_name TEXT NOT NULL,
                group_id TEXT NOT NULL,
                group_name TEXT NOT NULL,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                last_activity TIMESTAMP,
                is_beta_tester BOOLEAN DEFAULT 0
            )
        ''')
        
        await db.execute('''
            CREATE TABLE IF NOT EXISTS schedule_cache (
                group_id TEXT NOT NULL,
                faculty_id TEXT NOT NULL,
                target_date DATE NOT NULL,
                schedule_data TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (group_id, faculty_id, target_date)
            )
        ''')
        await db.commit()
    logger.info("✅ База данных инициализирована")

async def save_user_settings(user_id: int, faculty_id: str, faculty_name: str, group_id: str, group_name: str):
    is_beta = 1 if (BETA_MODE and user_id == BETA_TESTER_ID) else 0
    
    async with aiosqlite.connect('users.db') as db:
        await db.execute('''
            INSERT OR REPLACE INTO users 
            (user_id, faculty_id, faculty_name, group_id, group_name, last_activity, is_beta_tester)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, faculty_id, faculty_name, group_id, group_name, datetime.now(), is_beta))
        await db.commit()
    
    logger.info(f"✅ Пользователь {user_id} сохранен")

async def get_user_settings(user_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect('users.db') as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('''
            SELECT faculty_id, faculty_name, group_id, group_name, is_beta_tester 
            FROM users WHERE user_id = ?
        ''', (user_id,)) as cursor:
            row = await cursor.fetchone()
    
    return dict(row) if row else None

async def delete_user_settings(user_id: int):
    async with aiosqlite.connect('users.db') as db:
        await db.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
        await db.commit()
    logger.info(f"✅ Пользователь {user_id} удален")

async def get_all_users() -> List[Tuple[int, str, str]]:
    async with aiosqlite.connect('users.db') as db:
        if BETA_MODE and BROADCAST_MODE == "beta":
            cursor = await db.execute('''
                SELECT user_id, faculty_id, group_id 
                FROM users WHERE is_beta_tester = 1 AND is_active = 1
            ''')
        elif BROADCAST_MODE == "specific":
            cursor = await db.execute('''
                SELECT user_id, faculty_id, group_id 
                FROM users WHERE user_id = ? AND is_active = 1
            ''', (SPECIFIC_USER_ID,))
        else:
            cursor = await db.execute('''
                SELECT user_id, faculty_id, group_id 
                FROM users WHERE is_active = 1
            ''')
        
        users = await cursor.fetchall()
    
    return users

async def get_user_count() -> int:
    async with aiosqlite.connect('users.db') as db:
        cursor = await db.execute('SELECT COUNT(*) FROM users WHERE is_active = 1')
        count = await cursor.fetchone()
    return count[0] if count else 0

async def deactivate_user(user_id: int):
    async with aiosqlite.connect('users.db') as db:
        await db.execute('UPDATE users SET is_active = 0 WHERE user_id = ?', (user_id,))
        await db.commit()
    logger.info(f"⚠️ Пользователь {user_id} деактивирован")

# ==================== КЕШИРОВАНИЕ ====================
async def get_cached_schedule(faculty_id: str, group_id: str, target_date: date) -> Optional[List[Dict]]:
    async with aiosqlite.connect('users.db') as db:
        cursor = await db.execute('''
            SELECT schedule_data, updated_at 
            FROM schedule_cache 
            WHERE group_id = ? AND faculty_id = ? AND target_date = ?
        ''', (group_id, faculty_id, target_date.isoformat()))
        row = await cursor.fetchone()
    
    if row:
        data, updated_at = row
        updated = datetime.fromisoformat(updated_at)
        if datetime.now() - updated < timedelta(hours=CACHE_TTL_HOURS):
            return json.loads(data)
    
    return None

async def save_schedule_to_cache(faculty_id: str, group_id: str, target_date: date, schedule: List[Dict]):
    async with aiosqlite.connect('users.db') as db:
        await db.execute('''
            INSERT OR REPLACE INTO schedule_cache (group_id, faculty_id, target_date, schedule_data, updated_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (group_id, faculty_id, target_date.isoformat(), json.dumps(schedule, ensure_ascii=False), datetime.now()))
        await db.commit()

# ==================== RATE LIMITING ====================
async def check_rate_limit() -> bool:
    global request_timestamps
    now = datetime.now()
    request_timestamps = [ts for ts in request_timestamps if now - ts < timedelta(minutes=1)]
    
    if len(request_timestamps) >= MAX_REQUESTS_PER_MINUTE:
        wait_time = 60 - (now - request_timestamps[0]).seconds
        logger.warning(f"⚠️ Достигнут лимит запросов. Ожидание {wait_time} секунд")
        await asyncio.sleep(wait_time)
        return await check_rate_limit()
    
    request_timestamps.append(now)
    return True

# ==================== ПАРСИНГ ====================
async def fetch_html(url: str, retry: int = 3) -> Optional[str]:
    """Получение HTML с повторными попытками"""
    global http_session
    
    for attempt in range(retry):
        await check_rate_limit()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
        }
        
        try:
            logger.info(f"📡 Попытка {attempt + 1}/{retry}: {url}")
            async with http_session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    html = await response.text()
                    logger.info(f"✅ Успешно получен HTML ({len(html)} символов)")
                    return html
                else:
                    logger.warning(f"⚠️ Статус ответа: {response.status}")
                    
        except asyncio.TimeoutError:
            logger.warning(f"⏰ Таймаут {attempt + 1}/{retry}")
        except aiohttp.ClientConnectorError as e:
            logger.warning(f"🔌 Ошибка подключения {attempt + 1}/{retry}: {e}")
        except Exception as e:
            logger.warning(f"❌ Ошибка {attempt + 1}/{retry}: {e}")
        
        if attempt < retry - 1:
            wait = 5 * (attempt + 1)
            logger.info(f"⏳ Ожидание {wait} сек перед следующей попыткой...")
            await asyncio.sleep(wait)
    
    logger.error(f"❌ Все {retry} попыток провалились для {url}")
    return None

async def load_all_groups():
    """Загружает все группы со всех факультетов"""
    global all_groups_cache, groups_loaded
    all_groups_cache = {}
    
    html = await fetch_html(SCHEDULE_URL)
    if not html:
        logger.error("❌ Не удалось загрузить главную страницу")
        return
    
    soup = BeautifulSoup(html, 'html.parser')
    faculty_select = soup.find('select', {'name': 'faculty'})
    if not faculty_select:
        logger.error("❌ Не найден выбор факультета")
        return
    
    faculties = {}
    for option in faculty_select.find_all('option'):
        faculty_id = option.get('value')
        faculty_name = option.text.strip()
        if faculty_id and faculty_id != '0':
            faculties[faculty_id] = faculty_name
    
    logger.info(f"📚 Загружено факультетов: {len(faculties)}")
    
    for faculty_id, faculty_name in faculties.items():
        url = f"{SCHEDULE_URL}?faculty={faculty_id}&group=&date="
        try:
            html = await fetch_html(url)
            if not html:
                continue
                
            soup = BeautifulSoup(html, 'html.parser')
            select_div = soup.find('div', {'data-component': 'SelectAutocomplete'})
            if select_div:
                options_json = select_div.get(':options')
                if options_json:
                    all_options = json.loads(options_json)
                    for item in all_options:
                        if isinstance(item, dict):
                            group_name = item.get('label')
                            group_id = item.get('value')
                            if group_name and group_id and group_id != 0 and 'Не выбрана' not in group_name:
                                all_groups_cache[group_name] = {
                                    'faculty_id': faculty_id,
                                    'group_id': str(group_id),
                                    'faculty_name': faculty_name
                                }
        except Exception as e:
            logger.error(f"Ошибка загрузки групп для {faculty_name}: {e}")
            continue
    
    groups_loaded = True
    logger.info(f"✅ Загружено групп: {len(all_groups_cache)}")

async def parse_daily_schedule(faculty_id: str, group_id: str, target_date: date, use_cache: bool = True) -> List[Dict]:
    """Парсинг расписания на конкретную дату с сохранением нумерации пар"""
    if use_cache:
        cached = await get_cached_schedule(faculty_id, group_id, target_date)
        if cached is not None:
            if cached and len(cached) > 0 and 'number' in cached[0]:
                return cached
            else:
                logger.info("⚠️ Кеш устарел (нет поля number), парсим заново")
    
    week_number = target_date.isocalendar()[1]
    year = target_date.year
    
    params = {
        'faculty': faculty_id,
        'group': group_id,
        'week': week_number,
        'year': year
    }
    url = f"{SCHEDULE_URL}?{urlencode(params)}"
    
    logger.info(f"🌐 Запрос расписания: {url}")
    
    html = await fetch_html(url)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Ищем таблицу
    table = soup.find('table')
    if not table:
        logger.error("❌ Таблица не найдена")
        return []
    
    # Находим заголовки с днями
    header_row = table.find('tr')
    if not header_row:
        return []
    
    headers = header_row.find_all('th')
    
    # Определяем индекс нужного дня
    target_date_str = target_date.strftime('%d %B').lower()
    day_index = None
    
    for i, th in enumerate(headers):
        th_text = th.get_text(strip=True).lower()
        if target_date_str in th_text or str(target_date.day) in th_text:
            day_index = i
            logger.info(f"✅ Найден день {target_date_str} в колонке {i}")
            break
    
    if day_index is None:
        logger.error(f"❌ День {target_date_str} не найден")
        return []
    
    # Парсим пары с сохранением нумерации
    lessons = []
    rows = table.find_all('tr')[1:]  # Пропускаем строку с заголовками
    
    for row_idx, row in enumerate(rows, 1):  # row_idx - это номер пары
        # Ищем время в первой колонке
        time_cell = row.find('td')
        if not time_cell:
            continue
        
        time_divs = time_cell.find_all('div')
        if len(time_divs) < 2:
            continue
        
        start_time = time_divs[0].get_text(strip=True)
        end_time = time_divs[1].get_text(strip=True)
        
        # Получаем ячейку с парой
        cells = row.find_all('td')
        if len(cells) <= day_index:
            continue
        
        lesson_cell = cells[day_index]
        
        # Проверяем, есть ли пара
        cell_text = lesson_cell.get_text(strip=True)
        if not cell_text or cell_text == '':
            continue
        
        # Парсим информацию о паре
        lesson_info = lesson_cell.find('div')
        if not lesson_info:
            continue
        
        # Ищем тип пары
        type_badge = lesson_info.find('span', class_='schedule-lesson-type-badge')
        lesson_type = "лекция"
        if type_badge:
            badge_text = type_badge.get_text(strip=True)
            if 'Лек' in badge_text:
                lesson_type = "лекция"
            elif 'Лаб' in badge_text:
                lesson_type = "лабораторная"
            elif 'Упр' in badge_text or 'Пр' in badge_text:
                lesson_type = "практика"
        
        # Получаем текст ячейки
        cell_text = lesson_info.get_text(separator=' ', strip=True)
        
        # Убираем тип из текста
        if type_badge:
            cell_text = cell_text.replace(type_badge.get_text(strip=True), '').strip()
        
        # Ищем предмет
        subject = "Предмет"
        
        # Ищем ссылку на преподавателя
        teacher_link = lesson_info.find('a', href=re.compile(r'/schedule-frame/lecturer'))
        teacher = "Не указан"
        if teacher_link:
            teacher = teacher_link.get_text(strip=True)
            parts = cell_text.split(teacher)[0].strip().rstrip(',')
            if parts:
                subject = parts
        
        # Ищем аудиторию
        audience = "Не указана"
        aud_link = lesson_info.find('a', href=re.compile(r'/schedule-frame/classroom'))
        if aud_link:
            audience = aud_link.get_text(strip=True)
        
        lessons.append({
            'number': row_idx,
            'start': start_time,
            'end': end_time,
            'type': lesson_type,
            'subject': subject,
            'teacher': teacher,
            'audience': audience
        })
        
        logger.info(f"➕ Добавлена {row_idx}-я пара: {start_time}-{end_time} {subject}")
    
    logger.info(f"📊 Всего найдено пар: {len(lessons)}")
    
    if use_cache and lessons:
        await save_schedule_to_cache(faculty_id, group_id, target_date, lessons)
    
    return lessons

# ==================== ГЕНЕРАЦИЯ СООБЩЕНИЙ ====================
async def generate_daily_message(user_id: int, target_date: date) -> Optional[str]:
    """Генерация сообщения с расписанием с правильной нумерацией пар"""
    settings = await get_user_settings(user_id)
    if not settings:
        return None
    
    lessons = await parse_daily_schedule(
        settings['faculty_id'],
        settings['group_id'],
        target_date,
        use_cache=True
    )
    
    if not lessons:
        return None
    
    # Русские названия месяцев
    month_rus = {
        1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля', 5: 'мая', 6: 'июня',
        7: 'июля', 8: 'августа', 9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря'
    }
    
    # Дни недели
    weekday_rus = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье']
    
    # Формируем заголовок
    day_name = weekday_rus[target_date.weekday()].capitalize()
    month_name = month_rus[target_date.month]
    
    # Собираем сообщение по частям
    message_parts = []
    
    # Заголовок с кастомным эмодзи календаря
    message_parts.append(f"{emoji('calendar')} <b>{day_name}, {target_date.day} {month_name} | {settings['faculty_name']}, гр. {settings['group_name']}</b>")
    message_parts.append("")
    
    for lesson in lessons:
        # Сокращаем тип пары
        lesson_type_short = {
            'лекция': 'лек',
            'практика': 'пр',
            'лабораторная': 'лаб'
        }.get(lesson['type'], lesson['type'])
        
        # Форматируем каждую пару с номером
        message_parts.append(f"<b>{lesson['number']}-я пара:</b> <code>{lesson['start']} – {lesson['end']}</code> — <b>{lesson['subject']} ({lesson_type_short})</b>")
        message_parts.append(f"Ауд. {lesson['audience']} • {lesson['teacher']}")
        message_parts.append("")
    
    # Объединяем все части с переносами строк
    return "\n".join(message_parts).strip()

# ==================== ПЛАНИРОВЩИК НАПОМИНАНИЙ ====================
reminder_tasks: Dict[str, asyncio.Task] = {}

async def schedule_reminders_for_user(user_id: int, faculty_id: str, group_id: str, target_date: date):
    """Планирование напоминаний на день"""
    task_key = f"{user_id}_{target_date}"
    if task_key in reminder_tasks:
        reminder_tasks[task_key].cancel()
    
    lessons = await parse_daily_schedule(faculty_id, group_id, target_date, use_cache=True)
    if not lessons:
        return
    
    now = datetime.now(LOCAL_TIMEZONE)
    
    for lesson in lessons:
        lesson_time = datetime.strptime(lesson['start'], '%H:%M').time()
        lesson_datetime = datetime.combine(target_date, lesson_time)
        lesson_datetime = LOCAL_TIMEZONE.localize(lesson_datetime)
        reminder_time = lesson_datetime - timedelta(minutes=20)
        
        if reminder_time < now:
            continue
        
        async def send_reminder(uid, lsn, rem_time):
            wait_seconds = (rem_time - datetime.now(LOCAL_TIMEZONE)).total_seconds()
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
                try:
                    lesson_type_short = {
                        'лекция': 'лек',
                        'практика': 'пр',
                        'лабораторная': 'лаб'
                    }.get(lsn['type'], lsn['type'])
                    
                    msg = (
                        f"{emoji('reminder')} <b>Напоминание!</b>\n"
                        f"Через 20 минут, в {lsn['start']}, начинается:\n\n"
                        f"<b>{lsn['subject']} ({lesson_type_short})</b>\n"
                        f"Ауд. {lsn['audience']} • {lsn['teacher']}"
                    )
                    await bot.send_message(uid, msg, parse_mode="HTML")
                except Exception as e:
                    if "bot was blocked" in str(e).lower():
                        await deactivate_user(uid)
        
        task = asyncio.create_task(send_reminder(user_id, lesson, reminder_time))
        reminder_tasks[task_key] = task

# ==================== БЕТА-ФУНКЦИИ ====================
async def send_test_broadcast(user_id: int = None):
    if user_id:
        users = [(user_id, "", "")]
    else:
        users = await get_all_users()
    
    schedule_date = datetime.now(LOCAL_TIMEZONE).date()
    success_count = 0
    fail_count = 0
    
    for uid, _, _ in users:
        try:
            message = await generate_daily_message(uid, schedule_date)
            if message:
                settings = await get_user_settings(uid)
                await bot.send_message(uid, message, parse_mode="HTML")
                
                if settings:
                    await schedule_reminders_for_user(
                        uid, settings['faculty_id'], settings['group_id'], schedule_date
                    )
                
                success_count += 1
            await asyncio.sleep(0.5)
        except Exception as e:
            fail_count += 1
            if "bot was blocked" in str(e).lower():
                await deactivate_user(uid)
    
    return success_count, fail_count

async def send_all_messages(user_id: int):
    """Отправляет все возможные сообщения бота для проверки (с обычными эмодзи)"""
    
    # 1. Приветствие для нового пользователя
    await bot.send_message(
        user_id,
        f"👋 Привет! Я бот для расписания РГРТУ.\n\n"
        f"Напиши /group чтобы выбрать свою группу!\n\n"
        f"/help - помощь"
    )
    await asyncio.sleep(1)
    
    # 2. Приветствие для зарегистрированного
    await bot.send_message(
        user_id,
        f"ℹ️ Ты уже зарегистрирован!\n\n"
        f"🎓 ФВТ, гр. 430\n\n"
        f"Напиши /group чтобы сменить группу.\n"
        f"/help - помощь"
    )
    await asyncio.sleep(1)
    
    # 3. Запрос новой группы
    await bot.send_message(
        user_id,
        f"👥 Введи номер новой группы:"
    )
    await asyncio.sleep(1)
    
    # 4. Успешная смена группы
    await bot.send_message(
        user_id,
        f"✅ Группа успешно изменена!\n\n"
        f"🎓 ФВТ, гр. 431\n\n"
        f"Теперь ты будешь получать расписание для новой группы."
    )
    await asyncio.sleep(1)
    
    # 5. Успешная регистрация
    await bot.send_message(
        user_id,
        f"✅ Регистрация завершена!\n\n"
        f"🎓 ФВТ, гр. 430\n\n"
        f"📅 Что дальше?\n"
        f"• Каждое утро в 6:00 я буду присылать расписание\n"
        f"• За 20 минут до пары придет напоминание\n\n"
        f"/help — помощь"
    )
    await asyncio.sleep(1)
    
    # 6. Группа не найдена
    await bot.send_message(
        user_id,
        f"❌ Группа '430' не найдена.\n\n"
        f"Примеры групп:\n"
        f"520, 520М, 522, 523, 524, 525, 5020, 5023, 5211, 5213\n\n"
        f"Попробуй еще раз или введи /group для выбора другой группы:"
    )
    await asyncio.sleep(1)
    
    # 7. Отмена ввода
    await bot.send_message(
        user_id,
        f"ℹ️ Ввод группы отменен.\n"
        f"Используй /group чтобы выбрать группу"
    )
    await asyncio.sleep(1)
    
    # 8. Help для нового пользователя
    await bot.send_message(
        user_id,
        f"📚 Все доступные команды:\n\n"
        f"/group — выбрать группу\n"
        f"/help — это сообщение"
    )
    await asyncio.sleep(1)
    
    # 9. Help для зарегистрированного
    await bot.send_message(
        user_id,
        f"📚 Все доступные команды:\n\n"
        f"/group — сменить группу\n"
        f"/today — расписание на сегодня\n"
        f"/tomorrow — расписание на завтра\n"
        f"/settings — настройки\n"
        f"/reset — сбросить настройки\n"
        f"/help — это сообщение"
    )
    await asyncio.sleep(1)
    
    # 10. Настройки
    await bot.send_message(
        user_id,
        f"⚙️ Твои настройки\n\n"
        f"🎓 Факультет: ФВТ\n"
        f"👥 Группа: 430\n\n"
        f"/group — сменить группу\n"
        f"/reset — сбросить настройки"
    )
    await asyncio.sleep(1)
    
    # 11. Сброс настроек
    await bot.send_message(
        user_id,
        f"✅ Настройки сброшены.\n"
        f"Используй /group для новой регистрации."
    )
    await asyncio.sleep(1)
    
    # 12. Ошибка: не зарегистрирован
    await bot.send_message(
        user_id,
        f"❌ Сначала нужно зарегистрироваться!\n"
        f"Напиши /group чтобы выбрать группу."
    )
    await asyncio.sleep(1)
    
    # 13. Неизвестная команда
    await bot.send_message(
        user_id,
        f"❌ Неизвестная команда.\n"
        f"Используй /help для списка команд"
    )
    await asyncio.sleep(1)
    
    # 14. Расписание на сегодня (с парами)
    await bot.send_message(
        user_id,
        f"📅 Пятница, 20 февраля | ФВТ, гр. 430\n\n"
        f"1-я пара: 08:10 – 09:45 — Метрология (лек)\n"
        f"Ауд. 302 C • доц. Кряков В.Г.\n\n"
        f"2-я пара: 09:55 – 11:30 — Схемотехника ЭС (лек)\n"
        f"Ауд. 333 C • доц. Копейкин Ю.А."
    )
    await asyncio.sleep(1)
    
    # 15. Расписание на сегодня (нет пар)
    await bot.send_message(
        user_id,
        f"📅 На сегодня пар нет"
    )
    await asyncio.sleep(1)
    
    # 16. Напоминание
    await bot.send_message(
        user_id,
        f"⏰ Напоминание!\n"
        f"Через 20 минут, в 11:40, начинается:\n\n"
        f"Электротехника и электроника (лек)\n"
        f"Ауд. 404 C • доц. Копейкин Ю.А."
    )
    
    # Отправляем финальное сообщение
    await bot.send_message(
        user_id,
        f"✅ Все 16 сообщений отправлены!\n"
        f"Проверь, как они выглядят."
    )

# ==================== ФОНОВЫЕ ЗАДАЧИ ====================
async def daily_schedule_sender():
    while True:
        try:
            now = datetime.now(LOCAL_TIMEZONE)
            target = time(6, 0, 0)
            next_run = datetime.combine(now.date() + timedelta(days=1), target)
            next_run = LOCAL_TIMEZONE.localize(next_run)
            sleep_seconds = (next_run - now).total_seconds()
            
            logger.info(f"⏰ Следующая рассылка через {sleep_seconds/3600:.2f} часов")
            await asyncio.sleep(sleep_seconds)
            
            users = await get_all_users()
            schedule_date = datetime.now(LOCAL_TIMEZONE).date()
            
            for user_id, faculty_id, group_id in users:
                try:
                    message = await generate_daily_message(user_id, schedule_date)
                    if message:
                        await bot.send_message(user_id, message, parse_mode="HTML")
                        await schedule_reminders_for_user(user_id, faculty_id, group_id, schedule_date)
                    
                    await asyncio.sleep(0.5)
                except Exception as e:
                    if "bot was blocked" in str(e).lower():
                        await deactivate_user(user_id)
                    logger.error(f"❌ Ошибка рассылки для {user_id}: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Ошибка в daily_schedule_sender: {e}")
            await asyncio.sleep(60)

# ==================== КОМАНДЫ ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    """Начало работы с ботом"""
    await state.clear()
    
    settings = await get_user_settings(message.from_user.id)
    
    if settings:
        text = (
            f"{emoji('info')} Ты уже зарегистрирован!\n\n"
            f"{emoji('faculty')} {settings['faculty_name']}, гр. {settings['group_name']}\n\n"
            f"Напиши /group чтобы сменить группу.\n"
            f"/help - помощь"
        )
    else:
        text = (
            f"{emoji('welcome')} Привет! Я бот для расписания РГРТУ.\n\n"
            f"Напиши /group чтобы выбрать свою группу!\n\n"
            f"/help - помощь"
        )
    
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("help"))
async def cmd_help(message: types.Message, state: FSMContext):
    """Помощь"""
    await state.clear()
    
    settings = await get_user_settings(message.from_user.id)
    
    if settings:
        text = (
            f"{emoji('commands')} <b>Все доступные команды:</b>\n\n"
            f"<code>/group</code> — сменить группу\n"
            f"<code>/today</code> — расписание на сегодня\n"
            f"<code>/tomorrow</code> — расписание на завтра\n"
            f"<code>/settings</code> — настройки\n"
            f"<code>/reset</code> — сбросить настройки\n"
            f"<code>/help</code> — это сообщение"
        )
    else:
        text = (
            f"{emoji('commands')} <b>Все доступные команды:</b>\n\n"
            f"<code>/group</code> — выбрать группу\n"
            f"<code>/help</code> — это сообщение"
        )
    
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("group"))
async def cmd_group(message: types.Message, state: FSMContext):
    """Смена группы"""
    settings = await get_user_settings(message.from_user.id)
    if not settings:
        await message.answer(
            f"{emoji('error')} Сначала нужно зарегистрироваться!\n"
            f"Напиши /group чтобы выбрать группу.",
            parse_mode="HTML"
        )
        return
    
    await state.clear()
    await state.set_state(Form.waiting_for_group)
    
    await message.answer(
        f"{emoji('group')} Введи номер новой группы:",
        parse_mode="HTML"
    )

@dp.message(Command("settings"))
async def cmd_settings(message: types.Message):
    """Настройки пользователя"""
    settings = await get_user_settings(message.from_user.id)
    
    if not settings:
        await message.answer(
            f"{emoji('error')} Сначала нужно зарегистрироваться!\n"
            f"Напиши /group чтобы выбрать группу.",
            parse_mode="HTML"
        )
        return
    
    text = (
        f"{emoji('settings')} <b>Твои настройки</b>\n\n"
        f"{emoji('faculty')} Факультет: {settings['faculty_name']}\n"
        f"{emoji('group')} Группа: {settings['group_name']}\n\n"
        f"<code>/group</code> — сменить группу\n"
        f"<code>/reset</code> — сбросить настройки"
    )
    
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("today"))
async def cmd_today(message: types.Message):
    settings = await get_user_settings(message.from_user.id)
    if not settings:
        await message.answer(
            f"{emoji('error')} Сначала нужно зарегистрироваться!\n"
            f"Напиши /group чтобы выбрать группу.",
            parse_mode="HTML"
        )
        return
    
    today_msg = await generate_daily_message(message.from_user.id, datetime.now().date())
    
    if today_msg:
        await message.answer(today_msg, parse_mode="HTML")
    else:
        await message.answer(
            f"{emoji('calendar')} На сегодня пар нет",
            parse_mode="HTML"
        )

@dp.message(Command("tomorrow"))
async def cmd_tomorrow(message: types.Message):
    settings = await get_user_settings(message.from_user.id)
    if not settings:
        await message.answer(
            f"{emoji('error')} Сначала нужно зарегистрироваться!\n"
            f"Напиши /group чтобы выбрать группу.",
            parse_mode="HTML"
        )
        return
    
    tomorrow = datetime.now().date() + timedelta(days=1)
    tomorrow_msg = await generate_daily_message(message.from_user.id, tomorrow)
    
    if tomorrow_msg:
        await message.answer(tomorrow_msg, parse_mode="HTML")
    else:
        await message.answer(
            f"{emoji('calendar')} На завтра пар нет",
            parse_mode="HTML"
        )

@dp.message(Command("reset"))
async def cmd_reset(message: types.Message, state: FSMContext):
    await delete_user_settings(message.from_user.id)
    await state.clear()
    await message.answer(
        f"{emoji('success')} Настройки сброшены.\n"
        f"Используй /group для новой регистрации.",
        parse_mode="HTML"
    )

@dp.message(Command("beta"))
async def cmd_beta(message: types.Message):
    if message.from_user.id != BETA_TESTER_ID:
        await message.answer(
            f"{emoji('error')} Эта команда только для разработчика",
            parse_mode="HTML"
        )
        return
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📊 Статистика", callback_data="beta_stats")],
        [types.InlineKeyboardButton(text="📢 Сделать рассылку", callback_data="beta_broadcast")],
        [types.InlineKeyboardButton(text="🧪 Тестовая рассылка всем", callback_data="beta_broadcast_all")],
        [types.InlineKeyboardButton(text="🧪 Тестовая рассылка мне", callback_data="beta_broadcast_me")],
        [types.InlineKeyboardButton(text="📋 Список пользователей", callback_data="beta_users")],
        [types.InlineKeyboardButton(text="📨 Все сообщения бота", callback_data="beta_all_messages")]
    ])
    
    mode_desc = {
        "all": "📢 Всем пользователям",
        "beta": "🔬 Только бета-тестеру",
        "specific": "🎯 Конкретному ID"
    }
    
    text = (
        f"{emoji('beta')} <b>Панель бета-тестирования</b>\n\n"
        f"Текущий режим: {mode_desc.get(BROADCAST_MODE, 'Неизвестно')}\n"
        f"Пользователей в БД: {await get_user_count()}\n"
        f"Бета-тестер: {BETA_TESTER_ID}\n\n"
        f"Выбери действие:"
    )
    
    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")

# ==================== ОБРАБОТЧИКИ БЕТА-КОМАНД ====================
@dp.callback_query(lambda c: c.data == "beta_broadcast")
async def beta_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало создания рассылки"""
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    await callback.answer()
    
    await callback.message.edit_text(
        f"{emoji('broadcast')} <b>Создание рассылки</b>\n\n"
        f"Отправь мне текст сообщения для рассылки.\n"
        f"Ты можешь использовать <b>HTML-теги</b> и <b>кастомные эмодзи</b>.\n\n"
        f"<i>Пример:</i>\n"
        f"<code>&lt;b&gt;Важное объявление!&lt;/b&gt;\n\n"
        f"Привет, {emoji('welcome')} пользователи!</code>\n\n"
        f"После текста я попрошу прикрепить медиа (фото/видео).",
        parse_mode="HTML"
    )
    
    await state.set_state(BroadcastStates.waiting_for_text)

@dp.message(BroadcastStates.waiting_for_text)
async def beta_broadcast_text(message: types.Message, state: FSMContext):
    """Получение текста для рассылки"""
    if message.from_user.id != BETA_TESTER_ID:
        await message.answer(f"{emoji('error')} Ты не разработчик", parse_mode="HTML")
        await state.clear()
        return
    
    await state.update_data(broadcast_text=message.html_text)
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="✅ Без медиа", callback_data="broadcast_no_media")],
        [types.InlineKeyboardButton(text="🖼 Отмена", callback_data="broadcast_cancel")]
    ])
    
    await message.answer(
        f"{emoji('info')} Текст сохранён.\n\n"
        f"Теперь отправь <b>фото или видео</b> для рассылки (или нажми кнопку \"Без медиа\").",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    
    await state.set_state(BroadcastStates.waiting_for_media)

@dp.message(BroadcastStates.waiting_for_media, lambda msg: msg.photo or msg.video)
async def beta_broadcast_media(message: types.Message, state: FSMContext):
    """Получение медиа для рассылки"""
    if message.from_user.id != BETA_TESTER_ID:
        await message.answer(f"{emoji('error')} Ты не разработчик", parse_mode="HTML")
        await state.clear()
        return
    
    data = await state.get_data()
    broadcast_text = data.get('broadcast_text')
    
    if message.photo:
        media_file_id = message.photo[-1].file_id
        media_type = "photo"
    elif message.video:
        media_file_id = message.video.file_id
        media_type = "video"
    
    await state.update_data(media_file_id=media_file_id, media_type=media_type)
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="✅ Отправить всем", callback_data="broadcast_send_all")],
        [types.InlineKeyboardButton(text="🔬 Только бета-тестеру", callback_data="broadcast_send_beta")],
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel")]
    ])
    
    await message.answer(
        f"{emoji('success')} Медиа получено!\n\n"
        f"<b>Текст рассылки:</b>\n{broadcast_text}\n\n"
        f"Кому отправляем?",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(lambda c: c.data == "broadcast_no_media")
async def broadcast_no_media(callback: types.CallbackQuery, state: FSMContext):
    """Рассылка без медиа"""
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    data = await state.get_data()
    broadcast_text = data.get('broadcast_text')
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="✅ Отправить всем", callback_data="broadcast_send_all")],
        [types.InlineKeyboardButton(text="🔬 Только бета-тестеру", callback_data="broadcast_send_beta")],
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel")]
    ])
    
    await callback.message.edit_text(
        f"{emoji('success')} Текст готов!\n\n"
        f"<b>Текст рассылки:</b>\n{broadcast_text}\n\n"
        f"Кому отправляем?",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(lambda c: c.data == "broadcast_cancel")
async def broadcast_cancel(callback: types.CallbackQuery, state: FSMContext):
    """Отмена рассылки"""
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    await state.clear()
    await callback.message.edit_text(
        f"{emoji('info')} Рассылка отменена.",
        parse_mode="HTML"
    )

@dp.callback_query(lambda c: c.data.startswith("broadcast_send_"))
async def broadcast_send(callback: types.CallbackQuery, state: FSMContext):
    """Отправка рассылки"""
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    await callback.answer()
    
    target = callback.data.replace("broadcast_send_", "")
    
    data = await state.get_data()
    broadcast_text = data.get('broadcast_text')
    media_file_id = data.get('media_file_id')
    media_type = data.get('media_type')
    
    if target == "beta":
        users = await get_all_users()
    else:
        users = await get_all_users()
    
    await callback.message.edit_text(
        f"{emoji('broadcast')} Начинаю рассылку {len(users)} пользователям...\n\n"
        f"<b>Текст:</b>\n{broadcast_text}",
        parse_mode="HTML"
    )
    
    success = 0
    fail = 0
    
    for user_id, _, _ in users:
        try:
            if media_file_id and media_type:
                if media_type == "photo":
                    await bot.send_photo(
                        chat_id=user_id,
                        photo=media_file_id,
                        caption=broadcast_text,
                        parse_mode="HTML"
                    )
                elif media_type == "video":
                    await bot.send_video(
                        chat_id=user_id,
                        video=media_file_id,
                        caption=broadcast_text,
                        parse_mode="HTML"
                    )
            else:
                await bot.send_message(
                    chat_id=user_id,
                    text=broadcast_text,
                    parse_mode="HTML"
                )
            success += 1
        except Exception as e:
            fail += 1
            if "bot was blocked" in str(e).lower():
                await deactivate_user(user_id)
            logger.error(f"Ошибка отправки пользователю {user_id}: {e}")
        
        await asyncio.sleep(0.05)
    
    await callback.message.answer(
        f"{emoji('success')} Рассылка завершена!\n\n"
        f"✅ Успешно: {success}\n"
        f"❌ Ошибок: {fail}",
        parse_mode="HTML"
    )
    
    await state.clear()

@dp.callback_query(lambda c: c.data == "beta_stats")
async def beta_stats(callback: types.CallbackQuery):
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    await callback.answer()
    
    users = await get_all_users()
    text = (
        f"{emoji('stats')} <b>Статистика</b>\n\n"
        f"Всего пользователей: {await get_user_count()}\n"
        f"Активных сегодня: {len(users)}\n"
        f"Режим рассылки: {BROADCAST_MODE}\n"
        f"Бета-тестер ID: {BETA_TESTER_ID}"
    )
    await callback.message.edit_text(text, parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "beta_broadcast_all")
async def beta_broadcast_all(callback: types.CallbackQuery):
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    await callback.answer()
    
    await callback.message.edit_text(f"{emoji('test')} Запускаю тестовую рассылку ВСЕМ пользователям...", parse_mode="HTML")
    success, fail = await send_test_broadcast()
    text = (
        f"{emoji('success')} Тестовая рассылка завершена!\n"
        f"Успешно: {success}\n"
        f"Ошибок: {fail}"
    )
    await callback.message.answer(text, parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "beta_broadcast_me")
async def beta_broadcast_me(callback: types.CallbackQuery):
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    await callback.answer()
    
    await callback.message.edit_text(f"{emoji('test')} Запускаю тестовую рассылку лично вам...", parse_mode="HTML")
    success, fail = await send_test_broadcast(BETA_TESTER_ID)
    text = (
        f"{emoji('success')} Тестовая рассылка завершена!\n"
        f"Успешно: {success}\n"
        f"Ошибок: {fail}"
    )
    await callback.message.answer(text, parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "beta_users")
async def beta_users(callback: types.CallbackQuery):
    """Список пользователей с кликабельными ID"""
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    await callback.answer()
    
    async with aiosqlite.connect('users.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT user_id, faculty_name, group_name, is_beta_tester, is_active 
            FROM users ORDER BY registered_at DESC LIMIT 20
        ''')
        users = await cursor.fetchall()
    
    text_lines = [f"{emoji('list')} <b>Последние 20 пользователей:</b>\n"]
    
    for u in users:
        beta_mark = "🔬" if u['is_beta_tester'] else "👤"
        active_mark = "✅" if u['is_active'] else "❌"
        user_link = f"<a href='tg://user?id={u['user_id']}'>{u['user_id']}</a>"
        text_lines.append(f"{beta_mark} {user_link} {active_mark}")
        text_lines.append(f"   {u['faculty_name']} — {u['group_name']}\n")
    
    await callback.message.edit_text("\n".join(text_lines), parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "beta_all_messages")
async def beta_all_messages(callback: types.CallbackQuery):
    """Отправка всех сообщений бота"""
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    await callback.answer()
    
    await callback.message.edit_text(
        f"{emoji('broadcast')} Начинаю отправку всех сообщений бота...",
        parse_mode="HTML"
    )
    
    await send_all_messages(callback.from_user.id)

# ==================== ОБРАБОТЧИК ВВОДА ГРУППЫ ====================
@dp.message(Form.waiting_for_group)
async def process_group_input(message: types.Message, state: FSMContext):
    """Обработка введенного номера группы"""
    
    if message.text.startswith('/'):
        command = message.text.lower()
        await state.clear()
        
        if command == '/help':
            await cmd_help(message, state)
        elif command == '/start':
            await cmd_start(message, state)
        elif command == '/today':
            await cmd_today(message)
        elif command == '/tomorrow':
            await cmd_tomorrow(message)
        elif command == '/settings':
            await cmd_settings(message)
        elif command == '/group':
            await state.set_state(Form.waiting_for_group)
            await message.answer(f"{emoji('group')} Введи номер новой группы:", parse_mode="HTML")
            return
        elif command == '/reset':
            await cmd_reset(message, state)
        elif command == '/beta' and message.from_user.id == BETA_TESTER_ID:
            await cmd_beta(message)
        elif command == '/cancel':
            await message.answer(
                f"{emoji('info')} Ввод группы отменен.\n"
                f"Используй /group чтобы выбрать группу",
                parse_mode="HTML"
            )
        else:
            await message.answer(
                f"{emoji('error')} Неизвестная команда.\n"
                f"Используй /help для списка команд",
                parse_mode="HTML"
            )
        return
    
    group_input = message.text.strip().upper()
    
    if not all_groups_cache:
        await message.answer(f"{emoji('search')} Загружаю список групп, подожди секунду...", parse_mode="HTML")
        await load_all_groups()
    
    if group_input in all_groups_cache:
        group_info = all_groups_cache[group_input]
        old_settings = await get_user_settings(message.from_user.id)
        
        await save_user_settings(
            message.from_user.id,
            group_info['faculty_id'],
            group_info['faculty_name'],
            group_info['group_id'],
            group_input
        )
        
        if old_settings:
            text = (
                f"{emoji('success')} <b>Группа успешно изменена!</b>\n\n"
                f"{emoji('faculty')} {group_info['faculty_name']}, гр. {group_input}\n\n"
                f"Теперь ты будешь получать расписание для новой группы."
            )
            
            today = datetime.now().date()
            task_key = f"{message.from_user.id}_{today}"
            if task_key in reminder_tasks:
                reminder_tasks[task_key].cancel()
                del reminder_tasks[task_key]
            
            now = datetime.now(LOCAL_TIMEZONE)
            if now.hour < 23:
                await schedule_reminders_for_user(
                    message.from_user.id,
                    group_info['faculty_id'],
                    group_info['group_id'],
                    today
                )
            
        else:
            text = (
                f"{emoji('success')} <b>Регистрация завершена!</b>\n\n"
                f"{emoji('faculty')} {group_info['faculty_name']}, гр. {group_input}\n\n"
                f"{emoji('calendar')} <b>Что дальше?</b>\n"
                f"{emoji('dot')} Каждое утро в 6:00 я буду присылать расписание\n"
                f"{emoji('dot')} За 20 минут до пары придет напоминание\n\n"
                f"<code>/help</code> — помощь"
            )
            
            await asyncio.sleep(1)
            
            today_msg = await generate_daily_message(message.from_user.id, datetime.now().date())
            if today_msg:
                await message.answer(today_msg, parse_mode="HTML")
            else:
                await message.answer(f"{emoji('calendar')} На сегодня пар нет", parse_mode="HTML")
            
            await schedule_reminders_for_user(
                message.from_user.id,
                group_info['faculty_id'],
                group_info['group_id'],
                datetime.now().date()
            )
        
        await message.answer(text, parse_mode="HTML")
        await state.clear()
        
    else:
        examples = list(all_groups_cache.keys())[:30]
        examples_text = ", ".join(examples)
        
        await message.answer(
            f"{emoji('error')} Группа '{group_input}' не найдена.\n\n"
            f"<b>Примеры групп:</b>\n"
            f"<code>{examples_text}</code>\n\n"
            f"Попробуй еще раз или введи /group для выбора другой группы:",
            parse_mode="HTML"
        )

# ==================== HEALTH CHECK ДЛЯ ХОСТИНГА ====================
async def handle_health(request):
    """Health check для хостинга"""
    status = f"OK (groups loaded: {groups_loaded})"
    return web.Response(text=status, status=200)

async def run_health_server():
    """Запускает минимальный сервер для health check"""
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/kaithheathcheck", handle_health)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=8080)
    await site.start()
    logger.info(f"✅ Health check сервер запущен на порту 8080")

async def load_groups_background():
    """Загрузка групп в фоне"""
    global all_groups_cache, groups_loaded
    try:
        await load_all_groups()
        groups_loaded = True
        logger.info(f"✅ Все группы загружены в кеш ({len(all_groups_cache)} групп)")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки групп: {e}")

# ==================== ЗАПУСК ====================
async def on_startup():
    global http_session
    http_session = aiohttp.ClientSession()
    await init_db()
    asyncio.create_task(load_groups_background())
    logger.info("✅ HTTP сессия создана")

async def on_shutdown():
    global http_session
    if http_session:
        await http_session.close()
    logger.info("👋 HTTP сессия закрыта")

async def main():
    print("\n" + "="*50)
    print("🚀 ЗАПУСК БОТА (ФИНАЛЬНАЯ ВЕРСИЯ С РАССЫЛКАМИ)")
    print("="*50)
    print("="*50 + "\n")
    
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    asyncio.create_task(run_health_server())
    asyncio.create_task(daily_schedule_sender())
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен")

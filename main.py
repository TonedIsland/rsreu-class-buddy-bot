"""
Telegram Bot для расписания РГРТУ
ФИНАЛЬНАЯ ВЕРСИЯ - РАССЫЛКА ВСЕМ ПОЛЬЗОВАТЕЛЯМ
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
import html
from typing import Optional, Dict, List, Tuple, Any
import pytz
from urllib.parse import urlencode
import os
from dotenv import load_dotenv
from pathlib import Path

# ==================== ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ====================
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

BOT_TOKEN = os.getenv('BOT_TOKEN')
BETA_TESTER_ID = int(os.getenv('BETA_TESTER_ID', '0'))
BROADCAST_MODE = os.getenv('BROADCAST_MODE', 'beta')
SPECIFIC_USER_ID = int(os.getenv('SPECIFIC_USER_ID', '123456789'))

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения! Проверь файл .env")

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def escape_html(text: str) -> str:
    """Экранирует HTML-символы в тексте"""
    return html.escape(text)

# ==================== СОСТОЯНИЯ FSM ====================
class Form(StatesGroup):
    waiting_for_group = State()

class BroadcastStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_media = State()
    waiting_for_time = State()

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
    'time': {'id': '5382146496416196771', 'fallback': '⏰'},
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

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ====================
http_session: Optional[aiohttp.ClientSession] = None
request_timestamps: List[datetime] = []
all_groups_cache: Dict[str, Dict[str, str]] = {}
groups_loaded = False

# ==================== НАСТРОЙКИ ВРЕМЕНИ РАССЫЛКИ ====================
schedule_hour = 6
schedule_minute = 0

# ==================== ОПИСАНИЯ РЕЖИМОВ ====================
mode_desc = {
    "all": "📢 Всем пользователям",
    "beta": "🔬 Только бета-тестеру",
    "specific": "🎯 Конкретному ID"
}

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
    
    logger.info(f"✅ Пользователь {user_id} сохранен: {faculty_name} - {group_name}")

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
    """Удаление настроек пользователя из БД"""
    async with aiosqlite.connect('users.db') as db:
        # Проверяем, есть ли пользователь
        cursor = await db.execute('SELECT COUNT(*) FROM users WHERE user_id = ?', (user_id,))
        count = await cursor.fetchone()
        
        if count[0] == 0:
            logger.info(f"⚠️ Пользователь {user_id} уже отсутствует в БД")
            return
        
        # Удаляем
        await db.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
        await db.commit()
        
        # Проверяем результат
        cursor = await db.execute('SELECT COUNT(*) FROM users WHERE user_id = ?', (user_id,))
        new_count = await cursor.fetchone()
        
        if new_count[0] == 0:
            logger.info(f"✅ Пользователь {user_id} успешно удален из БД")
        else:
            logger.error(f"❌ Ошибка: пользователь {user_id} НЕ УДАЛЕН!")
    
    logger.info(f"✅ Пользователь {user_id} удален")

async def get_all_users() -> List[Tuple[int, str, str]]:
    """Получение ВСЕХ пользователей из БД БЕЗ ИСКЛЮЧЕНИЙ"""
    async with aiosqlite.connect('users.db') as db:
        cursor = await db.execute('''
            SELECT user_id, faculty_id, group_id 
            FROM users
        ''')
        users = await cursor.fetchall()
    
    logger.info(f"📊 ВСЕГО пользователей в БД: {len(users)}")
    return users

async def get_user_count() -> int:
    """Получение количества ВСЕХ пользователей"""
    async with aiosqlite.connect('users.db') as db:
        cursor = await db.execute('SELECT COUNT(*) FROM users')
        count = await cursor.fetchone()
    return count[0] if count else 0

async def deactivate_user(user_id: int):
    """Деактивация пользователя (если заблокировал бота)"""
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

# ==================== ЗАГРУЗКА ГРУПП В ФОНЕ ====================
async def load_groups_for_faculty(faculty_id: str, faculty_name: str):
    """Загружает группы для одного факультета"""
    global all_groups_cache
    url = f"{SCHEDULE_URL}?faculty={faculty_id}&group=&date="
    try:
        html = await fetch_html(url)
        if not html:
            return
        
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
        logger.info(f"✅ Загружено групп для {faculty_name}: {len([g for g in all_groups_cache.values() if g['faculty_name'] == faculty_name])}")
    except Exception as e:
        logger.error(f"Ошибка загрузки групп для {faculty_name}: {e}")

async def load_all_groups_background():
    """Загружает все группы в фоне (запускается после старта бота)"""
    global all_groups_cache, groups_loaded
    all_groups_cache = {}
    
    try:
        html = await fetch_html(SCHEDULE_URL)
        if not html:
            logger.error("❌ Не удалось загрузить главную страницу")
            groups_loaded = True
            return
        
        soup = BeautifulSoup(html, 'html.parser')
        faculty_select = soup.find('select', {'name': 'faculty'})
        if not faculty_select:
            logger.error("❌ Не найден выбор факультета")
            groups_loaded = True
            return
        
        faculties = {}
        for option in faculty_select.find_all('option'):
            faculty_id = option.get('value')
            faculty_name = option.text.strip()
            if faculty_id and faculty_id != '0':
                faculties[faculty_id] = faculty_name
        
        logger.info(f"📚 Найдено факультетов: {len(faculties)}")
        
        for faculty_id, faculty_name in faculties.items():
            await load_groups_for_faculty(faculty_id, faculty_name)
            await asyncio.sleep(1)
        
        groups_loaded = True
        logger.info(f"✅ Все группы загружены в кеш (всего {len(all_groups_cache)} групп)")
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки групп: {e}")
        groups_loaded = True

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
    
    table = soup.find('table')
    if not table:
        logger.error("❌ Таблица не найдена")
        return []
    
    header_row = table.find('tr')
    if not header_row:
        return []
    
    headers = header_row.find_all('th')
    
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
    
    lessons = []
    rows = table.find_all('tr')[1:]
    
    for row_idx, row in enumerate(rows, 1):
        time_cell = row.find('td')
        if not time_cell:
            continue
        
        time_divs = time_cell.find_all('div')
        if len(time_divs) < 2:
            continue
        
        start_time = time_divs[0].get_text(strip=True)
        end_time = time_divs[1].get_text(strip=True)
        
        cells = row.find_all('td')
        if len(cells) <= day_index:
            continue
        
        lesson_cell = cells[day_index]
        
        cell_text = lesson_cell.get_text(strip=True)
        if not cell_text or cell_text == '':
            continue
        
        lesson_info = lesson_cell.find('div')
        if not lesson_info:
            continue
        
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
        
        cell_text = lesson_info.get_text(separator=' ', strip=True)
        
        if type_badge:
            cell_text = cell_text.replace(type_badge.get_text(strip=True), '').strip()
        
        subject = "Предмет"
        
        teacher_link = lesson_info.find('a', href=re.compile(r'/schedule-frame/lecturer'))
        teacher = "Не указан"
        if teacher_link:
            teacher = teacher_link.get_text(strip=True)
            parts = cell_text.split(teacher)[0].strip().rstrip(',')
            if parts:
                subject = parts
        
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
    
    month_rus = {
        1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля', 5: 'мая', 6: 'июня',
        7: 'июля', 8: 'августа', 9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря'
    }
    
    weekday_rus = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье']
    
    day_name = weekday_rus[target_date.weekday()].capitalize()
    month_name = month_rus[target_date.month]
    
    message_parts = []
    
    message_parts.append(f"{emoji('calendar')} <b>{day_name}, {target_date.day} {month_name} | {settings['faculty_name']}, гр. {settings['group_name']}</b>")
    message_parts.append("")
    
    for lesson in lessons:
        lesson_type_short = {
            'лекция': 'лек',
            'практика': 'пр',
            'лабораторная': 'лаб'
        }.get(lesson['type'], lesson['type'])
        
        message_parts.append(f"<b>{lesson['number']}-я пара:</b> <code>{lesson['start']} – {lesson['end']}</code> — <b>{lesson['subject']} ({lesson_type_short})</b>")
        message_parts.append(f"Ауд. {lesson['audience']} • {lesson['teacher']}")
        message_parts.append("")
    
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

# ==================== ОСНОВНАЯ ФУНКЦИЯ РАССЫЛКИ ====================
async def send_daily_schedule():
    """Отдельная функция для отправки расписания"""
    try:
        now = datetime.now(LOCAL_TIMEZONE)
        schedule_date = now.date()
        weekday = now.weekday()
        weekday_names = ['пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс']
        
        logger.info("="*60)
        logger.info(f"📅 ДАТА РАССЫЛКИ: {schedule_date}, день недели: {weekday_names[weekday]}")
        
        users = await get_all_users()
        logger.info(f"📨 НАЧИНАЮ РАССЫЛКУ {len(users)} ПОЛЬЗОВАТЕЛЯМ")
        
        if not users:
            logger.info("📭 НЕТ ПОЛЬЗОВАТЕЛЕЙ ДЛЯ РАССЫЛКИ")
            logger.info("="*60)
            return
        
        success = 0
        skip = 0
        fail = 0
        
        for user_id, faculty_id, group_id in users:
            try:
                logger.info(f"👤 Обрабатываю пользователя {user_id}")
                message = await generate_daily_message(user_id, schedule_date)
                
                if message:
                    await bot.send_message(user_id, message, parse_mode="HTML")
                    await schedule_reminders_for_user(user_id, faculty_id, group_id, schedule_date)
                    success += 1
                    logger.info(f"✅ Отправлено пользователю {user_id}")
                else:
                    skip += 1
                    logger.info(f"⏭️ У пользователя {user_id} нет пар на сегодня")
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                fail += 1
                if "bot was blocked" in str(e).lower():
                    await deactivate_user(user_id)
                    logger.info(f"🔇 Пользователь {user_id} заблокировал бота, деактивирован")
                else:
                    logger.error(f"❌ Ошибка для пользователя {user_id}: {e}")
        
        logger.info(f"📊 ИТОГО: ✅ {success} отправлено, ⏭️ {skip} пропущено, ❌ {fail} ошибок")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА В send_daily_schedule: {e}")
        logger.exception(e)

# ==================== ФОНОВАЯ ЗАДАЧА РАССЫЛКИ ====================
async def daily_schedule_sender():
    """Ежедневная рассылка расписания в заданное время"""
    global schedule_hour, schedule_minute
    logger.info("🔥🔥🔥 ФОНОВАЯ ЗАДАЧА РАССЫЛКИ ЗАПУЩЕНА 🔥🔥🔥")
    
    last_run_date = None
    
    while True:
        try:
            now = datetime.now(LOCAL_TIMEZONE)
            
            # Проверяем, совпали ли часы и минуты
            is_time_to_send = (now.hour == schedule_hour and now.minute == schedule_minute)
            
            if is_time_to_send:
                # Проверяем, делали ли мы уже рассылку сегодня
                if last_run_date != now.date():
                    logger.info("="*60)
                    logger.info(f"⏰⏰⏰ ПРОСНУЛСЯ! НАЧИНАЮ РАССЫЛКУ В {schedule_hour:02d}:{schedule_minute:02d}! ⏰⏰⏰")
                    logger.info("="*60)
                    
                    await send_daily_schedule()
                    
                    last_run_date = now.date()
                    logger.info("="*60)
            
            # Спим 30 секунд
            await asyncio.sleep(30)
                
        except asyncio.CancelledError:
            logger.error("❌ ЗАДАЧА РАССЫЛКИ БЫЛА ОТМЕНЕНА!")
            break
        except Exception as e:
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА В daily_schedule_sender: {e}")
            logger.exception(e)
            logger.info("🔄 Перезапуск задачи через 60 секунд...")
            await asyncio.sleep(60)

# ==================== ДИАГНОСТИЧЕСКИЕ КОМАНДЫ ====================
@dp.message(Command("debug_time"))
async def cmd_debug_time(message: types.Message):
    """Показать текущее время бота и настройки рассылки"""
    if message.from_user.id != BETA_TESTER_ID:
        return
    
    now = datetime.now(LOCAL_TIMEZONE)
    users = await get_all_users()
    
    text = (
        f"{emoji('time')} <b>Диагностика времени:</b>\n\n"
        f"🕒 Время бота: {escape_html(now.strftime('%H:%M:%S'))}\n"
        f"📅 Дата: {escape_html(now.strftime('%d.%m.%Y'))} (день {now.weekday()})\n"
        f"🌍 Часовой пояс: {escape_html(str(LOCAL_TIMEZONE))}\n\n"
        f"⏰ <b>Настройки рассылки:</b>\n"
        f"Время: {schedule_hour:02d}:{schedule_minute:02d}\n"
        f"Сегодня {'выходной' if now.weekday() >= 5 else 'будний'}\n\n"
        f"👥 <b>Пользователи:</b>\n"
        f"Всего: {len(users)}"
    )
    
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("force_send"))
async def cmd_force_send(message: types.Message):
    """Принудительно отправить расписание всем (только в установленное время)"""
    if message.from_user.id != BETA_TESTER_ID:
        return
    
    now = datetime.now(LOCAL_TIMEZONE)
    current_time = now.strftime('%H:%M')
    schedule_time = f"{schedule_hour:02d}:{schedule_minute:02d}"
    
    if current_time != schedule_time:
        await message.answer(
            f"{emoji('error')} <b>Нельзя запустить рассылку сейчас!</b>\n\n"
            f"🕒 Текущее время: {escape_html(current_time)}\n"
            f"⏰ Время рассылки: {escape_html(schedule_time)}\n\n"
            f"Команда работает только в {schedule_time}",
            parse_mode="HTML"
        )
        return
    
    await message.answer(
        f"{emoji('broadcast')} <b>Время совпадает! Запускаю рассылку...</b>",
        parse_mode="HTML"
    )
    
    await send_daily_schedule()
    
    await message.answer(
        f"{emoji('success')} Рассылка завершена!",
        parse_mode="HTML"
    )

@dp.message(Command("check_user"))
async def cmd_check_user(message: types.Message):
    """Проверить расписание для конкретного пользователя"""
    if message.from_user.id != BETA_TESTER_ID:
        return
    
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Используй: /check_user [user_id]")
        return
    
    try:
        user_id = int(args[1])
        settings = await get_user_settings(user_id)
        
        if not settings:
            await message.answer(f"{emoji('error')} Пользователь {escape_html(str(user_id))} не найден в БД", parse_mode="HTML")
            return
        
        today = datetime.now(LOCAL_TIMEZONE).date()
        lessons = await parse_daily_schedule(
            settings['faculty_id'],
            settings['group_id'],
            today,
            use_cache=False
        )
        
        text = (
            f"{emoji('group')} <b>Пользователь {escape_html(str(user_id))}</b>\n\n"
            f"{emoji('faculty')} {escape_html(settings['faculty_name'])}, гр. {escape_html(settings['group_name'])}\n\n"
            f"{emoji('calendar')} <b>Расписание на сегодня:</b>\n"
        )
        
        if lessons:
            for lesson in lessons:
                text += f"\n• {lesson['start']} – {lesson['end']} — {lesson['subject']} ({lesson['type']})"
        else:
            text += f"\n{emoji('dot')} Пар нет"
        
        await message.answer(text, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"{emoji('error')} Ошибка: {escape_html(str(e))}", parse_mode="HTML")

@dp.message(Command("db_all"))
async def cmd_db_all(message: types.Message):
    """Показать ВСЕХ пользователей из БД"""
    if message.from_user.id != BETA_TESTER_ID:
        return
    
    async with aiosqlite.connect('users.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT user_id, faculty_name, group_name, is_active, is_beta_tester 
            FROM users ORDER BY registered_at DESC
        ''')
        users = await cursor.fetchall()
    
    text = f"{emoji('list')} <b>ВСЕ пользователи в БД:</b>\n\n"
    
    for u in users:
        status = "✅" if u['is_active'] else "❌"
        beta = "🔬" if u['is_beta_tester'] else "👤"
        text += f"{beta} {status} ID: {u['user_id']}\n"
        text += f"   {u['faculty_name']} — {u['group_name']}\n\n"
    
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("db_check"))
async def cmd_db_check(message: types.Message):
    """Проверка статуса всех пользователей"""
    if message.from_user.id != BETA_TESTER_ID:
        return
    
    async with aiosqlite.connect('users.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT user_id, faculty_name, group_name, is_active, is_beta_tester 
            FROM users
        ''')
        users = await cursor.fetchall()
    
    text = f"{emoji('list')} <b>Статус пользователей:</b>\n\n"
    
    for u in users:
        status = "✅ АКТИВЕН" if u['is_active'] else "❌ НЕАКТИВЕН"
        beta = "🔬" if u['is_beta_tester'] else "👤"
        text += f"{beta} ID: {u['user_id']} — {status}\n"
        text += f"   {u['faculty_name']} — {u['group_name']}\n\n"
    
    await message.answer(text, parse_mode="HTML")

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
    
    messages = [
        "👋 Привет! Я бот для расписания РГРТУ.\n\nНапиши /start чтобы начать работу!",
        "ℹ️ Ты уже зарегистрирован!\n\n🎓 ФВТ, гр. 430\n\nНапиши /group чтобы сменить группу.",
        "👥 Введи номер новой группы:",
        "✅ Группа успешно изменена!\n\n🎓 ФВТ, гр. 431\n\nТеперь ты будешь получать расписание для новой группы.",
        "✅ Регистрация завершена!\n\n🎓 ФВТ, гр. 430\n\n📅 Что дальше?\n• Каждое утро в 6:00 я буду присылать расписание\n• За 20 минут до пары придет напоминание",
        "❌ Группа '430' не найдена.\n\nПримеры групп:\n520, 520М, 522, 523, 524, 525",
        "ℹ️ Ввод группы отменен.\nИспользуй /start для регистрации.",
        "📚 Все доступные команды:\n\n/start — начать регистрацию\n/help — это сообщение",
        "📚 Все доступные команды:\n\n/start — главное меню\n/group — сменить группу\n/today — расписание на сегодня\n/tomorrow — расписание на завтра\n/settings — настройки\n/reset — сбросить настройки\n/help — это сообщение",
        "⚙️ Твои настройки\n\n🎓 Факультет: ФВТ\n👥 Группа: 430\n\n/group — сменить группу\n/reset — сбросить настройки",
        "✅ Настройки сброшены.\nИспользуй /start для новой регистрации.",
        "❌ Сначала нужно зарегистрироваться!\nНапиши /start чтобы начать.",
        "❌ Неизвестная команда.\nИспользуй /help для списка команд",
        "📅 Пятница, 20 февраля | ФВТ, гр. 430\n\n1-я пара: 08:10 – 09:45 — Метрология (лек)\nАуд. 302 C • доц. Кряков В.Г.\n\n2-я пара: 09:55 – 11:30 — Схемотехника ЭС (лек)\nАуд. 333 C • доц. Копейкин Ю.А.",
        "📅 На сегодня пар нет",
        "⏰ Напоминание!\nЧерез 20 минут, в 11:40, начинается:\n\nЭлектротехника и электроника (лек)\nАуд. 404 C • доц. Копейкин Ю.А."
    ]
    
    for i, msg in enumerate(messages, 1):
        await bot.send_message(user_id, f"<b>Сообщение {i}:</b>\n\n{msg}", parse_mode="HTML")
        await asyncio.sleep(1)
    
    await bot.send_message(user_id, "✅ Все 16 сообщений отправлены! Проверь, как они выглядят.")

# ==================== КОМАНДЫ ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    """Начало работы с ботом"""
    await state.clear()
    
    settings = await get_user_settings(message.from_user.id)
    logger.info(f"👤 Команда /start от пользователя {message.from_user.id} (@{message.from_user.username})")
    
    if settings:
        text = (
            f"{emoji('info')} <b>Ты уже зарегистрирован!</b>\n\n"
            f"{emoji('faculty')} {escape_html(settings['faculty_name'])}, гр. {escape_html(settings['group_name'])}\n\n"
            f"Напиши /group чтобы сменить группу.\n"
            f"/help - помощь"
        )
        await message.answer(text, parse_mode="HTML")
    else:
        await message.answer(
            f"{emoji('welcome')} <b>Привет! Я бот для расписания РГРТУ.</b>\n\n"
            f"Для начала работы <b>введи номер своей группы</b> (например: 430, 520М, ИО1):",
            parse_mode="HTML"
        )
        await state.set_state(Form.waiting_for_group)

@dp.message(Command("help"))
async def cmd_help(message: types.Message, state: FSMContext):
    """Помощь"""
    await state.clear()
    
    settings = await get_user_settings(message.from_user.id)
    
    if settings:
        text = (
            f"{emoji('commands')} <b>Все доступные команды:</b>\n\n"
            f"<code>/start</code> — главное меню\n"
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
            f"<code>/start</code> — начать регистрацию\n"
            f"<code>/help</code> — это сообщение\n\n"
            f"<i>После регистрации станут доступны другие команды.</i>"
        )
    
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("group"))
async def cmd_group(message: types.Message, state: FSMContext):
    """Смена группы (только для зарегистрированных)"""
    settings = await get_user_settings(message.from_user.id)
    
    if not settings:
        await message.answer(
            f"{emoji('info')} <b>Сначала нужно зарегистрироваться!</b>\n\n"
            f"Напиши /start чтобы начать.",
            parse_mode="HTML"
        )
        return
    
    await state.clear()
    await state.set_state(Form.waiting_for_group)
    await message.answer(
        f"{emoji('group')} Введи номер новой группы (сейчас: {escape_html(settings['group_name'])}):",
        parse_mode="HTML"
    )

@dp.message(Command("settings"))
async def cmd_settings(message: types.Message):
    """Настройки пользователя"""
    settings = await get_user_settings(message.from_user.id)
    
    if not settings:
        await message.answer(
            f"{emoji('info')} <b>Сначала нужно зарегистрироваться!</b>\n\n"
            f"Напиши /start чтобы начать.",
            parse_mode="HTML"
        )
        return
    
    text = (
        f"{emoji('settings')} <b>Твои настройки</b>\n\n"
        f"{emoji('faculty')} Факультет: {escape_html(settings['faculty_name'])}\n"
        f"{emoji('group')} Группа: {escape_html(settings['group_name'])}\n\n"
        f"<code>/group</code> — сменить группу\n"
        f"<code>/reset</code> — сбросить настройки"
    )
    
    await message.answer(text, parse_mode="HTML")

@dp.message(Command("today"))
async def cmd_today(message: types.Message):
    """Расписание на сегодня"""
    settings = await get_user_settings(message.from_user.id)
    if not settings:
        await message.answer(
            f"{emoji('info')} <b>Сначала нужно зарегистрироваться!</b>\n\n"
            f"Напиши /start чтобы начать.",
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
    """Расписание на завтра"""
    settings = await get_user_settings(message.from_user.id)
    if not settings:
        await message.answer(
            f"{emoji('info')} <b>Сначала нужно зарегистрироваться!</b>\n\n"
            f"Напиши /start чтобы начать.",
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
    """Сброс настроек и удаление из БД"""
    
    # Сначала проверим, есть ли пользователь в БД
    settings = await get_user_settings(message.from_user.id)
    
    if settings:
        logger.info(f"🗑️ Пользователь {message.from_user.id} найден в БД, удаляем...")
        await delete_user_settings(message.from_user.id)
        logger.info(f"✅ Пользователь {message.from_user.id} удален из БД")
    else:
        logger.info(f"ℹ️ Пользователь {message.from_user.id} не был в БД")
    
    # Очищаем состояние
    await state.clear()
    
    # Отправляем подтверждение
    await message.answer(
        f"{emoji('success')} <b>Настройки сброшены.</b>\n\n"
        f"Ты удален из базы данных.\n"
        f"Используй /start для новой регистрации.",
        parse_mode="HTML"
    )

@dp.message(Command("beta"))
async def cmd_beta(message: types.Message):
    """Панель разработчика"""
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
        [types.InlineKeyboardButton(text="📨 Все сообщения бота", callback_data="beta_all_messages")],
        [types.InlineKeyboardButton(text="⏰ Установить время рассылки", callback_data="beta_set_time")]
    ])
    
    text = (
        f"{emoji('beta')} <b>Панель бета-тестирования</b>\n\n"
        f"Текущий режим: {mode_desc.get(BROADCAST_MODE, 'Неизвестно')}\n"
        f"Пользователей в БД: {await get_user_count()}\n"
        f"Бета-тестер: {BETA_TESTER_ID}\n"
        f"⏰ Время рассылки: {schedule_hour:02d}:{schedule_minute:02d} МСК\n\n"
        f"Выбери действие:"
    )
    
    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")

# ==================== ОБРАБОТЧИКИ БЕТА-КОМАНД ====================
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
        text_lines.append(f"   {escape_html(u['faculty_name'])} — {escape_html(u['group_name'])}\n")
    
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

@dp.callback_query(lambda c: c.data == "beta_broadcast")
async def beta_broadcast(callback: types.CallbackQuery, state: FSMContext):
    """Начало создания рассылки"""
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    await callback.answer()
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="◀️ Назад", callback_data="beta_back")]
    ])
    
    await callback.message.edit_text(
        f"{emoji('broadcast')} <b>Создание рассылки</b>\n\n"
        f"Отправь мне текст сообщения для рассылки.\n"
        f"Ты можешь использовать <b>HTML-теги</b> и <b>кастомные эмодзи</b>.",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await state.set_state(BroadcastStates.waiting_for_text)

@dp.callback_query(lambda c: c.data == "beta_set_time")
async def beta_set_time(callback: types.CallbackQuery, state: FSMContext):
    """Ручная установка времени рассылки"""
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="6:00", callback_data="time_preset_6_0")],
        [types.InlineKeyboardButton(text="7:00", callback_data="time_preset_7_0")],
        [types.InlineKeyboardButton(text="8:00", callback_data="time_preset_8_0")],
        [types.InlineKeyboardButton(text="9:00", callback_data="time_preset_9_0")],
        [types.InlineKeyboardButton(text="10:00", callback_data="time_preset_10_0")],
        [types.InlineKeyboardButton(text="✏️ Ввести вручную", callback_data="time_manual")],
        [types.InlineKeyboardButton(text="◀️ Назад", callback_data="beta_back")]
    ])
    
    await callback.message.edit_text(
        f"{emoji('time')} <b>Установка времени рассылки</b>\n\n"
        f"Текущее время: {schedule_hour:02d}:{schedule_minute:02d} МСК\n\n"
        f"Выбери предустановленное время или введи своё:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(lambda c: c.data == "time_manual")
async def time_manual(callback: types.CallbackQuery, state: FSMContext):
    """Ручной ввод времени"""
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="◀️ Назад", callback_data="beta_set_time")]
    ])
    
    await callback.message.edit_text(
        f"{emoji('time')} <b>Ручной ввод времени</b>\n\n"
        f"Введи время в формате <b>ЧЧ:ММ</b> (например, 06:30, 14:15, 23:45)\n\n"
        f"Текущее время: {schedule_hour:02d}:{schedule_minute:02d} МСК",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    
    await state.set_state(BroadcastStates.waiting_for_time)

@dp.message(BroadcastStates.waiting_for_time)
async def process_time_input(message: types.Message, state: FSMContext):
    """Обработка введенного времени"""
    global schedule_hour, schedule_minute
    
    if message.from_user.id != BETA_TESTER_ID:
        await message.answer(f"{emoji('error')} Ты не разработчик", parse_mode="HTML")
        await state.clear()
        return
    
    time_input = message.text.strip()
    
    time_pattern = re.compile(r'^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$')
    match = time_pattern.match(time_input)
    
    if not match:
        await message.answer(
            f"{emoji('error')} <b>Неверный формат!</b>\n\n"
            f"Введи время в формате <b>ЧЧ:ММ</b>\n"
            f"Например: 06:30, 14:15, 23:45",
            parse_mode="HTML"
        )
        return
    
    hour = int(match.group(1))
    minute = int(match.group(2))
    
    schedule_hour = hour
    schedule_minute = minute
    
    await message.answer(
        f"{emoji('success')} <b>Время рассылки установлено на {hour:02d}:{minute:02d} МСК!</b>",
        parse_mode="HTML"
    )
    
    await state.clear()
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📊 Статистика", callback_data="beta_stats")],
        [types.InlineKeyboardButton(text="📢 Сделать рассылку", callback_data="beta_broadcast")],
        [types.InlineKeyboardButton(text="🧪 Тестовая рассылка всем", callback_data="beta_broadcast_all")],
        [types.InlineKeyboardButton(text="🧪 Тестовая рассылка мне", callback_data="beta_broadcast_me")],
        [types.InlineKeyboardButton(text="📋 Список пользователей", callback_data="beta_users")],
        [types.InlineKeyboardButton(text="📨 Все сообщения бота", callback_data="beta_all_messages")],
        [types.InlineKeyboardButton(text="⏰ Установить время рассылки", callback_data="beta_set_time")]
    ])
    
    text = (
        f"{emoji('beta')} <b>Панель бета-тестирования</b>\n\n"
        f"Текущий режим: {mode_desc.get(BROADCAST_MODE, 'Неизвестно')}\n"
        f"Пользователей в БД: {await get_user_count()}\n"
        f"Бета-тестер: {BETA_TESTER_ID}\n"
        f"⏰ Время рассылки: {schedule_hour:02d}:{schedule_minute:02d} МСК\n\n"
        f"Выбери действие:"
    )
    
    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")

@dp.callback_query(lambda c: c.data.startswith("time_preset_"))
async def time_preset(callback: types.CallbackQuery):
    """Установка предустановленного времени"""
    global schedule_hour, schedule_minute
    
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    time_str = callback.data.replace("time_preset_", "")
    hour, minute = map(int, time_str.split('_'))
    
    schedule_hour = hour
    schedule_minute = minute
    
    await callback.answer(f"✅ Время установлено на {hour:02d}:{minute:02d}")
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📊 Статистика", callback_data="beta_stats")],
        [types.InlineKeyboardButton(text="📢 Сделать рассылку", callback_data="beta_broadcast")],
        [types.InlineKeyboardButton(text="🧪 Тестовая рассылка всем", callback_data="beta_broadcast_all")],
        [types.InlineKeyboardButton(text="🧪 Тестовая рассылка мне", callback_data="beta_broadcast_me")],
        [types.InlineKeyboardButton(text="📋 Список пользователей", callback_data="beta_users")],
        [types.InlineKeyboardButton(text="📨 Все сообщения бота", callback_data="beta_all_messages")],
        [types.InlineKeyboardButton(text="⏰ Установить время рассылки", callback_data="beta_set_time")]
    ])
    
    text = (
        f"{emoji('beta')} <b>Панель бета-тестирования</b>\n\n"
        f"Текущий режим: {mode_desc.get(BROADCAST_MODE, 'Неизвестно')}\n"
        f"Пользователей в БД: {await get_user_count()}\n"
        f"Бета-тестер: {BETA_TESTER_ID}\n"
        f"⏰ Время рассылки: {schedule_hour:02d}:{schedule_minute:02d} МСК\n\n"
        f"Выбери действие:"
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "beta_back")
async def beta_back(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в главное меню бета-панели"""
    if callback.from_user.id != BETA_TESTER_ID:
        await callback.answer(f"{emoji('error')} Недостаточно прав", parse_mode="HTML")
        return
    
    await state.clear()
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📊 Статистика", callback_data="beta_stats")],
        [types.InlineKeyboardButton(text="📢 Сделать рассылку", callback_data="beta_broadcast")],
        [types.InlineKeyboardButton(text="🧪 Тестовая рассылка всем", callback_data="beta_broadcast_all")],
        [types.InlineKeyboardButton(text="🧪 Тестовая рассылка мне", callback_data="beta_broadcast_me")],
        [types.InlineKeyboardButton(text="📋 Список пользователей", callback_data="beta_users")],
        [types.InlineKeyboardButton(text="📨 Все сообщения бота", callback_data="beta_all_messages")],
        [types.InlineKeyboardButton(text="⏰ Установить время рассылки", callback_data="beta_set_time")]
    ])
    
    text = (
        f"{emoji('beta')} <b>Панель бета-тестирования</b>\n\n"
        f"Текущий режим: {mode_desc.get(BROADCAST_MODE, 'Неизвестно')}\n"
        f"Пользователей в БД: {await get_user_count()}\n"
        f"Бета-тестер: {BETA_TESTER_ID}\n"
        f"⏰ Время рассылки: {schedule_hour:02d}:{schedule_minute:02d} МСК\n\n"
        f"Выбери действие:"
    )
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")

# ==================== ОБРАБОТЧИКИ РАССЫЛКИ ====================
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
        f"{emoji('info')} <b>Текст сохранён.</b>\n\n"
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
    else:
        return
    
    await state.update_data(media_file_id=media_file_id, media_type=media_type)
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="✅ Отправить всем", callback_data="broadcast_send_all")],
        [types.InlineKeyboardButton(text="🔬 Только бета-тестеру", callback_data="broadcast_send_beta")],
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel")],
        [types.InlineKeyboardButton(text="◀️ Назад", callback_data="beta_broadcast")]
    ])
    
    await message.answer(
        f"{emoji('success')} <b>Медиа получено!</b>\n\n"
        f"<b>Текст рассылки:</b>\n{escape_html(broadcast_text)}\n\n"
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
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel")],
        [types.InlineKeyboardButton(text="◀️ Назад", callback_data="beta_broadcast")]
    ])
    
    await callback.message.edit_text(
        f"{emoji('success')} <b>Текст готов!</b>\n\n"
        f"<b>Текст рассылки:</b>\n{escape_html(broadcast_text)}\n\n"
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
    
    users = await get_all_users()
    
    await callback.message.edit_text(
        f"{emoji('broadcast')} <b>Начинаю рассылку {len(users)} пользователям...</b>\n\n"
        f"<b>Текст:</b>\n{escape_html(broadcast_text)}",
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
        f"{emoji('success')} <b>Рассылка завершена!</b>\n\n"
        f"✅ Успешно: {success}\n"
        f"❌ Ошибок: {fail}",
        parse_mode="HTML"
    )
    
    await state.clear()

# ==================== ОБРАБОТЧИК ВВОДА ГРУППЫ ====================
@dp.message(Form.waiting_for_group)
async def process_group_input(message: types.Message, state: FSMContext):
    """Обработка введенного номера группы"""
    
    logger.info(f"👤 Пользователь {message.from_user.id} вводит группу: {message.text}")
    
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
            await cmd_start(message, state)
            return
        elif command == '/reset':
            await cmd_reset(message, state)
        elif command == '/beta' and message.from_user.id == BETA_TESTER_ID:
            await cmd_beta(message)
        elif command == '/cancel':
            await message.answer(
                f"{emoji('info')} Ввод группы отменен.\n"
                f"Используй /start для регистрации.",
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
        await message.answer(
            f"{emoji('search')} Группы ещё загружаются, подожди несколько секунд и попробуй ещё раз...",
            parse_mode="HTML"
        )
        return
    
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
            # Смена группы - с кастомными эмодзи
            text = (
                f"{emoji('success')} <b>Группа успешно изменена!</b>\n\n"
                f"{emoji('faculty')} {escape_html(group_info['faculty_name'])}, гр. {escape_html(group_input)}\n\n"
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
            
            await message.answer(text, parse_mode="HTML")
            
        else:
            # Новая регистрация - с кастомными эмодзи
            text = (
                f"{emoji('success')} <b>Регистрация завершена!</b>\n\n"
                f"{emoji('faculty')} {escape_html(group_info['faculty_name'])}, гр. {escape_html(group_input)}\n\n"
                f"{emoji('calendar')} <b>Что дальше?</b>\n"
                f"{emoji('dot')} Каждое утро в 6:00 я буду присылать расписание\n"
                f"{emoji('dot')} За 20 минут до пары придет напоминание"
            )
            
            await message.answer(text, parse_mode="HTML")
            
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
        
        await state.clear()
        logger.info(f"✅ Пользователь {message.from_user.id} зарегистрирован с группой {group_input}")
        
    else:
        examples = list(all_groups_cache.keys())[:30]
        examples_text = ", ".join(examples)
        
        await message.answer(
            f"{emoji('error')} <b>Группа '{escape_html(group_input)}' не найдена.</b>\n\n"
            f"<b>Примеры групп:</b>\n"
            f"{escape_html(examples_text)}\n\n"
            f"Попробуй еще раз или введи /cancel для отмены:",
            parse_mode="HTML"
        )

# ==================== ЗАПУСК ====================
async def on_startup():
    global http_session
    http_session = aiohttp.ClientSession()
    await init_db()
    
    # ЗАПУСКАЕМ ФОНОВЫЕ ЗАДАЧИ ЗДЕСЬ
    asyncio.create_task(load_all_groups_background())
    asyncio.create_task(daily_schedule_sender())
    
    logger.info("✅ HTTP сессия создана")
    logger.info("✅ Загрузка групп запущена в фоне")
    logger.info("🔥 Фоновые задачи запущены")

async def on_shutdown():
    global http_session
    if http_session:
        await http_session.close()
    logger.info("👋 HTTP сессия закрыта")

async def main():
    print("\n" + "="*50)
    print("🚀 ЗАПУСК БОТА (ФИНАЛЬНАЯ ВЕРСИЯ - РАССЫЛКА ВСЕМ)")
    print("="*50)
    print("="*50 + "\n")
    
    now = datetime.now(LOCAL_TIMEZONE)
    logger.info(f"🕒 ВРЕМЯ ЗАПУСКА: {now.strftime('%H:%M:%S')}")
    logger.info(f"📅 ДАТА ЗАПУСКА: {now.date()}, день недели: {now.weekday()}")
    logger.info(f"⏰ УСТАНОВЛЕННОЕ ВРЕМЯ РАССЫЛКИ: {schedule_hour:02d}:{schedule_minute:02d}")
    
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    # Запускаем бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен")
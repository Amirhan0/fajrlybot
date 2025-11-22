import logging
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.error import Conflict, RetryAfter, TimedOut, NetworkError
import requests
from datetime import datetime, timedelta
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import asyncio
import os
import sys
import time
from aiohttp import web, ClientSession, ClientTimeout
from aiohttp.web import Response
from dotenv import load_dotenv
from database import Database
from duas_data import get_duas_by_category, get_all_categories, search_duas

# Загрузка переменных из .env
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class ImprovedKeepAlive:
    """Улучшенный keep-alive механизм для Render"""
    
    def __init__(self, app_url=None):
        self.app_url = app_url or os.getenv('RENDER_EXTERNAL_URL', '')
        self.interval = 5 * 60  # 5 минут
        self.is_running = False
        self.task = None
        
    async def ping_self(self):
        """Пингует внешний URL приложения"""
        if not self.app_url:
            return False
            
        health_url = f"{self.app_url.rstrip('/')}/health"
        
        try:
            timeout = ClientTimeout(total=10)
            async with ClientSession(timeout=timeout) as session:
                async with session.get(health_url) as response:
                    if response.status == 200:
                        logger.info(f"✅ Keep-alive ping успешен [{datetime.now().strftime('%H:%M:%S')}]")
                        return True
                    else:
                        logger.warning(f"⚠️ Keep-alive ping вернул {response.status}")
                        return False
        except asyncio.TimeoutError:
            logger.warning("⏱️ Keep-alive ping timeout")
            return False
        except Exception as e:
            logger.debug(f"Keep-alive ping ошибка: {e}")
            return False
    
    async def keep_alive_loop(self):
        """Основной цикл keep-alive"""
        if not self.app_url:
            logger.warning("⚠️ RENDER_EXTERNAL_URL не установлен - keep-alive отключен")
            return
            
        logger.info(f"🔄 Keep-alive запущен (интервал: {self.interval // 60} мин)")
        logger.info(f"📍 Target URL: {self.app_url}/health")
        
        await asyncio.sleep(30)
        
        self.is_running = True
        consecutive_failures = 0
        
        while self.is_running:
            try:
                success = await self.ping_self()
                
                if success:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        logger.error(f"🚨 {consecutive_failures} неудачных пингов подряд!")
                
                await asyncio.sleep(self.interval)
                
            except asyncio.CancelledError:
                logger.info("🛑 Keep-alive остановлен")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в keep-alive loop: {e}")
                await asyncio.sleep(60)
    
    def start(self):
        """Запустить keep-alive"""
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.keep_alive_loop())
            return self.task
        return self.task
    
    async def stop(self):
        """Остановить keep-alive"""
        self.is_running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass


class IslamicBot:
    def __init__(self, token):
        self.token = token
        self.scheduler = AsyncIOScheduler(timezone=pytz.timezone('Asia/Almaty'))
        self.app = None
        self.db = Database()
        self.http_server = None
        self.keep_alive = ImprovedKeepAlive()
        
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start"""
        user_id = update.effective_user.id
        username = update.effective_user.username
        first_name = update.effective_user.first_name
        
        await self.db.add_user(user_id, username, first_name)
        
        keyboard = [
            [KeyboardButton("🕌 Время намаза"), KeyboardButton("📿 Дуа")],
            [KeyboardButton("📖 Аят дня"), KeyboardButton("📊 Статистика")],
            [KeyboardButton("🕌 Найти мечеть"), KeyboardButton("⚙️ Настройки")]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        welcome_message = (
            "Ассаляму алейкум! ☪️\n\n"
            "Добро пожаловать в исламского бота-помощника.\n\n"
            "Я помогу вам:\n"
            "• Узнавать время намазов\n"
            "• Получать напоминания о молитвах\n"
            "• Читать ежедневные аяты и хадисы\n"
            "• Изучать дуа\n"
            "• Отслеживать статистику намазов\n\n"
            "Для начала укажите ваш город командой:\n"
            "/setcity Алматы"
        )
        
        await update.message.reply_text(welcome_message, reply_markup=reply_markup)

    def get_prayer_times_sync(self, city, country):
        """Синхронное получение времени намазов через Aladhan API"""
        try:
            url = f"http://api.aladhan.com/v1/timingsByCity"
            params = {
                'city': city,
                'country': country,
                'method': 2
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data['code'] == 200:
                timings = data['data']['timings']
                return {
                    'Фаджр': timings['Fajr'],
                    'Восход': timings['Sunrise'],
                    'Зухр': timings['Dhuhr'],
                    'Аср': timings['Asr'],
                    'Магриб': timings['Maghrib'],
                    'Иша': timings['Isha']
                }
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении времени намазов: {e}")
            return None

    async def get_prayer_times(self, city, country):
        """Асинхронная обёртка для получения времени намазов"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get_prayer_times_sync, city, country)

    async def prayer_times_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда для показа времени намазов"""
        user_id = update.effective_user.id
        user = await self.db.get_user(user_id)
        
        if not user or not user.get('city'):
            await update.message.reply_text(
                "Пожалуйста, сначала укажите ваш город:\n/setcity Алматы"
            )
            return
        
        city = user['city']
        country = user['country']
        
        await self.db.update_last_active(user_id)
        await update.message.reply_text("⏳ Получаю время намазов...")
        
        times = await self.get_prayer_times(city, country)
        
        if times:
            message = f"🕌 Время намазов для {city}:\n\n"
            for prayer, time in times.items():
                message += f"{prayer}: {time}\n"
            
            next_prayer = self.get_next_prayer(times)
            if next_prayer:
                message += f"\n⏰ Следующий намаз: {next_prayer}"
            
            await update.message.reply_text(message)
        else:
            await update.message.reply_text(
                "❌ Не удалось получить время намазов. Проверьте правильность названия города."
            )

    def get_next_prayer(self, times):
        """Определение следующего намаза"""
        now = datetime.now()
        prayer_names = ['Фаджр', 'Зухр', 'Аср', 'Магриб', 'Иша']
        
        next_prayer = None
        min_time_diff = None
        
        for prayer in prayer_names:
            prayer_time_str = times.get(prayer, '')
            if not prayer_time_str:
                continue
            
            try:
                prayer_hour, prayer_minute = map(int, prayer_time_str.split(':'))
                prayer_datetime = now.replace(hour=prayer_hour, minute=prayer_minute, second=0, microsecond=0)
                
                if prayer_datetime <= now:
                    prayer_datetime += timedelta(days=1)
                
                time_diff = (prayer_datetime - now).total_seconds()
                
                if min_time_diff is None or time_diff < min_time_diff:
                    min_time_diff = time_diff
                    next_prayer = (prayer, prayer_time_str, prayer_datetime)
            except (ValueError, AttributeError):
                continue
        
        if next_prayer:
            prayer_name, prayer_time_str, prayer_datetime = next_prayer
            if prayer_datetime.date() > now.date():
                return f"{prayer_name} в {prayer_time_str} (завтра)"
            else:
                return f"{prayer_name} в {prayer_time_str}"
        
        return None

    async def set_city(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Установка города пользователя"""
        user_id = update.effective_user.id
        
        if context.args:
            city = ' '.join(context.args)
            await self.set_user_city(user_id, city, update)
            return
        
        keyboard = [
            [
                InlineKeyboardButton("🏙 Алматы", callback_data="set_city_Almaty"),
                InlineKeyboardButton("🏛 Астана", callback_data="set_city_Astana")
            ],
            [
                InlineKeyboardButton("🌊 Шымкент", callback_data="set_city_Shymkent"),
                InlineKeyboardButton("🏭 Караганда", callback_data="set_city_Karaganda")
            ],
            [
                InlineKeyboardButton("🌉 Актобе", callback_data="set_city_Aktobe"),
                InlineKeyboardButton("🏔 Тараз", callback_data="set_city_Taraz")
            ],
            [
                InlineKeyboardButton("🌆 Павлодар", callback_data="set_city_Pavlodar"),
                InlineKeyboardButton("🏘 Усть-Каменогорск", callback_data="set_city_Oskemen")
            ],
            [
                InlineKeyboardButton("✏️ Ввести другой город", callback_data="set_city_input")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "🏙 Выберите ваш город или введите другой:",
            reply_markup=reply_markup
        )
    
    async def set_user_city(self, user_id, city, update_or_query):
        """Установить город пользователя"""
        city_mapping = {
            'алматы': 'Almaty', 'алмата': 'Almaty',
            'астана': 'Astana', 'нур-султан': 'Astana',
            'шымкент': 'Shymkent', 'караганда': 'Karaganda',
            'актобе': 'Aktobe', 'тараз': 'Taraz',
            'павлодар': 'Pavlodar', 'усть-каменогорск': 'Oskemen',
            'oskemen': 'Oskemen', 'almaty': 'Almaty',
            'astana': 'Astana', 'shymkent': 'Shymkent',
            'karaganda': 'Karaganda', 'aktobe': 'Aktobe',
            'taraz': 'Taraz', 'pavlodar': 'Pavlodar'
        }
        
        city_lower = city.lower().strip()
        normalized_city = city_mapping.get(city_lower, city)
        country = "Kazakhstan"
        
        await self.db.update_user_city(user_id, normalized_city, country)
        
        message = (
            f"✅ Город установлен: {normalized_city}\n\n"
            f"Теперь вы можете узнать время намазов, нажав на кнопку '🕌 Время намаза'"
        )
        
        if hasattr(update_or_query, 'edit_message_text'):
            await update_or_query.answer()
            await update_or_query.edit_message_text(message)
        else:
            await update_or_query.message.reply_text(message)
        
        await self.schedule_prayer_notifications(user_id, normalized_city, country)

    async def schedule_prayer_notifications(self, user_id, city, country):
        """Планирование напоминаний о намазах"""
        times = await self.get_prayer_times(city, country)
        
        if not times:
            return
        
        for job in self.scheduler.get_jobs():
            if str(user_id) in job.id:
                job.remove()
        
        prayers = {
            'Фаджр': times['Фаджр'],
            'Зухр': times['Зухр'],
            'Аср': times['Аср'],
            'Магриб': times['Магриб'],
            'Иша': times['Иша']
        }
        
        for prayer_name, prayer_time in prayers.items():
            hour, minute = map(int, prayer_time.split(':'))
            
            self.scheduler.add_job(
                self.send_prayer_notification,
                CronTrigger(hour=hour, minute=minute),
                args=[user_id, prayer_name],
                id=f"prayer_{user_id}_{prayer_name}",
                replace_existing=True
            )

    async def send_prayer_notification(self, user_id, prayer_name):
        """Отправка напоминания о намазе"""
        user = await self.db.get_user(user_id)
        if user and user.get('notifications_enabled'):
            message = f"🕌 Время {prayer_name}!\n\nАллаху Акбар! Пришло время молитвы."
            try:
                await self.app.bot.send_message(chat_id=user_id, text=message)
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления: {e}")

    async def daily_ayah(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Аят дня"""
        await update.message.reply_text(
            "📖 Аят дня:\n\n"
            "\"О те, которые уверовали! Обращайтесь за помощью к терпению и молитве. "
            "Воистину, Аллах - с терпеливыми.\"\n\n"
            "(Сура Аль-Бакара, 2:153)"
        )

    async def daily_dua(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Меню с категориями дуа"""
        categories = get_all_categories()
        
        keyboard = []
        row = []
        for cat_key, cat_name in categories.items():
            row.append(InlineKeyboardButton(cat_name, callback_data=f"dua_cat_{cat_key}"))
            if len(row) == 2:
                keyboard.append(row)
                row = []
        
        if row:
            keyboard.append(row)
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "📿 Выберите категорию дуа:\n\n"
            "Здесь собраны дуа на все случаи жизни",
            reply_markup=reply_markup
        )
    
    async def show_dua_category(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать дуа из категории"""
        query = update.callback_query
        await query.answer()
        
        category = query.data.replace("dua_cat_", "")
        duas = get_duas_by_category(category)
        
        if not duas:
            await query.edit_message_text("Дуа в этой категории скоро будут добавлены")
            return
        
        dua = duas[0]
        message = self.format_dua(dua)
        
        keyboard = []
        if len(duas) > 1:
            keyboard.append([
                InlineKeyboardButton("Следующее ➡️", callback_data=f"dua_{category}_1")
            ])
        keyboard.append([InlineKeyboardButton("◀️ Назад к категориям", callback_data="dua_menu")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(message, reply_markup=reply_markup)
    
    def format_dua(self, dua):
        """Форматирование дуа для отправки"""
        return (
            f"📿 {dua['title']}\n\n"
            f"🕋 {dua['arabic']}\n\n"
            f"📝 {dua.get('transcription', '')}\n\n"
            f"💬 {dua['translation']}"
        )

    async def islamic_calendar(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Исламский календарь"""
        await update.message.reply_text(
            "📅 Важные исламские даты:\n\n"
            "🌙 Рамадан 1446: ~28 февраля 2025\n"
            "🎉 Ид аль-Фитр: ~30 марта 2025\n"
            "🕋 Ид аль-Адха: ~6 июня 2025\n"
            "📖 День Арафат: ~5 июня 2025\n"
            "🌟 Исра и Мирадж: ~27 января 2025"
        )

    async def show_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать статистику намазов"""
        user_id = update.effective_user.id
        stats = await self.db.get_prayer_stats(user_id, days=30)
        
        if not stats:
            keyboard = [
                [
                    InlineKeyboardButton("🌅 Фаджр", callback_data="mark_prayer_Фаджр"),
                    InlineKeyboardButton("☀️ Зухр", callback_data="mark_prayer_Зухр")
                ],
                [
                    InlineKeyboardButton("🌤 Аср", callback_data="mark_prayer_Аср"),
                    InlineKeyboardButton("🌆 Магриб", callback_data="mark_prayer_Магриб")
                ],
                [
                    InlineKeyboardButton("🌙 Иша", callback_data="mark_prayer_Иша")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                "📊 У вас пока нет статистики намазов.\n\n"
                "Выберите намаз, который вы выполнили:",
                reply_markup=reply_markup
            )
            return
        
        completed = [s for s in stats if s['completed']]
        completed_count = len(completed)
        total_count = len(stats)
        percentage = (completed_count/total_count*100) if total_count > 0 else 0
        
        streak = await self.calculate_streak(user_id)
        
        prayer_counts = {}
        for stat in completed:
            prayer_name = stat['prayer_name']
            prayer_counts[prayer_name] = prayer_counts.get(prayer_name, 0) + 1
        
        message = f"📊 СТАТИСТИКА НАМАЗОВ\n\n"
        message += f"📅 Период: последние 30 дней\n\n"
        message += f"✅ Выполнено: {completed_count} из {total_count}\n"
        message += f"📈 Процент: {percentage:.1f}%\n"
        message += f"🔥 Streak: {streak} дней подряд\n\n"
        
        message += "📊 По намазам:\n"
        for prayer, count in sorted(prayer_counts.items()):
            message += f"  {prayer}: {count}\n"
        
        message += "\n📈 Последние 7 дней:\n"
        last_7_days = await self.get_last_7_days_chart(user_id)
        message += last_7_days
        
        keyboard = [
            [
                InlineKeyboardButton("🌅 Фаджр", callback_data="mark_prayer_Фаджр"),
                InlineKeyboardButton("☀️ Зухр", callback_data="mark_prayer_Зухр")
            ],
            [
                InlineKeyboardButton("🌤 Аср", callback_data="mark_prayer_Аср"),
                InlineKeyboardButton("🌆 Магриб", callback_data="mark_prayer_Магриб")
            ],
            [
                InlineKeyboardButton("🌙 Иша", callback_data="mark_prayer_Иша")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(message, reply_markup=reply_markup)
    
    async def calculate_streak(self, user_id):
        """Подсчёт дней подряд с выполненными намазами"""
        streak = 0
        current_date = datetime.now().date()
        
        while True:
            stats = await self.db.get_prayer_stats(user_id, days=1)
            day_stats = [s for s in stats if str(s['prayer_date']) == str(current_date) and s['completed']]
            
            if day_stats:
                streak += 1
                current_date -= timedelta(days=1)
            else:
                break
            
            if streak > 100:
                break
        
        return streak
    
    async def get_last_7_days_chart(self, user_id):
        """Текстовый график последних 7 дней"""
        chart = ""
        
        for i in range(6, -1, -1):
            date = datetime.now().date() - timedelta(days=i)
            date_str = date.strftime("%d.%m")
            
            stats = await self.db.get_prayer_stats(user_id, days=7)
            day_stats = [s for s in stats if str(s['prayer_date']) == str(date) and s['completed']]
            
            completed_count = len(day_stats)
            bars = "█" * completed_count + "░" * (5 - completed_count)
            chart += f"{date_str} {bars} {completed_count}/5\n"
        
        return chart
    
    async def mark_prayer_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отметить намаз как выполненный"""
        user_id = update.effective_user.id
        
        if context.args:
            prayer_name = ' '.join(context.args)
            valid_prayers = ['Фаджр', 'Зухр', 'Аср', 'Магриб', 'Иша']
            if prayer_name in valid_prayers:
                await self.mark_prayer_completed(user_id, prayer_name, update)
                return
        
        keyboard = [
            [
                InlineKeyboardButton("🌅 Фаджр", callback_data="mark_prayer_Фаджр"),
                InlineKeyboardButton("☀️ Зухр", callback_data="mark_prayer_Зухр")
            ],
            [
                InlineKeyboardButton("🌤 Аср", callback_data="mark_prayer_Аср"),
                InlineKeyboardButton("🌆 Магриб", callback_data="mark_prayer_Магриб")
            ],
            [
                InlineKeyboardButton("🌙 Иша", callback_data="mark_prayer_Иша")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "📿 Выберите намаз, который вы выполнили:",
            reply_markup=reply_markup
        )
    
    async def mark_prayer_completed(self, user_id, prayer_name, update_or_query):
        """Отметить намаз как выполненный"""
        await self.db.mark_prayer_completed(user_id, prayer_name)
        
        streak = await self.calculate_streak(user_id)
        
        message = f"✅ {prayer_name} отмечен как выполненный!\n\n"
        
        if streak > 0:
            message += f"🔥 Ваш streak: {streak} дней подряд!"
            
            if streak == 7:
                message += "\n🎉 Отлично! Целая неделя!"
            elif streak == 30:
                message += "\n🌟 Машаллах! Целый месяц!"
            elif streak == 100:
                message += "\n👑 Невероятно! 100 дней подряд!"
        
        if hasattr(update_or_query, 'edit_message_text'):
            await update_or_query.answer()
            await update_or_query.edit_message_text(message)
        else:
            await update_or_query.message.reply_text(message)

    async def find_mosques(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Найти ближайшие мечети"""
        user_id = update.effective_user.id
        user = await self.db.get_user(user_id)
        
        if not user or not user.get('city'):
            await update.message.reply_text(
                "Сначала укажите ваш город:\n/setcity Алматы"
            )
            return
        
        city = user['city']
        
        await update.message.reply_text("🔍 Ищу мечети рядом...")
        
        mosques = await self.search_mosques_nominatim(city, user.get('country', 'Kazakhstan'))
        
        if not mosques:
            await update.message.reply_text(
                f"❌ Не удалось найти мечети в городе {city}.\n\n"
                f"Попробуйте указать более крупный город или воспользуйтесь картами."
            )
            return
        
        message = f"🕌 Мечети в городе {city}:\n\n"
        
        for i, mosque in enumerate(mosques[:10], 1):
            name = mosque.get('name', 'Мечеть')
            address = mosque.get('address', 'Адрес не указан')
            lat = mosque.get('lat')
            lon = mosque.get('lon')
            
            message += f"{i}. {name}\n"
            message += f"   📍 {address}\n"
            
            if lat and lon:
                message += f"   🗺 [Показать на карте](https://www.google.com/maps?q={lat},{lon})\n"
            
            message += "\n"
        
        message += "\n⏰ Время джума-намаза обычно после Зухр намаза\n"
        message += "📞 Уточняйте точное время в конкретной мечети"
        
        await update.message.reply_text(message, parse_mode='Markdown', disable_web_page_preview=True)
    
    async def search_mosques_nominatim(self, city, country):
        """Поиск мечетей через OpenStreetMap"""
        try:
            query = f"""
            [out:json];
            area["name"="{city}"]->.city;
            (
              node["amenity"="place_of_worship"]["religion"="muslim"](area.city);
              way["amenity"="place_of_worship"]["religion"="muslim"](area.city);
            );
            out center;
            """
            
            url = "https://overpass-api.de/api/interpreter"
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: requests.post(url, data={'data': query}, timeout=10)
            )
            
            if response.status_code == 200:
                data = response.json()
                mosques = []
                
                for element in data.get('elements', []):
                    mosque = {
                        'name': element.get('tags', {}).get('name', 'Мечеть'),
                        'lat': element.get('lat') or element.get('center', {}).get('lat'),
                        'lon': element.get('lon') or element.get('center', {}).get('lon'),
                        'address': element.get('tags', {}).get('addr:street', '')
                    }
                    mosques.append(mosque)
                
                return mosques
            
            return []
        except Exception as e:
            logger.error(f"Ошибка поиска мечетей: {e}")
            return []

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка сообщений с кнопок"""
        text = update.message.text
        
        if text == "🕌 Время намаза":
            await self.prayer_times_command(update, context)
        elif text == "📖 Аят дня":
            await self.daily_ayah(update, context)
        elif text == "📿 Дуа":
            await self.daily_dua(update, context)
        elif text == "📅 Исламский календарь":
            await self.islamic_calendar(update, context)
        elif text == "📊 Статистика":
            await self.show_stats(update, context)
        elif text == "🕌 Найти мечеть":
            await self.find_mosques(update, context)
        elif text == "⚙️ Настройки":
            await update.message.reply_text(
                "⚙️ Настройки:\n\n"
                "/setcity - Изменить город\n"
                "/notifications - Включить/выключить уведомления\n"
                "/stats - Статистика намазов\n"
                "/markprayer - Отметить выполненный намаз"
            )
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка нажатий на inline кнопки"""
        query = update.callback_query
        
        if query.data.startswith("dua_cat_"):
            await self.show_dua_category(update, context)
        elif query.data == "dua_menu":
            categories = get_all_categories()
            keyboard = []
            row = []
            for cat_key, cat_name in categories.items():
                row.append(InlineKeyboardButton(cat_name, callback_data=f"dua_cat_{cat_key}"))
                if len(row) == 2:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                "📿 Выберите категорию дуа:\n\n"
                "Здесь собраны дуа на все случаи жизни",
                reply_markup=reply_markup
            )
        elif query.data.startswith("mark_prayer_"):
            prayer_name = query.data.replace("mark_prayer_", "")
            user_id = update.effective_user.id
            await self.mark_prayer_completed(user_id, prayer_name, query)
        elif query.data.startswith("set_city_"):
            city_data = query.data.replace("set_city_", "")
            user_id = update.effective_user.id
            
            if city_data == "input":
                await query.answer()
                await query.edit_message_text(
                    "✏️ Введите название города:\n\n"
                    "Например: Алматы, Астана, Шымкент и т.д.\n\n"
                    "Или используйте команду: /setcity [название города]"
                )
            else:
                await self.set_user_city(user_id, city_data, query)

    async def toggle_notifications(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Включение/выключение уведомлений"""
        user_id = update.effective_user.id
        
        user = await self.db.get_user(user_id)
        if not user:
            await update.message.reply_text("Сначала укажите город: /setcity Алматы")
            return
        
        new_state = await self.db.toggle_notifications(user_id)
        
        if new_state is not None:
            status = "включены" if new_state else "выключены"
            await update.message.reply_text(f"✅ Уведомления {status}")

    async def health_check_handler(self, request):
        """Обработчик health check запросов"""
        return Response(text="OK", status=200)
    
    async def start_http_server(self):
        """Запуск HTTP сервера для health check"""
        try:
            port = int(os.getenv('PORT', 8080))
            app = web.Application()
            app.router.add_get('/', self.health_check_handler)
            app.router.add_get('/health', self.health_check_handler)
            app.router.add_get('/healtz', self.health_check_handler)
            
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, '0.0.0.0', port)
            await site.start()
            
            self.http_server = runner
            logger.info(f"✅ HTTP сервер запущен на 0.0.0.0:{port}")
            logger.info(f"📍 Health check endpoints: /, /health, /healtz")
        except Exception as e:
            logger.error(f"❌ Ошибка при запуске HTTP сервера: {e}")
    
    async def post_init(self, application: Application) -> None:
        """Инициализация после запуска"""
        try:
            await application.bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook успешно очищен")
        except Exception as e:
            logger.warning(f"ℹ️ Очистка webhook: {e}")
        
        self.keep_alive.start()
        
        self.scheduler.start()
        logger.info("✅ Планировщик запущен!")
    
    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обработчик ошибок"""
        logger.error(f"Ошибка при обработке обновления: {context.error}")
        
        if isinstance(context.error, Conflict):
            logger.warning("Обнаружен конфликт: другой экземпляр бота уже запущен.")
            try:
                await asyncio.sleep(5)
                await self.app.bot.delete_webhook(drop_pending_updates=True)
                logger.info("Webhook очищен после конфликта")
            except Exception as e:
                logger.error(f"Не удалось очистить webhook: {e}")
        elif isinstance(context.error, RetryAfter):
            logger.warning(f"Превышен лимит запросов. Повтор через {context.error.retry_after} секунд")
        elif isinstance(context.error, (TimedOut, NetworkError)):
            logger.warning("Ошибка сети. Бот продолжит работу.")
        else:
            logger.error(f"Необработанная ошибка: {context.error}", exc_info=context.error)

    async def start_services(self):
        """Запуск HTTP сервера и БД перед ботом"""
        logger.info("🌐 Запуск HTTP сервера...")
        await self.start_http_server()
        logger.info("💾 Инициализация базы данных...")
        await self.db.init_db()
        logger.info("✅ Сервисы запущены!")
    
    def run(self):
        """Запуск бота"""
        logger.info("=" * 60)
        logger.info("🤖 ЗАПУСК ИСЛАМСКОГО БОТА")
        logger.info("=" * 60)
        
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        logger.info("⚡ Запуск критически важных сервисов...")
        loop.run_until_complete(self.start_services())
        
        try:
            logger.info("🔐 Инициализация Telegram бота...")
            
            self.app = Application.builder().token(self.token).post_init(self.post_init).build()
            
            self.app.add_handler(CommandHandler("start", self.start))
            self.app.add_handler(CommandHandler("setcity", self.set_city))
            self.app.add_handler(CommandHandler("prayer", self.prayer_times_command))
            self.app.add_handler(CommandHandler("notifications", self.toggle_notifications))
            self.app.add_handler(CommandHandler("stats", self.show_stats))
            self.app.add_handler(CommandHandler("markprayer", self.mark_prayer_handler))
            self.app.add_handler(CommandHandler("mosques", self.find_mosques))
            self.app.add_handler(CallbackQueryHandler(self.handle_callback))
            self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
            
            self.app.add_error_handler(self.error_handler)
            
            logger.info("🚀 Запуск polling...")
            
            self.app.run_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
                close_loop=False,
                stop_signals=None
            )
            
        except Exception as e:
            logger.error("=" * 60)
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            logger.error("=" * 60)
            
            if "Logged out" in str(e) or "Bad Request" in str(e):
                logger.error("🔑 ПРОБЛЕМА С ТОКЕНОМ!")
                logger.error("")
                logger.error("📋 ЧТО ДЕЛАТЬ:")
                logger.error("   1. Откройте @BotFather в Telegram")
                logger.error("   2. Отправьте /mybots")
                logger.error("   3. Выберите вашего бота")
                logger.error("   4. Нажмите 'API Token'")
                logger.error("   5. Нажмите 'Revoke current token'")
                logger.error("   6. Скопируйте новый токен")
                logger.error("   7. Обновите BOT_TOKEN на Render")
                logger.error("")
                logger.error("⚠️ HTTP сервер продолжает работать для health check")
                logger.error("=" * 60)
                
                logger.info("💓 Бот не запущен, но HTTP сервер работает...")
                try:
                    while True:
                        time.sleep(60)
                        logger.info("💓 HTTP сервер активен...")
                except KeyboardInterrupt:
                    logger.info("👋 Остановка приложения...")
            else:
                logger.error(f"⚠️ Неожиданная ошибка: {e}")
                logger.info("🔄 HTTP сервер продолжает работать")
                try:
                    while True:
                        time.sleep(60)
                        logger.info("💓 HTTP сервер активен...")
                except KeyboardInterrupt:
                    logger.info("👋 Остановка приложения...")


if __name__ == '__main__':
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    RENDER_URL = os.getenv('RENDER_EXTERNAL_URL')
    
    if not BOT_TOKEN:
        raise ValueError("❌ BOT_TOKEN не найден в переменных окружения!")
    
    if not RENDER_URL:
        logger.warning("=" * 60)
        logger.warning("⚠️  ВНИМАНИЕ: RENDER_EXTERNAL_URL не установлен!")
        logger.warning("📝 Добавьте в Environment Variables на Render:")
        logger.warning("   RENDER_EXTERNAL_URL=https://your-app-name.onrender.com")
        logger.warning("🔄 Keep-alive не будет работать без этой переменной")
        logger.warning("💡 Бот будет засыпать через 15 минут неактивности")
        logger.warning("=" * 60)
    else:
        logger.info(f"✅ RENDER_EXTERNAL_URL установлен: {RENDER_URL}")
    
    bot = IslamicBot(BOT_TOKEN)
    bot.run()
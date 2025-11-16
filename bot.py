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

class IslamicBot:
    def __init__(self, token):
        self.token = token
        self.scheduler = AsyncIOScheduler(timezone=pytz.timezone('Asia/Almaty'))
        self.app = None
        self.db = Database()
        self.http_server = None
        self.keep_alive_task = None
        
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start"""
        user_id = update.effective_user.id
        username = update.effective_user.username
        first_name = update.effective_user.first_name
        
        # Добавляем пользователя в БД
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
                'method': 2  # ISNA метод расчета
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
        
        # Получаем пользователя из БД
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
            
            # Определяем следующий намаз
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
        
        # Проверяем каждый намаз и находим ближайший
        for prayer in prayer_names:
            prayer_time_str = times.get(prayer, '')
            if not prayer_time_str:
                continue
            
            try:
                prayer_hour, prayer_minute = map(int, prayer_time_str.split(':'))
                prayer_datetime = now.replace(hour=prayer_hour, minute=prayer_minute, second=0, microsecond=0)
                
                # Если намаз уже прошел сегодня, проверяем следующий день
                if prayer_datetime <= now:
                    prayer_datetime += timedelta(days=1)
                
                # Вычисляем разницу во времени
                time_diff = (prayer_datetime - now).total_seconds()
                
                # Если это ближайший намаз, сохраняем его
                if min_time_diff is None or time_diff < min_time_diff:
                    min_time_diff = time_diff
                    next_prayer = (prayer, prayer_time_str, prayer_datetime)
            except (ValueError, AttributeError):
                continue
        
        if next_prayer:
            prayer_name, prayer_time_str, prayer_datetime = next_prayer
            # Если намаз завтра, добавляем пометку
            if prayer_datetime.date() > now.date():
                return f"{prayer_name} в {prayer_time_str} (завтра)"
            else:
                return f"{prayer_name} в {prayer_time_str}"
        
        return None

    async def set_city(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Установка города пользователя"""
        user_id = update.effective_user.id
        
        # Если есть аргументы, обрабатываем напрямую
        if context.args:
            city = ' '.join(context.args)
            await self.set_user_city(user_id, city, update)
            return
        
        # Показываем кнопки с популярными городами
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
        """Установить город пользователя (общая функция)"""
        # Нормализуем название города для API
        city_mapping = {
            'алматы': 'Almaty',
            'алмата': 'Almaty',
            'астана': 'Astana',
            'нур-султан': 'Astana',
            'шымкент': 'Shymkent',
            'караганда': 'Karaganda',
            'актобе': 'Aktobe',
            'тараз': 'Taraz',
            'павлодар': 'Pavlodar',
            'усть-каменогорск': 'Oskemen',
            'oskemen': 'Oskemen',
            'almaty': 'Almaty',
            'astana': 'Astana',
            'shymkent': 'Shymkent',
            'karaganda': 'Karaganda',
            'aktobe': 'Aktobe',
            'taraz': 'Taraz',
            'pavlodar': 'Pavlodar'
        }
        
        city_lower = city.lower().strip()
        normalized_city = city_mapping.get(city_lower, city)
        country = "Kazakhstan"
        
        # Сохраняем в БД
        await self.db.update_user_city(user_id, normalized_city, country)
        
        message = (
            f"✅ Город установлен: {normalized_city}\n\n"
            f"Теперь вы можете узнать время намазов, нажав на кнопку '🕌 Время намаза'"
        )
        
        # Отправляем ответ в зависимости от типа обновления
        if hasattr(update_or_query, 'edit_message_text'):
            # Это callback query
            await update_or_query.answer()
            await update_or_query.edit_message_text(message)
        else:
            # Это обычное сообщение
            await update_or_query.message.reply_text(message)
        
        # Планируем напоминания
        await self.schedule_prayer_notifications(user_id, normalized_city, country)

    async def schedule_prayer_notifications(self, user_id, city, country):
        """Планирование напоминаний о намазах"""
        times = await self.get_prayer_times(city, country)
        
        if not times:
            return
        
        # Удаляем старые задачи для этого пользователя
        for job in self.scheduler.get_jobs():
            if str(user_id) in job.id:
                job.remove()
        
        # Создаем новые задачи для каждого намаза
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
        
        # Показываем первое дуа
        dua = duas[0]
        message = self.format_dua(dua)
        
        # Кнопки навигации если дуа больше одного
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
        """Показать расширенную статистику намазов"""
        user_id = update.effective_user.id
        
        # Статистика за последние 30 дней
        stats = await self.db.get_prayer_stats(user_id, days=30)
        
        if not stats:
            # Показываем кнопки для отметки намаза
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
        
        # Подсчёт выполненных намазов
        completed = [s for s in stats if s['completed']]
        completed_count = len(completed)
        total_count = len(stats)
        percentage = (completed_count/total_count*100) if total_count > 0 else 0
        
        # Подсчёт streak (дней подряд)
        streak = await self.calculate_streak(user_id)
        
        # Статистика по намазам
        prayer_counts = {}
        for stat in completed:
            prayer_name = stat['prayer_name']
            prayer_counts[prayer_name] = prayer_counts.get(prayer_name, 0) + 1
        
        # Формируем сообщение
        message = f"📊 СТАТИСТИКА НАМАЗОВ\n\n"
        message += f"📅 Период: последние 30 дней\n\n"
        message += f"✅ Выполнено: {completed_count} из {total_count}\n"
        message += f"📈 Процент: {percentage:.1f}%\n"
        message += f"🔥 Streak: {streak} дней подряд\n\n"
        
        message += "📊 По намазам:\n"
        for prayer, count in sorted(prayer_counts.items()):
            message += f"  {prayer}: {count}\n"
        
        # График последних 7 дней
        message += "\n📈 Последние 7 дней:\n"
        last_7_days = await self.get_last_7_days_chart(user_id)
        message += last_7_days
        
        # Кнопки
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
            # Проверяем, был ли хотя бы один намаз в этот день
            stats = await self.db.get_prayer_stats(user_id, days=1)
            day_stats = [s for s in stats if str(s['prayer_date']) == str(current_date) and s['completed']]
            
            if day_stats:
                streak += 1
                current_date -= timedelta(days=1)
            else:
                break
            
            if streak > 100:  # Ограничение для производительности
                break
        
        return streak
    
    async def get_last_7_days_chart(self, user_id):
        """Текстовый график последних 7 дней"""
        chart = ""
        
        for i in range(6, -1, -1):
            date = datetime.now().date() - timedelta(days=i)
            date_str = date.strftime("%d.%m")
            
            # Получаем намазы за этот день
            stats = await self.db.get_prayer_stats(user_id, days=7)
            day_stats = [s for s in stats if str(s['prayer_date']) == str(date) and s['completed']]
            
            completed_count = len(day_stats)
            
            # Рисуем график
            bars = "█" * completed_count + "░" * (5 - completed_count)
            chart += f"{date_str} {bars} {completed_count}/5\n"
        
        return chart
    
    async def mark_prayer_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отметить намаз как выполненный"""
        user_id = update.effective_user.id
        
        # Если есть аргументы, обрабатываем напрямую
        if context.args:
            prayer_name = ' '.join(context.args)
            valid_prayers = ['Фаджр', 'Зухр', 'Аср', 'Магриб', 'Иша']
            if prayer_name in valid_prayers:
                await self.mark_prayer_completed(user_id, prayer_name, update)
                return
        
        # Показываем кнопки для выбора намаза
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
        """Отметить намаз как выполненный (общая функция)"""
        # Отмечаем в БД
        await self.db.mark_prayer_completed(user_id, prayer_name)
        
        # Обновляем streak
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
        
        # Отправляем ответ в зависимости от типа обновления
        if hasattr(update_or_query, 'edit_message_text'):
            # Это callback query
            await update_or_query.answer()
            await update_or_query.edit_message_text(message)
        else:
            # Это обычное сообщение
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
        """Поиск мечетей через OpenStreetMap Nominatim"""
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
            # Обработка отметки намаза
            prayer_name = query.data.replace("mark_prayer_", "")
            user_id = update.effective_user.id
            await self.mark_prayer_completed(user_id, prayer_name, query)
        elif query.data.startswith("set_city_"):
            # Обработка выбора города
            city_data = query.data.replace("set_city_", "")
            user_id = update.effective_user.id
            
            if city_data == "input":
                # Пользователь хочет ввести город вручную
                await query.answer()
                await query.edit_message_text(
                    "✏️ Введите название города:\n\n"
                    "Например: Алматы, Астана, Шымкент и т.д.\n\n"
                    "Или используйте команду: /setcity [название города]"
                )
            else:
                # Город выбран из списка
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
            
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, '0.0.0.0', port)
            await site.start()
            
            self.http_server = runner
            logger.info(f"HTTP сервер запущен на порту {port}")
        except Exception as e:
            logger.error(f"Ошибка при запуске HTTP сервера: {e}")
    
    async def keep_alive_ping(self):
        """Keep-alive механизм - пинг самого себя каждые 10 минут"""
        try:
            port = int(os.getenv('PORT', 8080))
            url = f"http://localhost:{port}/health"
            
            while True:
                await asyncio.sleep(600)  # 10 минут
                try:
                    async with ClientSession() as session:
                        async with session.get(url, timeout=ClientTimeout(total=5)) as response:
                            if response.status == 200:
                                logger.debug("Keep-alive ping успешен")
                except Exception as e:
                    logger.warning(f"Keep-alive ping не удался: {e}")
        except Exception as e:
            logger.error(f"Ошибка в keep-alive механизме: {e}")
    
    async def post_init(self, application: Application) -> None:
        """Инициализация после запуска"""
        # Очистка webhook перед запуском polling с несколькими попытками
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await application.bot.delete_webhook(drop_pending_updates=True)
                logger.info("Webhook успешно очищен перед запуском polling")
                break
            except Conflict as e:
                logger.warning(f"Конфликт при очистке webhook (попытка {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
                else:
                    logger.error("Не удалось очистить webhook после всех попыток")
            except Exception as e:
                logger.warning(f"Ошибка при очистке webhook (попытка {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
        
        # Запускаем HTTP сервер для health check
        await self.start_http_server()
        
        # Запускаем keep-alive механизм
        self.keep_alive_task = asyncio.create_task(self.keep_alive_ping())
        
        await self.db.init_db()
        self.scheduler.start()
        logger.info("База данных и планировщик запущены!")
    
    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Обработчик ошибок"""
        logger.error(f"Ошибка при обработке обновления: {context.error}")
        
        if isinstance(context.error, Conflict):
            logger.warning("Обнаружен конфликт: другой экземпляр бота уже запущен. "
                          "Убедитесь, что запущен только один экземпляр.")
            # Пытаемся очистить webhook и подождать перед повторной попыткой
            try:
                await asyncio.sleep(5)
                await self.app.bot.delete_webhook(drop_pending_updates=True)
                logger.info("Webhook очищен после конфликта")
            except Exception as e:
                logger.error(f"Не удалось очистить webhook после конфликта: {e}")
        elif isinstance(context.error, RetryAfter):
            logger.warning(f"Превышен лимит запросов. Повтор через {context.error.retry_after} секунд")
        elif isinstance(context.error, (TimedOut, NetworkError)):
            logger.warning("Ошибка сети. Бот продолжит работу.")
        else:
            logger.error(f"Необработанная ошибка: {context.error}", exc_info=context.error)

    def run(self):
        """Запуск бота"""
        self.app = Application.builder().token(self.token).post_init(self.post_init).build()
        
        # Регистрация обработчиков
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("setcity", self.set_city))
        self.app.add_handler(CommandHandler("prayer", self.prayer_times_command))
        self.app.add_handler(CommandHandler("notifications", self.toggle_notifications))
        self.app.add_handler(CommandHandler("stats", self.show_stats))
        self.app.add_handler(CommandHandler("markprayer", self.mark_prayer_handler))
        self.app.add_handler(CommandHandler("mosques", self.find_mosques))
        self.app.add_handler(CallbackQueryHandler(self.handle_callback))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        # Регистрация обработчика ошибок
        self.app.add_error_handler(self.error_handler)
        
        logger.info("Бот запущен!")
        # Используем run_polling с дополнительными параметрами для надежности
        try:
            self.app.run_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
                close_loop=False,
                stop_signals=None  # Отключаем обработку сигналов для Render
            )
        except Conflict as e:
            logger.error(f"Критический конфликт при запуске polling: {e}")
            logger.info("Попытка повторного запуска через 10 секунд...")
            time.sleep(10)
            # Повторная попытка
            self.app.run_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
                close_loop=False,
                stop_signals=None
            )

if __name__ == '__main__':
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN не найден в .env файле!")
    
    bot = IslamicBot(BOT_TOKEN)
    bot.run()
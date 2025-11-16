import logging
from datetime import datetime
import os

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.pool = None
        # Для локальной разработки используй DATABASE_URL из .env
        # Для продакшена Railway/Render автоматически установит DATABASE_URL
        self.database_url = os.getenv('DATABASE_URL')
        
        # Fallback на SQLite если нет PostgreSQL (для локальной разработки)
        if not self.database_url:
            logger.warning("DATABASE_URL не найден, используется SQLite")
            self.use_sqlite = True
        else:
            self.use_sqlite = False
            # Импортируем asyncpg только если нужен PostgreSQL
            try:
                import asyncpg
                self.asyncpg = asyncpg
            except ImportError:
                logger.warning("asyncpg не установлен, переключаемся на SQLite")
                self.use_sqlite = True
    
    async def init_db(self):
        """Инициализация базы данных"""
        if self.use_sqlite:
            await self._init_sqlite()
        else:
            await self._init_postgres()
    
    async def _init_postgres(self):
        """Инициализация PostgreSQL"""
        try:
            # Создаём пул соединений
            self.pool = await self.asyncpg.create_pool(
                self.database_url,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
            
            async with self.pool.acquire() as conn:
                # Таблица пользователей
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        username TEXT,
                        first_name TEXT,
                        city TEXT,
                        country TEXT,
                        timezone TEXT DEFAULT 'Asia/Almaty',
                        notifications_enabled BOOLEAN DEFAULT TRUE,
                        language TEXT DEFAULT 'ru',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Таблица для статистики намазов
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS prayer_stats (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                        prayer_name TEXT NOT NULL,
                        prayer_date DATE NOT NULL,
                        completed BOOLEAN DEFAULT FALSE,
                        completed_at TIMESTAMP,
                        UNIQUE(user_id, prayer_name, prayer_date)
                    )
                ''')
                
                # Таблица хадисов (можно добавить свои)
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS hadiths (
                        id SERIAL PRIMARY KEY,
                        text_ru TEXT NOT NULL,
                        text_ar TEXT,
                        source TEXT,
                        category TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Таблица дуа
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS duas (
                        id SERIAL PRIMARY KEY,
                        title TEXT NOT NULL,
                        arabic TEXT NOT NULL,
                        transcription TEXT,
                        translation TEXT NOT NULL,
                        category TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Таблица избранного
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS favorites (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                        content_type TEXT NOT NULL,
                        content_id INTEGER NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_id, content_type, content_id)
                    )
                ''')
                
                # Таблица для отслеживания чтения Корана
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS quran_progress (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT REFERENCES users(user_id) ON DELETE CASCADE,
                        surah_number INTEGER NOT NULL,
                        ayah_number INTEGER NOT NULL,
                        read_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_id, surah_number, ayah_number)
                    )
                ''')
                
                logger.info("PostgreSQL база данных инициализирована")
        except Exception as e:
            logger.error(f"Ошибка инициализации PostgreSQL: {e}")
            raise
    
    async def _init_sqlite(self):
        """Fallback на SQLite для локальной разработки"""
        import aiosqlite
        async with aiosqlite.connect('islamic_bot.db') as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    city TEXT,
                    country TEXT,
                    timezone TEXT DEFAULT 'Asia/Almaty',
                    notifications_enabled BOOLEAN DEFAULT 1,
                    language TEXT DEFAULT 'ru',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS prayer_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    prayer_name TEXT,
                    prayer_date DATE,
                    completed BOOLEAN DEFAULT 0,
                    completed_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            await db.commit()
            logger.info("SQLite база данных инициализирована")
    
    async def add_user(self, user_id, username=None, first_name=None):
        """Добавление нового пользователя"""
        if self.use_sqlite:
            import aiosqlite
            async with aiosqlite.connect('islamic_bot.db') as db:
                await db.execute('''
                    INSERT OR IGNORE INTO users (user_id, username, first_name)
                    VALUES (?, ?, ?)
                ''', (user_id, username, first_name))
                await db.commit()
        else:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO users (user_id, username, first_name)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (user_id) DO NOTHING
                ''', user_id, username, first_name)
    
    async def update_user_city(self, user_id, city, country):
        """Обновление города пользователя"""
        if self.use_sqlite:
            import aiosqlite
            async with aiosqlite.connect('islamic_bot.db') as db:
                await db.execute('''
                    UPDATE users 
                    SET city = ?, country = ?, last_active = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                ''', (city, country, user_id))
                await db.commit()
        else:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    UPDATE users 
                    SET city = $1, country = $2, last_active = CURRENT_TIMESTAMP
                    WHERE user_id = $3
                ''', city, country, user_id)
    
    async def get_user(self, user_id):
        """Получение данных пользователя"""
        if self.use_sqlite:
            import aiosqlite
            async with aiosqlite.connect('islamic_bot.db') as db:
                db.row_factory = aiosqlite.Row
                async with db.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        return dict(row)
                    return None
        else:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
                if row:
                    return dict(row)
                return None
    
    async def toggle_notifications(self, user_id):
        """Переключение уведомлений"""
        if self.use_sqlite:
            import aiosqlite
            async with aiosqlite.connect('islamic_bot.db') as db:
                async with db.execute('SELECT notifications_enabled FROM users WHERE user_id = ?', (user_id,)) as cursor:
                    row = await cursor.fetchone()
                    if row:
                        new_state = not bool(row[0])
                        await db.execute('UPDATE users SET notifications_enabled = ? WHERE user_id = ?', (new_state, user_id))
                        await db.commit()
                        return new_state
        else:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow('SELECT notifications_enabled FROM users WHERE user_id = $1', user_id)
                if row:
                    new_state = not row['notifications_enabled']
                    await conn.execute('UPDATE users SET notifications_enabled = $1 WHERE user_id = $2', new_state, user_id)
                    return new_state
        return None
    
    async def mark_prayer_completed(self, user_id, prayer_name):
        """Отметить намаз как выполненный"""
        today = datetime.now().date()
        if self.use_sqlite:
            import aiosqlite
            async with aiosqlite.connect('islamic_bot.db') as db:
                await db.execute('''
                    INSERT OR REPLACE INTO prayer_stats 
                    (user_id, prayer_name, prayer_date, completed, completed_at)
                    VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
                ''', (user_id, prayer_name, today))
                await db.commit()
        else:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO prayer_stats 
                    (user_id, prayer_name, prayer_date, completed, completed_at)
                    VALUES ($1, $2, $3, TRUE, CURRENT_TIMESTAMP)
                    ON CONFLICT (user_id, prayer_name, prayer_date) 
                    DO UPDATE SET completed = TRUE, completed_at = CURRENT_TIMESTAMP
                ''', user_id, prayer_name, today)
    
    async def get_prayer_stats(self, user_id, days=7):
        """Получить статистику намазов за последние N дней"""
        if self.use_sqlite:
            import aiosqlite
            async with aiosqlite.connect('islamic_bot.db') as db:
                db.row_factory = aiosqlite.Row
                async with db.execute('''
                    SELECT prayer_name, prayer_date, completed
                    FROM prayer_stats
                    WHERE user_id = ? 
                    AND prayer_date >= date('now', '-' || ? || ' days')
                    ORDER BY prayer_date DESC
                ''', (user_id, days)) as cursor:
                    rows = await cursor.fetchall()
                    return [dict(row) for row in rows]
        else:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT prayer_name, prayer_date, completed
                    FROM prayer_stats
                    WHERE user_id = $1 
                    AND prayer_date >= CURRENT_DATE - INTERVAL '%s days'
                    ORDER BY prayer_date DESC
                ''' % days, user_id)
                return [dict(row) for row in rows]
    
    async def update_last_active(self, user_id):
        """Обновить время последней активности"""
        if self.use_sqlite:
            import aiosqlite
            async with aiosqlite.connect('islamic_bot.db') as db:
                await db.execute('UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE user_id = ?', (user_id,))
                await db.commit()
        else:
            async with self.pool.acquire() as conn:
                await conn.execute('UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE user_id = $1', user_id)
    
    async def get_user_count(self):
        """Получить количество пользователей"""
        if self.use_sqlite:
            import aiosqlite
            async with aiosqlite.connect('islamic_bot.db') as db:
                async with db.execute('SELECT COUNT(*) FROM users') as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0
        else:
            async with self.pool.acquire() as conn:
                return await conn.fetchval('SELECT COUNT(*) FROM users')
    
    async def add_dua(self, title, arabic, transcription, translation, category=None):
        """Добавить дуа в базу"""
        if not self.use_sqlite:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO duas (title, arabic, transcription, translation, category)
                    VALUES ($1, $2, $3, $4, $5)
                ''', title, arabic, transcription, translation, category)
    
    async def get_random_dua(self, category=None):
        """Получить случайное дуа"""
        if not self.use_sqlite:
            async with self.pool.acquire() as conn:
                if category:
                    row = await conn.fetchrow('''
                        SELECT * FROM duas WHERE category = $1 
                        ORDER BY RANDOM() LIMIT 1
                    ''', category)
                else:
                    row = await conn.fetchrow('SELECT * FROM duas ORDER BY RANDOM() LIMIT 1')
                
                if row:
                    return dict(row)
        return None
    
    async def save_quran_progress(self, user_id, surah, ayah):
        """Сохранить прогресс чтения Корана"""
        if not self.use_sqlite:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO quran_progress (user_id, surah_number, ayah_number)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (user_id, surah_number, ayah_number) DO NOTHING
                ''', user_id, surah, ayah)
    
    async def get_quran_progress(self, user_id):
        """Получить прогресс чтения Корана"""
        if not self.use_sqlite:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow('''
                    SELECT COUNT(*) as total_ayahs, 
                           MAX(surah_number) as last_surah,
                           MAX(ayah_number) as last_ayah
                    FROM quran_progress
                    WHERE user_id = $1
                ''', user_id)
                if row:
                    return dict(row)
        return None
    
    async def close(self):
        """Закрыть соединение с БД"""
        if not self.use_sqlite and self.pool:
            await self.pool.close()
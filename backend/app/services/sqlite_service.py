"""
Служебная база данных SQLite для хранения метаданных и логов
"""
import sqlite3
import os
import json
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLiteService:
    """Служебная база данных SQLite для метаданных и логов"""
    
    def __init__(self, db_path: str = "/app/data/service.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_database()
    
    def _init_database(self):
        """Инициализация базы данных и создание таблиц"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Таблица для метаданных файлов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    file_type TEXT NOT NULL,
                    upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'uploaded',
                    processing_started TIMESTAMP,
                    processing_completed TIMESTAMP,
                    rows_processed INTEGER DEFAULT 0,
                    error_message TEXT,
                    sink_config TEXT,
                    chunk_size INTEGER DEFAULT 1000
                )
            """)
            
            # Таблица для логов обработки
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processing_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_id INTEGER NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    FOREIGN KEY (file_id) REFERENCES file_metadata (id)
                )
            """)
            
            # Таблица для данных больших файлов (кэш)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS large_file_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_id INTEGER NOT NULL,
                    row_data TEXT NOT NULL,
                    row_index INTEGER NOT NULL,
                    processed BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (file_id) REFERENCES file_metadata (id)
                )
            """)
            
            conn.commit()
    
    def add_file_metadata(self, filename: str, file_path: str, file_size: int, file_type: str) -> int:
        """Добавление метаданных файла"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO file_metadata (filename, file_path, file_size, file_type)
                    VALUES (?, ?, ?, ?)
                """, (filename, file_path, file_size, file_type))
                file_id = cursor.lastrowid
                conn.commit()
                return file_id
    
    def update_file_status(self, file_id: int, status: str, error_message: str = None):
        """Обновление статуса файла"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                if status == 'processing':
                    cursor.execute("""
                        UPDATE file_metadata 
                        SET status = ?, processing_started = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (status, file_id))
                elif status == 'completed':
                    cursor.execute("""
                        UPDATE file_metadata 
                        SET status = ?, processing_completed = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (status, file_id))
                elif status == 'error':
                    cursor.execute("""
                        UPDATE file_metadata 
                        SET status = ?, error_message = ?
                        WHERE id = ?
                    """, (status, error_message, file_id))
                else:
                    cursor.execute("""
                        UPDATE file_metadata 
                        SET status = ?
                        WHERE id = ?
                    """, (status, file_id))
                conn.commit()
    
    def add_processing_log(self, file_id: int, level: str, message: str):
        """Добавление записи в лог обработки"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO processing_logs (file_id, level, message)
                    VALUES (?, ?, ?)
                """, (file_id, level, message))
                conn.commit()
    
    def update_rows_processed(self, file_id: int, rows_processed: int):
        """Обновление количества обработанных строк"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE file_metadata 
                    SET rows_processed = ?
                    WHERE id = ?
                """, (rows_processed, file_id))
                conn.commit()
    
    def get_file_metadata(self, file_id: int) -> Optional[Dict]:
        """Получение метаданных файла"""
        import json
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM file_metadata WHERE id = ?", (file_id,))
            row = cursor.fetchone()
            if row:
                metadata = dict(row)
                # Парсим JSON конфигурацию приёмника
                if metadata.get('sink_config'):
                    try:
                        metadata['sink_config'] = json.loads(metadata['sink_config'])
                    except json.JSONDecodeError:
                        metadata['sink_config'] = None
                return metadata
            return None
    
    def get_processing_logs(self, file_id: int, limit: int = 100) -> List[Dict]:
        """Получение логов обработки для файла"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM processing_logs 
                WHERE file_id = ? 
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (file_id, limit))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_files(self, status: str = None) -> List[Dict]:
        """Получение списка всех файлов"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if status:
                cursor.execute("SELECT * FROM file_metadata WHERE status = ? ORDER BY upload_time DESC", (status,))
            else:
                cursor.execute("SELECT * FROM file_metadata ORDER BY upload_time DESC")
            return [dict(row) for row in cursor.fetchall()]
    
    def cache_row_data(self, file_id: int, row_data: str, row_index: int):
        """Кэширование данных строки"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO large_file_cache (file_id, row_data, row_index)
                    VALUES (?, ?, ?)
                """, (file_id, row_data, row_index))
                conn.commit()
    
    def get_cached_rows(self, file_id: int, limit: int = 1000) -> List[Dict]:
        """Получение кэшированных строк"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM large_file_cache 
                WHERE file_id = ? AND processed = FALSE
                ORDER BY row_index
                LIMIT ?
            """, (file_id, limit))
            return [dict(row) for row in cursor.fetchall()]
    
    def mark_rows_processed(self, file_id: int, row_ids: List[int]):
        """Отметка строк как обработанных"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                placeholders = ','.join(['?' for _ in row_ids])
                cursor.execute(f"""
                    UPDATE large_file_cache 
                    SET processed = TRUE 
                    WHERE id IN ({placeholders})
                """, row_ids)
                conn.commit()
    
    def delete_file_data(self, file_id: int):
        """Удаление всех данных файла"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Удаляем таблицу файла, если она существует
                table_name = f"file_data_{file_id}"
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # Удаляем кэшированные данные
                cursor.execute("DELETE FROM large_file_cache WHERE file_id = ?", (file_id,))
                
                # Удаляем логи
                cursor.execute("DELETE FROM processing_logs WHERE file_id = ?", (file_id,))
                
                # Удаляем метаданные
                cursor.execute("DELETE FROM file_metadata WHERE id = ?", (file_id,))
                
                conn.commit()
    
    def create_file_table(self, file_id: int, headers: List[str]):
        """Создание таблицы для конкретного файла"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                table_name = f"file_data_{file_id}"

                # Создаем безопасные имена колонок
                safe_headers = []
                for header in headers:
                    # Заменяем недопустимые символы на подчеркивания
                    safe_header = header.replace(' ', '_').replace('-', '_').replace('.', '_')
                    # Убираем специальные символы
                    safe_header = ''.join(c for c in safe_header if c.isalnum() or c == '_')
                    # Убеждаемся, что имя не начинается с цифры
                    if safe_header and safe_header[0].isdigit():
                        safe_header = f"col_{safe_header}"
                    # Если имя пустое, используем номер колонки
                    if not safe_header:
                        safe_header = f"column_{len(safe_headers) + 1}"
                    safe_headers.append(safe_header)

                # Создаем SQL для создания таблицы
                columns = ', '.join([f'"{col}" TEXT' for col in safe_headers])
                columns += ', row_index INTEGER PRIMARY KEY'

                logger.info(f"Создаем таблицу {table_name} с колонками: {safe_headers}")

                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {columns}
                    )
                """)

                conn.commit()
                logger.info(f"Создана таблица {table_name} для файла {file_id}")
    
    def insert_file_row(self, file_id: int, headers: List[str], row: List[str], row_index: int):
        """Вставка строки в таблицу файла"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                table_name = f"file_data_{file_id}"

                # Создаем безопасные имена колонок (как в create_file_table)
                safe_headers = []
                for header in headers:
                    safe_header = header.replace(' ', '_').replace('-', '_').replace('.', '_')
                    safe_header = ''.join(c for c in safe_header if c.isalnum() or c == '_')
                    if safe_header and safe_header[0].isdigit():
                        safe_header = f"col_{safe_header}"
                    if not safe_header:
                        safe_header = f"column_{len(safe_headers) + 1}"
                    safe_headers.append(safe_header)

                # Подготавливаем данные для вставки
                values = row + [row_index]
                placeholders = ', '.join(['?' for _ in values])
                columns = ', '.join([f'"{col}"' for col in safe_headers] + ['row_index'])

                try:
                    cursor.execute(f"""
                        INSERT INTO {table_name} ({columns})
                        VALUES ({placeholders})
                    """, values)

                    conn.commit()
                    
                    # Логируем каждую 100-ю вставку
                    if row_index % 100 == 0:
                        logger.info(f"Вставлена строка {row_index} в таблицу {table_name}")
                        
                except Exception as e:
                    logger.error(f"Ошибка при вставке строки {row_index} в таблицу {table_name}: {e}")
                    raise
    
    def get_file_table_data(self, file_id: int, limit: int = 1000, offset: int = 0) -> List[Dict]:
        """Получение данных из таблицы файла"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            table_name = f"file_data_{file_id}"

            # Проверяем, существует ли таблица
            cursor.execute(f"""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='{table_name}'
            """)

            if not cursor.fetchone():
                logger.warning(f"Таблица {table_name} не существует")
                return []

            # Получаем количество строк в таблице
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            logger.info(f"В таблице {table_name} найдено {count} строк")

            # Получаем данные
            cursor.execute(f"""
                SELECT * FROM {table_name}
                ORDER BY row_index
                LIMIT ? OFFSET ?
            """, (limit, offset))

            rows = [dict(row) for row in cursor.fetchall()]
            logger.info(f"Возвращено {len(rows)} строк из таблицы {table_name}")
            return rows
    
    def update_sink_config(self, file_id: int, sink_config: Dict[str, Any]):
        """Обновление конфигурации приёмника для файла"""
        import json
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE file_metadata 
                    SET sink_config = ? 
                    WHERE id = ?
                """, (json.dumps(sink_config), file_id))
                conn.commit()
    
    def update_chunk_size(self, file_id: int, chunk_size: int):
        """Обновление размера чанка для файла"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE file_metadata 
                    SET chunk_size = ? 
                    WHERE id = ?
                """, (chunk_size, file_id))
                conn.commit()

# Глобальный экземпляр сервиса
sqlite_service = SQLiteService()

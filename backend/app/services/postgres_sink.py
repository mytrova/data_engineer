from __future__ import annotations

from typing import Any, Iterable, List, Sequence
from .base import DataSink
from .database_base import PostgreSQLConnection
from .type_analyzer import TypeAnalyzer


class PostgreSQLSink(DataSink):
    """Приёмник данных в PostgreSQL"""
    
    def __init__(self, connection_string: str, table_name: str, mode: str = "append"):
        self.connection_string = connection_string
        self.table_name = table_name
        self.mode = mode  # "append", "replace", "upsert"
        self.connection = PostgreSQLConnection(connection_string)
        self._connected = False
        
        # Подключаемся к базе
        if self.connection.connect():
            self._connected = True
        else:
            raise ConnectionError("Не удалось подключиться к PostgreSQL")
    
    def write(self, headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> Any:
        """Запись данных в таблицу"""
        try:
            from sqlalchemy import create_engine, text
            
            # Преобразуем данные в список словарей
            data = [dict(zip(headers, row)) for row in rows]
            
            # Подключаемся к базе
            engine = create_engine(self.connection_string)
            
            # Анализируем типы данных
            schema = TypeAnalyzer.analyze_dataframe_schema(data, list(headers))
            
            # Если режим replace, создаем таблицу с правильными типами
            if self.mode == "replace":
                # Сначала удаляем таблицу если существует
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {self.table_name}"))
                    conn.commit()
                
                # Создаем таблицу с правильными типами
                self._create_table_with_schema(engine, schema)
                
                # Записываем данные
                self._insert_data(engine, data)
            else:
                # Для append режима используем стандартный метод
                self._insert_data(engine, data)
            
            return {
                "status": "success",
                "table": self.table_name,
                "rows_written": len(data),
                "mode": self.mode,
                "schema_analyzed": True
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "table": self.table_name
            }
    
    def write_chunks(self, headers: Sequence[str], chunks: Iterable[List[Sequence[Any]]]) -> Any:
        """Запись данных чанками"""
        try:
            from sqlalchemy import create_engine, text
            import logging
            
            logger = logging.getLogger(__name__)
            logger.info(f"Начинаем запись данных в таблицу {self.table_name}, режим: {self.mode}")
            
            engine = create_engine(self.connection_string)
            total_rows = 0
            schema_analyzed = False
            chunk_count = 0
            
            logger.info("Начинаем итерацию по чанкам")
            for chunk in chunks:
                chunk_count += 1
                logger.info(f"Обрабатываем чанк {chunk_count}, размер: {len(chunk) if chunk else 0}")
                
                if not chunk:  # Пропускаем пустые чанки
                    logger.info(f"Чанк {chunk_count} пустой, пропускаем")
                    continue
                
                # Преобразуем чанк в список словарей
                chunk_data = [dict(zip(headers, row)) for row in chunk]
                logger.info(f"Чанк {chunk_count} преобразован в {len(chunk_data)} записей")
                
                # Анализируем схему только для первого чанка
                if not schema_analyzed:
                    logger.info(f"Анализируем схему данных для режима {self.mode}")
                    schema = TypeAnalyzer.analyze_dataframe_schema(chunk_data, list(headers))
                    logger.info(f"Проанализированная схема: {schema}")
                    
                    if self.mode == "replace":
                        # Удаляем таблицу если существует
                        with engine.begin() as conn:
                            conn.execute(text(f'DROP TABLE IF EXISTS "{self.table_name}"'))
                        logger.info(f"Таблица {self.table_name} удалена для режима replace")
                    
                    # Создаем таблицу с правильными типами (или проверяем существование)
                    table_exists = self._table_exists(engine)
                    logger.info(f"Таблица {self.table_name} существует: {table_exists}")
                    
                    if not table_exists:
                        logger.info(f"Создаем таблицу {self.table_name} с новой схемой")
                        success = self._create_table_with_schema(engine, schema)
                        if success:
                            logger.info(f"Таблица {self.table_name} успешно создана")
                        else:
                            logger.error(f"Не удалось создать таблицу {self.table_name}")
                            raise Exception(f"Не удалось создать таблицу {self.table_name}")
                    else:
                        logger.info(f"Таблица {self.table_name} уже существует, используем её")
                    
                    schema_analyzed = True
                
                # Записываем чанк
                logger.info(f"Записываем чанк {chunk_count} в базу данных")
                self._insert_data(engine, chunk_data)
                
                total_rows += len(chunk_data)
                logger.info(f"Чанк {chunk_count} записан. Всего строк: {total_rows}")
            
            logger.info(f"Запись завершена. Всего записано строк: {total_rows}")
            return {
                "status": "success",
                "table": self.table_name,
                "rows_written": total_rows,
                "mode": self.mode,
                "schema_analyzed": schema_analyzed
            }
            
        except Exception as e:
            logger.error(f"Ошибка при записи данных в таблицу {self.table_name}: {e}")
            import traceback
            logger.error(f"Детали ошибки: {traceback.format_exc()}")
            return {
                "status": "error",
                "error": str(e),
                "table": self.table_name
            }
    
    def test_connection(self) -> bool:
        """Тестирование подключения"""
        return self.connection.test_connection()
    
    def get_tables(self) -> List[str]:
        """Получение списка таблиц"""
        return self.connection.get_tables()
    
    def _table_exists(self, engine) -> bool:
        """Проверка существования таблицы"""
        try:
            from sqlalchemy import text
            import logging
            
            logger = logging.getLogger(__name__)
            
            with engine.connect() as conn:
                # Проверяем существование таблицы в текущей схеме
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = :table_name
                        AND table_schema = 'public'
                    )
                """), {"table_name": self.table_name})
                
                exists = result.fetchone()[0]
                logger.info(f"Проверка существования таблицы {self.table_name}: {exists}")
                return exists
                
        except Exception as e:
            logger.error(f"Ошибка проверки существования таблицы {self.table_name}: {e}")
            return False
    
    def _create_table_with_schema(self, engine, schema: List[dict]) -> bool:
        """Создание таблицы с правильными типами данных"""
        try:
            from sqlalchemy import text
            import logging
            
            logger = logging.getLogger(__name__)
            
            # Строим SQL для создания таблицы
            columns_sql = []
            for col in schema:
                col_name = col["column_name"]
                col_type = col["data_type"]
                nullable = col.get("nullable", True)
                
                # Определяем тип PostgreSQL
                pg_type = TypeAnalyzer.get_postgresql_type(col)
                
                # Добавляем NULL/NOT NULL
                null_constraint = "NULL" if nullable else "NOT NULL"
                
                columns_sql.append(f'"{col_name}" {pg_type} {null_constraint}')
                logger.info(f"Добавляем колонку: {col_name} {pg_type} {null_constraint}")
            
            # Создаем SQL запрос
            create_sql = f"""
            CREATE TABLE "{self.table_name}" (
                {', '.join(columns_sql)}
            )
            """
            
            logger.info(f"SQL для создания таблицы: {create_sql}")
            
            # Выполняем создание таблицы
            try:
                with engine.begin() as conn:
                    logger.info("Подключение к базе данных установлено")
                    conn.execute(text(create_sql))
                    logger.info("SQL запрос выполнен успешно")
                    logger.info("Транзакция зафиксирована автоматически")
            except Exception as e:
                logger.error(f"Ошибка при выполнении SQL: {e}")
                raise
            
            logger.info(f"Таблица {self.table_name} успешно создана")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка создания таблицы {self.table_name}: {e}")
            import traceback
            logger.error(f"Детали ошибки: {traceback.format_exc()}")
            return False
    
    def create_table(self, table_name: str, schema: List[dict]) -> bool:
        """Создание таблицы по схеме"""
        try:
            from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Text
            from sqlalchemy.types import VARCHAR, INTEGER, TEXT
            
            engine = create_engine(self.connection_string)
            metadata = MetaData()
            
            # Создаем таблицу
            columns = []
            for col in schema:
                col_name = col["column_name"]
                col_type = col["data_type"]
                
                # Маппинг типов PostgreSQL на SQLAlchemy
                if "varchar" in col_type.lower() or "character" in col_type.lower():
                    columns.append(Column(col_name, VARCHAR(255)))
                elif "text" in col_type.lower():
                    columns.append(Column(col_name, TEXT))
                elif "integer" in col_type.lower() or "int" in col_type.lower():
                    columns.append(Column(col_name, INTEGER))
                else:
                    columns.append(Column(col_name, VARCHAR(255)))  # По умолчанию
            
            table = Table(table_name, metadata, *columns)
            metadata.create_all(engine)
            
            return True
            
        except Exception as e:
            print(f"Ошибка создания таблицы {table_name}: {e}")
            return False
    
    def _insert_data(self, engine, data):
        """Вставляет данные в таблицу"""
        from sqlalchemy import text
        import logging
        
        logger = logging.getLogger(__name__)
        
        if not data:
            logger.info("Нет данных для вставки")
            return
        
        # Получаем заголовки из первого элемента
        headers = list(data[0].keys())
        logger.info(f"Вставляем {len(data)} записей в таблицу {self.table_name}")
        logger.info(f"Заголовки: {headers}")
        
        # Создаем SQL запрос для вставки с экранированием имен колонок
        columns = ', '.join([f'"{header}"' for header in headers])
        placeholders = ', '.join([f':{header}' for header in headers])
        insert_sql = f'INSERT INTO "{self.table_name}" ({columns}) VALUES ({placeholders})'
        
        logger.info(f"SQL запрос: {insert_sql}")
        logger.info(f"Первая запись для примера: {data[0] if data else 'Нет данных'}")
        
        # Подготавливаем все данные для пакетной вставки ВНЕ транзакции
        prepared_data = []
        for i, row in enumerate(data):
            try:
                # Убеждаемся, что все ключи в row соответствуют заголовкам
                row_dict = {header: row.get(header) for header in headers}
                
                # Обрабатываем пустые значения для timestamp полей
                for key, value in row_dict.items():
                    if value == "":
                        row_dict[key] = None
                
                prepared_data.append(row_dict)
                
            except Exception as e:
                logger.error(f"Ошибка при подготовке записи {i + 1}: {e}")
                logger.error(f"Данные записи: {row}")
                raise
        
        # Выполняем пакетную вставку в транзакции
        try:
            with engine.begin() as conn:
                conn.execute(text(insert_sql), prepared_data)
                logger.info(f"Все {len(prepared_data)} записей успешно вставлены пакетно")
        except Exception as e:
            logger.error(f"Ошибка при пакетной вставке: {e}")
            # Если пакетная вставка не удалась, пробуем по одной записи в новой транзакции
            logger.info("Пробуем вставить данные по одной записи...")
            for i, row_dict in enumerate(prepared_data):
                try:
                    with engine.begin() as single_conn:
                        single_conn.execute(text(insert_sql), row_dict)
                    if (i + 1) % 100 == 0:
                        logger.info(f"Вставлено {i + 1} записей")
                except Exception as single_e:
                    logger.error(f"Ошибка при вставке записи {i + 1}: {single_e}")
                    raise
            logger.info(f"Все {len(prepared_data)} записей вставлены по одной")
    
    def __del__(self):
        """Закрытие подключения при удалении объекта"""
        if hasattr(self, '_connected') and hasattr(self, 'connection') and self._connected and self.connection:
            self.connection.disconnect()

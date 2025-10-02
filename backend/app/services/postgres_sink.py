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
            
            engine = create_engine(self.connection_string)
            total_rows = 0
            schema_analyzed = False
            
            for chunk in chunks:
                if not chunk:  # Пропускаем пустые чанки
                    continue
                
                # Преобразуем чанк в список словарей
                chunk_data = [dict(zip(headers, row)) for row in chunk]
                
                # Анализируем схему только для первого чанка
                if not schema_analyzed and self.mode == "replace":
                    schema = TypeAnalyzer.analyze_dataframe_schema(chunk_data, list(headers))
                    
                    # Удаляем таблицу если существует
                    with engine.connect() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {self.table_name}"))
                        conn.commit()
                    
                    # Создаем таблицу с правильными типами
                    self._create_table_with_schema(engine, schema)
                    schema_analyzed = True
                
                # Записываем чанк
                if self.mode == "replace" and total_rows == 0 and not schema_analyzed:
                    self._insert_data(engine, chunk_data)
                else:
                    self._insert_data(engine, chunk_data)
                
                total_rows += len(chunk_data)
            
            return {
                "status": "success",
                "table": self.table_name,
                "rows_written": total_rows,
                "mode": self.mode,
                "schema_analyzed": schema_analyzed
            }
            
        except Exception as e:
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
    
    def _create_table_with_schema(self, engine, schema: List[dict]) -> bool:
        """Создание таблицы с правильными типами данных"""
        try:
            from sqlalchemy import text
            
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
            
            # Создаем SQL запрос
            create_sql = f"""
            CREATE TABLE "{self.table_name}" (
                {', '.join(columns_sql)}
            )
            """
            
            # Выполняем создание таблицы
            with engine.connect() as conn:
                conn.execute(text(create_sql))
                conn.commit()
            
            return True
            
        except Exception as e:
            print(f"Ошибка создания таблицы {self.table_name}: {e}")
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
        
        if not data:
            return
        
        # Получаем заголовки из первого элемента
        headers = list(data[0].keys())
        
        # Создаем SQL запрос для вставки
        columns = ', '.join(headers)
        placeholders = ', '.join([f':{header}' for header in headers])
        insert_sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
        
        with engine.connect() as conn:
            for row in data:
                conn.execute(text(insert_sql), row)
            conn.commit()
    
    def __del__(self):
        """Закрытие подключения при удалении объекта"""
        if hasattr(self, '_connected') and hasattr(self, 'connection') and self._connected and self.connection:
            self.connection.disconnect()

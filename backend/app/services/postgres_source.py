from __future__ import annotations

from typing import Any, Iterable, List, Sequence
from .base import DataSource
from .database_base import PostgreSQLConnection, DatabaseSource


class PostgreSQLSource(DataSource):
    """Источник данных из PostgreSQL"""
    
    def __init__(self, connection_string: str, table_name: str = None, query: str = None):
        self.connection = PostgreSQLConnection(connection_string)
        self.db_source = DatabaseSource(self.connection, table_name, query)
        self._connected = False
        
        # Подключаемся к базе
        if self.connection.connect():
            self._connected = True
        else:
            raise ConnectionError("Не удалось подключиться к PostgreSQL")
    
    def headers(self) -> List[str]:
        """Возвращает заголовки столбцов"""
        return self.db_source.get_headers()
    
    def read(self) -> Iterable[Sequence[Any]]:
        """Возвращает итератор по строкам данных"""
        data = self.db_source.get_data()
        return iter(data)
    
    def read_in_chunks(self, chunk_size: int) -> Iterable[List[Sequence[Any]]]:
        """Чтение данных чанками"""
        if chunk_size <= 0:
            chunk_size = 1000
        
        # Получаем все данные
        all_data = self.db_source.get_data()
        
        # Разбиваем на чанки
        chunk: List[Sequence[Any]] = []
        for row in all_data:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        
        if chunk:
            yield chunk
    
    def test_connection(self) -> bool:
        """Тестирование подключения"""
        return self.db_source.test_connection()
    
    def get_tables(self) -> List[str]:
        """Получение списка таблиц"""
        return self.connection.get_tables()
    
    def get_table_schema(self, table_name: str) -> List[dict]:
        """Получение схемы таблицы"""
        return self.connection.get_table_schema(table_name)
    
    def execute_custom_query(self, query: str, limit: int = 100) -> dict:
        """Выполнение произвольного запроса"""
        return self.connection.execute_query(query, limit)
    
    def __del__(self):
        """Закрытие подключения при удалении объекта"""
        if self._connected and self.connection:
            self.connection.disconnect()

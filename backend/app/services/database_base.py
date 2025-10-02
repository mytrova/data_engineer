from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


class DatabaseConnection(ABC):
    """Абстрактный класс для подключения к базам данных"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine: Optional[Engine] = None
    
    @abstractmethod
    def connect(self) -> bool:
        """Подключение к базе данных"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Отключение от базы данных"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Тестирование подключения"""
        pass
    
    @abstractmethod
    def get_tables(self) -> List[str]:
        """Получение списка таблиц"""
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Получение схемы таблицы"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, limit: int = 100) -> Dict[str, Any]:
        """Выполнение запроса"""
        pass


class PostgreSQLConnection(DatabaseConnection):
    """Подключение к PostgreSQL"""
    
    def connect(self) -> bool:
        try:
            self.engine = create_engine(self.connection_string)
            # Тестируем подключение
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"Ошибка подключения к PostgreSQL: {e}")
            return False
    
    def disconnect(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None
    
    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
    
    def get_tables(self) -> List[str]:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                """))
                return [row[0] for row in result]
        except Exception as e:
            print(f"Ошибка получения списка таблиц: {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_name = :table_name
                    ORDER BY ordinal_position
                """), {"table_name": table_name})
                
                schema = []
                for row in result:
                    schema.append({
                        "column_name": row[0],
                        "data_type": row[1],
                        "is_nullable": row[2] == "YES",
                        "column_default": row[3]
                    })
                return schema
        except Exception as e:
            print(f"Ошибка получения схемы таблицы {table_name}: {e}")
            return []
    
    def execute_query(self, query: str, limit: int = 100) -> Dict[str, Any]:
        try:
            # Добавляем LIMIT если его нет
            if "LIMIT" not in query.upper():
                query = f"{query.rstrip(';')} LIMIT {limit}"
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                
                # Получаем заголовки
                headers = list(result.keys())
                
                # Получаем данные
                rows = [list(row) for row in result.fetchall()]
                
                return {
                    "headers": headers,
                    "rows": rows,
                    "query": query
                }
        except Exception as e:
            return {
                "error": str(e),
                "headers": [],
                "rows": [],
                "query": query
            }


class DatabaseSource:
    """Источник данных из базы данных"""
    
    def __init__(self, connection: DatabaseConnection, table_name: str = None, query: str = None):
        self.connection = connection
        self.table_name = table_name
        self.query = query
        self._headers: List[str] = []
        self._rows: List[List[Any]] = []
    
    def get_headers(self) -> List[str]:
        """Получение заголовков"""
        if not self._headers and self.connection:
            if self.query:
                result = self.connection.execute_query(self.query, limit=1)
                self._headers = result.get("headers", [])
            elif self.table_name:
                schema = self.connection.get_table_schema(self.table_name)
                self._headers = [col["column_name"] for col in schema]
        return self._headers
    
    def get_data(self, limit: int = 100) -> List[List[Any]]:
        """Получение данных"""
        if not self._rows and self.connection:
            if self.query:
                result = self.connection.execute_query(self.query, limit=limit)
                self._rows = result.get("rows", [])
            elif self.table_name:
                query = f"SELECT * FROM {self.table_name}"
                result = self.connection.execute_query(query, limit=limit)
                self._rows = result.get("rows", [])
        return self._rows
    
    def test_connection(self) -> bool:
        """Тестирование подключения"""
        return self.connection.test_connection() if self.connection else False

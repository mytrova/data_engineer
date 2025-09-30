from __future__ import annotations

from typing import Any, Iterable, List, Sequence
from .base import DataSource
import requests
import logging

logger = logging.getLogger(__name__)


class ClickHouseSource(DataSource):
    """Источник данных из ClickHouse"""
    
    def __init__(self, host: str, port: int, database: str, username: str, password: str, table_name: str = None, query: str = None):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.table_name = table_name
        self.query = query
        self._connected = False
    
    def _get_base_url(self) -> str:
        """Получает базовый URL для ClickHouse HTTP API"""
        return f"http://{self.host}:{self.port}"
    
    def headers(self) -> List[str]:
        """Возвращает заголовки столбцов"""
        try:
            if self.query:
                sql_query = self.query
            else:
                sql_query = f"SELECT * FROM {self.table_name} LIMIT 0"
            
            response = requests.get(f"{self._get_base_url()}/", 
                                  params={'query': sql_query},
                                  auth=(self.username, self.password),
                                  headers={'X-ClickHouse-Database': self.database})
            
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if lines and lines[0]:
                    return lines[0].split('\t')
            return []
        except Exception as e:
            logger.error(f"Ошибка получения заголовков: {e}")
            return []
    
    def read(self) -> Iterable[Sequence[Any]]:
        """Возвращает итератор по строкам данных"""
        try:
            if self.query:
                sql_query = self.query
            else:
                sql_query = f"SELECT * FROM {self.table_name}"
            
            response = requests.get(f"{self._get_base_url()}/", 
                                  params={'query': sql_query},
                                  auth=(self.username, self.password),
                                  headers={'X-ClickHouse-Database': self.database})
            
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if lines and lines[0]:
                    headers = lines[0].split('\t')
                    for line in lines[1:]:
                        if line.strip():
                            yield line.split('\t')
            else:
                logger.error(f"Ошибка запроса к ClickHouse: {response.status_code}")
        except Exception as e:
            logger.error(f"Ошибка чтения данных: {e}")
    
    def read_in_chunks(self, chunk_size: int) -> Iterable[List[Sequence[Any]]]:
        """Чтение данных чанками"""
        if chunk_size <= 0:
            chunk_size = 1000
        
        try:
            if self.query:
                sql_query = f"{self.query} LIMIT {chunk_size}"
            else:
                sql_query = f"SELECT * FROM {self.table_name} LIMIT {chunk_size}"
            
            offset = 0
            while True:
                query_with_offset = f"{sql_query} OFFSET {offset}"
                response = requests.get(f"{self._get_base_url()}/", 
                                      params={'query': query_with_offset},
                                      auth=(self.username, self.password),
                                      headers={'X-ClickHouse-Database': self.database})
                
                if response.status_code != 200:
                    break
                
                lines = response.text.strip().split('\n')
                if not lines or not lines[0]:
                    break
                
                headers = lines[0].split('\t')
                chunk = []
                for line in lines[1:]:
                    if line.strip():
                        chunk.append(line.split('\t'))
                
                if not chunk:
                    break
                
                yield chunk
                offset += chunk_size
                
                if len(chunk) < chunk_size:
                    break
                    
        except Exception as e:
            logger.error(f"Ошибка чтения данных чанками: {e}")
    
    def test_connection(self) -> bool:
        """Тестирование подключения"""
        try:
            response = requests.get(f"{self._get_base_url()}/", 
                                  params={'query': 'SELECT 1'},
                                  auth=(self.username, self.password),
                                  headers={'X-ClickHouse-Database': self.database})
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Ошибка подключения к ClickHouse: {e}")
            return False
    
    def get_tables(self) -> List[str]:
        """Получение списка таблиц"""
        try:
            query = "SHOW TABLES"
            response = requests.get(f"{self._get_base_url()}/", 
                                  params={'query': query},
                                  auth=(self.username, self.password),
                                  headers={'X-ClickHouse-Database': self.database})
            
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                return [line.strip() for line in lines if line.strip()]
            return []
        except Exception as e:
            logger.error(f"Ошибка получения списка таблиц: {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> List[dict]:
        """Получение схемы таблицы"""
        try:
            query = f"DESCRIBE {table_name}"
            response = requests.get(f"{self._get_base_url()}/", 
                                  params={'query': query},
                                  auth=(self.username, self.password),
                                  headers={'X-ClickHouse-Database': self.database})
            
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                schema = []
                for line in lines:
                    if line.strip():
                        parts = line.split('\t')
                        if len(parts) >= 2:
                            schema.append({
                                'column_name': parts[0],
                                'data_type': parts[1],
                                'nullable': 'Nullable' in parts[1] if len(parts) > 1 else True
                            })
                return schema
            return []
        except Exception as e:
            logger.error(f"Ошибка получения схемы таблицы: {e}")
            return []
    
    def execute_custom_query(self, query: str, limit: int = 100) -> dict:
        """Выполнение произвольного запроса"""
        try:
            limited_query = f"{query} LIMIT {limit}"
            response = requests.get(f"{self._get_base_url()}/", 
                                  params={'query': limited_query},
                                  auth=(self.username, self.password),
                                  headers={'X-ClickHouse-Database': self.database})
            
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if lines and lines[0]:
                    headers = lines[0].split('\t')
                    data = []
                    for line in lines[1:]:
                        if line.strip():
                            data.append(line.split('\t'))
                    
                    return {
                        'status': 'success',
                        'headers': headers,
                        'data': data,
                        'row_count': len(data)
                    }
            else:
                return {
                    'status': 'error',
                    'error': f"HTTP {response.status_code}: {response.text}"
                }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }

from __future__ import annotations

from typing import Any, Iterable, List, Sequence
from .base import DataSink
import requests
import logging

logger = logging.getLogger(__name__)


class ClickHouseSink(DataSink):
    """Приёмник данных в ClickHouse"""
    
    def __init__(self, host: str, port: int, database: str, username: str, password: str, table_name: str, mode: str = "append"):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.table_name = table_name
        self.mode = mode  # "append", "replace"
        self._connected = False
        
        # Тестируем подключение
        if self.test_connection():
            self._connected = True
        else:
            raise ConnectionError("Не удалось подключиться к ClickHouse")
    
    def _get_base_url(self) -> str:
        """Получает базовый URL для ClickHouse HTTP API"""
        return f"http://{self.host}:{self.port}"
    
    def write(self, headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> Any:
        """Запись данных в таблицу"""
        try:
            rows_list = list(rows)
            if not rows_list:
                return {
                    "status": "success",
                    "table": self.table_name,
                    "rows_written": 0,
                    "mode": self.mode
                }
            
            # Подготавливаем данные для вставки
            values = []
            for row in rows_list:
                escaped_row = []
                for value in row:
                    if value is None:
                        escaped_row.append("NULL")
                    elif isinstance(value, str):
                        # Экранируем специальные символы и обрабатываем пустые строки
                        if value == '':
                            escaped_row.append("''")
                        else:
                            # Экранируем специальные символы для ClickHouse
                            escaped_value = (str(value)
                                          .replace("\\", "\\\\")  # Обратные слеши должны быть первыми
                                          .replace("'", "''")     # Одинарные кавычки
                                          .replace("\n", "\\n")   # Переносы строк
                                          .replace("\r", "\\r")   # Возврат каретки
                                          .replace("\t", "\\t"))  # Табуляция
                            escaped_row.append(f"'{escaped_value}'")
                    else:
                        escaped_row.append(str(value))
                
                values.append(f"({', '.join(escaped_row)})")
            
            # Формируем INSERT запрос
            columns = ", ".join(headers)
            insert_query = f"INSERT INTO {self.table_name} ({columns}) VALUES {', '.join(values)}"
            
            # Используем правильную аутентификацию для ClickHouse
            auth = (self.username, self.password)
            params = {'database': self.database}
            
            # Устанавливаем правильную кодировку для русских символов
            headers = {'Content-Type': 'text/plain; charset=utf-8'}
            
            response = requests.post(f"{self._get_base_url()}/", 
                                  data=insert_query.encode('utf-8'), 
                                  auth=auth, 
                                  params=params,
                                  headers=headers)
            
            if response.status_code == 200:
                return {
                    "status": "success",
                    "table": self.table_name,
                    "rows_written": len(rows_list),
                    "mode": self.mode
                }
            else:
                return {
                    "status": "error",
                    "error": f"HTTP {response.status_code}: {response.text}",
                    "table": self.table_name
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
            total_rows = 0
            schema_analyzed = False
            
            for chunk in chunks:
                if not chunk:  # Пропускаем пустые чанки
                    continue
                
                # Проверяем существование таблицы и создаем её при необходимости
                if not schema_analyzed:
                    if not self._table_exists(self.table_name):
                        logger.info(f"Таблица {self.table_name} не существует, анализируем схему и создаем таблицу")
                        
                        # Анализируем схему из первого чанка
                        from .type_analyzer import TypeAnalyzer
                        schema = TypeAnalyzer.analyze_dataframe_schema(chunk, headers)
                        
                        # Создаем таблицу
                        if self.create_table(self.table_name, schema):
                            logger.info(f"Таблица {self.table_name} успешно создана")
                        else:
                            logger.error(f"Не удалось создать таблицу {self.table_name}")
                            return {
                                "status": "error",
                                "error": f"Не удалось создать таблицу {self.table_name}",
                                "table": self.table_name,
                                "rows_written": total_rows
                            }
                    else:
                        logger.info(f"Таблица {self.table_name} уже существует")
                    
                    schema_analyzed = True
                
                # Подготавливаем данные для вставки
                values = []
                for row in chunk:
                    escaped_row = []
                    for value in row:
                        if value is None:
                            escaped_row.append("NULL")
                        elif isinstance(value, str):
                            # Экранируем специальные символы и обрабатываем пустые строки
                            if value == '':
                                escaped_row.append("''")
                            else:
                                # Экранируем специальные символы для ClickHouse
                                escaped_value = (str(value)
                                              .replace("\\", "\\\\")  # Обратные слеши должны быть первыми
                                              .replace("'", "''")     # Одинарные кавычки
                                              .replace("\n", "\\n")   # Переносы строк
                                              .replace("\r", "\\r")   # Возврат каретки
                                              .replace("\t", "\\t"))  # Табуляция
                                escaped_row.append(f"'{escaped_value}'")
                        else:
                            escaped_row.append(str(value))
                    
                    values.append(f"({', '.join(escaped_row)})")
                
                # Формируем INSERT запрос для чанка
                columns = ", ".join(headers)
                insert_query = f"INSERT INTO {self.table_name} ({columns}) VALUES {', '.join(values)}"
                
                # Используем правильную аутентификацию для ClickHouse
                auth = (self.username, self.password)
                params = {'database': self.database}
                
                # Устанавливаем правильную кодировку для русских символов
                headers = {'Content-Type': 'text/plain; charset=utf-8'}
                
                response = requests.post(f"{self._get_base_url()}/", 
                                      data=insert_query.encode('utf-8'), 
                                      auth=auth, 
                                      params=params,
                                      headers=headers)
                
                if response.status_code != 200:
                    return {
                        "status": "error",
                        "error": f"HTTP {response.status_code}: {response.text}",
                        "table": self.table_name,
                        "rows_written": total_rows
                    }
                
                total_rows += len(chunk)
            
            return {
                "status": "success",
                "table": self.table_name,
                "rows_written": total_rows,
                "mode": self.mode
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "table": self.table_name
            }
    
    def test_connection(self) -> bool:
        """Тестирование подключения"""
        try:
            url = f"{self._get_base_url()}/"
            logger.info(f"Testing ClickHouse connection to: {url}")
            
            # Используем правильную аутентификацию для ClickHouse
            auth = (self.username, self.password)
            params = {
                'query': 'SELECT 1',
                'database': self.database
            }
            
            response = requests.get(url, params=params, auth=auth, timeout=10)
            logger.info(f"ClickHouse connection response: {response.status_code}")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Ошибка подключения к ClickHouse: {e}")
            return False
    
    def get_tables(self) -> List[str]:
        """Получение списка таблиц"""
        try:
            query = "SHOW TABLES"
            auth = (self.username, self.password)
            params = {'query': query, 'database': self.database}
            response = requests.get(f"{self._get_base_url()}/", params=params, auth=auth)
            
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                return [line.strip() for line in lines if line.strip()]
            return []
        except Exception as e:
            logger.error(f"Ошибка получения списка таблиц: {e}")
            return []
    
    def _table_exists(self, table_name: str) -> bool:
        """Проверяет существование таблицы в ClickHouse"""
        try:
            # Используем SHOW TABLES для проверки существования таблицы
            check_sql = f"SHOW TABLES FROM {self.database} LIKE '{table_name}'"
            
            auth = (self.username, self.password)
            params = {'database': self.database}
            headers = {'Content-Type': 'text/plain; charset=utf-8'}
            
            response = requests.post(f"{self._get_base_url()}/", 
                                  data=check_sql.encode('utf-8'), 
                                  auth=auth, 
                                  params=params,
                                  headers=headers)
            
            if response.status_code == 200:
                # Если таблица существует, вернется её имя
                return table_name in response.text
            else:
                logger.error(f"Ошибка проверки существования таблицы: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при проверке существования таблицы {table_name}: {e}")
            return False

    def create_table(self, table_name: str, schema: List[dict]) -> bool:
        """Создание таблицы по схеме"""
        try:
            # Строим SQL для создания таблицы
            columns_sql = []
            for col in schema:
                col_name = col["column_name"]
                col_type = col["data_type"]
                nullable = col.get("nullable", True)
                
                # Определяем тип ClickHouse
                ch_type = self._get_clickhouse_type(col_type)
                
                # В ClickHouse все поля nullable по умолчанию, используем Nullable() для nullable полей
                if nullable:
                    ch_type = f"Nullable({ch_type})"
                
                columns_sql.append(f"`{col_name}` {ch_type}")
            
            # Создаем SQL запрос
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                {', '.join(columns_sql)}
            ) ENGINE = MergeTree()
            ORDER BY tuple()
            """
            
            # Используем правильную аутентификацию для ClickHouse
            auth = (self.username, self.password)
            params = {'database': self.database}
            
            # Устанавливаем правильную кодировку для русских символов
            headers = {'Content-Type': 'text/plain; charset=utf-8'}
            
            response = requests.post(f"{self._get_base_url()}/", 
                                  data=create_sql.encode('utf-8'), 
                                  auth=auth, 
                                  params=params,
                                  headers=headers)
            
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Ошибка создания таблицы {table_name}: {e}")
            return False
    
    def _get_clickhouse_type(self, data_type: str) -> str:
        """Преобразует тип данных в тип ClickHouse"""
        data_type_lower = data_type.lower()
        
        if "varchar" in data_type_lower or "character" in data_type_lower:
            return "String"
        elif "text" in data_type_lower:
            return "String"
        elif "integer" in data_type_lower or "int" in data_type_lower:
            return "Int32"
        elif "bigint" in data_type_lower:
            return "Int64"
        elif "float" in data_type_lower:
            return "Float32"
        elif "double" in data_type_lower:
            return "Float64"
        elif "boolean" in data_type_lower or "bool" in data_type_lower:
            return "UInt8"
        elif "date" in data_type_lower:
            return "Date"
        elif "timestamp" in data_type_lower or "datetime" in data_type_lower:
            return "DateTime"
        else:
            return "String"  # По умолчанию

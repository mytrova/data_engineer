"""
Модуль для анализа типов данных в источниках
"""
import re
from typing import Any, List, Dict, Optional
from datetime import datetime
import pandas as pd


class TypeAnalyzer:
    """Анализатор типов данных для определения подходящих типов PostgreSQL"""
    
    @staticmethod
    def analyze_column_data(column_data: List[Any]) -> Dict[str, Any]:
        """
        Анализирует данные столбца и определяет подходящий тип PostgreSQL
        
        Args:
            column_data: Список значений столбца
            
        Returns:
            Словарь с информацией о типе данных
        """
        if not column_data:
            return {"type": "TEXT", "max_length": 255, "nullable": True}
        
        # Убираем None значения для анализа
        non_null_data = [x for x in column_data if x is not None and str(x).strip() != '']
        
        if not non_null_data:
            return {"type": "TEXT", "max_length": 255, "nullable": True}
        
        # Анализируем типы
        analysis = {
            "is_integer": True,
            "is_float": True,
            "is_boolean": True,
            "is_date": True,
            "is_timestamp": True,
            "max_length": 0,
            "nullable": len(non_null_data) < len(column_data)
        }
        
        for value in non_null_data:
            str_value = str(value).strip()
            analysis["max_length"] = max(analysis["max_length"], len(str_value))
            
            # Проверяем целые числа
            if analysis["is_integer"]:
                try:
                    int(str_value)
                except ValueError:
                    analysis["is_integer"] = False
            
            # Проверяем числа с плавающей точкой
            if analysis["is_float"]:
                try:
                    float(str_value)
                except ValueError:
                    analysis["is_float"] = False
            
            # Проверяем булевы значения
            if analysis["is_boolean"]:
                if str_value.lower() not in ['true', 'false', '1', '0', 'yes', 'no', 'да', 'нет']:
                    analysis["is_boolean"] = False
            
            # Проверяем даты
            if analysis["is_date"]:
                if not TypeAnalyzer._is_date_like(str_value):
                    analysis["is_date"] = False
            
            # Проверяем временные метки
            if analysis["is_timestamp"]:
                if not TypeAnalyzer._is_timestamp_like(str_value):
                    analysis["is_timestamp"] = False
        
        # Определяем финальный тип
        if analysis["is_boolean"]:
            return {"type": "BOOLEAN", "nullable": analysis["nullable"]}
        elif analysis["is_integer"]:
            return {"type": "INTEGER", "nullable": analysis["nullable"]}
        elif analysis["is_float"]:
            return {"type": "NUMERIC", "nullable": analysis["nullable"]}
        elif analysis["is_timestamp"]:
            return {"type": "TIMESTAMP", "nullable": analysis["nullable"]}
        elif analysis["is_date"]:
            return {"type": "DATE", "nullable": analysis["nullable"]}
        else:
            # Текстовый тип
            max_len = analysis["max_length"]
            if max_len <= 255:
                return {"type": "VARCHAR", "max_length": max_len, "nullable": analysis["nullable"]}
            else:
                return {"type": "TEXT", "nullable": analysis["nullable"]}
    
    @staticmethod
    def _is_date_like(value: str) -> bool:
        """Проверяет, похоже ли значение на дату"""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY
            r'^\d{2}\.\d{2}\.\d{4}$',  # DD.MM.YYYY
            r'^\d{4}/\d{2}/\d{2}$',  # YYYY/MM/DD
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, value):
                return True
        
        # Пробуем парсить как дату
        try:
            datetime.strptime(value, '%Y-%m-%d')
            return True
        except ValueError:
            pass
        
        try:
            datetime.strptime(value, '%d.%m.%Y')
            return True
        except ValueError:
            pass
        
        try:
            datetime.strptime(value, '%m/%d/%Y')
            return True
        except ValueError:
            pass
        
        return False
    
    @staticmethod
    def _is_timestamp_like(value: str) -> bool:
        """Проверяет, похоже ли значение на временную метку"""
        timestamp_patterns = [
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',  # YYYY-MM-DD HH:MM:SS
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$',  # YYYY-MM-DDTHH:MM:SS
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$',  # YYYY-MM-DD HH:MM:SS.microseconds
        ]
        
        for pattern in timestamp_patterns:
            if re.match(pattern, value):
                return True
        
        # Пробуем парсить как timestamp
        try:
            datetime.fromisoformat(value.replace('T', ' '))
            return True
        except ValueError:
            pass
        
        return False
    
    @staticmethod
    def analyze_dataframe_schema(df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Анализирует DataFrame и возвращает схему с типами данных
        
        Args:
            df: DataFrame для анализа
            
        Returns:
            Список словарей с информацией о столбцах
        """
        schema = []
        
        for column in df.columns:
            column_data = df[column].tolist()
            column_analysis = TypeAnalyzer.analyze_column_data(column_data)
            
            schema.append({
                "column_name": column,
                "data_type": column_analysis["type"],
                "max_length": column_analysis.get("max_length"),
                "nullable": column_analysis["nullable"]
            })
        
        return schema
    
    @staticmethod
    def get_postgresql_type(analysis: Dict[str, Any]) -> str:
        """
        Преобразует результат анализа в тип PostgreSQL
        
        Args:
            analysis: Результат анализа столбца
            
        Returns:
            Строка с типом PostgreSQL
        """
        pg_type = analysis["type"]
        
        if pg_type == "VARCHAR" and "max_length" in analysis:
            return f"VARCHAR({analysis['max_length']})"
        elif pg_type == "NUMERIC":
            return "NUMERIC"
        elif pg_type == "INTEGER":
            return "INTEGER"
        elif pg_type == "BOOLEAN":
            return "BOOLEAN"
        elif pg_type == "DATE":
            return "DATE"
        elif pg_type == "TIMESTAMP":
            return "TIMESTAMP"
        else:
            return "TEXT"

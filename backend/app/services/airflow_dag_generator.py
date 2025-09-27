import os
import subprocess
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
from pathlib import Path

class AirflowDAGGenerator:
    """Генератор DAG'ов для Airflow на основе параметров переливки данных"""
    
    def __init__(self, dags_folder: str = "/app/airflow/dags"):
        self.dags_folder = Path(dags_folder)
        self.dags_folder.mkdir(parents=True, exist_ok=True)
    
    def generate_data_transfer_dag(
        self,
        dag_id: str,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        chunk_size: int,
        total_rows: int = None
    ) -> str:
        """
        Генерирует DAG для переливки данных из источника в приёмник
        
        Args:
            dag_id: Уникальный ID DAG'а
            source_config: Конфигурация источника данных
            sink_config: Конфигурация приёмника данных
            chunk_size: Размер чанка для обработки
            total_rows: Общее количество строк (опционально)
        
        Returns:
            Путь к созданному DAG файлу
        """
        
        # Получаем реальное количество записей из источника
        actual_total_rows = self._get_source_row_count(source_config, total_rows)
        
        # Вычисляем количество задач на основе реального количества записей
        if actual_total_rows > 0:
            num_tasks = (actual_total_rows + chunk_size - 1) // chunk_size
        else:
            # Если данных нет, создаем 1 задачу для создания пустой таблицы
            num_tasks = 1
        
        # Генерируем код DAG'а
        dag_code = self._generate_dag_code(
            dag_id=dag_id,
            source_config=source_config,
            sink_config=sink_config,
            chunk_size=chunk_size,
            num_tasks=num_tasks,
            total_rows=actual_total_rows
        )
        
        # Сохраняем DAG файл
        dag_file_path = self.dags_folder / f"{dag_id}.py"
        with open(dag_file_path, 'w', encoding='utf-8') as f:
            f.write(dag_code)
        
        # Принудительно перезагружаем DagBag в Airflow через HTTP API
        try:
            import requests
            response = requests.post(
                'http://airflow:8080/api/v1/dags/reserialize',
                timeout=30
            )
            if response.status_code == 200:
                print(f"DAG {dag_id} успешно зарегистрирован в Airflow")
            else:
                print(f"Предупреждение: не удалось перезагрузить DagBag: HTTP {response.status_code}")
        except Exception as e:
            print(f"Предупреждение: ошибка при перезагрузке DagBag: {e}")
        
        return str(dag_file_path)
    
    def _get_source_row_count(self, source_config: Dict[str, Any], total_rows: int = None) -> int:
        """
        Получает реальное количество записей из источника данных
        
        Args:
            source_config: Конфигурация источника данных
            total_rows: Предполагаемое количество строк (если известно)
        
        Returns:
            Реальное количество записей в источнике
        """
        try:
            if source_config['type'] == 'database':
                # Для базы данных подсчитываем записи
                import psycopg2
                conn = psycopg2.connect(
                    host=source_config['host'],
                    port=source_config['port'],
                    database=source_config['database'],
                    user=source_config['username'],
                    password=source_config['password']
                )
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {source_config['table_name']}")
                count = cursor.fetchone()[0]
                conn.close()
                return count
                
            elif source_config['type'] == 'file':
                file_path = source_config['file_path']
                file_type = source_config.get('file_type', 'csv')
                
                if file_type == 'csv':
                    import pandas as pd
                    delimiter = source_config.get('delimiter', ';')
                    df = pd.read_csv(file_path, delimiter=delimiter)
                    return len(df)
                    
                elif file_type == 'json':
                    import json
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        return len(data)
                    else:
                        return 1  # Одиночный объект
                        
            # Если не удалось получить количество, используем переданное значение
            return total_rows or 0
            
        except Exception as e:
            print(f"Ошибка при получении количества записей: {e}")
            # Если не удалось получить количество, используем переданное значение
            return total_rows or 0
    
    def _generate_dag_code(
        self,
        dag_id: str,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        chunk_size: int,
        num_tasks: int,
        total_rows: int
    ) -> str:
        """Генерирует Python код для DAG'а"""
        
        # Создаем конфигурацию для передачи в задачи
        config = {
            "source": source_config,
            "sink": sink_config,
            "chunk_size": chunk_size,
            "num_tasks": num_tasks,
            "total_rows": total_rows
        }
        
        dag_code = f'''"""
Автоматически сгенерированный DAG для переливки данных
Создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
DAG ID: {dag_id}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import json

# Конфигурация DAG
DAG_CONFIG = {json.dumps(config, indent=2, ensure_ascii=False)}

# Параметры по умолчанию для DAG
default_args = {{
    'owner': 'data-orchestrator',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

# Создание DAG
dag = DAG(
    '{dag_id}',
    default_args=default_args,
    description='Автоматическая переливка данных из {{source_type}} в PostgreSQL',
    schedule_interval=None,  # Ручной запуск
    catchup=False,
    tags=['data-transfer', 'auto-generated'],
    is_paused_upon_creation=False,  # DAG активен сразу после создания
)

def transfer_data_chunk(chunk_index: int, **context):
    """
    Переливает данные для конкретного чанка
    
    Args:
        chunk_index: Индекс чанка (0-based)
    """
    config = DAG_CONFIG
    source_config = config['source']
    sink_config = config['sink']
    chunk_size = config['chunk_size']
    
    print(f"Обработка чанка {{chunk_index + 1}} из {{config['num_tasks']}}")
    
    df = None  # Инициализируем переменную df
    
    try:
        # Подключение к источнику данных
        if source_config['type'] == 'database':
            # Подключение к источнику БД
            source_engine = create_engine(
                f"postgresql://{{source_config['username']}}:{{source_config['password']}}@"
                f"{{source_config['host']}}:{{source_config['port']}}/{{source_config['database']}}"
            )
            
            # Вычисляем OFFSET и LIMIT для текущего чанка
            offset = chunk_index * chunk_size
            limit = chunk_size
            
            # Загружаем данные из источника
            query = f"SELECT * FROM {{source_config['table_name']}} LIMIT {{limit}} OFFSET {{offset}}"
            df = pd.read_sql(query, source_engine)
            
            if df.empty:
                print(f"Чанк {{chunk_index + 1}}: Нет данных для обработки")
                return
            
            print(f"Чанк {{chunk_index + 1}}: Загружено {{len(df)}} строк")
            
        elif source_config['type'] == 'file':
            # Обработка файла (CSV, JSON, XML)
            file_path = source_config['file_path']
            file_type = source_config.get('file_type', 'csv')
            
            if file_type == 'csv':
                delimiter = source_config.get('delimiter', ';')
                offset = chunk_index * chunk_size
                # Читаем весь файл и берем нужный чанк
                full_df = pd.read_csv(file_path, delimiter=delimiter)
                start_idx = offset
                end_idx = min(offset + chunk_size, len(full_df))
                df = full_df.iloc[start_idx:end_idx].copy()
            elif file_type == 'json':
                # Для JSON файлов читаем весь файл и разбиваем на чанки
                offset = chunk_index * chunk_size
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    start_idx = offset
                    end_idx = min(offset + chunk_size, len(data))
                    df = pd.DataFrame(data[start_idx:end_idx])
                else:
                    df = pd.DataFrame([data])
            else:
                raise ValueError(f"Неподдерживаемый тип файла: {{file_type}}")
            
            if df.empty:
                print(f"Чанк {{chunk_index + 1}}: Нет данных для обработки")
                return
            
            print(f"Чанк {{chunk_index + 1}}: Загружено {{len(df)}} строк")
        
        # Подключение к приёмнику (PostgreSQL)
        sink_engine = create_engine(
            f"postgresql://{{sink_config['username']}}:{{sink_config['password']}}@"
            f"{{sink_config['host']}}:{{sink_config['port']}}/{{sink_config['database']}}"
        )
        
        # Сохраняем данные в приёмник
        table_name = sink_config['table_name']
        
        # Проверяем, существует ли таблица и есть ли в ней столбцы
        table_exists = False
        table_has_columns = False
        try:
            from sqlalchemy import inspect
            inspector = inspect(sink_engine)
            table_exists = table_name in inspector.get_table_names()
            if table_exists:
                # Проверяем, есть ли столбцы в таблице
                columns = inspector.get_columns(table_name)
                table_has_columns = len(columns) > 0
                print(f"Таблица {{table_name}}: существует={{table_exists}}, столбцов={{len(columns)}}")
        except Exception as e:
            print(f"Не удалось проверить существование таблицы: {{str(e)}}")
        
        # Определяем, нужно ли создавать таблицу
        need_create_table = (not table_exists) or (table_exists and not table_has_columns)
        
        if need_create_table and chunk_index == 0:
            # Создаем таблицу только для первого чанка
            if df is not None and not df.empty:
                # Создаем таблицу с данными
                df.to_sql(table_name, sink_engine, if_exists='replace', index=False)
                print(f"Создана таблица {{table_name}} с {{len(df)}} строками")
            else:
                # Создаем пустую таблицу с базовой структурой
                empty_df = pd.DataFrame(columns=['id'])  # Минимальная структура
                empty_df.to_sql(table_name, sink_engine, if_exists='replace', index=False)
                print(f"Создана пустая таблица {{table_name}}")
        elif table_exists and table_has_columns and df is not None and not df.empty:
            # Таблица существует и имеет столбцы - только добавляем данные
            df.to_sql(table_name, sink_engine, if_exists='append', index=False)
            print(f"Чанк {{chunk_index + 1}}: Добавлено {{len(df)}} строк в существующую таблицу {{table_name}}")
        elif df is not None and not df.empty:
            # Если это не первый чанк, но таблица не создана - пропускаем
            print(f"Чанк {{chunk_index + 1}}: Таблица {{table_name}} не создана, пропускаем вставку")
        else:
            print(f"Чанк {{chunk_index + 1}}: Нет данных для сохранения")
        
    except Exception as e:
        print(f"Ошибка в чанке {{chunk_index + 1}}: {{str(e)}}")
        # Если df не определена и это первый чанк, проверяем и создаем таблицу если нужно
        if df is None and chunk_index == 0:
            try:
                sink_engine = create_engine(
                    f"postgresql://{{sink_config['username']}}:{{sink_config['password']}}@"
                    f"{{sink_config['host']}}:{{sink_config['port']}}/{{sink_config['database']}}"
                )
                table_name = sink_config['table_name']
                
                # Проверяем, существует ли таблица с столбцами
                from sqlalchemy import inspect
                inspector = inspect(sink_engine)
                table_exists = table_name in inspector.get_table_names()
                table_has_columns = False
                
                if table_exists:
                    columns = inspector.get_columns(table_name)
                    table_has_columns = len(columns) > 0
                
                # Создаем таблицу только если её нет или в ней нет столбцов
                if not table_exists or not table_has_columns:
                    empty_df = pd.DataFrame(columns=['id'])  # Минимальная структура
                    empty_df.to_sql(table_name, sink_engine, if_exists='replace', index=False)
                    print(f"Создана пустая таблица {{table_name}} из-за ошибки")
                else:
                    print(f"Таблица {{table_name}} уже существует с столбцами")
            except Exception as table_error:
                print(f"Не удалось создать таблицу: {{str(table_error)}}")
        # Не поднимаем исключение, если это просто пустой чанк
        if df is None or (df is not None and df.empty):
            print(f"Чанк {{chunk_index + 1}}: Завершен без данных")
            return
        else:
            raise e

# Создаем задачи для каждого чанка
transfer_tasks = []

# Начальная задача
start_task = DummyOperator(
    task_id='start_transfer',
    dag=dag,
)

# Задачи для каждого чанка
for i in range({num_tasks}):
    task_id = f'transfer_chunk_{{i + 1}}'
    
    task = PythonOperator(
        task_id=task_id,
        python_callable=transfer_data_chunk,
        op_args=[i],
        dag=dag,
    )
    
    transfer_tasks.append(task)

# Финальная задача
end_task = DummyOperator(
    task_id='end_transfer',
    dag=dag,
)

# Настройка зависимостей
start_task >> transfer_tasks
transfer_tasks >> end_task
'''
        
        return dag_code
    
    def delete_dag(self, dag_id: str) -> bool:
        """Удаляет DAG файл"""
        try:
            # Сначала пробуем найти по имени файла
            dag_file_path = self.dags_folder / f"{dag_id}.py"
            if dag_file_path.exists():
                dag_file_path.unlink()
                return True
            
            # Если не найден по имени, ищем по содержимому
            for dag_file in self.dags_folder.glob("*.py"):
                if dag_file.name.startswith("."):
                    continue  # Пропускаем скрытые файлы
                
                try:
                    with open(dag_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Ищем DAG ID в файле (аналогично парсеру из airflow_client)
                    lines = content.split('\n')
                    found_dag_id = None
                    
                    # Сначала ищем многострочный DAG(
                    for i, line in enumerate(lines):
                        line = line.strip()
                        if 'DAG(' in line:
                            # Ищем следующую строку с dag_id
                            for j in range(i + 1, min(i + 5, len(lines))):
                                next_line = lines[j].strip()
                                # Проверяем, содержит ли строка dag_id в кавычках
                                if ("'" in next_line and next_line.startswith("'")) or ('"' in next_line and next_line.startswith('"')):
                                    # Извлекаем dag_id
                                    if next_line.startswith("'") and next_line.endswith("'"):
                                        found_dag_id = next_line[1:-1]
                                        break
                                    elif next_line.startswith('"') and next_line.endswith('"'):
                                        found_dag_id = next_line[1:-1]
                                        break
                                    else:
                                        # Частичная строка, ищем первую кавычку
                                        if "'" in next_line:
                                            start = next_line.find("'") + 1
                                            end = next_line.find("'", start)
                                            if end > start:
                                                found_dag_id = next_line[start:end]
                                                break
                                        elif '"' in next_line:
                                            start = next_line.find('"') + 1
                                            end = next_line.find('"', start)
                                            if end > start:
                                                found_dag_id = next_line[start:end]
                                                break
                            if found_dag_id:
                                break
                    
                    # Если не нашли многострочный, ищем в одной строке
                    if not found_dag_id:
                        for line in lines:
                            line = line.strip()
                            if 'DAG(' in line and "'" in line:
                                start = line.find("'") + 1
                                end = line.find("'", start)
                                if end > start:
                                    found_dag_id = line[start:end]
                                    break
                            elif 'dag_id=' in line or 'dag_id =' in line:
                                if "'" in line:
                                    start = line.find("'") + 1
                                    end = line.find("'", start)
                                    if end > start:
                                        found_dag_id = line[start:end]
                                        break
                                elif '"' in line:
                                    start = line.find('"') + 1
                                    end = line.find('"', start)
                                    if end > start:
                                        found_dag_id = line[start:end]
                                        break
                    
                    # Если найден нужный DAG ID, удаляем файл
                    if found_dag_id == dag_id:
                        dag_file.unlink()
                        return True
                        
                except Exception as e:
                    print(f"Ошибка при чтении файла {dag_file}: {e}")
                    continue
            
            return False
        except Exception as e:
            print(f"Ошибка при удалении DAG {dag_id}: {e}")
            return False
    
    def list_generated_dags(self) -> List[str]:
        """Возвращает список сгенерированных DAG'ов"""
        try:
            dag_files = list(self.dags_folder.glob("*.py"))
            return [f.stem for f in dag_files if f.stem != "__pycache__"]
        except Exception as e:
            print(f"Ошибка при получении списка DAG'ов: {e}")
            return []

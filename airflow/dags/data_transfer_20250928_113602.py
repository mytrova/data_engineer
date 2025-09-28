"""
Автоматически сгенерированный DAG для переливки данных
Создан: 2025-09-28 11:36:02
DAG ID: data_transfer_20250928_113602
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
DAG_CONFIG = {
  "source": {
    "type": "file",
    "file_path": "/opt/airflow/part-00000-37dced01-2ad2-48c8-a56d-54b4d8760599-c000.csv",
    "file_type": "csv",
    "delimiter": ";"
  },
  "sink": {
    "type": "postgresql",
    "host": "89.169.164.219",
    "port": "5432",
    "database": "api_db",
    "username": "api_user",
    "password": "secure_password",
    "table_name": "from_csv_big"
  },
  "chunk_size": 10000,
  "num_tasks": 1,
  "total_rows": 0
}

# Параметры по умолчанию для DAG
default_args = {
    'owner': 'data-orchestrator',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создание DAG
dag = DAG(
    'data_transfer_20250928_113602',
    default_args=default_args,
    description='Автоматическая переливка данных из {source_type} в PostgreSQL',
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
    
    print(f"Обработка чанка {chunk_index + 1} из {config['num_tasks']}")
    
    df = None  # Инициализируем переменную df
    
    try:
        # Подключение к источнику данных
        if source_config['type'] == 'database':
            # Подключение к источнику БД
            source_engine = create_engine(
                f"postgresql://{source_config['username']}:{source_config['password']}@"
                f"{source_config['host']}:{source_config['port']}/{source_config['database']}"
            )
            
            # Вычисляем OFFSET и LIMIT для текущего чанка
            offset = chunk_index * chunk_size
            limit = chunk_size
            
            # Загружаем данные из источника
            query = f"SELECT * FROM {source_config['table_name']} LIMIT {limit} OFFSET {offset}"
            df = pd.read_sql(query, source_engine)
            
            if df.empty:
                print(f"Чанк {chunk_index + 1}: Нет данных для обработки")
                return
            
            print(f"Чанк {chunk_index + 1}: Загружено {len(df)} строк")
            
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
                raise ValueError(f"Неподдерживаемый тип файла: {file_type}")
            
            if df.empty:
                print(f"Чанк {chunk_index + 1}: Нет данных для обработки")
                return
            
            print(f"Чанк {chunk_index + 1}: Загружено {len(df)} строк")
        
        # Подключение к приёмнику (PostgreSQL)
        sink_engine = create_engine(
            f"postgresql://{sink_config['username']}:{sink_config['password']}@"
            f"{sink_config['host']}:{sink_config['port']}/{sink_config['database']}"
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
                print(f"Таблица {table_name}: существует={table_exists}, столбцов={len(columns)}")
        except Exception as e:
            print(f"Не удалось проверить существование таблицы: {str(e)}")
        
        # Определяем, нужно ли создавать таблицу
        need_create_table = (not table_exists) or (table_exists and not table_has_columns)
        
        if need_create_table and chunk_index == 0:
            # Создаем таблицу только для первого чанка
            if df is not None and not df.empty:
                # Создаем таблицу с данными
                df.to_sql(table_name, sink_engine, if_exists='replace', index=False)
                print(f"Создана таблица {table_name} с {len(df)} строками")
            else:
                # Создаем пустую таблицу с базовой структурой
                empty_df = pd.DataFrame(columns=['id'])  # Минимальная структура
                empty_df.to_sql(table_name, sink_engine, if_exists='replace', index=False)
                print(f"Создана пустая таблица {table_name}")
        elif table_exists and table_has_columns and df is not None and not df.empty:
            # Таблица существует и имеет столбцы - только добавляем данные
            df.to_sql(table_name, sink_engine, if_exists='append', index=False)
            print(f"Чанк {chunk_index + 1}: Добавлено {len(df)} строк в существующую таблицу {table_name}")
        elif df is not None and not df.empty:
            # Если это не первый чанк, но таблица не создана - пропускаем
            print(f"Чанк {chunk_index + 1}: Таблица {table_name} не создана, пропускаем вставку")
        else:
            print(f"Чанк {chunk_index + 1}: Нет данных для сохранения")
        
    except Exception as e:
        print(f"Ошибка в чанке {chunk_index + 1}: {str(e)}")
        # Если df не определена и это первый чанк, проверяем и создаем таблицу если нужно
        if df is None and chunk_index == 0:
            try:
                sink_engine = create_engine(
                    f"postgresql://{sink_config['username']}:{sink_config['password']}@"
                    f"{sink_config['host']}:{sink_config['port']}/{sink_config['database']}"
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
                    print(f"Создана пустая таблица {table_name} из-за ошибки")
                else:
                    print(f"Таблица {table_name} уже существует с столбцами")
            except Exception as table_error:
                print(f"Не удалось создать таблицу: {str(table_error)}")
        # Не поднимаем исключение, если это просто пустой чанк
        if df is None or (df is not None and df.empty):
            print(f"Чанк {chunk_index + 1}: Завершен без данных")
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
for i in range(1):
    task_id = f'transfer_chunk_{i + 1}'
    
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

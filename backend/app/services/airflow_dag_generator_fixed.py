"""
Сервис для генерации DAG'ов Airflow для перелива данных из SQLite в целевую БД
"""
import os
import json
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class AirflowDAGGenerator:
    """Генератор DAG'ов для Airflow"""
    
    def __init__(self, dags_path: str = "/app/airflow/dags"):
        self.dags_path = dags_path
        
    def generate_data_transfer_dag(
        self,
        file_id: int = None,
        source_table: str = None,
        sink_config: Dict[str, Any] = None,
        source_config: Dict[str, Any] = None,
        chunk_size: int = 1000,
        total_rows: int = None,
        dag_id: Optional[str] = None
    ) -> str:
        """
        Генерирует DAG для перелива данных
        
        Args:
            file_id: ID файла в SQLite (для файловых источников)
            source_table: Имя таблицы в SQLite (file_data_{file_id})
            sink_config: Конфигурация приёмника данных
            source_config: Конфигурация источника данных
            chunk_size: Размер чанка для обработки
            total_rows: Общее количество строк
            dag_id: ID DAG'а (если не указан, генерируется автоматически)
        """
        logger.info("=== НАЧАЛО ГЕНЕРАЦИИ DAG ===")
        logger.info(f"file_id: {file_id}")
        logger.info(f"source_table: {source_table}")
        logger.info(f"sink_config: {sink_config}")
        logger.info(f"source_config: {source_config}")
        logger.info(f"chunk_size: {chunk_size}")
        logger.info(f"total_rows: {total_rows}")
        logger.info(f"dag_id: {dag_id}")
        
        if not dag_id:
            timestamp = int(datetime.now().timestamp())
            dag_id = f"data_transfer_{timestamp}"
            
        # Определяем тип источника и приёмника
        source_type = source_config.get('type', 'file') if source_config else 'file'
        sink_type = sink_config.get('type', 'database') if sink_config else 'database'
        
        # Отладочная информация
        logger.info(f"source_config: {source_config}")
        logger.info(f"sink_config: {sink_config}")
        logger.info(f"source_type: {source_type}, sink_type: {sink_type}")
        
        # Нормализуем типы
        if source_type == 'database':
            source_type = 'postgresql'  # По умолчанию PostgreSQL для database
        if sink_type == 'database':
            sink_type = 'postgresql'  # По умолчанию PostgreSQL для database
        
        # Генерируем DAG в зависимости от комбинации источника и приёмника
        if source_type in ['postgresql', 'clickhouse'] and sink_type in ['postgresql', 'clickhouse']:
            # Проверяем, что source_config содержит все необходимые поля
            required_fields = ['host', 'port', 'database', 'username', 'password', 'table_name']
            missing_fields = [field for field in required_fields if field not in source_config]
            if missing_fields:
                logger.warning(f"Отсутствуют поля в source_config: {missing_fields}")
                # Добавляем значения по умолчанию для отсутствующих полей
                for field in missing_fields:
                    if field == 'host':
                        source_config[field] = 'localhost'
                    elif field == 'port':
                        source_config[field] = 5432 if source_type == 'postgresql' else 8123
                    else:
                        source_config[field] = ''
            
            # Проверяем, что sink_config содержит все необходимые поля
            sink_required_fields = ['host', 'port', 'database', 'username', 'password', 'table_name']
            sink_missing_fields = [field for field in sink_required_fields if field not in sink_config]
            if sink_missing_fields:
                logger.warning(f"Отсутствуют поля в sink_config: {sink_missing_fields}")
                # Добавляем значения по умолчанию для отсутствующих полей
                for field in sink_missing_fields:
                    if field == 'host':
                        sink_config[field] = 'localhost'
                    elif field == 'port':
                        sink_config[field] = 5432 if sink_type == 'postgresql' else 8123
                    else:
                        sink_config[field] = ''
            
            return self._generate_database_to_database_dag(
                dag_id, source_config, sink_config, chunk_size, total_rows
            )
        elif source_type == 'file' and sink_type in ['postgresql', 'clickhouse']:
            return self._generate_file_to_database_dag(
                dag_id, file_id, source_table, sink_config, chunk_size
            )
        elif source_type == 'file' and sink_type == 'file':
            return self._generate_file_to_file_dag(
                dag_id, file_id, source_table, sink_config, chunk_size
            )
        elif source_type == 'file' and sink_type == 'kafka':
            return self._generate_file_to_kafka_dag(
                dag_id, file_id, source_table, sink_config, chunk_size
            )
        elif source_type in ['postgresql', 'clickhouse'] and sink_type == 'kafka':
            return self._generate_database_to_kafka_dag(
                dag_id, source_config, sink_config, chunk_size, total_rows
            )
        elif source_type == 'kafka' and sink_type in ['postgresql', 'clickhouse']:
            return self._generate_kafka_to_database_dag(
                dag_id, source_config, sink_config, chunk_size, total_rows
            )
        else:
            raise ValueError(f"Неподдерживаемая комбинация: {source_type} -> {sink_type}")
    
    def _generate_file_to_database_dag(
        self,
        dag_id: str,
        file_id: int,
        source_table: str,
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> str:
        """Генерирует DAG для перелива в базу данных"""
        
        # Извлекаем параметры подключения к БД
        db_host = sink_config.get('host', 'localhost')
        db_port = sink_config.get('port', 5432)
        db_name = sink_config.get('database', '')
        db_user = sink_config.get('username', '')
        db_password = sink_config.get('password', '')
        db_table = sink_config.get('table_name', '')
        
        # Определяем тип БД по порту или явно
        db_type = sink_config.get('db_type', 'postgresql')
        if db_port == 3306:
            db_type = 'mysql'
        elif db_port == 5432:
            db_type = 'postgresql'
            
        dag_content = f'''"""
DAG для перелива данных из SQLite в {db_type.upper()}
Файл ID: {file_id}
Источник: {source_table}
Приёмник: {db_table}@{db_host}:{db_port}/{db_name}
Размер чанка: {chunk_size}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import sqlite3
import logging

# Конфигурация DAG
default_args = {{
    "owner": "data-orchestrator",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}}

dag = DAG(
    "{dag_id}",
    default_args=default_args,
    description="Перелив данных из SQLite в {db_type.upper()}",
    schedule_interval=None,  # Запускается вручную
    catchup=False,
    tags=["data-transfer", "sqlite-to-{db_type}"],
)

# Конфигурация подключений
SQLITE_DB_PATH = '/app/data/service.db'
SOURCE_TABLE = "{source_table}"
CHUNK_SIZE = {chunk_size}

# Конфигурация приёмника
SINK_CONFIG = {{
    "host": "{db_host}",
    "port": {db_port},
    "database": "{db_name}",
    "username": "{db_user}",
    "password": "{db_password}",
    "table_name": "{db_table}",
    "db_type": "{db_type}"
}}

def get_total_rows():
    """Получает общее количество строк в исходной таблице"""
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {{SOURCE_TABLE}}")
    total_rows = cursor.fetchone()[0]
    conn.close()
    return total_rows

def process_chunk(chunk_offset: int, chunk_size: int):
    """Обрабатывает один чанк данных"""
    import pandas as pd
    import sqlite3
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        # Подключаемся к SQLite
        sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
        
        # Читаем чанк данных
        query = f"""
        SELECT * FROM {{SOURCE_TABLE}} 
        ORDER BY row_index 
        LIMIT {{chunk_size}} OFFSET {{chunk_offset}}
        """
        
        df = pd.read_sql_query(query, sqlite_conn)
        sqlite_conn.close()
        
        if df.empty:
            logger.info(f"Чанк {{chunk_offset}} пуст, пропускаем")
            return
            
        # Подготавливаем данные для вставки
        # Убираем row_index из данных
        if 'row_index' in df.columns:
            df = df.drop('row_index', axis=1)
            
        # Подключаемся к целевой БД
        if SINK_CONFIG["db_type"] == 'postgresql':
            import psycopg2
            from sqlalchemy import create_engine
            
            connection_string = f"postgresql://{{SINK_CONFIG["username"]}}:{{SINK_CONFIG["password"]}}@{{SINK_CONFIG["host"]}}:{{SINK_CONFIG["port"]}}/{{SINK_CONFIG["database"]}}"
            engine = create_engine(connection_string)
            
        elif SINK_CONFIG["db_type"] == 'mysql':
            from sqlalchemy import create_engine
            
            connection_string = f"mysql+pymysql://{{SINK_CONFIG["username"]}}:{{SINK_CONFIG["password"]}}@{{SINK_CONFIG["host"]}}:{{SINK_CONFIG["port"]}}/{{SINK_CONFIG["database"]}}"
            engine = create_engine(connection_string)
            
        else:
            raise ValueError(f"Неподдерживаемый тип БД: {{SINK_CONFIG["db_type"]}}")
        
        # Вставляем данные
        df.to_sql(
            SINK_CONFIG["table_name"],
            engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        logger.info(f"Успешно обработан чанк {{chunk_offset}}-{{chunk_offset + len(df)}} ({{len(df)}} строк)")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке чанка {{chunk_offset}}: {{str(e)}}")
        raise

def create_chunk_tasks():
    """Создает задачи для каждого чанка"""
    total_rows = get_total_rows()
    num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
    
    logging.info(f"Всего строк: {{total_rows}}, чанков: {{num_chunks}}")
    
    tasks = []
    
    for i in range(num_chunks):
        chunk_offset = i * CHUNK_SIZE
        
        task = PythonOperator(
            task_id=f'process_chunk_{{i}}',
            python_callable=process_chunk,
            op_args=[chunk_offset, CHUNK_SIZE],
            dag=dag,
        )
        tasks.append(task)
    
    return tasks

# Создаем задачи
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

chunk_tasks = create_chunk_tasks()

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Настраиваем зависимости
if chunk_tasks:
    start_task >> chunk_tasks >> end_task
else:
    start_task >> end_task
'''
        
        return dag_content
    
    def save_dag(self, dag_content: str, dag_id: str) -> str:
        """Сохраняет DAG в файл"""
        filename = f"{dag_id}.py"
        filepath = os.path.join(self.dags_path, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(dag_content)
            
            logger.info(f"DAG сохранен: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении DAG: {e}")
            raise
    
    def generate_and_save_dag(
        self,
        file_id: int = None,
        source_table: str = None,
        sink_config: Dict[str, Any] = None,
        source_config: Dict[str, Any] = None,
        chunk_size: int = 1000,
        total_rows: int = None,
        dag_id: Optional[str] = None
    ) -> str:
        """Генерирует и сохраняет DAG"""
        dag_content = self.generate_data_transfer_dag(
            file_id=file_id,
            source_table=source_table,
            sink_config=sink_config,
            source_config=source_config,
            chunk_size=chunk_size,
            total_rows=total_rows,
            dag_id=dag_id
        )
        
        if not dag_id:
            timestamp = int(datetime.now().timestamp())
            dag_id = f"data_transfer_{timestamp}"
            
        return self.save_dag(dag_content, dag_id)
    
    def list_generated_dags(self) -> list:
        """Получает список сгенерированных DAG'ов"""
        try:
            if not os.path.exists(self.dags_path):
                return []
            
            dag_files = []
            for filename in os.listdir(self.dags_path):
                if filename.endswith('.py') and filename.startswith('data_transfer_'):
                    filepath = os.path.join(self.dags_path, filename)
                    stat = os.stat(filepath)
                    dag_files.append({
                        'filename': filename,
                        'filepath': filepath,
                        'size': stat.st_size,
                        'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })
            
            return sorted(dag_files, key=lambda x: x['modified'], reverse=True)
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка DAG'ов: {e}")
            return []
    
    def delete_dag(self, dag_id: str) -> bool:
        """Удаляет DAG файл"""
        try:
            filename = f"{dag_id}.py"
            filepath = os.path.join(self.dags_path, filename)
            
            if os.path.exists(filepath):
                os.remove(filepath)
                logger.info(f"DAG удален: {filepath}")
                return True
            else:
                logger.warning(f"DAG файл не найден: {filepath}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при удалении DAG: {e}")
            return False

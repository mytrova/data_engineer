"""
Шаблоны DAG для разных типов переливов данных
"""
from typing import Dict, Any

class DAGTemplates:
    """Шаблоны DAG для различных сценариев переливов"""
    
    @staticmethod
    def database_to_database_template(
        dag_id: str,
        source_type: str,
        sink_type: str,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> str:
        """Шаблон DAG для перелива из базы данных в базу данных"""
        
        # Если это ClickHouse, добавляем создание таблицы
        if sink_type == 'clickhouse':
            create_table_task = f'''
    create_table = PythonOperator(
        task_id="create_clickhouse_table",
        python_callable=create_clickhouse_table_from_postgresql,
        op_kwargs={{
            "source_config": {source_config},
            "sink_config": {sink_config}
        }}
    )'''
            
            task_dependencies = '''
    create_table >> run_benthos'''
        else:
            create_table_task = ""
            task_dependencies = ""
        
        return f'''"""
DAG для перелива данных из {source_type.upper()} в {sink_type.upper()} с использованием Benthos
Источник: {source_config.get('table_name', '')}@{source_config.get('host', '')}:{source_config.get('port', '')}/{source_config.get('database', '')}
Приёмник: {sink_config.get('table_name', '')}@{sink_config.get('host', '')}:{sink_config.get('port', '')}/{sink_config.get('database', '')}
Размер чанка: {chunk_size}
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def create_clickhouse_table_from_postgresql(source_config, sink_config):
    """Создает таблицу в ClickHouse на основе схемы PostgreSQL"""
    logger.info(f"=== НАЧАЛО СОЗДАНИЯ ТАБЛИЦЫ CLICKHOUSE ===")
    logger.info("source_config: " + str(source_config))
    logger.info("sink_config: " + str(sink_config))
    try:
        import sys
        import os
        sys.path.append('/opt/airflow')
        sys.path.append('/app')
        try:
            from backend.app.services.schema_mapper import SchemaMapper
        except ImportError:
            # Fallback путь
            sys.path.append('/app/backend')
            from app.services.schema_mapper import SchemaMapper
        
        # Получаем схему из PostgreSQL
        schema = SchemaMapper.get_postgresql_schema(
            host=source_config.get('host', ''),
            port=source_config.get('port', 5432),
            database=source_config.get('database', ''),
            username=source_config.get('username', ''),
            password=source_config.get('password', ''),
            table_name=source_config.get('table_name', '')
        )
        
        if not schema:
            logger.error("Не удалось получить схему таблицы из PostgreSQL")
            logger.info(f"=== РЕЗУЛЬТАТ СОЗДАНИЯ ТАБЛИЦЫ: False ===")
            return False
        
        # Создаем таблицу в ClickHouse
        table_creation_success = SchemaMapper.create_clickhouse_table(
            host=sink_config.get('host', ''),
            port=sink_config.get('port', 8123),
            database=sink_config.get('database', ''),
            username=sink_config.get('username', 'default'),
            password=sink_config.get('password', ''),
            table_name=sink_config.get('table_name', ''),
            schema=schema
        )
        
        if table_creation_success:
            logger.info("=== ТАБЛИЦА В CLICKHOUSE УСПЕШНО СОЗДАНА ===")
            logger.info("=== РЕЗУЛЬТАТ СОЗДАНИЯ ТАБЛИЦЫ: True ===")
        else:
            logger.error("=== ОШИБКА СОЗДАНИЯ ТАБЛИЦЫ В CLICKHOUSE ===")
            logger.info("=== РЕЗУЛЬТАТ СОЗДАНИЯ ТАБЛИЦЫ: False ===")
        
        return table_creation_success
        
    except Exception:
        logger.error("Ошибка при создании таблицы в ClickHouse")
        logger.info("=== РЕЗУЛЬТАТ СОЗДАНИЯ ТАБЛИЦЫ: False ===")
        return False

with DAG(
    dag_id="{dag_id}",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["data-transfer", "benthos", "{source_type}-to-{sink_type}"]
) as dag:
{create_table_task}

    run_benthos = BashOperator(
        task_id="run_benthos",
        bash_command=f"""
            docker run --rm \\
              -v $PROJECT_ROOT/benthos/{dag_id}.yaml:/config.yaml \\
              docker.redpanda.com/redpandadata/connect run /config.yaml
        """
    ){task_dependencies}
'''
    
    @staticmethod
    def file_to_database_template(
        dag_id: str,
        file_id: int,
        source_table: str,
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> str:
        """Шаблон DAG для перелива из файла в базу данных"""
        db_type = sink_config.get('db_type', 'postgresql')
        db_host = sink_config.get('host', 'localhost')
        db_port = sink_config.get('port', 5432)
        db_name = sink_config.get('database', '')
        db_user = sink_config.get('username', '')
        db_password = sink_config.get('password', '')
        db_table = sink_config.get('table_name', '')
        
        return f'''"""
DAG для перелива данных из SQLite в {db_type.upper()} с использованием Benthos
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
import yaml
import subprocess
import tempfile
import os

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
    description="Перелив данных из SQLite в {db_type.upper()} с использованием Benthos",
    schedule_interval=None,  # Запускается вручную
    catchup=False,
    tags=["data-transfer", "benthos", "sqlite-to-{db_type}"],
)

# Конфигурация подключений
SQLITE_DB_PATH = '/app/data/service.db'
SOURCE_TABLE = "{source_table}"
CHUNK_SIZE = {chunk_size}
DAG_ID = "{dag_id}"

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

def create_benthos_config():
    """Создает конфигурацию Benthos для переливки данных"""
    config = {{
        "input": {{
            "sql_select": {{
                "driver": "sqlite3",
                "dsn": f"file:{{SQLITE_DB_PATH}}",
                "table": SOURCE_TABLE,
                "columns": ["*"],
                "where": "1=1",
                "batch_count": CHUNK_SIZE
            }}
        }},
        "buffer": {{
            "memory": {{
                "batch_size": CHUNK_SIZE
            }}
        }},
        "pipeline": {{
            "processors": [
                {{
                    "mapping": {{
                        "root": ".",
                        "mapping": "root = this"
                    }}
                }}
            ]
        }},
        "output": {{
            "sql_insert": {{
                "driver": "{db_type}",
                "dsn": f"{db_type}://{{SINK_CONFIG['username']}}:{{SINK_CONFIG['password']}}@{{SINK_CONFIG['host']}}:{{SINK_CONFIG['port']}}/{{SINK_CONFIG['database']}}",
                "table": SINK_CONFIG['table_name'],
                "columns": ["*"],
                "batch_size": CHUNK_SIZE
            }}
        }}
    }}
    
    # Сохраняем конфигурацию во временный файл
    config_file = f"/tmp/benthos_config_{{DAG_ID}}.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    return config_file

def run_benthos_transfer():
    """Запускает Benthos для переливки данных"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Создаем конфигурацию Benthos
        config_file = create_benthos_config()
        
        # Запускаем Benthos
        cmd = ["benthos", "-c", config_file, "stream"]
        logger.info(f"Запускаем Benthos с командой: {{' '.join(cmd)}}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1 час таймаут
        )
        
        if result.returncode == 0:
            logger.info("Benthos успешно завершил переливку данных")
            logger.info(f"Вывод: {{result.stdout}}")
        else:
            logger.error(f"Ошибка при выполнении Benthos: {{result.stderr}}")
            raise Exception(f"Benthos завершился с ошибкой: {{result.stderr}}")
            
    except subprocess.TimeoutExpired:
        logger.error("Benthos превысил время выполнения")
        raise Exception("Benthos превысил время выполнения")
    except Exception as e:
        logger.error(f"Ошибка при запуске Benthos: {{str(e)}}")
        raise
    finally:
        # Удаляем временный файл конфигурации
        try:
            if 'config_file' in locals():
                os.remove(config_file)
        except:
            pass

# Создаем задачу для переливки данных
transfer_task = PythonOperator(
    task_id='benthos_transfer',
    python_callable=run_benthos_transfer,
    dag=dag,
)

# Создаем задачи
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Настраиваем зависимости
start_task >> transfer_task >> end_task
'''
    
    @staticmethod
    def database_to_kafka_template(
        dag_id: str,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> str:
        """Шаблон DAG для перелива из базы данных в Kafka"""
        source_type = source_config.get('db_type', 'postgresql')
        kafka_servers = sink_config.get('host', 'localhost:9092')
        kafka_topic = sink_config.get('database', '')
        
        return f'''"""
DAG для перелива данных из {source_type.upper()} в Kafka с использованием Benthos
Источник: {source_config.get('table_name', '')}@{source_config.get('host', '')}:{source_config.get('port', '')}/{source_config.get('database', '')}
Приёмник: {kafka_topic}@{kafka_servers}
Размер чанка: {chunk_size}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import yaml
import subprocess
import tempfile
import os

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
    description="Перелив данных из {source_type.upper()} в Kafka с использованием Benthos",
    schedule_interval=None,  # Запускается вручную
    catchup=False,
    tags=["data-transfer", "benthos", "{source_type}-to-kafka"],
)

# Конфигурация источника
SOURCE_CONFIG = {{
    "host": "{source_config.get('host', '')}",
    "port": {source_config.get('port', '')},
    "database": "{source_config.get('database', '')}",
    "username": "{source_config.get('username', '')}",
    "password": "{source_config.get('password', '')}",
    "table_name": "{source_config.get('table_name', '')}",
    "db_type": "{source_type}"
}}

# Конфигурация Kafka
KAFKA_CONFIG = {{
    "bootstrap_servers": "{kafka_servers}",
    "topic": "{kafka_topic}"
}}

CHUNK_SIZE = {chunk_size}
DAG_ID = "{dag_id}"

def create_benthos_config():
    """Создает конфигурацию Benthos для переливки данных в Kafka"""
    config = {{
        "input": {{
            "sql_select": {{
                "driver": "{source_type}",
                "dsn": f"{source_type}://{{SOURCE_CONFIG['username']}}:{{SOURCE_CONFIG['password']}}@{{SOURCE_CONFIG['host']}}:{{SOURCE_CONFIG['port']}}/{{SOURCE_CONFIG['database']}}",
                "table": SOURCE_CONFIG['table_name'],
                "columns": ["*"],
                "where": "1=1",
                "batch_count": CHUNK_SIZE
            }}
        }},
        "buffer": {{
            "memory": {{
                "batch_size": CHUNK_SIZE
            }}
        }},
        "pipeline": {{
            "processors": [
                {{
                    "mapping": {{
                        "root": ".",
                        "mapping": "root = this"
                    }}
                }},
                {{
                    "json": {{
                        "operator": "to_json"
                    }}
                }}
            ]
        }},
        "output": {{
            "kafka": {{
                "addresses": [KAFKA_CONFIG['bootstrap_servers']],
                "topic": KAFKA_CONFIG['topic'],
                "key": "{{.id}}",
                "batch_size": CHUNK_SIZE
            }}
        }}
    }}
    
    # Сохраняем конфигурацию во временный файл
    config_file = f"/tmp/benthos_config_{{DAG_ID}}.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    return config_file

def run_benthos_transfer():
    """Запускает Benthos для переливки данных"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Создаем конфигурацию Benthos
        config_file = create_benthos_config()
        
        # Запускаем Benthos
        cmd = ["benthos", "-c", config_file, "stream"]
        logger.info(f"Запускаем Benthos с командой: {{' '.join(cmd)}}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1 час таймаут
        )
        
        if result.returncode == 0:
            logger.info("Benthos успешно завершил переливку данных")
            logger.info(f"Вывод: {{result.stdout}}")
        else:
            logger.error(f"Ошибка при выполнении Benthos: {{result.stderr}}")
            raise Exception(f"Benthos завершился с ошибкой: {{result.stderr}}")
            
    except subprocess.TimeoutExpired:
        logger.error("Benthos превысил время выполнения")
        raise Exception("Benthos превысил время выполнения")
    except Exception as e:
        logger.error(f"Ошибка при запуске Benthos: {{str(e)}}")
        raise
    finally:
        # Удаляем временный файл конфигурации
        try:
            if 'config_file' in locals():
                os.remove(config_file)
        except:
            pass

# Создаем задачу для переливки данных
transfer_task = PythonOperator(
    task_id='benthos_transfer',
    python_callable=run_benthos_transfer,
    dag=dag,
)

# Создаем задачи
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Настраиваем зависимости
start_task >> transfer_task >> end_task
'''

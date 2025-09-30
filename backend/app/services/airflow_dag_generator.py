"""
Сервис для генерации DAG'ов Airflow для перелива данных с использованием Benthos
DEPRECATED: Используйте AirflowDAGGeneratorSimple вместо этого класса
"""
import os
import json
import yaml
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import logging

from .airflow_dag_generator_simple import AirflowDAGGeneratorSimple

logger = logging.getLogger(__name__)

class AirflowDAGGenerator:
    """Генератор DAG'ов для Airflow с использованием Benthos"""
    
    def __init__(self, dags_path: str = "/app/airflow/dags"):
        self.dags_path = dags_path
        self._simple_generator = AirflowDAGGeneratorSimple(dags_path)
        
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
        """Генерирует DAG для перелива данных с использованием Benthos (делегирует к упрощенному генератору)"""
        return self._simple_generator.generate_data_transfer_dag(
            file_id=file_id,
            source_table=source_table,
            sink_config=sink_config,
            source_config=source_config,
            chunk_size=chunk_size,
            total_rows=total_rows,
            dag_id=dag_id
        )
    
    def _generate_file_to_database_dag(
        self,
        dag_id: str,
        file_id: int,
        source_table: str,
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> str:
        """Генерирует DAG для перелива из файла в базу данных с использованием Benthos"""
        
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
        elif db_port == 8123:
            db_type = 'clickhouse'
            
        # Создаем конфигурацию Benthos
        benthos_config = self._create_benthos_config(
            source_type='sqlite',
            sink_type=db_type,
            source_config={
                'database': '/app/data/service.db',
                'table': source_table,
                'chunk_size': chunk_size
            },
            sink_config={
                'host': db_host,
                'port': db_port,
                'database': db_name,
                'username': db_user,
                'password': db_password,
                'table': db_table
            }
        )
        
        dag_content = f'''"""
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
        
        return dag_content
    
    def _generate_database_to_database_dag(
        self,
        dag_id: str,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        chunk_size: int,
        total_rows: int
    ) -> str:
        """Генерирует DAG для перелива из базы данных в базу данных с использованием Benthos через Docker"""
        
        source_type = source_config.get('db_type', 'postgresql')
        sink_type = sink_config.get('db_type', 'postgresql')
        
        # Создаем конфигурацию Benthos
        benthos_config = self._create_benthos_config(source_config, sink_config, chunk_size)
        
        # Сохраняем конфигурацию в файл
        config_file_path = f"/app/benthos/{dag_id}.yaml"
        self._save_benthos_config(benthos_config, config_file_path)
        
        dag_content = f'''"""
DAG для перелива данных из {source_type.upper()} в {sink_type.upper()} с использованием Benthos
Источник: {source_config.get('table_name', '')}@{source_config.get('host', '')}:{source_config.get('port', '')}/{source_config.get('database', '')}
Приёмник: {sink_config.get('table_name', '')}@{sink_config.get('host', '')}:{sink_config.get('port', '')}/{sink_config.get('database', '')}
Размер чанка: {chunk_size}
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="{dag_id}",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["data-transfer", "benthos", "{source_type}-to-{sink_type}"]
) as dag:

    run_benthos = BashOperator(
        task_id="run_benthos",
        bash_command=f"""
            docker run --rm \\
              -v /app/benthos/{dag_id}.yaml:/config.yaml \\
              docker.redpanda.com/redpandadata/connect run /config.yaml
        """
    )
'''
        
        return dag_content
    
    def _create_benthos_config(self, source_config: Dict[str, Any], sink_config: Dict[str, Any], chunk_size: int) -> Dict[str, Any]:
        """Создает конфигурацию Benthos для переливки данных"""
        source_type = source_config.get('db_type', 'postgresql')
        sink_type = sink_config.get('db_type', 'postgresql')
        
        # Формируем DSN для источника
        source_dsn = f"{source_type}://{source_config.get('username', '')}:{source_config.get('password', '')}@{source_config.get('host', '')}:{source_config.get('port', '')}/{source_config.get('database', '')}"
        
        # Формируем DSN для приёмника
        if sink_type == 'clickhouse':
            sink_dsn = f"clickhouse://{sink_config.get('username', '')}:{sink_config.get('password', '')}@{sink_config.get('host', '')}:{sink_config.get('port', '')}/{sink_config.get('database', '')}"
        else:
            sink_dsn = f"{sink_type}://{sink_config.get('username', '')}:{sink_config.get('password', '')}@{sink_config.get('host', '')}:{sink_config.get('port', '')}/{sink_config.get('database', '')}"
        
        config = {
            "input": {
                "sql_select": {
                    "driver": source_type,
                    "dsn": source_dsn,
                    "table": source_config.get('table_name', ''),
                    "columns": ["*"],
                    "where": "1=1",
                    "batch_count": chunk_size
                }
            },
            "buffer": {
                "memory": {
                    "batch_size": chunk_size
                }
            },
            "pipeline": {
                "processors": [
                    {
                        "mapping": {
                            "root": ".",
                            "mapping": "root = this"
                        }
                    }
                ]
            },
            "output": {
                "sql_insert": {
                    "driver": sink_type,
                    "dsn": sink_dsn,
                    "table": sink_config.get('table_name', ''),
                    "columns": ["*"],
                    "batch_size": chunk_size
                }
            }
        }
        
        return config
    
    def _save_benthos_config(self, config: Dict[str, Any], file_path: str) -> None:
        """Сохраняет конфигурацию Benthos в файл"""
        import yaml
        import os
        import shutil
        
        logger.info(f"=== СОХРАНЕНИЕ КОНФИГУРАЦИИ BENTHOS ===")
        logger.info(f"Путь: {file_path}")
        logger.info(f"Конфигурация: {config}")
        
        # Создаем директорию если не существует
        dir_path = os.path.dirname(file_path)
        if dir_path and dir_path != file_path:  # Проверяем, что dir_path не равен file_path
            try:
                os.makedirs(dir_path, exist_ok=True)
                logger.info(f"Создана директория: {dir_path}")
            except Exception as e:
                logger.error(f"ОШИБКА при создании директории {dir_path}: {e}")
                raise Exception(f"Не удалось создать директорию: {dir_path}")
        
        # Дополнительная проверка - убеждаемся, что директория создалась
        if not os.path.exists(dir_path):
            logger.error(f"ОШИБКА: Директория {dir_path} не создалась!")
            raise Exception(f"Не удалось создать директорию: {dir_path}")
        
        # Удаляем файл если он существует как директория
        if os.path.exists(file_path):
            if os.path.isdir(file_path):
                shutil.rmtree(file_path)
                logger.info(f"Удалена директория: {file_path}")
            else:
                os.remove(file_path)
                logger.info(f"Удален файл: {file_path}")
        
        # Сохраняем конфигурацию как файл
        try:
            # Дополнительная проверка - убеждаемся, что путь не является директорией
            if os.path.exists(file_path) and os.path.isdir(file_path):
                logger.error(f"ОШИБКА: Путь {file_path} уже существует как директория!")
                shutil.rmtree(file_path)
                logger.info(f"Удалена директория: {file_path}")
            
            # Создаем файл с явным указанием режима 'w'
            with open(file_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False)
            
            # Проверяем, что файл создался правильно
            if os.path.exists(file_path) and os.path.isfile(file_path):
                file_size = os.path.getsize(file_path)
                logger.info(f"Конфигурация Benthos успешно сохранена как файл: {file_path} (размер: {file_size} байт)")
            else:
                logger.error(f"ОШИБКА: Файл не создался или создался как директория: {file_path}")
                # Пытаемся удалить директорию если она создалась
                if os.path.exists(file_path) and os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    logger.info(f"Удалена ошибочно созданная директория: {file_path}")
        except Exception as e:
            logger.error(f"ОШИБКА при сохранении конфигурации: {e}")
            raise
    
    def _generate_file_to_kafka_dag(
        self,
        dag_id: str,
        file_id: int,
        source_table: str,
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> str:
        """Генерирует DAG для перелива из файла в Kafka с использованием Benthos"""
        
        kafka_servers = sink_config.get('host', 'localhost:9092')
        kafka_topic = sink_config.get('database', '')
        
        dag_content = f'''"""
DAG для перелива данных из SQLite в Kafka с использованием Benthos
Файл ID: {file_id}
Источник: {source_table}
Приёмник: {kafka_topic}@{kafka_servers}
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
    description="Перелив данных из SQLite в Kafka с использованием Benthos",
    schedule_interval=None,  # Запускается вручную
    catchup=False,
    tags=["data-transfer", "benthos", "sqlite-to-kafka"],
)

# Конфигурация подключений
SQLITE_DB_PATH = '/app/data/service.db'
SOURCE_TABLE = "{source_table}"
CHUNK_SIZE = {chunk_size}
DAG_ID = "{dag_id}"

# Конфигурация Kafka
KAFKA_CONFIG = {{
    "bootstrap_servers": "{kafka_servers}",
    "topic": "{kafka_topic}"
}}

def create_benthos_config():
    """Создает конфигурацию Benthos для переливки данных в Kafka"""
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
        
        return dag_content
    
    def _generate_database_to_kafka_dag(
        self,
        dag_id: str,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        chunk_size: int,
        total_rows: int
    ) -> str:
        """Генерирует DAG для перелива из базы данных в Kafka с использованием Benthos"""
        
        source_type = source_config.get('db_type', 'postgresql')
        kafka_servers = sink_config.get('host', 'localhost:9092')
        kafka_topic = sink_config.get('database', '')
        
        dag_content = f'''"""
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
        
        return dag_content
    
    def _generate_kafka_to_database_dag(
        self,
        dag_id: str,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        chunk_size: int,
        total_rows: int
    ) -> str:
        """Генерирует DAG для перелива из Kafka в базу данных с использованием Benthos"""
        
        sink_type = sink_config.get('db_type', 'postgresql')
        kafka_servers = source_config.get('host', 'localhost:9092')
        kafka_topic = source_config.get('database', '')
        
        dag_content = f'''"""
DAG для перелива данных из Kafka в {sink_type.upper()} с использованием Benthos
Источник: {kafka_topic}@{kafka_servers}
Приёмник: {sink_config.get('table_name', '')}@{sink_config.get('host', '')}:{sink_config.get('port', '')}/{sink_config.get('database', '')}
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
    description="Перелив данных из Kafka в {sink_type.upper()} с использованием Benthos",
    schedule_interval=None,  # Запускается вручную
    catchup=False,
    tags=["data-transfer", "benthos", "kafka-to-{sink_type}"],
)

# Конфигурация Kafka
KAFKA_CONFIG = {{
    "bootstrap_servers": "{kafka_servers}",
    "topic": "{kafka_topic}",
    "group_id": "{source_config.get('username', 'benthos-consumer')}"
}}

# Конфигурация приёмника
SINK_CONFIG = {{
    "host": "{sink_config.get('host', '')}",
    "port": {sink_config.get('port', '')},
    "database": "{sink_config.get('database', '')}",
    "username": "{sink_config.get('username', '')}",
    "password": "{sink_config.get('password', '')}",
    "table_name": "{sink_config.get('table_name', '')}",
    "db_type": "clickhouse" if "{sink_type}" == "clickhouse" else "{sink_type}"
}}

CHUNK_SIZE = {chunk_size}
DAG_ID = "{dag_id}"

def create_benthos_config():
    """Создает конфигурацию Benthos для переливки данных из Kafka"""
    config = {{
        "input": {{
            "kafka": {{
                "addresses": [KAFKA_CONFIG['bootstrap_servers']],
                "topics": [KAFKA_CONFIG['topic']],
                "consumer_group": KAFKA_CONFIG['group_id']
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
                    "json": {{
                        "operator": "from_json"
                    }}
                }},
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
                "driver": "clickhouse" if "{sink_type}" == "clickhouse" else "{sink_type}",
                "dsn": f"clickhouse://{{SINK_CONFIG['username']}}:{{SINK_CONFIG['password']}}@{{SINK_CONFIG['host']}}:{{SINK_CONFIG['port']}}/{{SINK_CONFIG['database']}}" if "{sink_type}" == "clickhouse" else f"{sink_type}://{{SINK_CONFIG['username']}}:{{SINK_CONFIG['password']}}@{{SINK_CONFIG['host']}}:{{SINK_CONFIG['port']}}/{{SINK_CONFIG['database']}}",
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
        
        return dag_content
    
    def _generate_file_to_file_dag(
        self,
        dag_id: str,
        file_id: int,
        source_table: str,
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> str:
        """Генерирует DAG для перелива из файла в файл с использованием Benthos"""
        
        output_format = sink_config.get('type', 'csv')
        output_path = sink_config.get('file_path', f'/tmp/output_{dag_id}.{output_format}')
        
        dag_content = f'''"""
DAG для перелива данных из SQLite в {output_format.upper()} файл с использованием Benthos
Файл ID: {file_id}
Источник: {source_table}
Приёмник: {output_path}
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
    description="Перелив данных из SQLite в {output_format.upper()} с использованием Benthos",
    schedule_interval=None,  # Запускается вручную
    catchup=False,
    tags=["data-transfer", "benthos", "sqlite-to-{output_format}"],
)

# Конфигурация подключений
SQLITE_DB_PATH = '/app/data/service.db'
SOURCE_TABLE = "{source_table}"
CHUNK_SIZE = {chunk_size}
DAG_ID = "{dag_id}"
OUTPUT_PATH = "{output_path}"

def create_benthos_config():
    """Создает конфигурацию Benthos для переливки данных в файл"""
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
            "file": {{
                "path": OUTPUT_PATH,
                "codec": "{output_format}"
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
        
        return dag_content
    
    def _create_benthos_config_old(
        self,
        source_type: str,
        sink_type: str,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Создает конфигурацию Benthos для переливки данных"""
        
        # Базовая конфигурация
        config = {
            "buffer": {
                "memory": {
                    "batch_size": source_config.get('chunk_size', 1000)
                }
            },
            "pipeline": {
                "processors": [
                    {
                        "mapping": {
                            "root": ".",
                            "mapping": "root = this"
                        }
                    }
                ]
            }
        }
        
        # Конфигурация источника
        if source_type == 'sqlite':
            config["input"] = {
                "sql_select": {
                    "driver": "sqlite3",
                    "dsn": f"file:{source_config['database']}",
                    "table": source_config['table'],
                    "columns": ["*"],
                    "where": "1=1",
                    "batch_count": source_config.get('chunk_size', 1000)
                }
            }
        elif source_type in ['postgresql', 'clickhouse']:
            config["input"] = {
                "sql_select": {
                    "driver": source_type,
                    "dsn": f"{source_type}://{source_config['username']}:{source_config['password']}@{source_config['host']}:{source_config['port']}/{source_config['database']}",
                    "table": source_config['table'],
                    "columns": ["*"],
                    "where": "1=1",
                    "batch_count": source_config.get('chunk_size', 1000)
                }
            }
        elif source_type == 'kafka':
            config["input"] = {
                "kafka": {
                    "addresses": [source_config['bootstrap_servers']],
                    "topics": [source_config['topic']],
                    "consumer_group": source_config.get('group_id', 'benthos-consumer')
                }
            }
        
        # Конфигурация приёмника
        if sink_type in ['postgresql', 'clickhouse', 'mysql']:
            config["output"] = {
                "sql_insert": {
                    "driver": sink_type,
                    "dsn": f"{sink_type}://{sink_config['username']}:{sink_config['password']}@{sink_config['host']}:{sink_config['port']}/{sink_config['database']}",
                    "table": sink_config['table'],
                    "columns": ["*"],
                    "batch_size": sink_config.get('chunk_size', 1000)
                }
            }
        elif sink_type == 'kafka':
            config["output"] = {
                "kafka": {
                    "addresses": [sink_config['bootstrap_servers']],
                    "topic": sink_config['topic'],
                    "key": "{{.id}}",
                    "batch_size": sink_config.get('chunk_size', 1000)
                }
            }
            # Добавляем JSON сериализацию для Kafka
            config["pipeline"]["processors"].append({
                "json": {
                    "operator": "to_json"
                }
            })
        
        return config
    
    def save_dag(self, dag_content: str, dag_id: str) -> str:
        """Сохраняет DAG в файл (делегирует к упрощенному генератору)"""
        return self._simple_generator.save_dag(dag_content, dag_id)
    
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
        """Генерирует и сохраняет DAG (делегирует к упрощенному генератору)"""
        return self._simple_generator.generate_and_save_dag(
            file_id=file_id,
            source_table=source_table,
            sink_config=sink_config,
            source_config=source_config,
            chunk_size=chunk_size,
            total_rows=total_rows,
            dag_id=dag_id
        )
    
    def list_generated_dags(self) -> list:
        """Получает список сгенерированных DAG'ов (делегирует к упрощенному генератору)"""
        return self._simple_generator.list_generated_dags()
    
    def delete_dag(self, dag_id: str) -> bool:
        """Удаляет DAG файл (делегирует к упрощенному генератору)"""
        return self._simple_generator.delete_dag(dag_id)
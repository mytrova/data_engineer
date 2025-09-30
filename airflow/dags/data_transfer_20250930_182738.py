"""
DAG для перелива данных из POSTGRESQL в CLICKHOUSE с использованием Benthos
Источник: from_json_big@158.160.5.104:5432/api_db
Приёмник: from_postgresql@158.160.5.104:8123/testdb
Размер чанка: 10000
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def create_clickhouse_table_from_postgresql(source_config, sink_config):
    """Создает таблицу в ClickHouse на основе схемы PostgreSQL"""
    try:
        import sys
        import os
        sys.path.append('/opt/airflow')
        from backend.app.services.schema_mapper import SchemaMapper
        
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
            return False
        
        # Создаем таблицу в ClickHouse
        success = SchemaMapper.create_clickhouse_table(
            host=sink_config.get('host', ''),
            port=sink_config.get('port', 8123),
            database=sink_config.get('database', ''),
            username=sink_config.get('username', 'default'),
            password=sink_config.get('password', ''),
            table_name=sink_config.get('table_name', ''),
            schema=schema
        )
        
        if success:
            logger.info("Таблица в ClickHouse успешно создана")
        else:
            logger.error("Ошибка создания таблицы в ClickHouse")
            
        return success
        
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        return False

with DAG(
    dag_id="data_transfer_20250930_182738",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["data-transfer", "benthos", "postgresql-to-clickhouse"]
) as dag:

    create_table = PythonOperator(
        task_id="create_clickhouse_table",
        python_callable=create_clickhouse_table_from_postgresql,
        op_kwargs={
            "source_config": {'type': 'postgresql', 'host': '158.160.5.104', 'port': '5432', 'database': 'api_db', 'username': 'api_user', 'password': 'secure_password', 'table_name': 'from_json_big'},
            "sink_config": {'type': 'clickhouse', 'host': '158.160.5.104', 'port': '8123', 'database': 'testdb', 'username': 'myuser', 'password': 'mypassword', 'table_name': 'from_postgresql'}
        }
    )

    run_benthos = BashOperator(
        task_id="run_benthos",
        bash_command=f"""
            docker run --rm \
              -v $PROJECT_ROOT/benthos/data_transfer_20250930_182738.yaml:/config.yaml \
              docker.redpanda.com/redpandadata/connect run /config.yaml
        """
    )
    create_table >> run_benthos

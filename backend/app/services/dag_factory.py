"""
Фабрика для создания DAG'ов разных типов
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from .benthos_config import BenthosConfigManager
from .benthos_config_builder import BenthosConfigBuilder
from .dag_templates import DAGTemplates

logger = logging.getLogger(__name__)

class DAGFactory:
    """Фабрика для создания DAG'ов"""
    
    def __init__(self, dags_path: str = "/app/airflow/dags"):
        self.dags_path = dags_path
        self.benthos_config_manager = BenthosConfigManager()
        self.benthos_config_builder = BenthosConfigBuilder()
        self.dag_templates = DAGTemplates()
    
    def create_database_to_database_dag(
        self,
        dag_id: str,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        chunk_size: int,
        total_rows: int
    ) -> str:
        """Создает DAG для перелива из базы данных в базу данных"""
        logger.info(f"Создание DAG для перелива из БД в БД: {dag_id}")
        
        source_type = source_config.get('type', 'postgresql')
        sink_type = sink_config.get('type', 'postgresql')
        
        # Создаем конфигурацию Benthos
        benthos_config = self.benthos_config_builder.build_database_to_database_config(
            source_config, sink_config, chunk_size
        )
        
        # Сохраняем конфигурацию
        config_file_path = self.benthos_config_manager.get_config_path(dag_id)
        self.benthos_config_manager.save_config(benthos_config, config_file_path)
        
        # Создаем DAG
        return self.dag_templates.database_to_database_template(
            dag_id, source_type, sink_type, source_config, sink_config, chunk_size
        )
    
    def create_file_to_database_dag(
        self,
        dag_id: str,
        file_id: int,
        source_table: str,
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> str:
        """Создает DAG для перелива из файла в базу данных"""
        logger.info(f"Создание DAG для перелива из файла в БД: {dag_id}")
        
        return self.dag_templates.file_to_database_template(
            dag_id, file_id, source_table, sink_config, chunk_size
        )
    
    def create_database_to_kafka_dag(
        self,
        dag_id: str,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        chunk_size: int,
        total_rows: int
    ) -> str:
        """Создает DAG для перелива из базы данных в Kafka"""
        logger.info(f"Создание DAG для перелива из БД в Kafka: {dag_id}")
        
        return self.dag_templates.database_to_kafka_template(
            dag_id, source_config, sink_config, chunk_size
        )
    
    def create_kafka_to_database_dag(
        self,
        dag_id: str,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        chunk_size: int,
        total_rows: int
    ) -> str:
        """Создает DAG для перелива из Kafka в базу данных"""
        logger.info(f"Создание DAG для перелива из Kafka в БД: {dag_id}")
        
        # Создаем конфигурацию Benthos
        benthos_config = self.benthos_config_builder.build_kafka_to_database_config(
            source_config, sink_config, chunk_size
        )
        
        # Сохраняем конфигурацию
        config_file_path = self.benthos_config_manager.get_config_path(dag_id)
        self.benthos_config_manager.save_config(benthos_config, config_file_path)
        
        # Создаем DAG (используем тот же шаблон, что и для database_to_database)
        sink_type = sink_config.get('db_type', 'postgresql')
        source_type = 'kafka'
        
        return self.dag_templates.database_to_database_template(
            dag_id, source_type, sink_type, source_config, sink_config, chunk_size
        )
    
    def create_file_to_file_dag(
        self,
        dag_id: str,
        file_id: int,
        source_table: str,
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> str:
        """Создает DAG для перелива из файла в файл"""
        logger.info(f"Создание DAG для перелива из файла в файл: {dag_id}")
        
        # Для file-to-file используем тот же шаблон, что и для file-to-database
        # но с измененной конфигурацией приёмника
        return self.dag_templates.file_to_database_template(
            dag_id, file_id, source_table, sink_config, chunk_size
        )
    
    def create_file_to_kafka_dag(
        self,
        dag_id: str,
        file_id: int,
        source_table: str,
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> str:
        """Создает DAG для перелива из файла в Kafka"""
        logger.info(f"Создание DAG для перелива из файла в Kafka: {dag_id}")
        
        # Для file-to-kafka используем тот же шаблон, что и для database-to-kafka
        # но с измененной конфигурацией источника
        source_config = {
            'type': 'file',
            'table_name': source_table
        }
        
        return self.dag_templates.database_to_kafka_template(
            dag_id, source_config, sink_config, chunk_size
        )

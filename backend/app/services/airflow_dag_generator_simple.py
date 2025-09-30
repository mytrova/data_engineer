"""
Упрощенный генератор DAG'ов для Airflow
"""
import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from .dag_factory import DAGFactory

logger = logging.getLogger(__name__)

class AirflowDAGGeneratorSimple:
    """Упрощенный генератор DAG'ов для Airflow"""
    
    def __init__(self, dags_path: str = "/app/airflow/dags"):
        self.dags_path = dags_path
        self.dag_factory = DAGFactory(dags_path)
    
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
        Генерирует DAG для перелива данных с использованием Benthos
        """
        logger.info("=== НАЧАЛО ГЕНЕРАЦИИ DAG С BENTHOS ===")
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
        
        logger.info(f"source_type: {source_type}, sink_type: {sink_type}")
        
        # Нормализуем типы
        if source_type == 'database':
            source_type = 'postgresql'  # По умолчанию PostgreSQL для database
        if sink_type == 'database':
            sink_type = 'postgresql'  # По умолчанию PostgreSQL для database
        
        # Выбираем подходящий метод создания DAG
        if source_type in ['postgresql', 'clickhouse'] and sink_type in ['postgresql', 'clickhouse']:
            return self.dag_factory.create_database_to_database_dag(
                dag_id, source_config, sink_config, chunk_size, total_rows
            )
        elif source_type == 'file' and sink_type in ['postgresql', 'clickhouse']:
            return self.dag_factory.create_file_to_database_dag(
                dag_id, file_id, source_table, sink_config, chunk_size
            )
        elif source_type == 'file' and sink_type == 'file':
            return self.dag_factory.create_file_to_file_dag(
                dag_id, file_id, source_table, sink_config, chunk_size
            )
        elif source_type == 'file' and sink_type == 'kafka':
            return self.dag_factory.create_file_to_kafka_dag(
                dag_id, file_id, source_table, sink_config, chunk_size
            )
        elif source_type in ['postgresql', 'clickhouse'] and sink_type == 'kafka':
            return self.dag_factory.create_database_to_kafka_dag(
                dag_id, source_config, sink_config, chunk_size, total_rows
            )
        elif source_type == 'kafka' and sink_type in ['postgresql', 'clickhouse']:
            return self.dag_factory.create_kafka_to_database_dag(
                dag_id, source_config, sink_config, chunk_size, total_rows
            )
        else:
            raise ValueError(f"Неподдерживаемая комбинация: {source_type} -> {sink_type}")
    
    def save_dag(self, dag_content: str, dag_id: str) -> str:
        """Сохраняет DAG в файл"""
        filename = f"{dag_id}.py"
        filepath = os.path.join(self.dags_path, filename)
        
        try:
            # Создаем директорию, если она не существует
            os.makedirs(self.dags_path, exist_ok=True)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(dag_content)
            
            logger.info(f"DAG сохранен: {filepath}")
            logger.info(f"Размер файла: {os.path.getsize(filepath)} байт")
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
                logger.warning(f"Директория DAG'ов не существует: {self.dags_path}")
                return []
            
            dag_files = []
            all_files = os.listdir(self.dags_path)
            logger.info(f"Все файлы в директории DAG'ов: {all_files}")
            
            for filename in all_files:
                if filename.endswith('.py') and filename.startswith('data_transfer_'):
                    filepath = os.path.join(self.dags_path, filename)
                    stat = os.stat(filepath)
                    dag_files.append({
                        'filename': filename,
                        'filepath': filepath,
                        'size': stat.st_size,
                        'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })
            
            logger.info(f"Найдено DAG файлов: {len(dag_files)}")
            return sorted(dag_files, key=lambda x: x['modified'], reverse=True)
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка DAG'ов: {e}")
            import traceback
            logger.error(f"Детали ошибки: {traceback.format_exc()}")
            return []
    
    def delete_dag(self, dag_id: str) -> bool:
        """Удаляет DAG файл"""
        try:
            filename = f"{dag_id}.py"
            filepath = os.path.join(self.dags_path, filename)
            
            logger.info(f"Попытка удаления DAG: {dag_id}")
            logger.info(f"Путь к файлу: {filepath}")
            logger.info(f"Директория DAG'ов существует: {os.path.exists(self.dags_path)}")
            
            if os.path.exists(filepath):
                os.remove(filepath)
                logger.info(f"DAG успешно удален: {filepath}")
                return True
            else:
                logger.warning(f"DAG файл не найден: {filepath}")
                # Попробуем найти файл по частичному имени
                if os.path.exists(self.dags_path):
                    files = os.listdir(self.dags_path)
                    logger.info(f"Файлы в директории DAG'ов: {files}")
                    
                    # Ищем файлы, содержащие dag_id
                    matching_files = [f for f in files if dag_id in f and f.endswith('.py')]
                    if matching_files:
                        logger.info(f"Найдены файлы, содержащие '{dag_id}': {matching_files}")
                        # Удаляем первый найденный файл
                        file_to_delete = os.path.join(self.dags_path, matching_files[0])
                        os.remove(file_to_delete)
                        logger.info(f"DAG успешно удален (найден по частичному имени): {file_to_delete}")
                        return True
                    else:
                        logger.warning(f"Файлы, содержащие '{dag_id}', не найдены")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при удалении DAG: {e}")
            import traceback
            logger.error(f"Детали ошибки: {traceback.format_exc()}")
            return False

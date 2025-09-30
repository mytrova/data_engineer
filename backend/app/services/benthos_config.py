"""
Модуль для работы с конфигурациями Benthos
"""
import os
import shutil
import logging
from typing import Dict, Any

try:
    import yaml
except ImportError:
    # Fallback для случаев, когда PyYAML не установлен
    import json as yaml

logger = logging.getLogger(__name__)

class BenthosConfigManager:
    """Менеджер для работы с конфигурациями Benthos"""
    
    def __init__(self, config_dir: str = "benthos"):
        self.config_dir = config_dir
    
    def create_config_dir(self) -> None:
        """Создает директорию для конфигураций Benthos"""
        try:
            os.makedirs(self.config_dir, exist_ok=True)
            logger.info(f"Директория для конфигураций Benthos создана: {self.config_dir}")
        except Exception as e:
            logger.error(f"ОШИБКА при создании директории {self.config_dir}: {e}")
            raise Exception(f"Не удалось создать директорию: {self.config_dir}")
    
    def save_config(self, config: Dict[str, Any], file_path: str) -> None:
        """Сохраняет конфигурацию Benthos в файл"""
        logger.info(f"=== СОХРАНЕНИЕ КОНФИГУРАЦИИ BENTHOS ===")
        logger.info(f"Путь: {file_path}")
        logger.info(f"Конфигурация: {config}")
        
        # Убеждаемся, что базовая директория существует
        self.create_config_dir()
        
        # Создаем директорию если не существует
        dir_path = os.path.dirname(file_path)
        if dir_path and dir_path != file_path:
            try:
                os.makedirs(dir_path, exist_ok=True)
                logger.info(f"Создана директория: {dir_path}")
            except Exception as e:
                logger.error(f"ОШИБКА при создании директории {dir_path}: {e}")
                raise Exception(f"Не удалось создать директорию: {dir_path}")
        
        # Проверяем, что директория создалась
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
            
            # Создаем файл с правильным форматированием YAML
            with open(file_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            
            # Проверяем, что файл создался правильно
            if os.path.exists(file_path) and os.path.isfile(file_path):
                file_size = os.path.getsize(file_path)
                logger.info(f"Конфигурация Benthos успешно сохранена как файл: {file_path} (размер: {file_size} байт)")
                
                # Проверяем содержимое файла для отладки
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        logger.info(f"Содержимое файла: {content}")
                except Exception as e:
                    logger.error(f"ОШИБКА при чтении файла: {e}")
            else:
                logger.error(f"ОШИБКА: Файл не создался или создался как директория: {file_path}")
                # Пытаемся удалить директорию если она создалась
                if os.path.exists(file_path) and os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    logger.info(f"Удалена ошибочно созданная директория: {file_path}")
        except Exception as e:
            logger.error(f"ОШИБКА при сохранении конфигурации: {e}")
            raise
    
    def get_config_path(self, dag_id: str) -> str:
        """Возвращает путь к конфигурационному файлу для DAG"""
        return f"{self.config_dir}/{dag_id}.yaml"
    
    def save_config_with_timestamp(self, config: Dict[str, Any], prefix: str = "data_transfer") -> str:
        """Сохраняет конфигурацию с автоматическим именем файла на основе timestamp"""
        from datetime import datetime
        
        # Создаем имя файла с timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = f"{self.config_dir}/{prefix}_{timestamp}.yaml"
        
        # Сохраняем конфигурацию
        self.save_config(config, file_path)
        return file_path
    
    def delete_config(self, dag_id: str) -> bool:
        """Удаляет конфигурационный файл Benthos"""
        config_path = self.get_config_path(dag_id)
        try:
            if os.path.exists(config_path):
                if os.path.isdir(config_path):
                    shutil.rmtree(config_path)
                else:
                    os.remove(config_path)
                logger.info(f"Конфигурация Benthos удалена: {config_path}")
                return True
            else:
                logger.warning(f"Конфигурация Benthos не найдена: {config_path}")
                return False
        except Exception as e:
            logger.error(f"ОШИБКА при удалении конфигурации Benthos: {e}")
            return False

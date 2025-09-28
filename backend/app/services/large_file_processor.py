"""
Система обработки больших файлов с прямой переливкой в базу-приёмник
"""
import os
import asyncio
import threading
import time
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
from pathlib import Path

from .csv_source import CSVSource
from .json_source import JSONSource
from .xml_source import XMLSource
from .postgres_sink import PostgreSQLSink

logger = logging.getLogger(__name__)

class LargeFileProcessor:
    """Процессор для больших файлов с прямой переливкой в базу-приёмник"""
    
    def __init__(self, max_file_size_mb: float = 20.0):
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024  # 20MB по умолчанию
        self.processing_threads: Dict[str, threading.Thread] = {}
        self.processing_status: Dict[str, Dict[str, Any]] = {}
        self.status_file = Path("/tmp/processing_status.json")
        self._load_status()
    
    def is_large_file(self, file_size: int) -> bool:
        """Проверка, является ли файл большим"""
        return file_size > self.max_file_size_bytes
    
    async def process_large_file(self, file_content: bytes, filename: str, file_type: str, sink_config: Dict[str, Any], chunk_size: int = 1000) -> Dict[str, Any]:
        """Обработка большого файла с прямой переливкой в базу-приёмник"""
        try:
            # Создаем директорию для больших файлов
            large_files_dir = "/app/data/large_files"
            os.makedirs(large_files_dir, exist_ok=True)
            
            # Генерируем уникальное имя файла
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_filename = f"{timestamp}_{filename}"
            file_path = os.path.join(large_files_dir, safe_filename)
            
            # Сохраняем файл на диск
            with open(file_path, "wb") as f:
                f.write(file_content)
            
            # Создаем уникальный ID для отслеживания процесса
            process_id = f"{timestamp}_{filename}"
            
            # Запускаем фоновую обработку
            self._start_background_processing(process_id, file_path, file_type, sink_config, chunk_size)
            
            return {
                "process_id": process_id,
                "status": "processing",
                "message": f"Файл {filename} обрабатывается в фоновом режиме с прямой переливкой в базу данных.",
                "file_path": file_path
            }
            
        except Exception as e:
            logger.error(f"Ошибка при обработке большого файла: {e}")
            raise
    
    def _start_background_processing(self, process_id: str, file_path: str, file_type: str, sink_config: Dict[str, Any], chunk_size: int):
        """Запуск фоновой обработки файла с прямой переливкой"""
        def process_file():
            try:
                # Инициализируем статус обработки
                self.processing_status[process_id] = {
                    "status": "processing",
                    "started_at": datetime.now().isoformat(),
                    "rows_processed": 0,
                    "current_chunk": 0,
                    "total_chunks": 0,
                    "sink_config": sink_config
                }
                
                logger.info(f"Начата обработка файла {process_id}")
                
                # Создаем sink для прямой записи в базу данных
                sink = self._create_sink(sink_config)
                
                # Обрабатываем файл в зависимости от типа
                if file_type.lower() == "csv":
                    self._process_csv_file_direct(process_id, file_path, sink, chunk_size)
                elif file_type.lower() == "json":
                    self._process_json_file_direct(process_id, file_path, sink, chunk_size)
                elif file_type.lower() == "xml":
                    self._process_xml_file_direct(process_id, file_path, sink, chunk_size)
                else:
                    raise ValueError(f"Неподдерживаемый тип файла: {file_type}")
                
                # Завершаем обработку
                logger.info(f"Обработка файла {process_id} завершена успешно")
                
                # Обновляем статус
                self.processing_status[process_id]["status"] = "completed"
                self.processing_status[process_id]["completed_at"] = datetime.now().isoformat()
                self._save_status()
                
            except Exception as e:
                logger.error(f"Ошибка при обработке файла {process_id}: {e}")
                self.processing_status[process_id]["status"] = "error"
                self.processing_status[process_id]["error"] = str(e)
                self._save_status()
        
        # Запускаем в отдельном потоке
        thread = threading.Thread(target=process_file, daemon=True)
        thread.start()
        self.processing_threads[process_id] = thread
    
    def _create_sink(self, sink_config: Dict[str, Any]):
        """Создание sink для записи данных"""
        if sink_config.get('type') == 'database':
            # Формируем connection string
            connection_string = f"postgresql://{sink_config['username']}:{sink_config['password']}@{sink_config['host']}:{sink_config['port']}/{sink_config['database']}"
            
            return PostgreSQLSink(
                connection_string=connection_string,
                table_name=sink_config['table_name'],
                mode="append"
            )
        else:
            raise ValueError(f"Неподдерживаемый тип sink: {sink_config.get('type')}")
    
    def _process_csv_file_direct(self, process_id: str, file_path: str, sink, chunk_size: int):
        """Обработка CSV файла с прямой записью в sink"""
        try:
            import csv
            import io
            
            # Читаем файл по частям
            rows_processed = 0
            
            # Пробуем разные кодировки
            encodings = ['utf-8', 'utf-8-sig', 'cp1251', 'latin1']
            content = None
            
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    logger.info(f"Файл {process_id} прочитан с кодировкой: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            
            if content is None:
                raise Exception("Не удалось прочитать файл с любой из поддерживаемых кодировок")
            
            # Определяем разделитель по содержимому файла
            delimiter = ';' if ';' in content[:1000] else ','
            logger.info(f"Используется разделитель: '{delimiter}'")
            
            csv_reader = csv.reader(io.StringIO(content), delimiter=delimiter)
            headers = next(csv_reader, [])
            
            logger.info(f"Заголовки: {headers}")
            
            # Собираем все чанки для передачи в sink
            def chunk_generator():
                chunk = []
                nonlocal rows_processed
                
                for row in csv_reader:
                    chunk.append(row)
                    if len(chunk) >= chunk_size:
                        yield chunk
                        rows_processed += len(chunk)
                        self.processing_status[process_id]["rows_processed"] = rows_processed
                        logger.info(f"Обработано строк: {rows_processed}")
                        chunk = []
                        
                        # Небольшая пауза для неблокирующей обработки
                        time.sleep(0.01)
                        
                        # Проверяем статус процесса
                        if self.processing_status.get(process_id, {}).get("status") == "paused":
                            logger.info(f"Процесс {process_id} приостановлен")
                            return
                
                # Обрабатываем оставшиеся строки
                if chunk:
                    yield chunk
                    rows_processed += len(chunk)
                    self.processing_status[process_id]["rows_processed"] = rows_processed
            
            # Записываем все чанки в sink
            sink.write_chunks(headers, chunk_generator())
            
            logger.info(f"CSV файл {process_id} обработан полностью. Всего строк: {rows_processed}")
            
        except Exception as e:
            logger.error(f"Ошибка при обработке CSV файла {process_id}: {e}")
            raise
    
    def _process_json_file_direct(self, process_id: str, file_path: str, sink, chunk_size: int):
        """Обработка JSON файла с прямой записью в sink"""
        try:
            import json as json_lib
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json_lib.load(f)
                
                if isinstance(data, list):
                    # Массив объектов
                    rows_processed = 0
                    
                    # Получаем заголовки из первого объекта
                    if data:
                        headers = list(data[0].keys())
                        
                        def chunk_generator():
                            nonlocal rows_processed
                            
                            for i in range(0, len(data), chunk_size):
                                chunk = data[i:i + chunk_size]
                                # Преобразуем JSON объекты в строки
                                chunk_rows = []
                                for obj in chunk:
                                    if isinstance(obj, dict):
                                        chunk_rows.append([str(obj.get(key, '')) for key in headers])
                                
                                yield chunk_rows
                                rows_processed += len(chunk_rows)
                                self.processing_status[process_id]["rows_processed"] = rows_processed
                                logger.info(f"Обработано JSON объектов: {rows_processed}")
                                time.sleep(0.01)
                                
                                # Проверяем статус процесса
                                if self.processing_status.get(process_id, {}).get("status") == "paused":
                                    logger.info(f"Процесс {process_id} приостановлен")
                                    return
                        
                        sink.write_chunks(headers, chunk_generator())
                        
                elif isinstance(data, dict):
                    # Один объект
                    headers = list(data.keys())
                    chunk_rows = [[str(data.get(key, '')) for key in headers]]
                    sink.write_chunks(headers, [chunk_rows])
                    self.processing_status[process_id]["rows_processed"] = 1
                    logger.info("JSON объект обработан")
            
            logger.info(f"JSON файл {process_id} обработан полностью")
            
        except Exception as e:
            logger.error(f"Ошибка при обработке JSON файла {process_id}: {e}")
            raise
    
    def _process_xml_file_direct(self, process_id: str, file_path: str, sink, chunk_size: int):
        """Обработка XML файла с прямой записью в sink (потоковая обработка)"""
        try:
            import xml.etree.ElementTree as ET
            from xml.etree.ElementTree import iterparse
            
            logger.info(f"Начинаем потоковую обработку XML файла {process_id}")
            
            # Получаем размер файла
            file_size = os.path.getsize(file_path)
            logger.info(f"XML файл {process_id} размер: {file_size / (1024*1024):.2f} MB")
            
            # Для больших файлов используем потоковую обработку
            if file_size > 100 * 1024 * 1024:  # > 100MB
                logger.info(f"Используем потоковую обработку для большого XML файла")
                self._process_xml_streaming(process_id, file_path, sink, chunk_size)
            else:
                logger.info(f"Используем обычную обработку для небольшого XML файла")
                self._process_xml_normal(process_id, file_path, sink, chunk_size)
            
            logger.info(f"XML файл {process_id} обработан полностью. Всего элементов: {self.processing_status[process_id]['rows_processed']}")
            
        except Exception as e:
            logger.error(f"Ошибка при обработке XML файла {process_id}: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _process_xml_streaming(self, process_id: str, file_path: str, sink, chunk_size: int):
        """Потоковая обработка больших XML файлов"""
        import xml.etree.ElementTree as ET
        from xml.etree.ElementTree import iterparse
        
        logger.info(f"Потоковая обработка XML файла {process_id}")
        
        # Используем базовые заголовки для начала
        headers = ['text', 'tag']
        logger.info(f"Используем базовые заголовки: {headers}")
        
        def chunk_generator():
            chunk = []
            rows_processed = 0
            element_count = 0
            headers_found = False
            current_headers = headers.copy()  # Копируем заголовки
            
            logger.info("Начинаем потоковую обработку XML элементов...")
            
            try:
                for event, elem in iterparse(file_path, events=('end',)):
                    if elem.tag:
                        # Если еще не нашли заголовки, попробуем их определить
                        if not headers_found and elem.attrib:
                            current_headers = list(elem.attrib.keys()) + ['text', 'tag']
                            headers_found = True
                            logger.info(f"Найдены заголовки XML: {current_headers}")
                        
                        # Создаем строку данных
                        row = []
                        
                        # Добавляем атрибуты в том же порядке, что и в заголовках
                        for header in current_headers[:-2]:  # Все кроме 'text' и 'tag'
                            if header in elem.attrib:
                                row.append(str(elem.attrib[header]))
                            else:
                                row.append('')
                        
                        # Добавляем текст и тег
                        row.append(str(elem.text or ''))
                        row.append(str(elem.tag))
                        
                        chunk.append(row)
                        element_count += 1
                        
                        if len(chunk) >= chunk_size:
                            yield chunk
                            rows_processed += len(chunk)
                            self.processing_status[process_id]["rows_processed"] = rows_processed
                            logger.info(f"Обработано XML элементов: {rows_processed}")
                            chunk = []
                            time.sleep(0.01)  # Небольшая пауза для освобождения памяти
                            
                            # Проверяем статус процесса
                            if self.processing_status.get(process_id, {}).get("status") == "paused":
                                logger.info(f"Процесс {process_id} приостановлен")
                                return
                        
                        # Очищаем элемент из памяти
                        elem.clear()
                        
                        # Логируем прогресс каждые 10000 элементов
                        if element_count % 10000 == 0:
                            logger.info(f"Обработано {element_count} XML элементов, строк: {rows_processed}")
                
                # Обрабатываем оставшиеся элементы
                if chunk:
                    yield chunk
                    rows_processed += len(chunk)
                    self.processing_status[process_id]["rows_processed"] = rows_processed
                    
            except Exception as e:
                logger.error(f"Ошибка в потоковой обработке XML: {e}")
                # Если произошла ошибка, попробуем обработать оставшиеся данные
                if chunk:
                    yield chunk
                    rows_processed += len(chunk)
                    self.processing_status[process_id]["rows_processed"] = rows_processed
                raise
        
        logger.info(f"Начинаем запись XML данных в sink")
        sink.write_chunks(headers, chunk_generator())
    
    def _process_xml_normal(self, process_id: str, file_path: str, sink, chunk_size: int):
        """Обычная обработка небольших XML файлов"""
        import xml.etree.ElementTree as ET
        
        logger.info(f"Обычная обработка XML файла {process_id}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            logger.info(f"XML файл {process_id} прочитан, размер: {len(content)} символов")
            
            # Проверяем, что файл не пустой
            if not content.strip():
                raise ValueError("XML файл пустой")
            
            # Парсим XML с обработкой ошибок
            try:
                root = ET.fromstring(content)
                logger.info(f"XML корневой элемент: {root.tag}")
            except ET.ParseError as e:
                logger.error(f"Ошибка парсинга XML: {e}")
                # Попробуем очистить XML от проблемных символов
                import re
                cleaned_content = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x84\x86-\x9f]', '', content)
                logger.info("Попытка очистки XML от проблемных символов")
                root = ET.fromstring(cleaned_content)
                logger.info(f"XML корневой элемент после очистки: {root.tag}")
            except Exception as e:
                logger.error(f"Критическая ошибка парсинга XML: {e}")
                raise
            
            # Обрабатываем элементы
            elements = list(root.iter())
            logger.info(f"Найдено {len(elements)} XML элементов")
            
            if elements:
                # Простая обработка - берем атрибуты и текст первого элемента
                first_element = elements[0]
                headers = list(first_element.attrib.keys()) + ['text', 'tag']
                logger.info(f"Заголовки XML: {headers}")
                
                def chunk_generator():
                    chunk = []
                    rows_processed = 0
                    
                    for i, element in enumerate(elements):
                        if hasattr(element, 'attrib') and hasattr(element, 'text'):
                            row = [str(element.attrib.get(key, '')) for key in element.attrib.keys()]
                            row.append(str(element.text or ''))
                            row.append(str(element.tag))
                            chunk.append(row)
                            
                            if len(chunk) >= chunk_size:
                                yield chunk
                                rows_processed += len(chunk)
                                self.processing_status[process_id]["rows_processed"] = rows_processed
                                logger.info(f"Обработано XML элементов: {rows_processed}")
                                chunk = []
                                time.sleep(0.01)
                        
                        # Логируем прогресс каждые 1000 элементов
                        if i % 1000 == 0 and i > 0:
                            logger.info(f"Обработано {i} из {len(elements)} XML элементов")
                    
                    # Обрабатываем оставшиеся элементы
                    if chunk:
                        yield chunk
                        rows_processed += len(chunk)
                        self.processing_status[process_id]["rows_processed"] = rows_processed
                
                logger.info(f"Начинаем запись XML данных в sink")
                sink.write_chunks(headers, chunk_generator())
            else:
                logger.warning(f"XML файл {process_id} не содержит элементов")
    
    def get_processing_status(self, process_id: str) -> Optional[Dict[str, Any]]:
        """Получение статуса обработки файла"""
        return self.processing_status.get(process_id)
    
    def _load_status(self):
        """Загрузка статусов из файла"""
        try:
            if self.status_file.exists():
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processing_status = data
                    logger.info(f"Загружено {len(self.processing_status)} статусов из файла")
        except Exception as e:
            logger.warning(f"Не удалось загрузить статусы: {e}")
    
    def _save_status(self):
        """Сохранение статусов в файл"""
        try:
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(self.processing_status, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Не удалось сохранить статусы: {e}")
    
    def get_all_processing_status(self) -> Dict[str, Dict[str, Any]]:
        """Получение статуса всех обрабатываемых файлов"""
        return self.processing_status.copy()
    
    def pause_process(self, process_id: str):
        """Приостановка процесса обработки"""
        if process_id in self.processing_status:
            self.processing_status[process_id]["status"] = "paused"
            self.processing_status[process_id]["paused_at"] = datetime.now().isoformat()
            self._save_status()
            logger.info(f"Процесс {process_id} приостановлен")
        else:
            raise ValueError(f"Процесс {process_id} не найден")
    
    def resume_process(self, process_id: str):
        """Возобновление процесса обработки"""
        if process_id in self.processing_status:
            self.processing_status[process_id]["status"] = "processing"
            self.processing_status[process_id]["resumed_at"] = datetime.now().isoformat()
            self._save_status()
            logger.info(f"Процесс {process_id} возобновлен")
        else:
            raise ValueError(f"Процесс {process_id} не найден")
    
    def stop_process(self, process_id: str):
        """Остановка процесса обработки"""
        if process_id in self.processing_status:
            self.processing_status[process_id]["status"] = "stopped"
            self.processing_status[process_id]["stopped_at"] = datetime.now().isoformat()
            self._save_status()
            logger.info(f"Процесс {process_id} остановлен")
        else:
            raise ValueError(f"Процесс {process_id} не найден")
    
    def delete_process(self, process_id: str):
        """Удаление процесса обработки"""
        if process_id in self.processing_status:
            # Удаляем статус процесса
            del self.processing_status[process_id]
            
            # Останавливаем поток если он активен
            if process_id in self.processing_threads:
                thread = self.processing_threads[process_id]
                if thread.is_alive():
                    # Не можем принудительно остановить поток, но помечаем как остановленный
                    pass
                del self.processing_threads[process_id]
            
            self._save_status()
            logger.info(f"Процесс {process_id} удален")
        else:
            raise ValueError(f"Процесс {process_id} не найден")
    

# Глобальный экземпляр процессора
large_file_processor = LargeFileProcessor()

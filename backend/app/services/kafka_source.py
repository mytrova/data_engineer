from __future__ import annotations

from typing import Any, Iterable, List, Sequence
from .base import DataSource
import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaSource(DataSource):
    """Источник данных из Kafka"""
    
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str = None, auto_offset_reset: str = 'earliest'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id or f"data_transfer_{topic}"
        self.auto_offset_reset = auto_offset_reset
        self._connected = False
        self._consumer = None
    
    def _get_consumer(self) -> KafkaConsumer:
        """Получает или создает consumer"""
        if self._consumer is None:
            self._consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.auto_offset_reset,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                consumer_timeout_ms=1000  # Таймаут для чтения
            )
        return self._consumer
    
    def headers(self) -> List[str]:
        """Возвращает заголовки столбцов"""
        try:
            consumer = self._get_consumer()
            
            # Читаем несколько сообщений для определения структуры
            messages = []
            for message in consumer:
                if message.value:
                    messages.append(message.value)
                if len(messages) >= 10:  # Ограничиваем количество для анализа
                    break
            
            if messages:
                # Анализируем структуру первого сообщения
                first_message = messages[0]
                if isinstance(first_message, dict):
                    return list(first_message.keys())
                elif isinstance(first_message, list) and len(first_message) > 0:
                    if isinstance(first_message[0], dict):
                        return list(first_message[0].keys())
            
            return []
        except Exception as e:
            logger.error(f"Ошибка получения заголовков: {e}")
            return []
    
    def read(self) -> Iterable[Sequence[Any]]:
        """Возвращает итератор по строкам данных"""
        try:
            consumer = self._get_consumer()
            
            for message in consumer:
                if message.value:
                    if isinstance(message.value, dict):
                        yield list(message.value.values())
                    elif isinstance(message.value, list):
                        yield message.value
                    else:
                        yield [message.value]
        except Exception as e:
            logger.error(f"Ошибка чтения данных: {e}")
    
    def read_in_chunks(self, chunk_size: int) -> Iterable[List[Sequence[Any]]]:
        """Чтение данных чанками"""
        if chunk_size <= 0:
            chunk_size = 1000
        
        try:
            consumer = self._get_consumer()
            chunk = []
            
            for message in consumer:
                if message.value:
                    if isinstance(message.value, dict):
                        chunk.append(list(message.value.values()))
                    elif isinstance(message.value, list):
                        chunk.append(message.value)
                    else:
                        chunk.append([message.value])
                    
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
            
            # Возвращаем оставшиеся данные
            if chunk:
                yield chunk
                
        except Exception as e:
            logger.error(f"Ошибка чтения данных чанками: {e}")
    
    def test_connection(self) -> bool:
        """Тестирование подключения"""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                consumer_timeout_ms=5000
            )
            
            # Получаем список топиков
            topics = consumer.topics()
            consumer.close()
            
            return self.topic in topics
        except Exception as e:
            logger.error(f"Ошибка подключения к Kafka: {e}")
            return False
    
    def get_topics(self) -> List[str]:
        """Получение списка топиков"""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                consumer_timeout_ms=5000
            )
            
            topics = list(consumer.topics())
            consumer.close()
            
            return topics
        except Exception as e:
            logger.error(f"Ошибка получения списка топиков: {e}")
            return []
    
    def get_topic_info(self, topic: str) -> dict:
        """Получение информации о топике"""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                consumer_timeout_ms=5000
            )
            
            # Получаем метаданные топика
            metadata = consumer.list_consumer_group_offsets()
            partitions = consumer.partitions_for_topic(topic)
            
            consumer.close()
            
            return {
                'topic': topic,
                'partitions': len(partitions) if partitions else 0,
                'partitions_list': list(partitions) if partitions else []
            }
        except Exception as e:
            logger.error(f"Ошибка получения информации о топике: {e}")
            return {
                'topic': topic,
                'partitions': 0,
                'partitions_list': []
            }
    
    def __del__(self):
        """Закрытие подключения при удалении объекта"""
        if self._consumer:
            try:
                self._consumer.close()
            except:
                pass

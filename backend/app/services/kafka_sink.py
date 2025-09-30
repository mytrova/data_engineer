from __future__ import annotations

from typing import Any, Iterable, List, Sequence
from .base import DataSink
import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaSink(DataSink):
    """Приёмник данных в Kafka"""
    
    def __init__(self, bootstrap_servers: str, topic: str, key_field: str = None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.key_field = key_field
        self._connected = False
        self._producer = None
        
        # Тестируем подключение
        if self.test_connection():
            self._connected = True
        else:
            raise ConnectionError("Не удалось подключиться к Kafka")
    
    def _get_producer(self) -> KafkaProducer:
        """Получает или создает producer"""
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: json.dumps(x).encode('utf-8') if x else None,
                acks='all',  # Ждем подтверждения от всех реплик
                retries=3,
                retry_backoff_ms=100
            )
        return self._producer
    
    def write(self, headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> Any:
        """Запись данных в топик"""
        try:
            producer = self._get_producer()
            rows_list = list(rows)
            
            if not rows_list:
                return {
                    "status": "success",
                    "topic": self.topic,
                    "messages_sent": 0
                }
            
            # Преобразуем данные в словари
            messages = []
            for row in rows_list:
                message = dict(zip(headers, row))
                messages.append(message)
            
            # Отправляем сообщения
            sent_count = 0
            for message in messages:
                key = None
                if self.key_field and self.key_field in message:
                    key = message[self.key_field]
                
                future = producer.send(self.topic, value=message, key=key)
                future.get(timeout=10)  # Ждем подтверждения
                sent_count += 1
            
            return {
                "status": "success",
                "topic": self.topic,
                "messages_sent": sent_count
            }
                
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "topic": self.topic
            }
    
    def write_chunks(self, headers: Sequence[str], chunks: Iterable[List[Sequence[Any]]]) -> Any:
        """Запись данных чанками"""
        try:
            producer = self._get_producer()
            total_sent = 0
            
            for chunk in chunks:
                if not chunk:  # Пропускаем пустые чанки
                    continue
                
                # Преобразуем данные в словари
                messages = []
                for row in chunk:
                    message = dict(zip(headers, row))
                    messages.append(message)
                
                # Отправляем сообщения из чанка
                for message in messages:
                    key = None
                    if self.key_field and self.key_field in message:
                        key = message[self.key_field]
                    
                    future = producer.send(self.topic, value=message, key=key)
                    future.get(timeout=10)  # Ждем подтверждения
                    total_sent += 1
            
            return {
                "status": "success",
                "topic": self.topic,
                "messages_sent": total_sent
            }
            
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "topic": self.topic
            }
    
    def test_connection(self) -> bool:
        """Тестирование подключения"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            
            # Пробуем отправить тестовое сообщение
            future = producer.send(self.topic, value={'test': 'connection'})
            future.get(timeout=5)
            producer.close()
            
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к Kafka: {e}")
            return False
    
    def get_topics(self) -> List[str]:
        """Получение списка топиков"""
        try:
            from kafka import KafkaConsumer
            
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
    
    def create_topic(self, topic: str, partitions: int = 1, replication_factor: int = 1) -> bool:
        """Создание топика"""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            from kafka.errors import TopicAlreadyExistsError
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers
            )
            
            topic_list = [NewTopic(
                name=topic,
                num_partitions=partitions,
                replication_factor=replication_factor
            )]
            
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            admin_client.close()
            
            return True
        except TopicAlreadyExistsError:
            return True  # Топик уже существует
        except Exception as e:
            logger.error(f"Ошибка создания топика {topic}: {e}")
            return False
    
    def __del__(self):
        """Закрытие подключения при удалении объекта"""
        if self._producer:
            try:
                self._producer.flush()
                self._producer.close()
            except:
                pass

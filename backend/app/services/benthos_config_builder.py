"""
Модуль для создания конфигураций Benthos для разных типов переливов
"""
from typing import Dict, Any

class BenthosConfigBuilder:
    """Строитель конфигураций Benthos"""
    
    @staticmethod
    def build_database_to_database_config(
        source_config: Dict[str, Any], 
        sink_config: Dict[str, Any], 
        chunk_size: int
    ) -> Dict[str, Any]:
        """Создает конфигурацию для перелива из базы данных в базу данных"""
        source_type = source_config.get('type', 'postgresql')
        sink_type = sink_config.get('type', 'postgresql')
        
        # Исправляем драйвер PostgreSQL
        if source_type == 'postgresql':
            source_driver = 'postgres'
        else:
            source_driver = source_type
            
        if sink_type == 'postgresql':
            sink_driver = 'postgres'
        elif sink_type == 'clickhouse':
            sink_driver = 'clickhouse'
        else:
            sink_driver = sink_type
        
        # Формируем DSN для источника
        if source_type == 'postgresql':
            source_dsn = f"postgresql://{source_config.get('username', '')}:{source_config.get('password', '')}@{source_config.get('host', '')}:{source_config.get('port', '')}/{source_config.get('database', '')}?sslmode=disable"
        else:
            source_dsn = f"{source_type}://{source_config.get('username', '')}:{source_config.get('password', '')}@{source_config.get('host', '')}:{source_config.get('port', '')}/{source_config.get('database', '')}"
        
        # Формируем DSN для приёмника
        if sink_type == 'clickhouse':
            sink_dsn = f"clickhouse://{sink_config.get('username', '')}:{sink_config.get('password', '')}@{sink_config.get('host', '')}:{sink_config.get('port', '')}/{sink_config.get('database', '')}"
        else:
            sink_dsn = f"{sink_type}://{sink_config.get('username', '')}:{sink_config.get('password', '')}@{sink_config.get('host', '')}:{sink_config.get('port', '')}/{sink_config.get('database', '')}"
        
        # Создаем конфигурацию входа
        input_config = {
            "sql_select": {
                "driver": source_driver,
                "dsn": source_dsn,
                "table": source_config.get('table_name', ''),
                "columns": ["*"],
                "where": "1=1"
            }
        }
        
        # Создаем конфигурацию выхода
        if sink_type == 'clickhouse':
            # Для ClickHouse используем HTTP API
            import urllib.parse
            
            # Только INSERT запрос (таблицу нужно создать отдельно)
            insert_query = f"INSERT INTO {sink_config.get('database', '')}.{sink_config.get('table_name', '')} FORMAT JSONEachRow"
            encoded_query = urllib.parse.quote(insert_query)
            
            # Формируем URL с аутентификацией
            username = sink_config.get('username', 'default')
            password = sink_config.get('password', '')
            host = sink_config.get('host', '')
            port = sink_config.get('port', '8123')
            
            if username and password:
                url = f"http://{username}:{password}@{host}:{port}/?query={encoded_query}"
            else:
                url = f"http://{host}:{port}/?query={encoded_query}"
            
            output_config = {
                "http_client": {
                    "url": url,
                    "verb": "POST",
                    "headers": {
                        "Content-Type": "application/json"
                    }
                }
            }
        else:
            # Для других баз данных используем sql_insert
            output_config = {
                "sql_insert": {
                    "driver": sink_driver,
                    "dsn": sink_dsn,
                    "table": sink_config.get('table_name', ''),
                    "columns": ["*"],
                    "args_mapping": "root = [this]"
                }
            }
        
        if sink_type == 'clickhouse':
            return {
                "input": input_config,
                "pipeline": {
                    "processors": [
                        {
                            "mapping": "root = this"
                        }
                    ]
                },
                "output": output_config
            }
        else:
            return {
                "input": input_config,
                "pipeline": {
                    "processors": [
                        {
                            "mapping": "root = this"
                        }
                    ]
                },
                "output": output_config
            }
    
    @staticmethod
    def build_file_to_database_config(
        source_table: str,
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> Dict[str, Any]:
        """Создает конфигурацию для перелива из файла в базу данных"""
        db_type = sink_config.get('db_type', 'postgresql')
        
        return {
            "input": {
                "sql_select": {
                    "driver": "sqlite3",
                    "dsn": "file:/app/data/service.db",
                    "table": source_table,
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
                    "driver": db_type,
                    "dsn": f"{db_type}://{sink_config.get('username', '')}:{sink_config.get('password', '')}@{sink_config.get('host', '')}:{sink_config.get('port', '')}/{sink_config.get('database', '')}",
                    "table": sink_config.get('table_name', ''),
                    "columns": ["*"],
                    "batch_size": chunk_size
                }
            }
        }
    
    @staticmethod
    def build_database_to_kafka_config(
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> Dict[str, Any]:
        """Создает конфигурацию для перелива из базы данных в Kafka"""
        source_type = source_config.get('db_type', 'postgresql')
        kafka_servers = sink_config.get('host', 'localhost:9092')
        kafka_topic = sink_config.get('database', '')
        
        return {
            "input": {
                "sql_select": {
                    "driver": source_type,
                    "dsn": f"{source_type}://{source_config.get('username', '')}:{source_config.get('password', '')}@{source_config.get('host', '')}:{source_config.get('port', '')}/{source_config.get('database', '')}",
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
                    },
                    {
                        "json": {
                            "operator": "to_json"
                        }
                    }
                ]
            },
            "output": {
                "kafka": {
                    "addresses": [kafka_servers],
                    "topic": kafka_topic,
                    "key": "{{.id}}",
                    "batch_size": chunk_size
                }
            }
        }
    
    @staticmethod
    def build_kafka_to_database_config(
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        chunk_size: int
    ) -> Dict[str, Any]:
        """Создает конфигурацию для перелива из Kafka в базу данных"""
        sink_type = sink_config.get('db_type', 'postgresql')
        kafka_servers = source_config.get('host', 'localhost:9092')
        kafka_topic = source_config.get('database', '')
        
        return {
            "input": {
                "kafka": {
                    "addresses": [kafka_servers],
                    "topics": [kafka_topic],
                    "consumer_group": source_config.get('username', 'benthos-consumer')
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
                        "json": {
                            "operator": "from_json"
                        }
                    },
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
                    "dsn": f"{sink_type}://{sink_config.get('username', '')}:{sink_config.get('password', '')}@{sink_config.get('host', '')}:{sink_config.get('port', '')}/{sink_config.get('database', '')}",
                    "table": sink_config.get('table_name', ''),
                    "columns": ["*"],
                    "batch_size": chunk_size
                }
            }
        }

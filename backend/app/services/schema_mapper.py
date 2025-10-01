"""
Сервис для маппинга схем между базами данных
"""
import logging
from typing import Dict, Any, List
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

class SchemaMapper:
    """Сервис для маппинга схем между базами данных"""
    
    @staticmethod
    def get_postgresql_schema(host: str, port: int, database: str, username: str, password: str, table_name: str) -> List[Dict[str, Any]]:
        """Получает схему таблицы из PostgreSQL"""
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password
            )
            
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                
                schema = []
                for row in cursor.fetchall():
                    schema.append({
                        "column_name": row['column_name'],
                        "data_type": row['data_type'],
                        "is_nullable": row['is_nullable'] == 'YES',
                        "column_default": row['column_default']
                    })
                
                return schema
                
        except Exception as e:
            logger.error(f"Ошибка получения схемы PostgreSQL: {e}")
            return []
        finally:
            if 'conn' in locals():
                conn.close()
    
    @staticmethod
    def postgresql_to_clickhouse_type(postgresql_type: str) -> str:
        """Конвертирует тип PostgreSQL в тип ClickHouse"""
        type_mapping = {
            'integer': 'Int32',
            'bigint': 'Int64',
            'smallint': 'Int16',
            'text': 'String',
            'varchar': 'String',
            'character varying': 'String',
            'char': 'String',
            'timestamp': 'DateTime',
            'timestamp without time zone': 'DateTime',
            'timestamp with time zone': 'DateTime64',
            'date': 'Date',
            'boolean': 'UInt8',
            'numeric': 'Float64',
            'real': 'Float32',
            'double precision': 'Float64',
            'json': 'String',
            'jsonb': 'String',
            'uuid': 'String',
            'bytea': 'String'
        }
        
        return type_mapping.get(postgresql_type.lower(), 'String')
    
    @staticmethod
    def create_clickhouse_table_sql(schema: List[Dict[str, Any]], database: str, table_name: str) -> str:
        """Создает SQL для создания таблицы в ClickHouse"""
        columns = []
        for col in schema:
            col_name = col['column_name']
            ch_type = SchemaMapper.postgresql_to_clickhouse_type(col['data_type'])
            nullable = "Nullable(" + ch_type + ")" if col['is_nullable'] else ch_type
            columns.append(f"`{col_name}` {nullable}")
        
        return f"""
        CREATE TABLE IF NOT EXISTS `{database}`.`{table_name}` (
            {', '.join(columns)}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """
    
    @staticmethod
    def create_clickhouse_database(host: str, port: int, database: str, username: str, password: str) -> bool:
        """Создает базу данных в ClickHouse"""
        try:
            create_db_sql = f"CREATE DATABASE IF NOT EXISTS `{database}`"
            
            url = f"http://{username}:{password}@{host}:{port}/"
            response = requests.post(url, data=create_db_sql)
            
            if response.status_code == 200:
                logger.info(f"База данных {database} успешно создана в ClickHouse")
                return True
            else:
                logger.error(f"Ошибка создания базы данных в ClickHouse: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка создания базы данных в ClickHouse: {e}")
            return False

    @staticmethod
    def create_clickhouse_table(host: str, port: int, database: str, username: str, password: str, table_name: str, schema: List[Dict[str, Any]]) -> bool:
        """Создает таблицу в ClickHouse"""
        try:
            # Сначала создаем базу данных
            if not SchemaMapper.create_clickhouse_database(host, port, database, username, password):
                return False
            
            sql = SchemaMapper.create_clickhouse_table_sql(schema, database, table_name)
            
            url = f"http://{username}:{password}@{host}:{port}/"
            response = requests.post(url, data=sql)
            
            if response.status_code == 200:
                logger.info(f"Таблица {database}.{table_name} успешно создана в ClickHouse")
                return True
            else:
                logger.error(f"Ошибка создания таблицы в ClickHouse: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка создания таблицы в ClickHouse: {e}")
            return False

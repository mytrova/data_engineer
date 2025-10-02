from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel

from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

from .services.csv_source import CSVSource
from .services.json_source import JSONSource
from .services.xml_source import XMLSource
from .services.postgres_source import PostgreSQLSource
from .services.postgres_sink import PostgreSQLSink
from .services.clickhouse_source import ClickHouseSource
from .services.clickhouse_sink import ClickHouseSink
from .services.kafka_source import KafkaSource
from .services.kafka_sink import KafkaSink
from .services.base import DataSink
from .services.file_sinks import CSVFileSink, JSONFileSink, XMLFileSink
from .services.airflow_client import airflow_client
from .services.airflow_dag_generator_simple import AirflowDAGGeneratorSimple
from .services.sqlite_service import sqlite_service
from .services.large_file_processor import large_file_processor
from .services.llm_service import llm_service


class PreviewSink(DataSink):
    def __init__(self, max_rows: int = 10) -> None:
        self.max_rows = max_rows

    def write(self, headers: List[str], rows):
        preview_rows: List[List[Any]] = []
        for idx, row in enumerate(rows):
            if idx >= self.max_rows:
                break
            preview_rows.append(list(row))
        return {"headers": headers, "rows": preview_rows}


app = FastAPI(
    title="Data Transfer Orchestrator",
    # Увеличиваем лимиты для больших файлов
    max_request_size=1024 * 1024 * 1024,  # 1GB
)

# Настройка логирования
logger = logging.getLogger(__name__)

# Инициализация DAG генератора
dag_generator = AirflowDAGGeneratorSimple()

# Pydantic модели
class DAGGenerationRequest(BaseModel):
    source_config: Dict[str, Any]
    sink_config: Dict[str, Any]
    chunk_size: int
    total_rows: Optional[int] = None

class LLMMessageRequest(BaseModel):
    message: str

class DataAnalysisRequest(BaseModel):
    source_schema: Dict[str, Any]
    sink_schema: Dict[str, Any]
    source_type: str
    sink_type: str

# CORS (allow frontend origin configured via env in docker-compose)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/database/connect")
async def test_database_connection(
    connection_string: str = Form(None),
    database_type: str = Form("postgresql"),
    # ClickHouse параметры
    host: str = Form(None),
    port: str = Form(None),
    database: str = Form(None),
    username: str = Form(None),
    password: str = Form(None),
    # Kafka параметры
    bootstrap_servers: str = Form(None),
    topic: str = Form(None),
    group_id: str = Form(None)
):
    """Тестирование подключения к базе данных"""
    try:
        if database_type.lower() == "postgresql":
            if not connection_string:
                return JSONResponse({
                    "status": "error",
                    "error": "Не указана строка подключения"
                }, status_code=400)
            
            source = PostgreSQLSource(connection_string)
            is_connected = source.test_connection()
            tables = source.get_tables() if is_connected else []
            
            return JSONResponse({
                "status": "success" if is_connected else "error",
                "connected": is_connected,
                "tables": tables,
                "database_type": "postgresql"
            })
            
        elif database_type.lower() == "clickhouse":
            if not all([host, port, database, username, password]):
                return JSONResponse({
                    "status": "error",
                    "error": "Не указаны все параметры подключения к ClickHouse"
                }, status_code=400)
            
            source = ClickHouseSource(host, int(port), database, username, password)
            is_connected = source.test_connection()
            tables = source.get_tables() if is_connected else []
            
            return JSONResponse({
                "status": "success" if is_connected else "error",
                "connected": is_connected,
                "tables": tables,
                "database_type": "clickhouse"
            })
            
        elif database_type.lower() == "kafka":
            if not all([bootstrap_servers, topic]):
                return JSONResponse({
                    "status": "error",
                    "error": "Не указаны bootstrap_servers и topic для Kafka"
                }, status_code=400)
            
            source = KafkaSource(bootstrap_servers, topic, group_id)
            is_connected = source.test_connection()
            topics = source.get_topics() if is_connected else []
            
            return JSONResponse({
                "status": "success" if is_connected else "error",
                "connected": is_connected,
                "topics": topics,
                "database_type": "kafka"
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"Неподдерживаемый тип базы данных: {database_type}"
            }, status_code=400)
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.post("/database/tables")
async def get_database_tables(
    connection_string: str = Form(...),
    database_type: str = Form("postgresql")
):
    """Получение списка таблиц из базы данных"""
    try:
        if database_type.lower() == "postgresql":
            source = PostgreSQLSource(connection_string)
            tables = source.get_tables()
            
            return JSONResponse({
                "status": "success",
                "tables": tables
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"Неподдерживаемый тип базы данных: {database_type}"
            }, status_code=400)
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.post("/database/topic-info")
async def get_topic_info(
    bootstrap_servers: str = Form(...),
    topic: str = Form(...),
    database_type: str = Form("kafka")
):
    """Получение информации о топике Kafka"""
    try:
        if database_type.lower() == "kafka":
            source = KafkaSource(bootstrap_servers, topic)
            topic_info = source.get_topic_info(topic)
            
            return JSONResponse({
                "status": "success",
                "topic_info": topic_info
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"Неподдерживаемый тип для получения информации о топике: {database_type}"
            }, status_code=400)
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.post("/database/schema")
async def get_table_schema(
    connection_string: str = Form(None),
    table_name: str = Form(...),
    database_type: str = Form("postgresql"),
    # ClickHouse параметры
    host: str = Form(None),
    port: str = Form(None),
    database: str = Form(None),
    username: str = Form(None),
    password: str = Form(None)
):
    """Получение схемы таблицы"""
    try:
        if database_type.lower() == "postgresql":
            if not connection_string:
                return JSONResponse({
                    "status": "error",
                    "error": "Не указана строка подключения"
                }, status_code=400)
            
            source = PostgreSQLSource(connection_string)
            schema = source.get_table_schema(table_name)
            
            return JSONResponse({
                "status": "success",
                "table_name": table_name,
                "schema": schema
            })
            
        elif database_type.lower() == "clickhouse":
            if not all([host, port, database, username, password]):
                return JSONResponse({
                    "status": "error",
                    "error": "Не указаны все параметры подключения к ClickHouse"
                }, status_code=400)
            
            source = ClickHouseSource(host, int(port), database, username, password)
            schema = source.get_table_schema(table_name)
            
            return JSONResponse({
                "status": "success",
                "table_name": table_name,
                "schema": schema
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"Неподдерживаемый тип базы данных: {database_type}"
            }, status_code=400)
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.post("/database/query")
async def execute_database_query(
    connection_string: str = Form(...),
    query: str = Form(...),
    limit: int = Form(100),
    database_type: str = Form("postgresql")
):
    """Выполнение произвольного запроса к базе данных"""
    try:
        if database_type.lower() == "postgresql":
            source = PostgreSQLSource(connection_string)
            result = source.execute_custom_query(query, limit)
            
            return JSONResponse({
                "status": "success",
                "result": result
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"Неподдерживаемый тип базы данных: {database_type}"
            }, status_code=400)
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    source_type: str = Form("csv"),
    chunk_size: int = Form(10),
    delimiter: str = Form(";"),
):
    content_bytes = await file.read()
    
    # Проверяем размер файла
    if large_file_processor.is_large_file(len(content_bytes)):
        # Большой файл - сохраняем на диск и запускаем фоновую обработку
        try:
            result = await large_file_processor.save_large_file(
                content_bytes, file.filename, source_type
            )
            return JSONResponse({
                "status": "large_file",
                "message": result["message"],
                "file_id": result["file_id"],
                "file_path": result["file_path"]
            })
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
            logger.error(f"Ошибка: {str(e)}")
            logger.error(f"Детали: {error_details}")
            return JSONResponse({
                "status": "error",
                "error": f"Ошибка при обработке большого файла: {str(e)}"
            }, status_code=500)
    
    # Обычная обработка для небольших файлов
    try:
        text = content_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = content_bytes.decode("utf-8", errors="replace")

    st = (source_type or "csv").lower()
    if st == "csv":
        source = CSVSource(text_data=text, delimiter=delimiter)
    elif st == "json":
        source = JSONSource(text_data=text)
    elif st == "xml":
        source = XMLSource(text_data=text)
    elif st == "postgresql":
        # Для PostgreSQL нужна строка подключения
        return JSONResponse({"error": "Для PostgreSQL источника используйте /database/query эндпоинт"}, status_code=400)
    else:
        return JSONResponse({"error": f"Unknown source_type {source_type}"}, status_code=400)

    sink = PreviewSink(max_rows=10)

    result = sink.write(source.headers(), source.read())
    return JSONResponse(result)


# ===== LARGE FILE PROCESSING ENDPOINTS =====

@app.get("/large-files")
async def get_large_files():
    """Получение списка всех больших файлов"""
    try:
        files = sqlite_service.get_all_files()
        return JSONResponse({
            "status": "success",
            "files": files
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.delete("/large-files/{file_id}")
async def delete_large_file(file_id: int):
    """Удаление большого файла и всех связанных данных"""
    try:
        # Получаем информацию о файле
        file_info = sqlite_service.get_file_metadata(file_id)
        if not file_info:
            return JSONResponse({
                "status": "error",
                "error": f"Файл с ID {file_id} не найден"
            }, status_code=404)
        
        # Останавливаем обработку, если она идет
        if file_id in large_file_processor.processing_threads:
            # Помечаем файл как удаленный
            sqlite_service.update_file_status(file_id, "deleted")
            sqlite_service.add_processing_log(file_id, "INFO", "Файл помечен для удаления")
        
        # Удаляем файл с диска
        import os
        if os.path.exists(file_info['file_path']):
            os.remove(file_info['file_path'])
            sqlite_service.add_processing_log(file_id, "INFO", "Файл удален с диска")
        
        # Удаляем все данные из SQLite
        sqlite_service.delete_file_data(file_id)
        
        return JSONResponse({
            "status": "success",
            "message": f"Файл {file_info['filename']} успешно удален"
        })
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.get("/large-files/{file_id}")
async def get_large_file_info(file_id: int):
    """Получение информации о большом файле"""
    try:
        file_info = sqlite_service.get_file_metadata(file_id)
        if file_info:
            processing_status = large_file_processor.get_processing_status(file_id)
            return JSONResponse({
                "status": "success",
                "file_info": file_info,
                "processing_status": processing_status
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"Файл с ID {file_id} не найден"
            }, status_code=404)
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.get("/large-files/{file_id}/logs")
async def get_file_processing_logs(file_id: int, limit: int = 100):
    """Получение логов обработки файла"""
    try:
        logs = large_file_processor.get_processing_logs(file_id, limit)
        return JSONResponse({
            "status": "success",
            "logs": logs
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.get("/large-files/{file_id}/table-data")
async def get_file_table_data(file_id: int, limit: int = 1000, offset: int = 0):
    """Получение данных из таблицы файла"""
    try:
        data = sqlite_service.get_file_table_data(file_id, limit, offset)
        return JSONResponse({
            "status": "success",
            "data": data
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.get("/large-files/{file_id}/data")
async def get_file_cached_data(file_id: int, limit: int = 1000):
    """Получение кэшированных данных файла"""
    try:
        data = large_file_processor.get_cached_data(file_id, limit)
        return JSONResponse({
            "status": "success",
            "data": data
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.get("/large-files/status/all")
async def get_all_processing_status():
    """Получение статуса всех обрабатываемых файлов"""
    try:
        status = large_file_processor.get_all_processing_status()
        return JSONResponse({
            "status": "success",
            "processing_status": status
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.post("/transfer")
async def transfer(
    source_type: str = Form(...),
    sink_type: str = Form(...),
    chunk_size: int = Form(1000),
    file: UploadFile = File(...),
    delimiter: str = Form(";"),
):
    content_bytes = await file.read()
    
    # Проверяем размер файла
    if large_file_processor.is_large_file(len(content_bytes)):
        # Большой файл - сохраняем на диск и запускаем фоновую обработку
        try:
            result = await large_file_processor.save_large_file(
                content_bytes, file.filename, source_type
            )
            return JSONResponse({
                "status": "large_file",
                "message": f"Файл {file.filename} слишком большой для прямой обработки. Сохранен на диск и обрабатывается в фоновом режиме.",
                "file_id": result["file_id"],
                "file_path": result["file_path"]
            })
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
            logger.error(f"Ошибка: {str(e)}")
            logger.error(f"Детали: {error_details}")
            return JSONResponse({
                "status": "error",
                "error": f"Ошибка при обработке большого файла: {str(e)}"
            }, status_code=500)
    
    # Обычная обработка для небольших файлов
    try:
        text = content_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = content_bytes.decode("utf-8", errors="replace")

    # Build source
    if source_type.lower() == "csv":
        source = CSVSource(text, delimiter=delimiter)
    elif source_type.lower() == "json":
        source = JSONSource(text)
    elif source_type.lower() == "xml":
        source = XMLSource(text)
    elif source_type.lower() == "postgresql":
        # Для PostgreSQL нужна строка подключения
        return JSONResponse({"error": "Для PostgreSQL источника используйте /database/query эндпоинт"}, status_code=400)
    else:
        return JSONResponse({"error": f"Unknown source_type {source_type}"}, status_code=400)

    # Build sink. For demo, write to container-local /tmp and return path
    out_ext = sink_type.lower()
    out_path = f"/tmp/output.{out_ext}"
    if sink_type.lower() == "csv":
        sink = CSVFileSink(out_path, delimiter=delimiter)
    elif sink_type.lower() == "json":
        sink = JSONFileSink(out_path)
    elif sink_type.lower() == "xml":
        sink = XMLFileSink(out_path)
    elif sink_type.lower() == "postgresql":
        # Для PostgreSQL нужна строка подключения и имя таблицы
        return JSONResponse({"error": "Для PostgreSQL приёмника используйте специальный эндпоинт"}, status_code=400)
    elif sink_type.lower() == "preview":
        sink = PreviewSink(max_rows=10)  # ограничиваем предпросмотр 10 строками
    else:
        return JSONResponse({"error": f"Unknown sink_type {sink_type}"}, status_code=400)

    if isinstance(sink, PreviewSink):
        result = sink.write(source.headers(), source.read())
        return JSONResponse({"result": result, "headers": source.headers()})
    else:
        sink.write_chunks(source.headers(), source.read_in_chunks(chunk_size))
        media = {
            "csv": "text/csv",
            "json": "application/json",
            "xml": "application/xml",
        }.get(out_ext, "application/octet-stream")
        filename = f"export.{out_ext}"
        return FileResponse(path=out_path, media_type=media, filename=filename)


@app.post("/transfer/to-database")
async def transfer_to_database(
    source_type: str = Form(...),
    source_connection_string: str = Form(None),
    source_table_name: str = Form(None),
    source_query: str = Form(None),
    sink_connection_string: str = Form(None),
    sink_table_name: str = Form(None),
    sink_mode: str = Form("append"),
    chunk_size: int = Form(1000),
    file: UploadFile = File(None),
    delimiter: str = Form(";"),
    database_type: str = Form("postgresql"),
    # ClickHouse параметры приёмника
    sink_host: str = Form(None),
    sink_port: str = Form(None),
    sink_database: str = Form(None),
    sink_username: str = Form(None),
    sink_password: str = Form(None),
    # Kafka параметры приёмника
    sink_bootstrap_servers: str = Form(None),
    sink_topic: str = Form(None),
    sink_key_field: str = Form(None),
    # ClickHouse параметры источника
    source_host: str = Form(None),
    source_port: str = Form(None),
    source_database: str = Form(None),
    source_username: str = Form(None),
    source_password: str = Form(None),
    # Kafka параметры источника
    source_bootstrap_servers: str = Form(None),
    source_topic: str = Form(None),
    source_group_id: str = Form(None)
):
    """Перенос данных в базу данных PostgreSQL"""
    try:
        # Создаем источник данных
        if source_type.lower() == "csv" and file:
            content_bytes = await file.read()
            try:
                text = content_bytes.decode("utf-8")
            except UnicodeDecodeError:
                text = content_bytes.decode("utf-8", errors="replace")
            source = CSVSource(text, delimiter=delimiter)
        elif source_type.lower() == "json" and file:
            content_bytes = await file.read()
            try:
                text = content_bytes.decode("utf-8")
            except UnicodeDecodeError:
                text = content_bytes.decode("utf-8", errors="replace")
            source = JSONSource(text)
        elif source_type.lower() == "xml" and file:
            content_bytes = await file.read()
            try:
                text = content_bytes.decode("utf-8")
            except UnicodeDecodeError:
                text = content_bytes.decode("utf-8", errors="replace")
            source = XMLSource(text)
        elif source_type.lower() == "postgresql" and source_connection_string:
            if source_table_name:
                source = PostgreSQLSource(source_connection_string, table_name=source_table_name)
            elif source_query:
                source = PostgreSQLSource(source_connection_string, query=source_query)
            else:
                return JSONResponse({"error": "Для PostgreSQL источника нужны table_name или query"}, status_code=400)
        elif source_type.lower() == "clickhouse":
            if not all([source_host, source_port, source_database, source_username, source_password]):
                return JSONResponse({"error": "Для ClickHouse источника нужны все параметры подключения"}, status_code=400)
            
            if source_table_name:
                source = ClickHouseSource(source_host, int(source_port), source_database, source_username, source_password, table_name=source_table_name)
            elif source_query:
                source = ClickHouseSource(source_host, int(source_port), source_database, source_username, source_password, query=source_query)
            else:
                return JSONResponse({"error": "Для ClickHouse источника нужны table_name или query"}, status_code=400)
        elif source_type.lower() == "kafka":
            if not all([source_bootstrap_servers, source_topic]):
                return JSONResponse({"error": "Для Kafka источника нужны bootstrap_servers и topic"}, status_code=400)
            
            source = KafkaSource(source_bootstrap_servers, source_topic, source_group_id)
        else:
            return JSONResponse({"error": f"Неподдерживаемый источник: {source_type}"}, status_code=400)
        
        # Создаем приёмник данных
        if database_type.lower() == "postgresql":
            if not sink_connection_string or not sink_table_name:
                return JSONResponse({"error": "Для PostgreSQL приёмника нужны connection_string и table_name"}, status_code=400)
            sink = PostgreSQLSink(sink_connection_string, sink_table_name, sink_mode)
        elif database_type.lower() == "clickhouse":
            if not all([sink_host, sink_port, sink_database, sink_username, sink_password, sink_table_name]):
                return JSONResponse({"error": "Для ClickHouse приёмника нужны все параметры подключения"}, status_code=400)
            
            # Логируем параметры ClickHouse для отладки
            logger.info(f"ClickHouse parameters: host={sink_host}, port={sink_port}, database={sink_database}, username={sink_username}, table={sink_table_name}")
            
            try:
                sink = ClickHouseSink(sink_host, int(sink_port), sink_database, sink_username, sink_password, sink_table_name, sink_mode)
            except Exception as e:
                logger.error(f"Ошибка создания ClickHouse sink: {e}")
                return JSONResponse({"error": f"Не удалось подключиться к ClickHouse: {str(e)}"}, status_code=500)
        elif database_type.lower() == "kafka":
            if not all([sink_bootstrap_servers, sink_topic]):
                return JSONResponse({"error": "Для Kafka приёмника нужны bootstrap_servers и topic"}, status_code=400)
            sink = KafkaSink(sink_bootstrap_servers, sink_topic, sink_key_field)
        else:
            return JSONResponse({"error": f"Неподдерживаемый тип базы данных: {database_type}"}, status_code=400)
        
        # Выполняем перенос данных
        if sink_mode == "preview":
            # Для предпросмотра используем PreviewSink
            preview_sink = PreviewSink(max_rows=10)
            result = preview_sink.write(source.headers(), source.read())
            return JSONResponse({
                "status": "success",
                "result": result,
                "headers": source.headers()
            })
        else:
            # Записываем данные в базу
            result = sink.write_chunks(source.headers(), source.read_in_chunks(chunk_size))
            return JSONResponse({
                "status": "success",
                "result": result
            })
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


# ===== AIRFLOW API ENDPOINTS =====

@app.get("/airflow/dags")
async def get_airflow_dags():
    """Получение списка всех DAG в Airflow"""
    try:
        dags = await airflow_client.get_dags()
        return JSONResponse({
            "status": "success",
            "dags": dags
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.get("/airflow/dags/{dag_id}/status")
async def get_dag_status(dag_id: str):
    """Получение статуса конкретного DAG"""
    try:
        dag_info = await airflow_client.get_dag_status(dag_id)
        if dag_info:
            return JSONResponse({
                "status": "success",
                "dag": dag_info
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"DAG {dag_id} не найден"
            }, status_code=404)
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.get("/airflow/dags/{dag_id}/runs")
async def get_dag_runs(dag_id: str, limit: int = 10):
    """Получение списка запусков DAG"""
    try:
        runs = await airflow_client.get_dag_runs(dag_id, limit)
        return JSONResponse({
            "status": "success",
            "dag_id": dag_id,
            "runs": runs
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.post("/airflow/dags/{dag_id}/trigger")
async def trigger_dag(dag_id: str):
    """Запуск DAG"""
    try:
        result = await airflow_client.trigger_dag(dag_id)
        if result:
            return JSONResponse({
                "status": "success",
                "message": f"DAG {dag_id} успешно запущен",
                "dag_run": result
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"Не удалось запустить DAG {dag_id}"
            }, status_code=500)
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.post("/airflow/dags/{dag_id}/pause")
async def pause_dag(dag_id: str):
    """Приостановка DAG"""
    try:
        success = await airflow_client.pause_dag(dag_id)
        if success:
            return JSONResponse({
                "status": "success",
                "message": f"DAG {dag_id} приостановлен"
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"Не удалось приостановить DAG {dag_id}"
            }, status_code=500)
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.post("/airflow/dags/{dag_id}/unpause")
async def unpause_dag(dag_id: str):
    """Возобновление DAG"""
    try:
        success = await airflow_client.unpause_dag(dag_id)
        if success:
            return JSONResponse({
                "status": "success",
                "message": f"DAG {dag_id} возобновлен"
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"Не удалось возобновить DAG {dag_id}"
            }, status_code=500)
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.get("/airflow/dags/{dag_id}/ui-url")
async def get_dag_ui_url(dag_id: str):
    """Получение URL для перехода к DAG в веб-интерфейсе Airflow"""
    try:
        ui_url = await airflow_client.get_dag_ui_url(dag_id)
        return JSONResponse({
            "status": "success",
            "dag_id": dag_id,
            "ui_url": ui_url
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.get("/airflow/ui-url")
async def get_airflow_ui_url():
    """Получение URL для веб-интерфейса Airflow"""
    try:
        ui_url = await airflow_client.get_airflow_ui_url()
        return JSONResponse({
            "status": "success",
            "ui_url": ui_url
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.get("/airflow/dags/{dag_id}/tasks/{dag_run_id}")
async def get_dag_tasks(dag_id: str, dag_run_id: str):
    """Получение списка задач в DAG run"""
    try:
        tasks = await airflow_client.get_dag_tasks(dag_id, dag_run_id)
        return JSONResponse({
            "status": "success",
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "tasks": tasks
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


# ===== LARGE FILE PROCESSING ENDPOINTS =====

@app.post("/large-file/upload")
async def upload_large_file(
    file: UploadFile = File(...),
    sink_config: str = Form(None),
    chunk_size: int = Form(1000)
):
    """Загрузка большого файла для прямой переливки в базу-приёмник"""
    print(f"DEBUG: Начало загрузки большого файла {file.filename}")
    try:
        # Читаем содержимое файла
        content = await file.read()
        file_size = len(content)
        
        print(f"DEBUG: Загружен файл {file.filename}, размер: {file_size} байт")
        print(f"DEBUG: Порог больших файлов: {large_file_processor.max_file_size_bytes} байт")
        print(f"DEBUG: Файл большой? {large_file_processor.is_large_file(file_size)}")
        
        # Определяем тип файла по расширению
        file_extension = file.filename.split('.')[-1].lower() if '.' in file.filename else 'csv'
        file_type_map = {
            'csv': 'csv',
            'json': 'json', 
            'xml': 'xml',
            'txt': 'csv'  # По умолчанию для txt файлов
        }
        file_type = file_type_map.get(file_extension, 'csv')
        
        # Парсим конфигурацию приёмника
        sink_config_dict = None
        if sink_config:
            try:
                import json
                sink_config_dict = json.loads(sink_config)
                print(f"DEBUG: Конфигурация приёмника: {sink_config_dict}")
            except json.JSONDecodeError:
                return JSONResponse({
                    "status": "error",
                    "error": "Неверный формат конфигурации приёмника"
                }, status_code=400)
        else:
            return JSONResponse({
                "status": "error",
                "error": "Конфигурация приёмника обязательна для больших файлов"
            }, status_code=400)
        
        # Обрабатываем файл с прямой переливкой
        result = await large_file_processor.process_large_file(
            content, file.filename, file_type, sink_config_dict, chunk_size
        )
        
        return JSONResponse({
            "status": "processing",
            "message": f"Файл {file.filename} обрабатывается с прямой переливкой в базу данных",
            "process_id": result["process_id"],
            "file_path": result["file_path"]
        })
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        print(f"DEBUG: Ошибка при обработке большого файла: {str(e)}")
        import traceback
        traceback.print_exc()
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при обработке большого файла: {str(e)}"
        }, status_code=500)

@app.get("/large-file/status/{process_id}")
async def get_large_file_status(process_id: str):
    """Получение статуса обработки большого файла"""
    try:
        status = large_file_processor.get_processing_status(process_id)
        if status is None:
            return JSONResponse({
                "status": "error",
                "error": "Процесс не найден"
            }, status_code=404)
        
        return JSONResponse({
            "status": "success",
            "process_status": status
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при получении статуса: {str(e)}"
        }, status_code=500)

@app.get("/large-file/status")
async def get_all_large_file_status():
    """Получение статуса всех процессов обработки больших файлов"""
    try:
        print(f"DEBUG: Получение статуса всех процессов")
        all_status = large_file_processor.get_all_processing_status()
        print(f"DEBUG: Статусы процессов: {all_status}")
        return JSONResponse({
            "status": "success",
            "processes": all_status
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        print(f"DEBUG: Ошибка при получении статусов: {str(e)}")
        import traceback
        traceback.print_exc()
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при получении статусов: {str(e)}"
        }, status_code=500)

# ===== AIRFLOW DAG GENERATION ENDPOINTS =====

@app.post("/airflow/upload-file")
async def upload_file_for_airflow(file: UploadFile = File(...)):
    """Загрузка файла для использования в Airflow DAG'ах"""
    print(f"DEBUG: Начало загрузки файла {file.filename}")
    print("DEBUG: Эндпоинт /airflow/upload-file вызван")
    try:
        # Читаем содержимое файла
        content = await file.read()
        file_size = len(content)
        
        # Отладочная информация
        print(f"DEBUG: Загружен файл {file.filename}, размер: {file_size} байт")
        print(f"DEBUG: Порог больших файлов: {large_file_processor.max_file_size_bytes} байт")
        print(f"DEBUG: Файл большой? {large_file_processor.is_large_file(file_size)}")
        
        # Проверяем размер файла
        if large_file_processor.is_large_file(file_size):
            print("DEBUG: Файл определен как большой, переходим к фоновой обработке")
            # Большой файл - сохраняем на диск и запускаем фоновую обработку
            try:
                result = await large_file_processor.save_large_file(
                    content, file.filename, "csv"  # Предполагаем CSV для Airflow
                )
                return JSONResponse({
                    "status": "large_file",
                    "message": f"Файл {file.filename} слишком большой для прямой обработки. Сохранен на диск и обрабатывается в фоновом режиме.",
                    "file_id": result["file_id"],
                    "file_path": result["file_path"]
                })
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
                logger.error(f"Ошибка: {str(e)}")
                logger.error(f"Детали: {error_details}")
                return JSONResponse({
                    "status": "error",
                    "error": f"Ошибка при обработке большого файла: {str(e)}"
                }, status_code=500)
        
        # Обычная обработка для небольших файлов
        # Создаем директорию для загрузок, если её нет
        upload_dir = "/app/uploads"
        os.makedirs(upload_dir, exist_ok=True)
        
        # Сохраняем файл
        file_path = os.path.join(upload_dir, file.filename)
        with open(file_path, "wb") as buffer:
            buffer.write(content)
        
        # Копируем файл в контейнер Airflow
        import docker
        import tarfile
        import io
        
        client = docker.from_env()
        container = client.containers.get("data-orchestrator-airflow")
        
        # Создаем tar архив в памяти
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode='w') as tar:
            tarinfo = tarfile.TarInfo(name=file.filename)
            tarinfo.size = len(content)
            tar.addfile(tarinfo, io.BytesIO(content))
        
        tar_stream.seek(0)
        container.put_archive('/opt/airflow/', tar_stream.getvalue())
        
        return JSONResponse({
            "status": "success",
            "message": f"Файл {file.filename} успешно загружен и скопирован в Airflow",
            "file_name": file.filename,
            "file_path": f"/opt/airflow/{file.filename}"
        })
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при загрузке файла: {str(e)}"
        }, status_code=500)

@app.post("/airflow/generate-dag")
async def generate_airflow_dag(request: DAGGenerationRequest):
    """Создание DAG для переливки данных с помощью Airflow"""
    try:
        # Извлекаем параметры из запроса
        source_config = request.source_config
        sink_config = request.sink_config
        chunk_size = request.chunk_size
        total_rows = request.total_rows
        
        # Файл уже должен быть загружен в контейнер Airflow через /airflow/upload-file
        
        # Генерируем уникальный DAG ID
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dag_id = f"data_transfer_{timestamp}"
        
        # Создаем и сохраняем DAG
        logger.info(f"Создаем DAG с ID: {dag_id}")
        dag_file_path = dag_generator.generate_and_save_dag(
            dag_id=dag_id,
            source_config=source_config,
            sink_config=sink_config,
            chunk_size=chunk_size,
            total_rows=total_rows
        )
        
        logger.info(f"DAG создан и сохранен в: {dag_file_path}")
        
        # Проверяем, что файл действительно создался
        if os.path.exists(dag_file_path):
            file_size = os.path.getsize(dag_file_path)
            logger.info(f"Файл DAG существует, размер: {file_size} байт")
        else:
            logger.error(f"Файл DAG не найден: {dag_file_path}")
        
        return JSONResponse({
            "status": "success",
            "message": f"DAG {dag_id} успешно создан",
            "dag_id": dag_id,
            "dag_file_path": dag_file_path
        })
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при создании DAG: {str(e)}"
        }, status_code=500)


@app.get("/airflow/generated-dags")
async def get_generated_dags():
    """Получение списка сгенерированных DAG'ов"""
    try:
        dags = dag_generator.list_generated_dags()
        return JSONResponse({
            "status": "success",
            "dags": dags
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.get("/airflow/dags/{dag_id}")
async def get_dag_info(dag_id: str):
    """Получение информации о конкретном DAG"""
    try:
        # Используем airflow_client для получения информации
        dag_status = await airflow_client.get_dag_status(dag_id)
        if dag_status:
            return JSONResponse({
                "status": "success",
                "dag": dag_status
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"DAG {dag_id} не найден"
            }, status_code=404)
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при получении информации о DAG: {str(e)}"
        }, status_code=500)


@app.delete("/airflow/dags/{dag_id}")
async def delete_dag(dag_id: str):
    """Удаление DAG"""
    try:
        logger.info(f"=== НАЧАЛО УДАЛЕНИЯ DAG ===")
        logger.info(f"DAG ID: {dag_id}")
        
        # Удаляем файл DAG'а
        success = dag_generator.delete_dag(dag_id)
        if success:
            # Также удаляем из Airflow (если возможно)
            try:
                await airflow_client.delete_dag(dag_id)
                logger.info(f"DAG {dag_id} удален из Airflow")
            except Exception as e:
                logger.warning(f"Не удалось удалить DAG из Airflow: {e}")
            
            # Принудительно обновляем DAG'и в Airflow
            try:
                await airflow_client.reserialize_dags()
                logger.info("DAG'и обновлены в Airflow после удаления")
            except Exception as e:
                logger.warning(f"Не удалось обновить DAG'и в Airflow: {e}")
            
            logger.info(f"DAG {dag_id} успешно удален")
            return JSONResponse({
                "status": "success",
                "message": f"DAG {dag_id} удален"
            })
        else:
            logger.warning(f"DAG {dag_id} не найден")
            return JSONResponse({
                "status": "error",
                "error": f"DAG {dag_id} не найден"
            }, status_code=404)
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА ПРИ УДАЛЕНИИ DAG ===")
        logger.error(f"DAG ID: {dag_id}")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.get("/airflow/dags/files")
async def get_dag_files():
    """Получение списка всех DAG файлов"""
    try:
        files = dag_generator.list_generated_dags()
        return JSONResponse({
            "status": "success",
            "files": files
        })
    except Exception as e:
        logger.error(f"Ошибка при получении списка DAG файлов: {e}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)

@app.post("/airflow/dags/reserialize")
async def reserialize_dags():
    """Принудительное обновление DAG'ов в Airflow"""
    try:
        import docker
        
        # Подключаемся к Docker daemon
        client = docker.from_env()
        
        # Выполняем команду в airflow контейнере
        container = client.containers.get("data-orchestrator-airflow")
        result = container.exec_run("airflow dags reserialize")
        
        if result.exit_code == 0:
            return JSONResponse({
                "status": "success",
                "message": "DAG'и успешно обновлены",
                "output": result.output.decode('utf-8')
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"Команда обновления завершилась с ошибкой: {result.output.decode('utf-8')}"
            }, status_code=500)
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при обновлении DAG'ов: {str(e)}"
        }, status_code=500)


@app.post("/airflow/dags/{dag_id}/trigger-real")
async def trigger_dag_real(dag_id: str):
    """Реальный запуск DAG через CLI команду"""
    try:
        import docker
        import time
        
        # Подключаемся к Docker daemon
        client = docker.from_env()
        
        # Выполняем команду в airflow контейнере
        container = client.containers.get("data-orchestrator-airflow")
        result = container.exec_run(f"airflow dags trigger {dag_id}")
        
        if result.exit_code == 0:
            # Парсим вывод команды для получения dag_run_id
            output = result.output.decode('utf-8')
            output_lines = output.strip().split('\n')
            dag_run_id = f"manual__{int(time.time())}"
            
            # Ищем dag_run_id в выводе
            for line in output_lines:
                if 'manual__' in line and 'dag_run_id' in line:
                    # Извлекаем dag_run_id из строки
                    parts = line.split('|')
                    if len(parts) >= 2:
                        dag_run_id = parts[1].strip()
                        break
            
            return JSONResponse({
                "status": "success",
                "message": f"DAG {dag_id} успешно запущен через CLI",
                "dag_run": {
                    "dag_run_id": dag_run_id,
                    "state": "queued"
                }
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"CLI команда завершилась с ошибкой: {result.output.decode('utf-8')}"
            }, status_code=500)
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при выполнении CLI команды: {str(e)}"
        }, status_code=500)


@app.post("/large-file/pause/{process_id}")
async def pause_large_file_process(process_id: str):
    """Приостановка обработки большого файла"""
    try:
        status = large_file_processor.get_processing_status(process_id)
        if not status:
            return JSONResponse({
                "status": "error",
                "error": f"Процесс {process_id} не найден"
            }, status_code=404)
        
        if status["status"] != "processing":
            return JSONResponse({
                "status": "error",
                "error": f"Процесс {process_id} не находится в состоянии обработки"
            }, status_code=400)
        
        # Приостанавливаем процесс
        large_file_processor.pause_process(process_id)
        
        return JSONResponse({
            "status": "success",
            "message": f"Процесс {process_id} приостановлен"
        })
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при приостановке процесса: {str(e)}"
        }, status_code=500)


@app.post("/large-file/resume/{process_id}")
async def resume_large_file_process(process_id: str):
    """Возобновление обработки большого файла"""
    try:
        status = large_file_processor.get_processing_status(process_id)
        if not status:
            return JSONResponse({
                "status": "error",
                "error": f"Процесс {process_id} не найден"
            }, status_code=404)
        
        if status["status"] != "paused":
            return JSONResponse({
                "status": "error",
                "error": f"Процесс {process_id} не находится в состоянии паузы"
            }, status_code=400)
        
        # Возобновляем процесс
        large_file_processor.resume_process(process_id)
        
        return JSONResponse({
            "status": "success",
            "message": f"Процесс {process_id} возобновлен"
        })
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при возобновлении процесса: {str(e)}"
        }, status_code=500)


@app.delete("/large-file/delete/{process_id}")
async def delete_large_file_process(process_id: str):
    """Удаление процесса обработки большого файла (данные в БД остаются)"""
    try:
        status = large_file_processor.get_processing_status(process_id)
        if not status:
            return JSONResponse({
                "status": "error",
                "error": f"Процесс {process_id} не найден"
            }, status_code=404)
        
        # Останавливаем процесс если он активен
        if status["status"] in ["processing", "paused"]:
            large_file_processor.stop_process(process_id)
        
        # Удаляем процесс
        large_file_processor.delete_process(process_id)
        
        return JSONResponse({
            "status": "success",
            "message": f"Процесс {process_id} удален. Данные в базе данных сохранены."
        })
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В ГЕНЕРАЦИИ DAG ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при удалении процесса: {str(e)}"
        }, status_code=500)


# ===== LLM API ENDPOINTS =====

@app.post("/llm/ask")
async def ask_llm(request: LLMMessageRequest):
    """Отправка сообщения в LLM и получение ответа"""
    try:
        response = await llm_service.ask(request.message)
        return JSONResponse({
            "status": "success",
            "response": response
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В LLM API ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при обращении к LLM: {str(e)}"
        }, status_code=500)


@app.post("/llm/analyze-data")
async def analyze_data_structure(request: DataAnalysisRequest):
    """Анализ структуры данных для переноса через LLM"""
    try:
        logger.info(f"=== НАЧАЛО АНАЛИЗА ДАННЫХ ===")
        logger.info(f"Источник: {request.source_type}")
        logger.info(f"Приёмник: {request.sink_type}")
        logger.info(f"Размер схемы источника: {len(str(request.source_schema))} символов")
        logger.info(f"Размер схемы приёмника: {len(str(request.sink_schema))} символов")
        prompt = f"""Проанализируй структуру данных для переноса и дай краткие рекомендации.

ИСТОЧНИК ({request.source_type}):
{request.source_schema}

ПРИЁМНИК ({request.sink_type}):
{request.sink_schema}

КРИТИЧЕСКИ ВАЖНО:
- НЕ повторяй структуру таблиц в ответе
- НЕ включай исходные данные или схемы
- НЕ создавай инфографику о таблицах
- НЕ используй ASCII-диаграммы или таблицы
- НЕ создавай таблицы в ответе
- НЕ используй форматирование таблиц
- ТОЛЬКО plain text в ответе
- Дай только краткие выводы и рекомендации
- Будь конкретным и лаконичным

Ответь в следующем формате:

# 📊 Анализ структуры данных

## 🔍 Совместимость структур
- Кратко опиши совместимость полей и типов
- Укажи основные проблемы с типами

## 🔄 Необходимые преобразования
- Какие преобразования типов нужны
- Краткие рекомендации по конвертации

## 📈 Рекомендации по агрегации
- Нужны ли агрегации (да/нет и почему)
- Какие группировки рекомендованы

## 🎯 Правильность выбора БД приёмника
- Оцени выбор БД (хорошо/плохо и почему)
- Альтернативы (если есть)

## ⚠️ Потенциальные проблемы
- Основные риски при переносе
- Критические предупреждения

## 🚀 Рекомендации по оптимизации
- Ключевые советы по улучшению
- Оптимизация производительности

ПРАВИЛА ОТВЕТА:
- Максимум 3-4 предложения на раздел
- НЕ повторяй исходные таблицы
- НЕ создавай инфографику или диаграммы
- НЕ используй ASCII-таблицы
- НЕ создавай таблицы в ответе
- НЕ используй форматирование таблиц
- ТОЛЬКО plain text формат
- Будь конкретным и практичным
- Используй эмодзи для структуры
- Отвечай на русском языке"""

        logger.info(f"Размер промпта: {len(prompt)} символов")
        logger.info(f"Отправляем запрос в LLM...")
        
        response = await llm_service.ask(prompt)
        
        logger.info(f"=== ОТВЕТ ОТ LLM ПОЛУЧЕН ===")
        logger.info(f"Analysis response length: {len(response)} characters")
        logger.info(f"Analysis response preview: {response[:200]}...")
        
        return JSONResponse({
            "status": "success",
            "analysis": response
        })
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"=== ОШИБКА В АНАЛИЗЕ ДАННЫХ ===")
        logger.error(f"Ошибка: {str(e)}")
        logger.error(f"Детали: {error_details}")
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при анализе данных: {str(e)}"
        }, status_code=500)


from __future__ import annotations

import os
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
from .services.base import DataSink
from .services.file_sinks import CSVFileSink, JSONFileSink, XMLFileSink
from .services.airflow_client import airflow_client
from .services.airflow_dag_generator import AirflowDAGGenerator


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


app = FastAPI(title="Data Transfer Orchestrator")

# Инициализация DAG генератора
dag_generator = AirflowDAGGenerator()

# Pydantic модели
class DAGGenerationRequest(BaseModel):
    source_config: Dict[str, Any]
    sink_config: Dict[str, Any]
    chunk_size: int
    total_rows: Optional[int] = None

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
    connection_string: str = Form(...),
    database_type: str = Form("postgresql")
):
    """Тестирование подключения к базе данных"""
    try:
        if database_type.lower() == "postgresql":
            source = PostgreSQLSource(connection_string)
            is_connected = source.test_connection()
            tables = source.get_tables() if is_connected else []
            
            return JSONResponse({
                "status": "success" if is_connected else "error",
                "connected": is_connected,
                "tables": tables,
                "database_type": "postgresql"
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"Неподдерживаемый тип базы данных: {database_type}"
            }, status_code=400)
            
    except Exception as e:
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
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


@app.post("/database/schema")
async def get_table_schema(
    connection_string: str = Form(...),
    table_name: str = Form(...),
    database_type: str = Form("postgresql")
):
    """Получение схемы таблицы"""
    try:
        if database_type.lower() == "postgresql":
            source = PostgreSQLSource(connection_string)
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


@app.post("/transfer")
async def transfer(
    source_type: str = Form(...),
    sink_type: str = Form(...),
    chunk_size: int = Form(1000),
    file: UploadFile = File(...),
    delimiter: str = Form(";"),
):
    content_bytes = await file.read()
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
    sink_connection_string: str = Form(...),
    sink_table_name: str = Form(...),
    sink_mode: str = Form("append"),
    chunk_size: int = Form(1000),
    file: UploadFile = File(None),
    delimiter: str = Form(";"),
    database_type: str = Form("postgresql")
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
        else:
            return JSONResponse({"error": f"Неподдерживаемый источник: {source_type}"}, status_code=400)
        
        # Создаем приёмник данных
        if database_type.lower() == "postgresql":
            sink = PostgreSQLSink(sink_connection_string, sink_table_name, sink_mode)
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
        return JSONResponse({
            "status": "error",
            "error": str(e)
        }, status_code=500)


# ===== AIRFLOW DAG GENERATION ENDPOINTS =====

@app.post("/airflow/upload-file")
async def upload_file_for_airflow(file: UploadFile = File(...)):
    """Загрузка файла для использования в Airflow DAG'ах"""
    try:
        # Создаем директорию для загрузок, если её нет
        upload_dir = "/app/uploads"
        os.makedirs(upload_dir, exist_ok=True)
        
        # Сохраняем файл
        file_path = os.path.join(upload_dir, file.filename)
        with open(file_path, "wb") as buffer:
            content = await file.read()
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
        
        # Создаем DAG
        dag_file_path = dag_generator.generate_data_transfer_dag(
            dag_id=dag_id,
            source_config=source_config,
            sink_config=sink_config,
            chunk_size=chunk_size,
            total_rows=total_rows
        )
        
        return JSONResponse({
            "status": "success",
            "message": f"DAG {dag_id} успешно создан",
            "dag_id": dag_id,
            "dag_file_path": dag_file_path
        })
        
    except Exception as e:
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
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при получении информации о DAG: {str(e)}"
        }, status_code=500)


@app.delete("/airflow/dags/{dag_id}")
async def delete_dag(dag_id: str):
    """Удаление DAG"""
    try:
        # Удаляем файл DAG'а
        success = dag_generator.delete_dag(dag_id)
        if success:
            # Также удаляем из Airflow (если возможно)
            await airflow_client.delete_dag(dag_id)
            return JSONResponse({
                "status": "success",
                "message": f"DAG {dag_id} удален"
            })
        else:
            return JSONResponse({
                "status": "error",
                "error": f"DAG {dag_id} не найден"
            }, status_code=404)
    except Exception as e:
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
        return JSONResponse({
            "status": "error",
            "error": f"Ошибка при выполнении CLI команды: {str(e)}"
        }, status_code=500)


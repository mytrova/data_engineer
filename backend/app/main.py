from __future__ import annotations

from typing import Any, Dict, List

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



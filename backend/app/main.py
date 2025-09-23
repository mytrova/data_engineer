from __future__ import annotations

from typing import Any, Dict, List

from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse

from .services.csv_source import CSVSource
from .services.json_source import JSONSource
from .services.xml_source import XMLSource
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


@app.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    source_type: str = Form("csv"),
    chunk_size: int = Form(10),
):
    content_bytes = await file.read()
    try:
        text = content_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = content_bytes.decode("utf-8", errors="replace")

    st = (source_type or "csv").lower()
    if st == "csv":
        source = CSVSource(text_data=text)
    elif st == "json":
        source = JSONSource(text_data=text)
    elif st == "xml":
        source = XMLSource(text_data=text)
    else:
        return JSONResponse({"error": f"Unknown source_type {source_type}"}, status_code=400)

    sink = PreviewSink(max_rows=chunk_size)

    result = sink.write(source.headers(), source.read())
    return JSONResponse(result)


@app.post("/transfer")
async def transfer(
    source_type: str = Form(...),
    sink_type: str = Form(...),
    chunk_size: int = Form(1000),
    file: UploadFile = File(...),
):
    content_bytes = await file.read()
    try:
        text = content_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = content_bytes.decode("utf-8", errors="replace")

    # Build source
    if source_type.lower() == "csv":
        source = CSVSource(text)
    elif source_type.lower() == "json":
        source = JSONSource(text)
    elif source_type.lower() == "xml":
        source = XMLSource(text)
    else:
        return JSONResponse({"error": f"Unknown source_type {source_type}"}, status_code=400)

    # Build sink. For demo, write to container-local /tmp and return path
    out_ext = sink_type.lower()
    out_path = f"/tmp/output.{out_ext}"
    if sink_type.lower() == "csv":
        sink = CSVFileSink(out_path)
    elif sink_type.lower() == "json":
        sink = JSONFileSink(out_path)
    elif sink_type.lower() == "xml":
        sink = XMLFileSink(out_path)
    elif sink_type.lower() == "preview":
        sink = PreviewSink(max_rows=chunk_size)  # not chunk-aware, but acceptable for preview
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



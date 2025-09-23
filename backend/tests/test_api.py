from __future__ import annotations

from fastapi.testclient import TestClient
from app.main import app


client = TestClient(app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_upload_preview_csv():
    files = {"file": ("sample.csv", b"a,b\n1,2\n3,4\n", "text/csv")}
    r = client.post("/upload", files=files)
    assert r.status_code == 200
    data = r.json()
    assert data["headers"] == ["a", "b"]
    assert data["rows"][0] == ["1", "2"]


def test_transfer_csv_to_json(tmp_path):
    files = {"file": ("sample.csv", b"a,b\n1,2\n3,4\n", "text/csv")}
    data = {"source_type": "csv", "sink_type": "json", "chunk_size": "1"}
    r = client.post("/transfer", files=files, data=data)
    assert r.status_code == 200
    resp = r.json()
    assert resp["headers"] == ["a", "b"]



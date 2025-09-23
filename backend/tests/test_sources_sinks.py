from __future__ import annotations

from app.services.csv_source import CSVSource
from app.services.json_source import JSONSource
from app.services.xml_source import XMLSource
from app.services.file_sinks import CSVFileSink, JSONFileSink, XMLFileSink

import os


def test_csv_source_read_headers():
    text = "a,b\n1,2\n3,4\n"
    src = CSVSource(text)
    assert src.headers() == ["a", "b"]
    rows = list(src.read())
    assert rows == [["1", "2"], ["3", "4"]]


def test_json_source_objects():
    text = "[{\"x\":1,\"y\":2},{\"x\":3,\"y\":4}]"
    src = JSONSource(text)
    assert src.headers() == ["x", "y"]
    assert list(src.read()) == [[1, 2], [3, 4]]


def test_xml_source_items(tmp_path):
    text = """
    <items>
      <item><a>1</a><b>2</b></item>
      <item><a>3</a><b>4</b></item>
    </items>
    """
    src = XMLSource(text)
    assert src.headers() == ["a", "b"]
    assert list(src.read()) == [["1", "2"], ["3", "4"]]


def test_file_sinks(tmp_path):
    headers = ["a", "b"]
    rows = [[1, 2], [3, 4]]

    csv_path = tmp_path / "out.csv"
    CSVFileSink(str(csv_path)).write(headers, rows)
    assert os.path.exists(csv_path)

    json_path = tmp_path / "out.json"
    JSONFileSink(str(json_path)).write(headers, rows)
    assert os.path.exists(json_path)

    xml_path = tmp_path / "out.xml"
    XMLFileSink(str(xml_path)).write(headers, rows)
    assert os.path.exists(xml_path)



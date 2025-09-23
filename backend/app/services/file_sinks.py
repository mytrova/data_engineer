from __future__ import annotations

import csv
import json
from typing import Any, Iterable, List, Mapping, Sequence

from .base import DataSink


class CSVFileSink(DataSink):
    def __init__(self, path: str, delimiter: str = ",") -> None:
        self._path = path
        self._delimiter = delimiter

    def write(self, headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> Any:
        with open(self._path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=self._delimiter)
            writer.writerow(list(headers))
            for row in rows:
                writer.writerow(list(row))
        return {"written_to": self._path}


class JSONFileSink(DataSink):
    def __init__(self, path: str) -> None:
        self._path = path

    def write(self, headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> Any:
        # Write as list of objects using headers
        objects = []
        for row in rows:
            obj = {str(h): v for h, v in zip(headers, row)}
            objects.append(obj)
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(objects, f, ensure_ascii=False, indent=2)
        return {"written_to": self._path}


class XMLFileSink(DataSink):
    def __init__(self, path: str, root_tag: str = "items", item_tag: str = "item") -> None:
        self._path = path
        self._root_tag = root_tag
        self._item_tag = item_tag

    def write(self, headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> Any:
        from xml.etree.ElementTree import Element, SubElement, ElementTree

        root = Element(self._root_tag)
        for row in rows:
            item = SubElement(root, self._item_tag)
            for h, v in zip(headers, row):
                el = SubElement(item, str(h))
                el.text = "" if v is None else str(v)
        ElementTree(root).write(self._path, encoding="utf-8", xml_declaration=True)
        return {"written_to": self._path}



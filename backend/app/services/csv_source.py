from __future__ import annotations

import csv
from io import StringIO
from typing import Any, Iterable, List, Sequence

from .base import DataSource


class CSVSource(DataSource):
    def __init__(self, text_data: str, delimiter: str = ",") -> None:
        self._text_data = text_data
        self._delimiter = delimiter
        self._headers: List[str] = []

        # Pre-parse just the headers for quick access
        sio = StringIO(self._text_data)
        reader = csv.reader(sio, delimiter=self._delimiter)
        try:
            first_row = next(reader)
        except StopIteration:
            first_row = []
        self._headers = [str(h) for h in first_row]

    def headers(self) -> List[str]:
        return list(self._headers)

    def read(self) -> Iterable[Sequence[Any]]:
        sio = StringIO(self._text_data)
        reader = csv.reader(sio, delimiter=self._delimiter)
        # Skip header row
        try:
            next(reader)
        except StopIteration:
            return []
        return reader



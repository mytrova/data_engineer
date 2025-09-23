from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Sequence

from .base import DataSource


class JSONSource(DataSource):
    def __init__(self, text_data: str) -> None:
        self._data = json.loads(text_data or "null")
        self._headers: List[str] = []
        self._rows: List[List[Any]] = []
        self._parse()

    def _parse(self) -> None:
        # Accept: array of objects, or object with key 'items' as array, or array of arrays with headers in first element as object keys
        data = self._data
        items: List[Dict[str, Any]] = []
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            items = data["items"]
        elif isinstance(data, list):
            if data and all(isinstance(x, dict) for x in data):
                items = data  # list of dicts
            else:
                # list of lists not supported for headers inference; treat as rows, headers idx0..n
                arr_rows = data
                max_len = max((len(r) for r in arr_rows if isinstance(r, list)), default=0)
                self._headers = [f"col{i+1}" for i in range(max_len)]
                self._rows = [list(r) for r in arr_rows if isinstance(r, list)]
                return
        else:
            items = []

        # Infer headers from union of keys preserving insertion order by first item, then others
        header_order: List[str] = []
        seen = set()
        for obj in items:
            if not isinstance(obj, dict):
                continue
            for k in obj.keys():
                if k not in seen:
                    seen.add(k)
                    header_order.append(k)
        self._headers = header_order
        self._rows = [[obj.get(h) for h in self._headers] for obj in items if isinstance(obj, dict)]

    def headers(self) -> List[str]:
        return list(self._headers)

    def read(self) -> Iterable[Sequence[Any]]:
        return iter(self._rows)



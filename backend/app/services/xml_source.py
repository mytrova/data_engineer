from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Iterable, List, Sequence

from .base import DataSource


class XMLSource(DataSource):
    def __init__(self, text_data: str, item_tag: str = "item") -> None:
        self._text = text_data
        self._item_tag = item_tag
        self._headers: List[str] = []
        self._rows: List[List[Any]] = []
        self._parse()

    def _parse(self) -> None:
        try:
            root = ET.fromstring(self._text)
        except ET.ParseError:
            self._headers = []
            self._rows = []
            return
        items = list(root.findall(f'.//{self._item_tag}')) or list(root)
        # collect headers as union of child tags
        header_order: List[str] = []
        seen = set()
        for it in items:
            for child in list(it):
                tag = child.tag
                if tag not in seen:
                    seen.add(tag)
                    header_order.append(tag)
        self._headers = header_order
        for it in items:
            row = []
            for h in self._headers:
                el = it.find(h)
                row.append(el.text if el is not None else None)
            self._rows.append(row)

    def headers(self) -> List[str]:
        return list(self._headers)

    def read(self) -> Iterable[Sequence[Any]]:
        return iter(self._rows)



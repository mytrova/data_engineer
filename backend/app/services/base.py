from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


class DataSource(ABC):
    """Abstract data source.

    Implementations must yield rows as sequences of values, with a header list
    describing the column names.
    """

    @abstractmethod
    def read(self) -> Iterable[Sequence[Any]]:
        """Return an iterable of rows (sequences) from the source."""
        raise NotImplementedError

    @abstractmethod
    def headers(self) -> List[str]:
        """Return the header/column names for the data set."""
        raise NotImplementedError

    def read_in_chunks(self, chunk_size: int) -> Iterable[List[Sequence[Any]]]:
        """Yield data in chunks of given size, defaulting to iterating over read()."""
        if chunk_size <= 0:
            chunk_size = 1000
        chunk: List[Sequence[Any]] = []
        for row in self.read():
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


class DataSink(ABC):
    """Abstract data sink.

    Implementations consume row iterables with headers and perform an action
    (store, forward, preview, etc.).
    """

    @abstractmethod
    def write(self, headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> Any:
        """Consume data and return an optional result object."""
        raise NotImplementedError

    def write_chunks(self, headers: Sequence[str], chunks: Iterable[List[Sequence[Any]]]) -> Any:
        """Default chunked implementation funnels into write by flattening."""
        def flatten():
            for chunk in chunks:
                for row in chunk:
                    yield row
        return self.write(headers, flatten())



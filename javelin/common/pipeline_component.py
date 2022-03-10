from abc import ABC, abstractmethod
from rx import from_iterable

class PipelineComponent(ABC):
    @abstractmethod
    def yield_per(rows: int) -> dict:
        """Yields rows from datasource

        Args:
            rows (int): number of rows to yield per batch

        Returns:
            dict: rows
        """
        
    def observable_per(self, rows: int = 10000):
        return from_iterable(self.yield_per(rows))
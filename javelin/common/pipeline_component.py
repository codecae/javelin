from abc import ABC, abstractmethod
from rx import from_iterable

class PipelineComponent(ABC):
    @abstractmethod
    def yield_per(self, partition_rows: int = 10000, yield_rows: int = 10000) -> dict:
        """Yields rows from datasource

        Args:
            rows (int): number of rows to yield per batch

        Returns:
            dict: rows
        """
        
    def observable_per(self, partition_rows: int = 10000, yield_rows: int = 10000):
        return from_iterable(self.yield_per(partition_rows=partition_rows, yield_rows=yield_rows))
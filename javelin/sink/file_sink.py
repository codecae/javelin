from abc import ABC, abstractmethod
from ..common import ComponentMessage

class FileSink(ABC):
    @abstractmethod
    def write(self, message: ComponentMessage):
        """Write bytes to the sink Location"""
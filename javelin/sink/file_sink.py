from datetime import datetime
import time
import os
from abc import ABC, abstractmethod
from ..common import ComponentMessage, ComponentMetadata
from dataclasses import dataclass

@dataclass
class FileInfo:
    path: str = ""
    filename: str = ""
    @property
    def full_path(self):
        return os.path.join(self.path, self.filename)

class FileSink(ABC):
    def file(self, header: ComponentMetadata) -> FileInfo:
        _date = datetime.fromtimestamp(time.time())
        _iso_date = _date.isoformat().replace(":", "_")
        return FileInfo(
            path = f"{header.name}/{header.object_namespace}/{header.object_name}/{_date.year}/{_date.month}/{_date.day}",
            filename = f"{header.object_name}-{_iso_date}.{header.object_format}"
        )

    @abstractmethod
    def write(self, message: ComponentMessage):
        """Write bytes to the sink Location"""
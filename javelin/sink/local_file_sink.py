import os
from .file_sink import FileSink
from ..common import ComponentMessage

class LocalFileSink(FileSink):
    def __init__(self, base_path: str):
        self._base_path = base_path

    def write(self, message: ComponentMessage):
        _file = self.file(message.header)
        _dir = os.path.join(self._base_path, _file.path)
        _full_file_path = os.path.join(_dir, _file.filename)
        _mode = 0o777
        if not os.path.exists(_dir):
            os.makedirs(_dir, _mode)
        f = open(_full_file_path, "wb+")
        f.write(message.content)
        f.close()
        
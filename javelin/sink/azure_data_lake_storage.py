import time
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from azure.identity import ClientSecretCredential
from .file_sink import FileSink
from ..common import ComponentMessage

class AzureDataLakeStorageSink(FileSink):
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, storage_acct: str, container: str):
        self._credential = ClientSecretCredential(client_id=client_id,tenant_id=tenant_id,client_secret=client_secret)
        self._service_client = DataLakeServiceClient(account_url=f"https://{storage_acct}.dfs.core.windows.net", credential=self._credential)
        self._container = container

    def write(self, message: ComponentMessage):
        _date = datetime.fromtimestamp(time.time())
        _iso_date = _date.isoformat().replace(":", "_")
        _path = f"{message.header.name}/{message.header.object_namespace}/{message.header.object_name}/{_date.year}/{_date.month}/{_date.day}"
        _filename = f"{message.header.object_name}-{_iso_date}.{message.header.object_format}"
        _fs_client = self._service_client.get_file_system_client(file_system=self._container)
        _fs_client.create_directory(_path)
        _directory_client = _fs_client.get_directory_client(_path)
        _file_client = _directory_client.create_file(_filename)
        _len = len(message.content)
        _file_client.append_data(data=message.content, offset=0, length=_len)
        _file_client.flush_data(_len)

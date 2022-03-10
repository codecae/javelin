from os.path import join
import json
from .secret_provider import SecretProvider

class VaultInjectorSecretProvider(SecretProvider):
    def __init__(self, filename: str, path: str = "/vault/secrets"):
        self._path=path
        self._filename=filename
        self._full_path=join(path,filename)

    @property
    def secrets(self) -> dict:
        _secrets = {}
        _file = open(self._full_path,"r")
        _secrets.update(json.load(_file))
        _file.close()
        return _secrets

from abc import ABC, abstractmethod

class SecretProvider(ABC):
    @property
    @abstractmethod
    def secrets(self) -> dict:
        """Returns:
            dict: dict of secrets
        """
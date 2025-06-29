from abc import ABC, abstractmethod
from config import config


class BaseStorageConnector(ABC):
    @abstractmethod
    def upload_dataframe(self, df, key: str) -> bool:
        pass

    @abstractmethod
    def download_dataframe(self, key: str) -> pd.DataFrame:
        pass


class BaseCHConnector(ABC):
    @abstractmethod
    def execute_query(self, query: str):
        pass
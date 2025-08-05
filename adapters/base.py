from core.config import ReconciliationConfig
from abc import ABC, abstractmethod
from typing import Dict, List
from core.config import DatastoreConfig, AdapterConfig
from core.query import Query

class Adapter(ABC):
    def __init__(self, store_config: DatastoreConfig, adapter_config:AdapterConfig, role: str):
        self.store_config = store_config
        self.adapter_config = adapter_config
        self.role = role  # 'source','sink','state'

    @abstractmethod
    def connect(self): 
        pass

    @abstractmethod
    def fetch(self, query: Query, op_name: str) -> List[Dict]: ...

    @abstractmethod
    def fetch_one(self, query: Query, op_name: str) -> Dict: ...

    @abstractmethod
    def execute(self, sql: str, params=None): ...

    @abstractmethod
    def load_data(self, data, r_config: ReconciliationConfig): ...

    @abstractmethod
    def insert_or_update(self, table: str, row: dict): ...

    @abstractmethod
    def close(self): ...

    def __eq__(self, value: 'Adapter') -> bool:
        return self.adapter_config == value.adapter_config


class ExternalAdapter(ABC):
    pass
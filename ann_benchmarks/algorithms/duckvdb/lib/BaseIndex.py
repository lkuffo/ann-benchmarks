from abc import ABC, abstractmethod
import duckdb


class BaseIndex(ABC):
    def __init__(self, cursor, schema, vector_table, metric, name="BaseIndex"):
        self.cursor: duckdb.DuckDBPyConnection = cursor
        self.schema: str = schema
        self.vector_table: str = vector_table
        self.metric: str = metric
        self.name: str = name

    @abstractmethod
    def build(self, dimensions, **kwargs) -> None:
        pass

    @abstractmethod
    def query(self, q_vector, dimensions, k, debug) -> [float, list]:
        pass

    def describe(self):
        print(f"DuckVDB(index={self.name}, metric={self.metric})")


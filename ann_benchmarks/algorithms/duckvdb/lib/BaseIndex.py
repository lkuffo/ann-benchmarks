from abc import ABC, abstractmethod


class BaseIndex(ABC):
    def __init__(self, cursor, schema, vector_table, metric, name="BaseIndex"):
        self.cursor = cursor
        self.schema = schema
        self.vector_table = vector_table
        self.metric = metric
        self.name = name

    @abstractmethod
    def build(self, **kwargs) -> None:
        pass

    @abstractmethod
    def query(self, q_vector, dimensions, k, debug) -> [float, list]:
        pass

    def describe(self):
        print(f"DuckVDB(index={self.name}, metric={self.metric})")


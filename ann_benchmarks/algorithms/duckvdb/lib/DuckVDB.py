import duckdb
import datetime
from .RPLSH import RPLSH
from .LinearScan import LinearScan
from .BaseIndex import BaseIndex

INDEXES = {
    "linear-scan": LinearScan,
    "rplsh": RPLSH
}


class DuckVDBLib:
    def __init__(self, db_name, metric, index="linear-scan", debug=False):
        self.cursor = duckdb.connect(db_name)
        self.metric = metric
        self.vector_table_name = 'array_table'
        self.schema_name = 'mydb'
        self.debug = debug
        self.Index: BaseIndex = INDEXES[index](self.cursor, self.schema_name, self.vector_table_name, self.metric, self.debug)

    def create_vector_table(self):
        print('Loading DB...')
        self.cursor.execute(
            f"""
                CREATE SCHEMA IF NOT EXISTS {self.schema_name};
            """
        )

        self.cursor.execute(
            f"""
                DROP TABLE IF EXISTS {self.schema_name}.{self.vector_table_name};
            """
        )

        self.cursor.execute(
            f"""
            CREATE TABLE {self.schema_name}.{self.vector_table_name} (
                id INT, 
                vector FLOAT[]
            );
            """
        )

    def create_index(self, dimensions, **kwargs):
        self.Index.build(dimensions, **kwargs)

    def set_cores(self, cores):
        self.cursor.execute(f"PRAGMA threads={cores};")

    def populate_vector_table(self, data, dimensions, use_vectors=False):
        self.cursor.execute(
            f"""
            INSERT INTO {self.schema_name}.{self.vector_table_name} (id, vector) SELECT id, vector FROM data;
            """
        )

        if use_vectors:  # DuckDB does not have conversion from Array to DataFrame
            print("Casting...")
            self.cursor.execute(
                f"""
                 ALTER TABLE {self.schema_name}.{self.vector_table_name} ALTER vector TYPE FLOAT[{dimensions}];
                 """
            )
        print("Data Finished Loading")

    def execute_query(self, query, dimensions, k, bench=False, repetition=1):
        if self.debug:
            print("Querying...")
        total_ms = 0
        res = None
        for _ in range(repetition):
            query_time, res = self.Index.query(query, dimensions, k)
            total_ms += query_time
        if bench:
            print('Query took in average %.2f ms' % (total_ms / repetition))
        return res




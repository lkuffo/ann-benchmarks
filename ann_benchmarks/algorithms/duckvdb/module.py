import subprocess
import sys

import duckdb
import pandas as pd
import datetime

from ..base.module import BaseANN


class DuckVDB(BaseANN):
    def __init__(self, metric, args):
        self._metric = metric
        self._cur = None
        if metric not in ['euclidean', 'angular']:
            raise RuntimeError(f"unknown metric {self._metric}")
        self.index = args.get('method')
        self.name = 'duckvdb_' + self.index

    def fit(self, X):
        db_fn = './duckvdb.duckdb'
        cursor = duckdb.connect(db_fn)

        print('Loading DB...')
        cursor.execute(
            """
                CREATE SCHEMA IF NOT EXISTS mydb;
            """
        )

        cursor.execute(
            """
                DROP TABLE IF EXISTS mydb.array_table;
            """
        )

        print("Creating structures...")
        cursor.execute(
            f"""
            CREATE TABLE mydb.array_table (
                id INT,
                vector FLOAT[]
            );
            """
        )

        print("Copying data...")
        tmp_df = pd.DataFrame({'id': range(len(X)), 'vector': list(X)})

        print("Inserting data...")
        cursor.execute(
            """
            INSERT INTO mydb.array_table (id, vector) SELECT id, vector FROM tmp_df;
            """
        )
        print("Casting...")
        cursor.execute(
            f"""
                ALTER TABLE mydb.array_table ALTER vector TYPE FLOAT[{X.shape[1]}];
            """
        )

        print("Creating index...")
        if self.index == 'lsh':
            # Alter table to add index
            pass
        elif self.index == 'linear-scan':
            # Nothing to do
            pass
        print("Done setup!")
        self._cur = cursor

    def set_query_arguments(self, ef_search):
        pass

    def query(self, v, n):
        if self.index == 'linear-scan':
            query = self.get_linear_scan_query(v, n)
            # tic = datetime.datetime.now()
            res = self._cur.execute(
                query,
                [v]
            )
            # tac = datetime.datetime.now()
            # tictac = tac - tic
            # bench = tictac.total_seconds() * 1000
            # print(bench)
            return [id[0] for id in res.fetchall()]
        elif self.index == 'lsh':
            raise RuntimeError("Not implemented yet")

    def get_linear_scan_query(self, v, n):
        if self._metric == "angular":
            query = f"""
                SELECT id FROM mydb.array_table ORDER BY array_cosine_similarity(
                    CAST (? AS FLOAT[{len(v)}]),
                    vector
                ) DESC LIMIT {n};
            """
        elif self._metric == "euclidean":
            query = f"""
                SELECT id FROM mydb.array_table ORDER BY array_distance(
                    CAST (? AS FLOAT[{len(v)}]),
                    vector
                ) ASC LIMIT {n};
            """
        else:
            raise RuntimeError(f"unknown metric {self._metric}")
        return query

    def __str__(self):
        return f"DuckVDB(index={self.index})"

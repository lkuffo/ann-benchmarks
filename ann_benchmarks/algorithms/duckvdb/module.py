import subprocess
import sys

import pandas as pd

from ..base.module import BaseANN
from .lib.DuckVDB import DuckVDBLib


class DuckVDB(BaseANN):
    def __init__(self, metric, args):
        self.metric = metric
        self.duckvdb: DuckVDBLib = None
        if metric not in ['euclidean', 'angular']:
            raise RuntimeError(f"unknown metric {self.metric}")
        self.index = args.get('method')
        if self.index not in ['rplsh', 'linear-scan']:
            raise RuntimeError(f"unknown index {self.metric}")
        self.name = 'duckvdb_' + self.index
        if self.index == 'rplsh':
            self.lsh_hash_distance = args.get('rplsh_hash_distance', 15)

    def fit(self, X):
        dimensions = X.shape[1]

        duckvdb = DuckVDBLib('./duckvdb.duckdb', self.metric, self.index, False)

        duckvdb.create_vector_table()

        tmp_df = pd.DataFrame({'id': range(len(X)), 'vector': list(X)})

        # TODO: For some reason there is a bug when using vectors in RP-LSH
        use_vectors = self.index == 'linear-scan'

        duckvdb.populate_vector_table(tmp_df, dimensions, use_vectors)

        # Create Index
        if self.index == 'rplsh':
            duckvdb.create_index(dimensions, lsh_distance_threshold=self.lsh_hash_distance)
        elif self.index == 'linear-scan':
            duckvdb.create_index(dimensions)

        self.duckvdb = duckvdb

    def set_query_arguments(self, ef_search):
        pass

    def query(self, v, n):
        return self.duckvdb.execute_query(v, len(v), n, False, 1)

    def __str__(self):
        return self.duckvdb.Index.describe()

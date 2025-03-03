from .BaseIndex import BaseIndex
import datetime


class LinearScan(BaseIndex):
    def __init__(self, cursor, schema, vector_table, metric, debug):
        super().__init__(cursor, schema, vector_table, metric, debug, "LinearScan")

    def build(self, dimensions, **kwargs) -> None:
        print('Linear-Scan | Not neccessary to create index...')

    def query(self, q_vector, dimensions, k) -> [float, list]:
        query: str = ""
        if self.metric == 'cosine' or self.metric == 'angular':
            query = f"""
                SELECT id FROM {self.schema}.{self.vector_table} ORDER BY array_cosine_similarity(
                    CAST (? AS FLOAT[{dimensions}]),
                    vector
                ) DESC LIMIT {k};
            """
        elif self.metric == 'euclidean':
            query = f"""
                SELECT id FROM {self.schema}.{self.vector_table} ORDER BY array_distance(
                    CAST (? AS FLOAT[{dimensions}]),
                    vector
                ) ASC LIMIT {k};
            """
        tic = datetime.datetime.now()
        res = self.cursor.execute(
            query,
            [q_vector]
        )
        toc = datetime.datetime.now()
        bench = (toc - tic).total_seconds() * 1000
        if self.debug:
            print(query)
            print('Took {} ms'.format(bench))
        return bench, [id[0] for id in res.fetchall()]


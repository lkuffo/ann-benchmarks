from BaseIndex import BaseIndex


class RPLSH(BaseIndex):
    def __init__(self, cursor, schema, vector_table, metric):
        super().__init__(cursor, schema, vector_table, metric, "RPLSH")

    def build(self, n_planes=128) -> None:
        self.build_planes(n_planes)
        self.lsh_vectors()

    def build_planes(self, n_planes):
        pass

    def lsh_vectors(self):
        pass

    def query(self, q_vector, dimensions, k, debug) -> [float, list]:
        return 0, [0, 0, 0, 0]


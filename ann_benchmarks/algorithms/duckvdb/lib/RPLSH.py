from .BaseIndex import BaseIndex
import datetime


class RPLSH(BaseIndex):
    def __init__(self, cursor, schema, vector_table, metric, debug):
        super().__init__(cursor, schema, vector_table, metric, debug, "RPLSH")
        self.index_vector_table = "vector_table_lsh"
        self.lsh_distance_threshold = None
        self.n_planes = None
        self.hash_datatype = None

    def build(self, dimensions, n_planes=64, lsh_distance_threshold=15) -> None:
        self.lsh_distance_threshold = lsh_distance_threshold
        self.n_planes = n_planes
        hash_datatype = "UHUGEINT"
        if n_planes == 64:
            hash_datatype = "UBIGINT"
        elif n_planes == 128:
            hash_datatype = "UHUGEINT"
        self.hash_datatype = hash_datatype

        self.build_planes(dimensions, n_planes)
        self.lsh_vectors(dimensions, n_planes)

    def build_planes(self, dimensions, n_planes):
        vector_samples = n_planes * 2

        # TODO: Add back dimensions in table creation
        print("Creating planes...")
        self.cursor.execute(f"""
            CREATE TABLE {self.schema}.planes (  		
                normal FLOAT[],
                median FLOAT[]
            );
        """)

        self.cursor.execute(
            f"""
                DROP TABLE IF EXISTS {self.schema}.{self.index_vector_table};
            """
        )

        # TODO Add dimensions
        self.cursor.execute(
            f"""
            CREATE TABLE {self.schema}.{self.index_vector_table} (
                id INT, 
                vector FLOAT[],
                lsh {self.hash_datatype}
            );
            """
        )

        self.cursor.execute(f"""
            INSERT INTO {self.schema}.planes
            SELECT 
                list_transform(
                    generate_series(1, {dimensions}),
                    idx -> v1[idx] - v2[idx]
                ) AS normal,
                list_transform(
                    generate_series(1, {dimensions}),
                    idx -> (v1[idx] + v2[idx]) / 2
                ) AS median
            FROM (
                SELECT 
                    min(vector) AS v1, 
                    max(vector) AS v2 
                FROM (
                    SELECT 
                        id, vector, row_number() OVER () % {n_planes} AS plane_pair_id
                    FROM 
                        {self.schema}.{self.vector_table} 
                    USING SAMPLE {vector_samples}
                )
                GROUP BY plane_pair_id
            )
        """)
        print(f"{n_planes} planes created")

    def lsh_vectors(self, dimensions, n_planes):
        print("Creating hash column")
        # TODO THIS IS WRONG HUGE INT
        self.cursor.execute(
            f"""
                ALTER TABLE {self.schema}.{self.vector_table}
                ADD COLUMN lsh UHUGEINT;
            """
        )
        self.cursor.execute(
            f"""
                CREATE INDEX lsh_index ON {self.schema}.{self.vector_table}(lsh);
            """
        )

        print("Hashing...")
        self.cursor.execute(f"""
                WITH rp AS (
                    SELECT
                        list(normal) AS normals,
                        list(median) AS offsets
                    FROM
                        {self.schema}.planes
                )
                INSERT INTO {self.schema}.{self.index_vector_table}(id, vector, lsh)
                SELECT
                    id,
                    vector,
                    list_sum(
                        list_transform(
                            generate_series(1, {n_planes}),
                            plane_idx -> ((list_dot_product(
                                list_transform(
                                    generate_series(1, {dimensions}),
                                    idx -> vector[idx] - rp.offsets[plane_idx][idx]
                                ),
                                rp.normals[plane_idx] 
                            ) > 0)::{self.hash_datatype} << (plane_idx - 1))
                        )
                    ) AS lsh
                FROM
                    {self.schema}.{self.vector_table},
                    rp
                ;
            """
        )
        print('Finishing creating hash')

        # df = x.fetch_df()
        # print(df)
        # print(df['lsh'][0])
        # print(len(df['lsh'][0]))

    def query(self, q_vector, dimensions, k) -> [float, list]:
        distance_function_duckdb = "array_cosine_similarity"
        ordering_order = "DESC"
        if self.metric == "cosine" or self.metric == 'angular':
            distance_function_duckdb = "array_cosine_similarity"
            ordering_order = "DESC"
        elif self.metric == "euclidean":
            distance_function_duckdb = "array_distance"
            ordering_order = "ASC"

        distance_function_duckdb = distance_function_duckdb.replace('array', 'list')

        query = f"""
            WITH 
                rp AS (
                    SELECT
                        list(normal) AS normals,
                        list(median) AS offsets
                    FROM
                        {self.schema}.planes
                ),
                q_cte AS (
                    SELECT CAST (? AS FLOAT[]) AS query_vector
                ),
                q_hash_cte AS ( 
                    SELECT
                        list_sum(
                            list_transform(
                                generate_series(1, {self.n_planes}),
                                plane_idx -> ((list_dot_product(
                                    list_transform(
                                        generate_series(1, {dimensions}),
                                        idx -> q_cte.query_vector[idx] - rp.offsets[plane_idx][idx]
                                    ),
                                    rp.normals[plane_idx] 
                                ) > 0)::{self.hash_datatype} << (plane_idx - 1))
                            )
                        ) AS q_lsh
                    FROM
                        q_cte, rp
                ) 
            SELECT 
                id,
                bit_count(xor(q_hash_cte.q_lsh, lsh)) AS approx_distance,
                {distance_function_duckdb}(
                    CAST (? AS FLOAT[]),
                    vector
                ) AS real_score
            FROM 
                {self.schema}.{self.index_vector_table}, q_hash_cte
            WHERE 
                approx_distance <= {self.lsh_distance_threshold}
            ORDER BY real_score {ordering_order}
            LIMIT {10};
        """
        tic = datetime.datetime.now()
        res = self.cursor.execute(
            query, [
                q_vector,
                q_vector
            ])
        toc = datetime.datetime.now()
        bench = (toc - tic).total_seconds() * 1000
        if self.debug:
            print(query)
            print('Took {} ms'.format(bench))
        results = res.fetchall()
        return bench, [id[0] for id in results]

    def describe(self):
        return f"DuckVDB(index={self.name}, metric={self.metric}, lsh_distance_threshold={self.lsh_distance_threshold})"

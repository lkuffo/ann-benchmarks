from BaseIndex import BaseIndex


class RPLSH(BaseIndex):
    def __init__(self, cursor, schema, vector_table, metric):
        super().__init__(cursor, schema, vector_table, metric, "RPLSH")
        self.index_vector_table = "vector_table_lsh"

    def build(self, dimensions, n_planes=64) -> None:
        self.build_planes(dimensions, n_planes)
        self.lsh_vectors()

    def build_planes(self, dimensions, n_planes):
        vector_samples = n_planes * 2
        HASH_TYPE = "UHUGEINT"
        if n_planes == 64:
            HASH_TYPE = "UBIGINT"
        elif n_planes == 128:
            HASH_TYPE = "UHUGEINT"

        print("Hashing vectors...")
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
                lsh {HASH_TYPE}
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

        print("Creating hash column")
        self.cursor.execute(
            f"""
                ALTER TABLE {self.schema}.{self.vector_table}
                ADD COLUMN lsh HUGEINT;
            """
        )
        self.cursor.execute(
            f"""
                CREATE INDEX lsh_index ON {self.schema}.{self.vector_table}(lsh);
            """
        )

        self.cursor.execute(f"""
                INSERT INTO {self.schema}.{self.index_vector_table}
                WITH rp AS (
                    SELECT
                        list(normal) as normals,
                        list(median) as offsets
                    FROM
                        {self.schema}.planes
                )
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
                            ) > 0)::{HASH_TYPE} << (plane_idx - 1))
                        )
                    ) AS lsh
                FROM
                    {self.schema}.{self.vector_table},
                    rp
                ;
            """
        )

        x = self.cursor.execute(
            f"""
                SELECT * FROM {self.schema}.{self.index_vector_table};
            """
        )

        # x = self.cursor.execute(f"""
        #     WITH rp AS (
        #         SELECT 42 AS x
        #     )
        #     SELECT * FROM rp;
        # """)

        # r = self.cursor.execute(
        #     "SELECT * FROM mydb.planes;"
        # )
        df = x.fetch_df()
        print(df)
        print(df['lsh'][0])
        # print(len(df['lsh'][0]))

    def lsh_vectors(self):
        pass

    def query(self, q_vector, dimensions, k, debug) -> [float, list]:
        return 0, [0, 0, 0, 0]



import os
from DataGenerator import DataGenerator
from DuckVDB import DuckVDBLib

if __name__ == "__main__":

    db_fn = f'./duckvdb.duckdb'
    CORES = 8
    DIMENSIONS = 300
    DATASET_SIZE = 210000
    METRIC = 'cosine'  # 'euclidean', 'angular'
    K = 10
    BENCH = True
    REPETITION = 5
    DEBUG = True
    INDEX = 'rplsh'

    # TODO: For some reason there is a bug when using vectors in LSH
    USE_VECTORS = False

    THRESHOLD_HASH_DISTANCE = 15

    try:
        # Setup duckdb
        duckvdb = DuckVDBLib(db_fn, METRIC, INDEX, DEBUG)
        print(duckvdb.Index.describe())

        # Setup data on duckdb
        duckvdb.set_cores(CORES)
        duckvdb.create_vector_table()
        dg = DataGenerator()
        df = dg.generate(DATASET_SIZE, DIMENSIONS)
        duckvdb.populate_vector_table(df, DIMENSIONS, USE_VECTORS)

        # Create Index
        duckvdb.create_index(DIMENSIONS, lsh_distance_threshold=THRESHOLD_HASH_DISTANCE)

        # Generate Query Vector
        QUERY_VECTOR = DataGenerator().generate_query(DIMENSIONS)

        # Query
        res = duckvdb.execute_query(QUERY_VECTOR, DIMENSIONS, K, BENCH, REPETITION)
        print(res)

    except Exception as e:
        print('\nERROR: ')
        print(e)

    finally:
        print('\nRemoving DB...')
        os.remove(db_fn)
        print('Finished')


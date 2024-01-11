
import os
from DataGenerator import DataGenerator
from DuckVDB import DuckVDB

if __name__ == "__main__":

    db_fn = f'./duckvdb.duckdb'
    CORES = 8
    DIMENSIONS = 300
    DATASET_SIZE = 210000
    METRIC = 'cosine'  # euclidean
    K = 10
    BENCH = False
    REPETITION = 1
    DEBUG = False
    INDEX = 'lsh'

    try:
        # Setup duckdb
        duckvdb = DuckVDB(db_fn, METRIC, INDEX)
        duckvdb.Index.describe()

        # Setup data on duckdb
        duckvdb.set_cores(CORES)
        duckvdb.create_vector_table()
        df = DataGenerator().generate(DATASET_SIZE, DIMENSIONS)
        duckvdb.populate_vector_table(df, DIMENSIONS)

        # Create Index
        duckvdb.create_index()

        # Generate Query Vector
        QUERY_VECTOR = DataGenerator().generate_query(DIMENSIONS)

        # Query
        res = duckvdb.execute_query(QUERY_VECTOR, DIMENSIONS, K, BENCH, REPETITION, DEBUG)
        print(res)

    except Exception as e:
        print('\nERROR: ')
        print(e)

    finally:
        print('\nRemoving DB...')
        os.remove(db_fn)
        print('Finished')


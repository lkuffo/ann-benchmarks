import pandas as pd
import numpy as np


class DataGenerator:
    def __init__(self):
        pass

    def generate(self, dataset_size, dimensions, distribution="uniform"):
        print("Generating data...")
        dataset = []
        for _ in range(dataset_size):
            sampl = np.random.uniform(low=0.5, high=13.3, size=(dimensions,))
            dataset.append(sampl)
        df = pd.DataFrame({'vector': dataset, 'id': range(len(dataset))})
        return df

    def generate_query(self, dimensions, distribution="uniform"):
        return np.random.uniform(low=0.5, high=13.3, size=(dimensions,))

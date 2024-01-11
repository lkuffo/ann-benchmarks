import pandas as pd
import numpy as np
import random


class DataGenerator:
    def __init__(self):
        pass

    def generate(self, dataset_size, dimensions, distribution="uniform"):
        print("Generating data...")
        dataset = []
        for _ in range(dimensions):
            sampl = np.random.normal(loc=0, scale=1, size=(dataset_size,))
            dataset.append(sampl)
        correct_samples = list(np.array(dataset).T)
        self.saved_smp = correct_samples[0]
        df = pd.DataFrame({'vector': correct_samples, 'id': range(dataset_size)})
        return df

    def generate_query(self, dimensions, distribution="uniform"):
        return np.random.normal(loc=0, scale=1, size=(dimensions,))

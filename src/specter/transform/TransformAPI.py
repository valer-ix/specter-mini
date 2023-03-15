import ast
import re

import numpy as np
import pandas as pd


class TransformAPI:
    def __init__(self, path_input: str, path_output: str):
        self.path_input = path_input
        self.path_output = path_output

    def clean_data(self):
        df = pd.read_csv(self.path_input)

        # Replace "- -" values with NaN
        df = df.replace('- -', np.nan)

        # Normalize Total Visits
        df['Total Visits'] = df['Total Visits'].apply(self.convert_suffix_to_num)

        # Normalize Avg Visit Duration to seconds
        df['Avg Visit Duration'] = pd.to_timedelta(df['Avg Visit Duration']).apply(lambda x: x.total_seconds())

        # Normalize Total Visits History
        df['Total Visits History'] = df['Total Visits History'].apply(ast.literal_eval)
        df['Total Visits History'] = df['Total Visits History'].apply(lambda x: {k: self.convert_suffix_to_num(v) for k, v in x.items()})

        # Normalize Age Distribution
        df['Age Distribution'] = df['Age Distribution'].apply(ast.literal_eval)
        df['Age Distribution'] = df['Age Distribution'].apply(lambda x: {k: self.replace_blank_dictval(v) for k, v in x.items()})

        df.to_csv(self.path_output)

    @staticmethod
    def convert_suffix_to_num(value):
        suffixes = {'K': 1e3, 'M': 1e6, 'B': 1e9}
        if value[-1] in suffixes:
            return int(float(value[:-1]) * suffixes[value[-1]])
        else:
            return value

    @staticmethod
    def replace_blank_dictval(value):
        if not re.search(r'\d', value):
            return np.nan
        return value


if __name__ == '__main__':
    from definitions import ROOT_DIR

    transformer = TransformAPI(f'{ROOT_DIR}/data/processed/semi/scrape_data.csv', f'{ROOT_DIR}/data/processed/full/scrape_data.csv')
    transformer.clean_data()

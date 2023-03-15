import ast

import pandas as pd
import sqlite3
import matplotlib.pyplot as plt


class AnalyseAPI:

    def __init__(self, path_db: str):
        self.path_db = path_db
        self.conn = None
        self.df = None

        self.load_df()

    def create_conn(self) -> None:
        self.conn = sqlite3.connect(self.path_db)

    def close_conn(self) -> None:
        self.conn.close()

    def load_df(self) -> None:
        self.create_conn()
        self.df = pd.read_sql_query('SELECT * FROM website_metrics', self.conn)
        self.close_conn()

    def clean_data(self) -> None:
        self.df['rank_history'] = self.df['rank_history'].apply(ast.literal_eval)
        self.df['total_visits_history'] = self.df['total_visits_history'].apply(ast.literal_eval)
        self.df['top_countries'] = self.df['top_countries'].apply(ast.literal_eval)
        self.df['age_distribution'] = self.df['age_distribution'].apply(ast.literal_eval)

    def calculate_growth_metrics(self) -> None:
        self.df['growth_months'] = [['Nov', 'Dec']] * len(self.df)
        self.df['visits_growth'] = self.df['total_visits_history'].apply(self.calculate_visit_growth)
        self.df['rank_growth'] = self.df['rank_history'].apply(self.calculate_rank_growth)
        self.df['visits_growth_avg'] = self.df['visits_growth'].apply(lambda x: sum(x) / len(x))
        self.df['rank_growth_avg'] = self.df['rank_growth'].apply(lambda x: sum(x) / len(x))

    @staticmethod
    def calculate_visit_growth(dict_visits):
        # TODO: Remove hardcoded months
        return [
            (dict_visits.get('Nov', 1) / dict_visits.get('Oct', 1) - 1) * 100,
            (dict_visits.get('Dec', 1) / dict_visits.get('Nov', 1) - 1) * 100,
        ]

    @staticmethod
    def calculate_rank_growth(dict_rank):
        # TODO: Remove hardcoded months
        return [
            dict_rank.get('Nov', 1) - dict_rank.get('Oct', 1),
            dict_rank.get('Dec', 1) - dict_rank.get('Nov', 1),
        ]

    def rank_data(self) -> None:
        self.df['visits_growth_rank'] = self.df['visits_growth_avg'].rank(ascending=False)
        self.df['rank_growth_rank'] = self.df['rank_growth_avg'].rank(ascending=False)
        self.df['rank_score'] = (self.df['visits_growth_rank'] + self.df['rank_growth_rank']) / 2
        self.df['rank_score'] = self.df['rank_score'].rank(ascending=True)

    def plot_growth_metrics(self, path_output: str) -> None:
        plt.figure(figsize=(10, 6))
        plt.subplot(2, 1, 1)
        for i, row in self.df.iterrows():
            plt.plot(row['growth_months'], row['visits_growth'], label=row['site'])
        plt.title('Month-on-month growth on web visits')
        plt.xlabel('Month')
        plt.ylabel('Growth / %')
        plt.tight_layout()
        plt.legend()
        #
        plt.subplot(2, 1, 2)
        for i, row in self.df.iterrows():
            plt.plot(row['growth_months'], row['rank_growth'], label=row['site'])
        plt.title('Month-on-month growth on rank changes')
        plt.xlabel('Month')
        plt.ylabel('Rank Change')
        plt.tight_layout()
        plt.legend()
        plt.savefig(path_output)

    def plot_ranking(self, path_output: str) -> None:
        df = self.df.sort_values('rank_score')
        plt.figure(figsize=(10, 6))
        plt.scatter(df['site'], df['rank_score'])
        plt.title('Ranking based on growth in visits and rank')
        plt.xticks(rotation=45)
        plt.xlabel('Website')
        plt.ylabel('Rank score')
        plt.tight_layout()
        plt.savefig(path_output)


if __name__ == '__main__':
    from definitions import ROOT_DIR

    analyser = AnalyseAPI(f'{ROOT_DIR}/data/db/website_metrics.db')
    analyser.clean_data()
    analyser.calculate_growth_metrics()
    analyser.rank_data()
    analyser.plot_growth_metrics(f'{ROOT_DIR}/data/plots/growth_metrics.png')
    analyser.plot_ranking(f'{ROOT_DIR}/data/plots/ranking.png')


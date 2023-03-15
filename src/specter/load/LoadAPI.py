import sqlite3
import pandas as pd


class LoadAPI:
    def __init__(self, csv_file_path, db_file_path):
        self.csv_file_path = csv_file_path
        self.db_file_path = db_file_path

    def import_csv_to_sqlite(self):
        df = pd.read_csv(self.csv_file_path,
                         dtype={'Global Rank': int, 'Total Visits': int, 'Bounce Rate': str, 'Pages Per Visit': float,
                                'Avg Visit Duration': float, 'Last Month Traffic Change': str})

        # Create connection to SQLite database
        conn = sqlite3.connect(self.db_file_path)
        cur = conn.cursor()

        # Create table in SQLite database with appropriate data types
        cur.execute('''CREATE TABLE IF NOT EXISTS website_metrics
                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     global_rank INTEGER,
                     total_visits INTEGER,
                     bounce_rate TEXT,
                     pages_per_visit REAL,
                     avg_visit_duration REAL,
                     rank_history TEXT,
                     total_visits_history TEXT,
                     last_month_traffic_change TEXT,
                     top_countries TEXT,
                     age_distribution TEXT)''')

        # Insert data from DataFrame into SQLite database
        for row in df.itertuples():
            cur.execute('''INSERT INTO website_metrics
                        (global_rank, total_visits, bounce_rate, pages_per_visit, avg_visit_duration, rank_history, 
                        total_visits_history, last_month_traffic_change, top_countries, age_distribution)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]))

        conn.commit()
        conn.close()


if __name__ == '__main__':
    from definitions import ROOT_DIR
    loader = LoadAPI(f'{ROOT_DIR}/data/processed/full/scrape_data.csv', 'path/to/db')
    loader.import_csv_to_sqlite()

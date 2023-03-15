import sqlite3
import pandas as pd


class LoadAPI:
    """Load processed CSV into SQLite."""
    def __init__(self, csv_file_path, db_file_path):
        self.csv_file_path = csv_file_path
        self.db_file_path = db_file_path

    def import_csv_to_sqlite(self, destroy_table: bool = False):
        df = pd.read_csv(self.csv_file_path,
                         dtype={'Global Rank': int, 'Total Visits': int, 'Bounce Rate': str, 'Pages Per Visit': float,
                                'Avg Visit Duration': float, 'Last Month Traffic Change': str})

        # Create connection to SQLite database
        conn = sqlite3.connect(self.db_file_path)
        curr = conn.cursor()

        # Destroy table to avoid loading duplicates during reruns
        if destroy_table:
            curr.execute('DROP TABLE website_metrics')

        # Create table in SQLite database with appropriate data types
        curr.execute('''CREATE TABLE IF NOT EXISTS website_metrics
                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     site TEXT,
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
            curr.execute('''INSERT INTO website_metrics
                        (site, global_rank, total_visits, bounce_rate, pages_per_visit, avg_visit_duration, rank_history, 
                        total_visits_history, last_month_traffic_change, top_countries, age_distribution)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12]))

        conn.commit()
        conn.close()


if __name__ == '__main__':
    from definitions import ROOT_DIR
    loader = LoadAPI(f'{ROOT_DIR}/data/processed/full/scrape_data.csv', f'{ROOT_DIR}/data/db/website_metrics.db')
    loader.import_csv_to_sqlite(destroy_table=True)

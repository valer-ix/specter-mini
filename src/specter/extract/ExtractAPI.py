import csv
from typing import List
from pathlib import Path

from bs4 import BeautifulSoup

from HTMLHandler import HTMLHandler, HTMLHandlerWeb


class Scraper:
    def __init__(self, html_targetloc, html_handler: HTMLHandler = HTMLHandlerWeb()):
        self.html_target = html_targetloc
        self.html_handler = html_handler
        self.soup = BeautifulSoup(self.html_handler.get_html(self.html_target), 'html.parser')
        self.global_rank = None

    def get_global_rank(self):
        pass


class ExtractAPI:
    def __init__(self, scrapers: List[Scraper]):
        self.scrapers = scrapers
        self.data = None

    def extract_data(self, filepath: str = 'data'):
        self.get_data()
        self.save_data(filepath=filepath)

    def get_data(self):
        self.data = [
            'Global Rank',
        ]
        for scraper in self.scrapers:
            row = [
                scraper.get_global_rank(),
            ]
            self.data.append(row)

    def save_data(self, filepath: str = 'data'):
        if not Path(filepath).exists():
            print('Path does not exist')
            return
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(self.data)


if __name__ == "__main__":
    locs = [
        'https://www.similarweb.com/website/tryspecter.com/#overview',
        'https://www.similarweb.com/website/byte-trading.com/#overview',
        'https://www.similarweb.com/website/crunchbase.com/#overview',
        'https://www.similarweb.com/website/pitchbook.com/#overview',
        'https://www.similarweb.com/website/stripe.com/#overview',
        'https://www.similarweb.com/website/google.com/#overview',
    ]
    scrapers_ = [Scraper(loc, HTMLHandlerWeb()) for loc in locs]
    extract_api = ExtractAPI(scrapers_)
    extract_api.extract_data(filepath='data/processed')

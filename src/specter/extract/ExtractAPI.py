import re
import csv
import logging
from typing import List, Optional
from pathlib import Path

from bs4 import BeautifulSoup

from src.specter.extract.HTMLHandler import HTMLHandler, HTMLHandlerLocal


log = logging.getLogger(__name__)


class Scraper:
    def __init__(self, html_targetloc, html_handler: HTMLHandler = HTMLHandlerLocal()):
        self.html_target: str = html_targetloc
        self.html_handler: HTMLHandler = html_handler
        self.soup: BeautifulSoup = BeautifulSoup(self.html_handler.get_html(self.html_target), 'html.parser')
        self.global_rank: Optional[str] = None

    def get_global_rank(self) -> Optional[str]:
        try:
            global_rank = self.soup.select_one('p.wa-rank-list__value').text.strip()
            self.global_rank = re.sub(r'\D', '', global_rank)
        except AttributeError:
            logging.warning('Global Rank not found.')
            self.global_rank = None
        return self.global_rank


class ExtractAPI:
    def __init__(self, scrapers: List[Scraper]):
        self.scrapers = scrapers
        self.data = None

    def extract_data(self, filepath: str):
        self.get_data()
        self.save_data(filepath=filepath)

    def get_data(self):
        self.data = [[
            'Global Rank',
        ]]
        for scraper in self.scrapers:
            row = [
                scraper.get_global_rank(),
            ]
            self.data.append(row)

    def save_data(self, filepath):
        if not Path(filepath).parent.exists():
            log.error('Path does not exist.')
            return
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(self.data)


if __name__ == "__main__":
    from definitions import ROOT_DIR
    # locs = [
    #     'https://www.similarweb.com/website/tryspecter.com/#overview',
    #     'https://www.similarweb.com/website/byte-trading.com/#overview',
    #     'https://www.similarweb.com/website/crunchbase.com/#overview',
    #     'https://www.similarweb.com/website/pitchbook.com/#overview',
    #     'https://www.similarweb.com/website/stripe.com/#overview',
    #     'https://www.similarweb.com/website/google.com/#overview',
    # ]
    locs = [
        f'{ROOT_DIR}/data/raw/similarweb-byte-trading-com.html',
        f'{ROOT_DIR}/data/raw/similarweb-crunchbase-com.html',
        f'{ROOT_DIR}/data/raw/similarweb-google-com.html',
        f'{ROOT_DIR}/data/raw/similarweb-pitchbook-com.html',
        f'{ROOT_DIR}/data/raw/similarweb-stripe-com.html',
    ]
    scrapers_ = [Scraper(loc, HTMLHandlerLocal()) for loc in locs]
    extract_api = ExtractAPI(scrapers_)
    extract_api.extract_data(filepath=f'{ROOT_DIR}/data/processed/scrape_data.csv')

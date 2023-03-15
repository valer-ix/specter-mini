import os.path
import re
import csv
import logging
from typing import List, Optional, Dict
from pathlib import Path

from bs4 import BeautifulSoup

from src.specter.extract.HTMLHandler import HTMLHandler, HTMLHandlerLocal


log = logging.getLogger(__name__)


class Scraper:
    """Retrieve website metrics."""
    def __init__(self, html_targetloc, html_handler: HTMLHandler = HTMLHandlerLocal()):
        self.html_target: str = html_targetloc
        self.html_handler: HTMLHandler = html_handler
        self.soup: BeautifulSoup = BeautifulSoup(self.html_handler.get_html(self.html_target), 'html.parser')
        self.global_rank: Optional[str] = None
        self.total_visits: Optional[str] = None
        self.bounce_rate: Optional[str] = None
        self.pages_per_visit: Optional[str] = None
        self.avg_visit_duration: Optional[str] = None
        self.rank_history: Optional[Dict[str, str]] = {}
        self.total_visits_history: Optional[Dict[str, str]] = {}
        self.last_month_traffic_change: Optional[str] = None
        self.top_countries: Optional[Dict[str, str]] = {}
        self.age_distribution: Optional[Dict[str, str]] = {}

    def get_global_rank(self) -> Optional[str]:
        try:
            global_rank = self.soup.select_one('p.wa-rank-list__value').text.strip()
            self.global_rank = re.sub(r'\D', '', global_rank)
        except AttributeError:
            logging.warning('Global Rank not found.')
            self.global_rank = None
        return self.global_rank

    def get_total_visits(self) -> Optional[str]:
        try:
            total_visits = self.soup.find('p', {'data-test': 'total-visits'}).find_next_sibling('p').text.strip()
            self.total_visits = re.sub(r'[<>]\s+', '', total_visits)
        except AttributeError:
            logging.warning('Total Visits not found.')
            self.total_visits = None
        return self.total_visits

    def get_bounce_rate(self) -> Optional[str]:
        try:
            self.bounce_rate = self.soup.find('p', {'data-test': 'bounce-rate'}).find_next_sibling('p').text.strip()
        except AttributeError:
            logging.warning('Bounce Rate not found.')
            self.bounce_rate = None
        return self.bounce_rate

    def get_pages_per_visit(self) -> Optional[str]:
        try:
            self.pages_per_visit = self.soup.find('p', {'data-test': 'pages-per-visit'}).find_next_sibling('p').text.strip()
        except AttributeError:
            logging.warning('Pages Per Visit not found.')
            self.pages_per_visit = None
        return self.pages_per_visit

    def get_avg_visit_duration(self) -> Optional[str]:
        try:
            self.avg_visit_duration = self.soup.find('p', {'data-test': 'avg-visit-duration'}).find_next_sibling('p').text.strip()
        except AttributeError:
            logging.warning('Avg Visit Duration not found.')
            self.avg_visit_duration = None
        return self.avg_visit_duration

    def get_rank_history(self) -> Optional[Dict[str, str]]:
        try:
            # Unable to extract 3-month rank history directly. It may be possible to use the point coordinates and the
            # y-axis in the SVG chart itself to calculate the rank, but this seems highly sub-optimal.
            raise AttributeError
        except AttributeError:
            logging.warning('Rank History not found.')
            self.rank_history = {}
        return self.rank_history

    def get_total_visits_history(self) -> Optional[Dict[str, str]]:
        try:
            total_visits_history = self.soup.select('tspan.wa-traffic__chart-data-label')
            xlabels = total_visits_history[0].find_parent('g').find_parent('g').find_next_sibling('g').find_all('text')
            xlabels = [text.text for text in xlabels]
            total_visits_history = [tspan.text for tspan in total_visits_history]
            self.total_visits_history = dict(zip(xlabels, total_visits_history))
        except IndexError:
            logging.warning('Total Visits History not found.')
            self.total_visits_history = {}
        return self.total_visits_history

    def get_last_month_traffic_change(self) -> Optional[str]:
        try:
            span_title = self.soup.select_one('span.wa-traffic__engagement-item-title:-soup-contains("Last Month Change")')
            span_content = span_title.find_next_sibling('span')
            self.last_month_traffic_change = span_content.text
            if 'data-parameter-change--down' in span_content.find('span').get('class'):
                self.last_month_traffic_change = '-' + self.last_month_traffic_change
        except AttributeError:
            logging.warning('Last Month Traffic Change not found.')
            self.last_month_traffic_change = None
        return self.last_month_traffic_change

    def get_top_countries(self) -> Optional[Dict[str, str]]:
        try:
            div_top_countries_legend = self.soup.find('div', {'class': 'wa-geography__legend wa-geography__chart-legend'})
            div_top_countries_name = div_top_countries_legend.find_all(class_='wa-geography__country-name')
            top_countries_name = [a.text for a in div_top_countries_name]
            div_top_countries_pct = div_top_countries_legend.select('span.wa-geography__country-traffic-value')
            top_countries_pct = [span.text for span in div_top_countries_pct]
            self.top_countries = dict(zip(top_countries_name, top_countries_pct))
        except AttributeError:
            logging.warning('Top Countries not found.')
            self.top_countries = {}
        return self.top_countries

    def get_age_distribution(self) -> Optional[Dict[str, str]]:
        try:
            age_values = self.soup.find_all(class_='wa-demographics__age-data-label')
            values = [tspan.text for tspan in age_values]
            xlabels = age_values[0].find_parent('g').find_parent('g').find_next_sibling('g').find_all('text')
            xlabels = [text.text for text in xlabels]
            self.age_distribution = dict(zip(xlabels, values))
        except AttributeError:
            logging.warning('Age Distribution not found.')
            self.age_distribution = {}
        return self.age_distribution


class ExtractAPI:
    """Handle multiple Scrapers."""
    def __init__(self, scrapers: List[Scraper]):
        self.scrapers = scrapers
        self.data = None

    def extract_data(self, filepath: str) -> None:
        self.get_data()
        self.save_data(filepath=filepath)

    def get_data(self) -> None:
        self.data = [[
            'Site',
            'Global Rank',
            'Total Visits',
            'Bounce Rate',
            'Pages Per Visit',
            'Avg Visit Duration',
            'Rank History',
            'Total Visits History',
            'Last Month Traffic Change',
            'Top Countries',
            'Age Distribution',
        ]]
        for scraper in self.scrapers:
            row = [
                os.path.basename(scraper.html_target),
                scraper.get_global_rank(),
                scraper.get_total_visits(),
                scraper.get_bounce_rate(),
                scraper.get_pages_per_visit(),
                scraper.get_avg_visit_duration(),
                scraper.get_rank_history(),
                scraper.get_total_visits_history(),
                scraper.get_last_month_traffic_change(),
                scraper.get_top_countries(),
                scraper.get_age_distribution(),
            ]
            self.data.append(row)

    def save_data(self, filepath: str) -> None:
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
    extract_api.extract_data(filepath=f'{ROOT_DIR}/data/processed/semi/scrape_data.csv')

from definitions import ROOT_DIR
from src.specter.extract.HTMLHandler import HTMLHandlerLocal
from src.specter.extract.ExtractAPI import Scraper, ExtractAPI
from src.specter.transform.TransformAPI import TransformAPI
from src.specter.load.LoadAPI import LoadAPI
from src.specter.analyse.AnalyseAPI import AnalyseAPI


# Extract #
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

# Transform #
transformer = TransformAPI(f'{ROOT_DIR}/data/processed/semi/scrape_data.csv', f'{ROOT_DIR}/data/processed/full/scrape_data.csv')
transformer.clean_data()

# Load #
loader = LoadAPI(f'{ROOT_DIR}/data/processed/full/scrape_data.csv', f'{ROOT_DIR}/data/db/website_metrics.db')
loader.import_csv_to_sqlite(destroy_table=True)

# Analyze #
analyser = AnalyseAPI(f'{ROOT_DIR}/data/db/website_metrics.db')
analyser.clean_data()
analyser.calculate_growth_metrics()
analyser.rank_data()
analyser.plot_growth_metrics(f'{ROOT_DIR}/data/plots/growth_metrics.png')
analyser.plot_ranking(f'{ROOT_DIR}/data/plots/ranking.png')

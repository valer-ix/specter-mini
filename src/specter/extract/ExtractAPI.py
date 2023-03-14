from bs4 import BeautifulSoup

from HTMLHandler import HTMLHandler


class ExtractAPI:
    def __init__(self, html_targetloc, html_handler: HTMLHandler):
        self.html_target = html_targetloc
        self.html_handler = html_handler
        self.soup = BeautifulSoup(self.html_handler.get_html(self.html_target), 'html.parser')
        self.global_rank = None

    def get_global_rank(self):
        pass

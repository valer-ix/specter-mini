import requests
from abc import ABC, abstractmethod


class HTMLHandler(ABC):
    @abstractmethod
    def get_html(self, url):
        pass


class HTMLHandlerWeb(HTMLHandler):
    def get_html(self, url):
        return requests.get(url).content


class HTMLHandlerLocal(HTMLHandler):
    def get_html(self, filepath):
        with open(filepath, 'r') as f:
            return f.read()

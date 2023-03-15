import requests
from abc import ABC, abstractmethod


class HTMLHandler(ABC):
    @abstractmethod
    def get_html(self, url: str):
        pass


class HTMLHandlerWeb(HTMLHandler):
    def get_html(self, url: str):
        return requests.get(url).content


class HTMLHandlerLocal(HTMLHandler):
    def get_html(self, filepath: str):
        with open(filepath, 'r') as f:
            return f.read()

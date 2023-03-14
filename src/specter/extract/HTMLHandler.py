import requests
from abc import ABC, abstractmethod


class HTMLSource(ABC):
    @abstractmethod
    def get_html(self, url):
        pass


class HTMLSourceWeb(HTMLSource):
    def get_html(self, url):
        return requests.get(url).content


class HTMLSourceLocal(HTMLSource):
    def get_html(self, filepath):
        with open(filepath, 'r') as f:
            return f.read()

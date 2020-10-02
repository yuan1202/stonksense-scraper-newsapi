from setuptools import setup, find_packages

setup(
    name         = 'stonksense_scraper_newsapi',
    version      = '0.11',
    packages     = find_packages(),
    entry_points = {'scrapy': ['settings = stonksense_scraper_newsapi.settings']},
)
import os
from datetime import datetime

import scrapy
from newsapi import NewsApiClient

    
class newsapi_spider(scrapy.Spider):
    name = 'newsapi_spider'
    start_urls = ['http://quotes.toscrape.com/page/1/']

    def __init__(self, query, from_date, to_date, page_size=100, api_key='30329c8f74c440f2a7fb7ada24fcfc47', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = NewsApiClient(api_key=api_key)
        self.query = query
        self.from_date = from_date
        self.to_date = to_date
        self.page_size = page_size
    
    def parse(self, response):
        
        try:
            # request data from newsapi
            articles = self.api.get_everything(
                q=self.query,
                from_param=self.from_date,
                to=self.to_date,
                language='en',
                sort_by='relevancy',
                page_size=self.page_size,
                page=1,
            )['articles']
        except Exception as e:
            print(e)
            yield {}

        for _, n in enumerate(articles):

            yield {
                'source_id': n['source']['id'],
                'source_name': n['source']['name'],
                'author': n['author'],
                'title': n['title'],
                'description': n['description'],
                'url_news': n['url'],
                'url_img': n['urlToImage'],
                'datetime': datetime.strptime(n['publishedAt'], '%Y-%m-%dT%H:%M:%SZ'),
                'content': None,
                'flag': self.query,
            }
    


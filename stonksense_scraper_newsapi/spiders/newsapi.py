import os
from datetime import datetime

import scrapy
from newsapi import NewsApiClient

    
class newsapi_spider(scrapy.Spider):
    name = 'newsapi_spider'
    start_urls = ['http://quotes.toscrape.com/page/1/']

    def __init__(self, query, from_date, to_date, page_size=100, *args, **kwargs):
        
        super().__init__(*args, **kwargs)
        self.query = query
        self.from_date = from_date
        self.to_date = to_date
        self.page_size = page_size
    
    def parse(self, response):
        
        try:
            # setup newsapi
            api = NewsApiClient(api_key=self.settings.get('NEWSAPI_KEY'))
            # request data from newsapi
            articles = api.get_everything(
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
    


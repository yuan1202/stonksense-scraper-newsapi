import psycopg2
from . import settings
import datetime


class newsapi(object):
    def __init__(self, host, port, database, user, password):
        self.connection = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        self.cursor = self.connection.cursor()
    
    def process_item(self, item, spider):
        
        self.cursor.execute(
            '''
            insert into news (source_id, source_name, author, title, description, url_news, url_img, datetime, content, flag) \
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            ''',
            (
                item['source_id'],
                item['source_name'],
                item['author'],
                item['title'],
                item['description'],
                item['url_news'],
                item['url_img'],
                item['datetime'],
                item['content'],
                item['flag'],
            )
        )
        self.connection.commit()
            
        return item

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get('HOST'),
            port=crawler.settings.get('PORT'),
            database=crawler.settings.get('DATABASE'),
            user=crawler.settings.get('USER'),
            password=crawler.settings.get('PASSWORD'),
        )

    def close_spider(self, spider):
        self.connection.close()
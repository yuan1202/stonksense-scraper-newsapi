import psycopg2
import datetime


class newsapi(object):
    def __init__(self, host, port, database, user, password):
        self.connection = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        self.cursor = self.connection.cursor()
    
    def process_item(self, item, spider):
        
        # use try catch to catch any error and roll back database
        try:
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
        except Exception as e:
            spider.logger.error(e)
            spider.logger.info('Rolling back database.')
            self.connection.rollback()
            
        return item

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get('DB_HOST'),
            port=crawler.settings.get('DB_PORT'),
            database=crawler.settings.get('DB_DATABASE'),
            user=crawler.settings.get('DB_USER'),
            password=crawler.settings.get('DB_PASSWORD'),
        )

    def close_spider(self, spider):
        self.connection.close()
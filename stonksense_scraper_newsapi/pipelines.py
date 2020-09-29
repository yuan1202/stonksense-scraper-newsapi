# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import psycopg2
from fxst import settings
import datetime

class FxstPipeline(object):
    def __init__(self):
        self.connect = mysql.connector.connect(\
                            host = settings.mysql_host,\
                            database = settings.mysql_db,\
                            user = settings.mysql_user,\
                            password = settings.mysql_pw)
        self.cursor = self.connect.cursor()
    
    def process_item(self, item, spider):
        #try:
            # test if data exist
        dat_test = (\
                #datetime.datetime.strptime(item['time'], '%Y-%m-%d %H:%M:%S'),\
                item['time'].split(' ')[0] + '%',\
                item['currency'],\
                item['country'],\
                item['title'])
        #print(item['time'].split(' ')[0] + '%')
        query_test = "SELECT date_time, currency, title, volatility, actual, actual_unit, consensus, consensus_unit, previous, previous_unit, comment\
                        FROM CALENDAR_EVENTS \
                        where date_time like %s and currency = %s and country = %s and title = %s "
        self.cursor.execute(query_test, dat_test)
        test_result = self.cursor.fetchall()
        if len(test_result) == 0:
            # write to database
            dat_add = (\
                    item['time'],\
                    item['currency'],\
                    item['country'],\
                    item['title'],\
                    item['volatility'],\
                    item['actual'],\
                    item['actual_unit'],\
                    item['consensus'],\
                    item['consensus_unit'],\
                    item['previous'],\
                    item['previous_unit'])
            query_add = (\
                "insert into CALENDAR_EVENTS "\
                "(date_time, currency, country, title, volatility, actual, actual_unit, consensus, consensus_unit, previous, previous_unit) "\
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            self.cursor.execute(query_add, dat_add)
            self.connect.commit()
        elif len(test_result) == 1:
            dat_updt = []
            cmt = test_result[0][-1]
            if cmt == None:
                cmt = ''
            if any([item['time'] != test_result[0][0].strftime('%Y-%m-%d %H:%M:%S'),\
                    item['volatility'] != test_result[0][3],\
                    float(item['actual']) != test_result[0][4],\
                    item['actual_unit'] != test_result[0][5],\
                    float(item['consensus']) != test_result[0][6],\
                    item['consensus_unit'] != test_result[0][7],\
                    float(item['previous']) != test_result[0][8],\
                    item['previous_unit'] != test_result[0][9]]):
                print("===================== update duplicate entries ... =====================")
                cmt = cmt + 'volatility:' + test_result[0][3] + ','\
                          + 'actual:' + str(test_result[0][4]) + ','\
                          + 'actual_unit:' + test_result[0][5] + ','\
                          + 'consensus:' + str(test_result[0][6]) + ','\
                          + 'consensus_unit:' + test_result[0][7] + ','\
                          + 'previous:' + str(test_result[0][8]) + ','\
                          + 'previous_unit:' + test_result[0][9] + '||'
                #print(cmt)
                dat_updt = (\
                        item['time'],\
                        item['volatility'],\
                        item['actual'],\
                        item['actual_unit'],\
                        item['consensus'],\
                        item['consensus_unit'],\
                        item['previous'],\
                        item['previous_unit'],\
                        cmt,\
                        item['time'].split(' ')[0] + '%',\
                        item['currency'],\
                        item['country'],\
                        item['title'])
                
                query_updt = "UPDATE CALENDAR_EVENTS SET \
                              date_time = %s, volatility = %s, actual = %s, actual_unit = %s, consensus = %s, consensus_unit = %s, previous = %s, previous_unit = %s, comment = %s \
                              where date_time like %s and currency = %s and country = %s and title = %s "
                self.cursor.execute(query_updt, dat_updt)
                self.connect.commit()
        else:
            for r in test_result:
                print(r)
            
        return item

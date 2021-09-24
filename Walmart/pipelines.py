# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd
from datetime import datetime
import logging
# import sqlite3
# needed for postgresql
import psycopg2
from sqlalchemy import create_engine
import csv


class PostgresPipeline(object):
    def open_spider(self, spider):
        print('********************************************************************** POSTGRES PIPELINE')
        # self.engine = create_engine('postgresql+psycopg2://postgres:Amazonfba20@104.154.134.98:5432/cyber_analytics')
        self.engine = create_engine('postgresql+psycopg2://postgres:Amazonfba20@localhost:5433/cyber_analytics')
    
    def close_spider(self, spider):
        self.engine.close()

    def process_item(self, item, spider):
        print('********************************************************************** POSTGRES CONVERT DICT TO DF')
        
        append_dict = [{
            'dt_nm': item['dt_nm'], 'scrape_time': str(item['scrape_time']),
            'url_nbr': item['url_nbr'], 'title': item['title'],
            'upc_str': item['upc_str'], 'upc': item['upc'], 'price': item['price'],
            'min_price': item['min_price'], 'max_price': item['max_price'],
            'max_page': item['max_page'], 'page': item['page'], 'page_item_nbr': item['page_item_nbr'],
            'level_1': item['level_1'], 'level_2': item['level_2'], 'level_3': item['level_3'],
            'department': item['department'], 'product_category': item['product_category'],
            'product_sub_category': item['product_sub_category'], 'seller_name': item['seller_name'], 'walmart_id': item['walmart_id'],
            'ip_address': item['ip_address'], 'user_agent': item['user_agent'], 'url': item['url']
            }]

        print('********************************************************************** POSTGRES APPEND DICT LOADED')
        append_df = pd.DataFrame.from_dict(append_dict)
        print('********************************************************************** POSTGRES APPEND DICT TO DATAFRAME')
        # print(append_df['scrape_time'])
        append_df.to_sql('walmart_catalog_insert', con=self.engine, if_exists='append', index=False)

        print('********************************************************************** DATA INSERTED INTO POSTGRES TABLE')
        return item


class WalmartPipeline:
    def open_spider(self, spider):
        logging.warning("SPIDER OPENED FROM PIPELINE")
    
    def close_spider(self, spider):
        logging.warning("SPIDER CLOSED FROM PIPELINE")

    def process_item(self, item, spider):
        print('********************************************************************** WALMART PIPELINE')
        append_dict = {}
        print('********************************************************************** APPEND DICT EMPTIED')
        print(item)
        append_dict = [{
            'dt_nm': item['dt_nm'], 'scrape_time': item['scrape_time'], 'url_nbr': item['url_nbr'], 'title': item['title'],
            'upc_str': item['upc_str'], 'upc': item['upc'], 'price': item['price'],
            'min_price': item['min_price'], 'max_price': item['max_price'],
            'max_page': item['max_page'], 'page': item['page'], 'page_item_nbr': item['page_item_nbr'],
            'level_1': item['level_1'], 'level_2': item['level_2'], 'level_3': item['level_3'],
            'department': item['department'], 'product_category': item['product_category'],
            'product_sub_category': item['product_sub_category'], 'seller_name': item['seller_name'], 'walmart_id': item['walmart_id'],
            'ip_address': item['ip_address'], 'user_agent': item['user_agent'], 'url': item['url']
            }]
        print('********************************************************************** APPEND DICT LOADED')
        append_df = pd.DataFrame.from_dict(append_dict)
        print('********************************************************************** APPEND DICT TO DATAFRAME')
        
        # # write to only one file - this will overwrite each iteration
        # product_catalog.to_csv('20201121 Walmart - Product Catalog.csv', index=False)

        # # append to the file
        with open('Product Catalogs/20210123 Walmart Catalog - Test.csv', 'a') as f:
            append_df.to_csv(f, sep=',', header=False, index=False)
            print('********************************************************************** CSV UPLOADED')
        return 1
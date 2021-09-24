import scrapy
import json
from scrapy.shell import inspect_response
from scrapy.exceptions import CloseSpider
import pandas as pd
from datetime import datetime
from math import ceil
import time



class WalmartSpiderSpider(scrapy.Spider):
    name = 'walmart_spider'
    allowed_domains = ['www.walmart.com']
    # start_urls = ['https://www.walmart.com/browse/office-supplies/calendars-and-planners/1229749_8412191/']


    def start_requests(self):
        print('START_REQUESTS: GIVE THE URLS THAT WE ARE SCRAPING')
        # make sure there's level_1, level_2, level_3 included
        url_df = pd.read_csv('URLs/20210205 Walmart Exercise and Fitness URLs.csv', index_col=False)
        
        url_sleep = 0
        for url_nbr in range(0,97): #CHANGE
            # wait 20 minutes every "url_sleep"
            url_sleep = url_sleep + 1
            if url_sleep == 20:
                time.sleep(60*13.4)
                url_sleep = 0
            else:
                print(url_nbr)


            max_page = int(url_df.iloc[url_nbr, 3])

            for page in range(1, max_page + 1):
                if url_nbr == -1 and page <= 5:   #CHANGE
                    # skip these records
                    print('Skip page ' + str(page))
                else:
                    url_beg = str(url_df.iloc[url_nbr, 2])
                    # url_end = str(url_df.iloc[url_nbr, 3]).replace('nan', '')
                    url = url_beg + str(page) #+ url_end
                    print('\n********************************************************************** BEFORE REQUEST IN START_REQUESTS FOR: ' + url)
                    req = scrapy.Request(url=url, callback=self.parse, meta={
                        'page': page,
                        'max_page': max_page,
                        'level_1': url_df.iloc[url_nbr, 4],
                        'level_2': url_df.iloc[url_nbr, 5],
                        'level_3': url_df.iloc[url_nbr, 6],
                        'url_nbr': url_nbr
                        })
                    print('\n********************************************************************** AFTER REQUEST IN START_REQUESTS FOR: ' + url)
                    print(req)
                    print()
                    yield req
            

    def parse(self, response):
        print('\n********************************************************************** RESPONSE IN START_REQUESTS: ')
        product_catalog = str(response.xpath("//script[@id='searchContent']/text()").get())
        print('FIRST PART OF JSON: ' + product_catalog[0:100])
        # inspect_response(response, self)
        url = response.url
        user_agent = str(response.request.headers['User-Agent'])
        ip_address = ''

        # check if the url was blocked before extracting data, if blocked only return data that we have
        if 'blocked?url' in response.url:
            print('********************************************************************** BLOCKED URL')
            append_data = {}
            now = datetime.now()
            yield   {
                'url_nbr': response.request.meta['url_nbr'], 'dt_nm': now.strftime("%Y-%m-%d"), 'scrape_time': now.strftime("%H:%M:%S"),
                'title': 'Blocked', 'upc_str': None, 'upc': None,
                'price': None, 'min_price': None, 'max_price': None,
                'max_page': response.request.meta['max_page'], 'page': response.request.meta['page'], 'page_item_nbr': None,
                'level_1': response.request.meta['level_1'], 'level_2': response.request.meta['level_2'],
                'level_3': response.request.meta['level_3'], 'department': None, 'product_category': None, 
                'product_sub_category': None, 'seller_name': None, 'walmart_id': None, 'ip_address': ip_address,
                'user_agent': user_agent, 'url': url
            }          
        # if not product_catalog:
        #     print('********************************************************************** EMPTY PAGE: not product_catalog')
        #     print(response.url)
        #     print('**********************************************************************')
        # elif product_catalog == 'None':
        #     print('********************************************************************** EMPTY PAGE: product_catalog = None')
        #     print(response.url)
        #     print('**********************************************************************')
        else:
            print('********************************************************************** NON-EMPTY PAGE, SEND TO JSON_TO_DICT')
            product_catalog_dict = self.json_to_dict(product_catalog)
            print('********************************************************************** DICT RETURNED TO PARSE')

            walmart_df = self.walmart_json_decode(product_catalog_dict)
            print('********************************************************************** WALMART DICT RETURNED TO PARSE')

            for row in range(0, len(walmart_df)):
                walmart_df_row = walmart_df.iloc[row]
                yield {
                    'url_nbr': response.request.meta['url_nbr'], 'dt_nm': walmart_df_row['dt_nm'], 'scrape_time': walmart_df_row['scrape_time'],
                    'title': walmart_df_row['title'], 'upc_str': walmart_df_row['upc_str'], 'upc': walmart_df_row['upc'],
                    'price': walmart_df_row['price'], 'min_price': walmart_df_row['min_price'], 'max_price': walmart_df_row['max_price'],
                    'max_page': response.request.meta['max_page'], 'page': response.request.meta['page'], 'page_item_nbr': walmart_df_row['page_item_nbr'],
                    'level_1': response.request.meta['level_1'], 'level_2': response.request.meta['level_2'],
                    'level_3': response.request.meta['level_3'], 'department': walmart_df_row['department'],
                    'product_category': walmart_df_row['product_category'], 'product_sub_category': walmart_df_row['product_sub_category'],
                    'seller_name': walmart_df_row['seller_name'], 'walmart_id': walmart_df_row['walmart_id'],
                    'ip_address': ip_address, 'user_agent': user_agent, 'url': url
                }


    # CONVERT JSON AS STRING TO PYTHON DICTIONARY
    def json_to_dict(self, json_str):
        # json_str = json_str[51:]
        # json_str = json_str[0:(len(json_str) - 9)]
        print('Front of javascript (after edit): ' + str(json_str[0:100]))
        print('End of javascript (after edit): ' + str(json_str[(len(json_str) - 20):(len(json_str))]))

        # write the string to a json file using json package
        json_file = open("json_to_dict.json", "w")
        json_file.write(json_str)
        json_file.close()
        print('********************************************************************** JSON WRITTEN')

        # create a dictionary for the json using the json package
        with open("json_to_dict.json") as f:
            json_data = json.load(f)
        print('********************************************************************** JSON LOADED')

        return json_data

    
    # TRANSLATE WALMART JSON INTO PRODUCT CATALOG PYTHON DICTIONARY
    def walmart_json_decode(self, walmart_json):
        # product_catalog_dict = walmart_json['product_catalog_dict']
        # go to product catalogs
        preso_items = walmart_json['searchContent']['preso']['items']

        product_catalog = pd.DataFrame()

        for i in range(0, len(preso_items)):
            product = preso_items[i]

            # store product title and UPC
            title = None
            if 'title' in product.keys():
                title = product['title']

            walmart_id = None
            if 'usItemId' in product.keys():
                walmart_id = product['usItemId']

            department = None
            if 'department' in product.keys():
                department = product['department']
            
            product_category = None
            if 'productCategory' in product.keys():
                product_category = product['productCategory']

            product_sub_category = None
            if 'seeAllName' in product.keys():
                product_sub_category = product['seeAllName']

            seller_name = None
            if 'sellerName' in product.keys():
                seller_name = product['sellerName']

            # remove leading 0s
            # upc_int = None
            upc_str = ''
            if 'standardUpc' in product.keys():
                upc_str = str(product['standardUpc'])
                upc = upc_str.strip("[").strip("]")
                upc = str(upc.strip("'").strip("'"))
                upc = str(upc.lstrip("0"))
                upc = str(upc)

            # extract price and price range
            price = None
            min_price = None
            max_price = None
            if 'primaryOffer' in product.keys():
                primaryOffer = product['primaryOffer']
            
                # calculate price - if there's a range, use the lower bound
                if 'offerPrice' in primaryOffer:
                    price = primaryOffer['offerPrice']
                if 'minPrice' in primaryOffer:
                    min_price = primaryOffer['minPrice']
                if 'maxPrice' in primaryOffer:
                    max_price = primaryOffer['maxPrice']

                # if no price is listed, use the smaller price
                if price is None:
                    price = min_price
            

            append_data = {}
            now = datetime.now()
            append_data = [{
                'dt_nm': now.strftime("%Y-%m-%d"), 'scrape_time': now.strftime("%H:%M:%S"),
                'department': department, 'product_category': product_category,
                'product_sub_category': product_sub_category, 'seller_name': seller_name,
                'page_item_nbr': i, 'title': title, 'upc_str': upc_str, 'upc': upc, 'price': price,
                'min_price': min_price, 'max_price': max_price, 'walmart_id': walmart_id,
                }]
            product_catalog = product_catalog.append(append_data, ignore_index=True)

        print('********************************************************************** WALMART DATAFRAME LOADED')
        return product_catalog
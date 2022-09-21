
import os
from shutil import which
import logging
import scrapy
from scrapy_selenium import SeleniumRequest

#from scrapy.loader import ItemLoader # https://docs.scrapy.org/en/latest/topics/loaders.html

from pathlib import Path


from selenium import webdriver # https://stackoverflow.com/questions/17975471/selenium-with-scrapy-for-dynamic-page
#from selenium.webdriver.remote.remote_connection import LOGGER
#LOGGER.setLevel(logging.ERROR)

from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Callable, List
import typing

from items import List_item, Page_item

from scrapy.exporters import CsvItemExporter, JsonItemExporter

#a = JsonItemExporter()


#!def set_level_loggers(log_level):
#!        
#!    for logger in [logging.getLogger(name) for name in logging.root.manager.loggerDict]:
#!        logger.setLevel(log_level)

import pandas as pd
from util import *
class AmazonSpider_Base(scrapy.Spider):
    
    def __init__(self, amazon_start_urls: list, max_page_number: int, *args, **kwargs):
        self.start_urls = amazon_start_urls
        self.max_page = max_page_number - 1 if (max_page_number > 0) else float('inf')
        
        self.__df = None
        self._init_df()
        self.page_counter = 0

        #self.start_urls = amazon_start_urls
        
        
        #logging.getLogger('scrapy.spidermiddlewares.httperror').setLevel(logging.ERROR)
        #logging.getLogger('scrapy').setLevel(logging.ERROR)
        #logging.getLogger('scrapy').propagate = False
    
        super().__init__(*args, **kwargs)
    
        #logging.getLogger(self.name).setLevel(logging.ERROR)
    
    def _init_df(self):
        raise NotImplementedError("Must be overridded!")
        
    def start_requests(self):
        
        for url in self.start_urls:
            #yield scrapy.Request(url = url,
            #                     callback = self.parse,
            #                     cookies = None, # None to prevent bans
            #                     dont_filter = True) #This is used when you want to perform an identical request multiple times, to ignore the duplicates filter


            #@ Using selenium for getting dynamic content
            yield SeleniumRequest(url = url,
                                  callback = self.parse,
                                  cookies = None,
                                  dont_filter = True)

        
    def save_page(self, response, file_name):
        with open(f"{file_name}.html", "w") as f:
            f.write(str(response.body, encoding = "utf-8"))
    
        

    def parse(self, response):
        if self.page_counter > self.max_page:
            return None
                
        #@ div_items = response.xpath('//div[@class="sg-col-4-of-12 s-result-item s-asin sg-col-4-of-16 sg-col s-widget-spacing-small sg-col-4-of-20"]').getall()
        div_items = response.xpath('//div[contains(@class, "s-result-item") and contains(@class, "s-asin")]')
        
        yield from self.continue_parsing(div_items)
    
        
        # Following next page of items
        if ( next_page_url := response.xpath("//a[contains(@class, 's-pagination-next')]/@href").get() ):
            self.page_counter += 1
            next_page_url = get_abs_url(next_page_url, "https://www.amazon.com.br")
            
            yield SeleniumRequest(url = next_page_url,
                                  callback = self.parse,
                                  cookies = None,
                                  dont_filter = True)
            
    def continue_parsing(self, div_items: scrapy.Selector):
        raise NotImplementedError("Must be overridded!")
    
    def get_df(self):
        return self.__df
    
    @property
    def df(self):
        return self.__df
    
    def save_df(self, filename = "../extracted_data/data"):
        filename = filename + ".csv" if (".csv" not in filename) else filename
        filepath = Path(filename)
        filepath.parent.mkdir(parents = True, exist_ok = True)
        
        self.__df.to_csv(filepath)
        

# def check_data(data):
#     if data:
#         return data
#     return pd.NA


class AmazonSpider_pp(AmazonSpider_Base):
    """
    Spider for data extraction from each product page.
    """
    def __init__(self, *args, **kwargs):
        super(AmazonSpider_Base, self).__init__(*args, **kwargs)
    
    def _init_df(self):
        self.__df = pd.DataFrame({"description": [],
                                  "rating": [],
                                  "votes": [],
                                  "price": [],
                                  "availability": []})
    
    def continue_parsing(self, div_items: scrapy.Selector):

        for item in div_items:
            page_url = item.xpath(".//a[contains(@class, 'a-link-normal')]/@href").get()
            #@page_url2 = item.xpath("./descendant::a[contains(@class, 'a-link-normal')]/@href").getall()
        
            page_url = get_abs_url(page_url, "https://www.amazon.com.br")
            
            yield SeleniumRequest(url = page_url,
                                  callback = self.parse_product_page,
                                  cookies = None,
                                  dont_filter = True)
        
        
    def parse_product_page(self, response):
        
        
        #description = str(response.xpath('//span[@id="productTitle"]/text()').get())
        #rate = float(str(response.xpath('//span[@id="acrPopover"]/@title').get()).split(" out of ")[0])
        #votes_number = float(str(response.xpath('//span[@id="acrCustomerReviewText"]/text()').get()).split(' ')[0].replace(",", '.'))
        #price = float(str(response.xpath('//span[@id="price_inside_buybox"]/text()').get()).replace("$", ""))
#
        #availability = str(response.xpath('//div[@id="availability"]/span/text()')).strip().lower()
        #availability = "in stock" in availability
        return Page_item(url = response.url,
                         description = response.xpath('//span[@id="productTitle"]/text()').get(),
                         rate = response.xpath('//span[@id="acrPopover"]/@title').get(),
                         votes_number = response.xpath('//span[@id="acrCustomerReviewText"]/text()').get(),
                         price = response.xpath('//span[@id="price_inside_buybox"]/text()').get(),
                         availability = response.xpath('//div[@id="availability"]/span/text()').get())()
        
        return Page_item(description,
                         rate,
                         votes_number,
                         price,
                         availability)()
        
        
        
        

class AmazonSpider_pl(AmazonSpider_Base):
    """
    Spider for data extraction directly from list items page.
    """
    def __init__(self, *args, **kwargs):
        #?self.start_urls = amazon_start_urls
        super(AmazonSpider_pl, self).__init__(*args, **kwargs)
        
    def _init_df(self):
        self.__df = pd.DataFrame({"description": [],
                                  "rating": [],
                                  "votes": [],
                                  "price": []})
        
    def continue_parsing(self, div_items: scrapy.Selector) -> None:
        
        for item in div_items:
            yield self.extract_data_from_plist(item)
            #?self.__df = self.__df.append(self.extract_data_from_plist(item), ignore_index = True)     
            #?yields = self.extract_data_from_plist(item)
        
        return None
    
    def extract_data_from_plist(self, selector: scrapy.Selector) -> pd.DataFrame:
        """
        Extracts data available from products list, without opening each product page.
        
        Gets description, rating, number of votes and price
        """
        
        rate_and_votes = selector.xpath(".//div[@class='a-row a-size-small']/span/@aria-label").getall()
        rate_and_votes = rate_and_votes[:2] if len(rate_and_votes) > 1 else [None]*2
        
        
        return List_item(get_abs_url(selector.xpath(".//a[contains(@class, 'a-link-normal')]/@href").get(), "https://www.amazon.com.br"),
                         selector.xpath(".//span[contains(@class, 'a-size-base-plus')]/text()").get(),
                         *rate_and_votes,
                         selector.xpath(".//span[@class='a-price']/span[@class='a-offscreen']/text()").get())()

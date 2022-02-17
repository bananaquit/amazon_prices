
import os
from shutil import which
import logging
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy_selenium import SeleniumRequest

#from scrapy.loader import ItemLoader # https://docs.scrapy.org/en/latest/topics/loaders.html

from pathlib import Path


from selenium import webdriver # https://stackoverflow.com/questions/17975471/selenium-with-scrapy-for-dynamic-page
#from selenium.webdriver.remote.remote_connection import LOGGER
#LOGGER.setLevel(logging.ERROR)

from pprint import pprint
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

def get_abs_urls(rel_urls: list, domain = "https://www.amazon.com"):
    check_list = list(map(lambda url: domain in url, rel_urls))
    
    if not all(check_list):
        return list(map(lambda url: os.path.join(domain, url), rel_urls))
    elif any(check_list):
        raise ValueError("All urls must be relative or all must be absolute.")
    
    scrapy.Spider.logger.info("All urls are already absolute.")
    #logging.info("All urls are already absolute.")
    
    return rel_urls

def get_abs_url(rel_url: str, domain: str = "https://www.amazon.com"):
    if rel_url.startswith('/') ^ domain.endswith('/'):
        return domain + rel_url
    
    return os.path.join(domain, rel_url)



class AmazonSpider_Base(scrapy.Spider):
    
    def __init__(self, *args, **kwargs):
        
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
                
        #div_items = response.css("div.s-result-item").getall()
        #print(len(div_items))
        #@ div_items = response.xpath('//div[@class="sg-col-4-of-12 s-result-item s-asin sg-col-4-of-16 sg-col s-widget-spacing-small sg-col-4-of-20"]').getall()
        div_items = response.xpath('//div[contains(@class, "s-result-item") and contains(@class, "s-asin")]')
        
        yield from self.continue_parsing(div_items)
    
        """
        pprint(div_items)
        df = pd.DataFrame({"description": [],
                             "rating": [],
                             "votes": [],
                             "price": []})
        
        for item in div_items:
            df = df.append(self.extract_data_from_list(item), ignore_index = True)
            continue
        
        
        
        
        
        
            page_url = item.xpath(".//a[contains(@class, 'a-link-normal')]/@href").get()
            #@page_url2 = item.xpath("./descendant::a[contains(@class, 'a-link-normal')]/@href").getall()
        
            page_url = get_abs_url(page_url)
            
            yield SeleniumRequest(url = page_url,
                                  callback = self.parse_item,
                                  cookies = None,
                                  dont_filter = True)
        
        print(df)
        """
    
        if self.page_counter > 3:
            return None
        # Following next page of items
        if ( (next_page_url := response.xpath("//a[contains(@class, 's-pagination-next')]/@href").get()) is not None ):
            self.page_counter += 1
            next_page_url = get_abs_url(next_page_url)
            
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
    def __init__(self, amazon_start_urls, *args, **kwargs):
        self.custom_lambdas
        super(AmazonSpider_Base, self).__init__(amazon_start_urls, *args, **kwargs)
    
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
        
            page_url = get_abs_url(page_url)
            
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
        return Page_item(description = response.xpath('//span[@id="productTitle"]/text()').get(),
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
    def __init__(self, amazon_start_urls, *args, **kwargs):
        self.start_urls = amazon_start_urls
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
        
        #print(self.__df)
        
        return None
    
    def extract_data_from_plist(self, selector: scrapy.Selector) -> pd.DataFrame:
        """
        Extracts data available from products list, without opening each product page.
        
        Gets description, rating, votes (number of) and price
        """
        
        return List_item(selector.xpath(".//span[contains(@class, 'a-size-base-plus')]/text( )").get(),
                         *selector.xpath(".//div[@class='a-row a-size-small']/span/@aria-label").getall()[:2],
                         selector.xpath(".//span[@class='a-price']/span[@class='a-offscreen']/text()").get())()
        
        if ((description := selector.xpath(".//span[contains(@class, 'a-size-base-plus')]/text( )").get()) == None):
            description = pd.NA
            
        try:
            rating_str, n_votes = selector.xpath(".//div[@class='a-row a-size-small']/span/@aria-label").getall()
        except:
            rating, n_votes = pd.NA, pd.NA
        else:
            #! rating_str = rating_str.replace(" out of ","/")
            rating = float(rating_str.split(" out of ")[0])
            n_votes = float(n_votes.replace(",", "."))
        
        if ((price := selector.xpath(".//span[@class='a-price']/span[@class='a-offscreen']/text()").get()) != None):
            price = float(price.replace("$", ""))
        else:
            price = pd.NA
        
            
        #print(description, rating, n_votes, price)
        return pd.DataFrame({"description": [description],
                             "rating": [rating],
                             "votes": [n_votes],
                             "price": [price]})
        
        #return {"description": description,
        #        "rating": rating,
        #        "votes": n_votes,
        #        "price": price}
            
def main():

    settings_selenium = {"FEEDS": {"items.json": {"format": "json"}}, # https://docs.scrapy.org/en/latest/topics/feed-exports.html
                         "SELENIUM_DRIVER_NAME": "firefox",
                         "SELENIUM_DRIVER_EXECUTABLE_PATH": which("geckodriver"),
                         "SELENIUM_DRIVER_ARGUMENTS": ['-headless'], # '--headless' if using chrome instead of firefox
                         "DOWNLOADER_MIDDLEWARES": {"scrapy_selenium.SeleniumMiddleware": 800},
                         "LOG_LEVEL": "ERROR"}
        
    process = CrawlerProcess(settings = settings_selenium)

    process.crawl(AmazonSpider_pl,
                  ["https://www.amazon.com/s?rh=n%3A16225007011&fs=true&ref=lp_16225007011_sar"],
                  "amazon_crawl")
    #process.crawl(spider2)
    #...
    
    process.start()

if __name__ == "__main__":
    main()

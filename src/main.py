from shutil import which
from scrapy.crawler import CrawlerProcess
from spiders import AmazonSpider_pl, AmazonSpider_pp



def main(*args, **kwargs):

    settings_selenium = {"FEEDS": {"items.csv": {"format": "csv"}}, # https://docs.scrapy.org/en/latest/topics/feed-exports.html
                         "SELENIUM_DRIVER_NAME": "firefox",
                         "SELENIUM_DRIVER_EXECUTABLE_PATH": which("geckodriver"),
                         "SELENIUM_DRIVER_ARGUMENTS": ['-headless'], # '--headless' if using chrome instead of firefox
                         "DOWNLOADER_MIDDLEWARES": {"scrapy_selenium.SeleniumMiddleware": 800},
                         "LOG_LEVEL": "ERROR"}
        
    process =   (settings = settings_selenium)
    
    #list_page_urls = ["https://www.amazon.com/s?i=specialty-aps&bbn=16225005011&rh=n%3A%2116225005011%2Cn%3A166777011&ref=nav_em__nav_desktop_sa_intl_feeding_0_2_10_9"]
    list_page_urls = ["https://www.amazon.com.br/s?k=panela+de+pressao+7%2C5+litros&dc&__mk_pt_BR=%C3%85M%C3%85%C5%BD%C3%95%C3%91&crid=16R5EOON4P9KC&qid=1654006500&rnid=19171727011&sprefix=panela+de+pressao+7+5+litro%2Caps%2C200&ref=sr_pg_1"]

    process.crawl(AmazonSpider_pl,
                  list_page_urls, # links
                  2, # max page to extract data
                  "amazon_crawl") # craw name
    #process.crawl(spider2)
    #...
    
    process.start()
    

if __name__ == "__main__":
    main()

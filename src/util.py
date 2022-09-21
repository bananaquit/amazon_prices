import os
import scrapy

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
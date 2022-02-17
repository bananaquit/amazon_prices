from selenium import webdriver

driver = webdriver.Firefox()

driver.get("https://www.amazon.com/Samsung-970-EVO-Plus-MZ-V7S2T0B/dp/B07MFZXR1B/ref=sr_1_29?qid=1643059516&s=computers-intl-ship&sr=1-29&th=1")

print(driver.find_element_by_xpath('//span[@id="productTitle"]/text()'))

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time
import mysql.connector
import json
import random
import re
import database
import text_similarity
import image_similarity
import image_similarity2
import logging

logging.basicConfig(
    filename='crawler.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

options = Options()
options.add_argument("--disable-blink-features=AutomationControlled")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)
driver.maximize_window()

def write_to_file(crawled_items):
    with open('output.json', 'a', encoding='utf-8') as file:
        json_data = [json.dumps(item, ensure_ascii=False) for item in crawled_items]
        file.write('\n'.join(json_data) + '\n')
    logging.info('saved')

def random_delay(time_start, time_end):
    delay = random.uniform(time_start, time_end)
    time.sleep(delay)

def save_to_db(crawled_item):
    connection = database.get_connection()

    cursor = connection.cursor()

    cursor.execute("""
            CREATE TABLE IF NOT EXISTS crawled_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            item_number VARCHAR(255),
            seller_name VARCHAR(255),
            seller_id VARCHAR(255),
            title VARCHAR(255),
            description TEXT,
            category VARCHAR(255),
            price VARCHAR(255),
            images JSON,
            downloaded TINYINT(1) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    insert_query = """
        INSERT INTO crawled_items (item_number, seller_name, seller_id, title, description, category, price, images)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    images_json = json.dumps(crawled_item['images'], ensure_ascii=False)
    data = (
        crawled_item['item_number'],
        crawled_item['seller_name'],
        crawled_item['seller_id'],
        crawled_item['title'],
        crawled_item['description'],
        crawled_item['category'],
        crawled_item['price'],
        images_json
    )

    cursor.execute(insert_query, data)
    connection.commit()

    cursor.close()
    
def get_listing_links(driver, num_items=10):
    time.sleep(3)
    result_count = 0
    hrefs = []

    while result_count < num_items:
        listing_divs = driver.find_elements(By.XPATH, "//div[starts-with(@data-testid, 'listing-card-')]")
        
        for div in listing_divs:
            try:
                #find a in div
                a_tags = div.find_elements(By.TAG_NAME, "a")
                
                for a in a_tags:
                    href = a.get_attribute("href")
                    if href and "/p/" in href:
                        hrefs.append(href)
                        result_count += 1
                        break  #get 1 link
                        
                if result_count >= num_items:
                    break

            except NoSuchElementException:
                continue

        #to load more content
        driver.execute_script("window.scrollBy(0, 1000)")
        time.sleep(2)

    logging.info(f"Collected {len(hrefs)} links.")
    return hrefs

def get_meta_number(driver):
    try:
        meta_tag = driver.find_element(By.XPATH, '//meta[@name="branch:deeplink:$deeplink_path"]')
        
        content_value = meta_tag.get_attribute('content')
        match = re.search(r'/p/(\d+)', content_value)
        if match:
            number = match.group(1)
            return number
        else:
            logging.info("No number found")
            return None

    except Exception as e:
        logging.info("Error found when get meta number")
        return None

def crawl_listing_page(driver, url):
    driver.get(url)
    time.sleep(3)

    try:
        #wait for load
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//div[@data-testid='new-listing-details-page-desktop-div-seller-contact-header']"))
        )
        number = get_meta_number(driver)
        #get seller content
        seller_container = driver.find_element(By.XPATH, "//div[@data-testid='new-listing-details-page-desktop-div-seller-contact-header']")
        
        #get the user id and name 
        a_tags = seller_container.find_elements(By.TAG_NAME, "a")
        if len(a_tags) >= 2:
            name_id_spans = a_tags[1].find_elements(By.TAG_NAME, "span")
            if len(name_id_spans) >= 2:
                sellername = name_id_spans[0].text
                sellerid = name_id_spans[1].text
            else:
                sellername = "N/A"
                sellerid = "N/A"
        else:
            sellername = "N/A"
            sellerid = "N/A"
            
    except Exception as e:
        logging.info(f"⚠️ 发生错误: {e}")
        sellername = "N/A"
        sellerid = "N/A"

    try:
        title = driver.find_element(By.XPATH, "//h1[@data-testid='new-listing-details-page-desktop-text-title']").text
    except NoSuchElementException:
        title = ""

    try:
        description = driver.find_element(By.XPATH, "//div[@id='FieldSetField-Container-field_description']//p").text
    except NoSuchElementException:
        description = ""

    try:
        price = driver.find_element(By.XPATH, "//div[@id='FieldSetField-Container-field_price']//h3").text
    except NoSuchElementException:
        price = ""

    try:
        category = driver.find_element(By.XPATH, "//a[starts-with(@href, '/categories/')]/span").text        
    except NoSuchElementException:
        category = ""

    try:
        image_elements = driver.find_elements(By.XPATH, '//div[@id="FieldSetField-Container-field_photo_viewer"]//button//img')
    except NoSuchElementException:
        image_elements = "N/A"

    image_urls = []
    for img in image_elements:
        src = img.get_attribute('src')
        if src: 
            image_urls.append(src)
    
    logging.info(f"Item Number: {number}")
    logging.info(f"Title: {title}")
    logging.info(f"Price: {price}")
    logging.info(f"Description: {description}")
    logging.info(f"Category: {category}")
    logging.info(f"Seller Name: {sellername}")
    logging.info(f"Seller ID: {sellerid}")
    
    item = {
        'item_number': number,
        'seller_name': sellername,
        'seller_id': sellerid,
        'title': title,
        'description': description,
        'category': category,
        'price': price,
        'images': image_urls
    }
    
    save_to_db(item)
    
    return item

def main():
    logging.info('Crawler 1...')

    driver.get("https://www.carousell.com.my/") 
    time.sleep(3)

    #find item link
    num_items_to_crawl = 10 #max item count
    listing_links = get_listing_links(driver, num_items=num_items_to_crawl)

    crawled_items = []
    logging.info('Crawler running...')

    for link in listing_links:
        logging.info(f'Crawling {link}')
        item = crawl_listing_page(driver, link)
        crawled_items.append(item)
        random_delay(1, 3)
    logging.info('Text similarity running...')
    text_similarity.main()
    logging.info('Image similarity running...')
    image_similarity.main()
    logging.info('Image similarity2 running...')
    image_similarity2.main()
    logging.info('Done')
    driver.quit()

if __name__ == "__main__":
    main()


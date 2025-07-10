from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pandas as pd
import time
import csv
import requests
from bs4 import BeautifulSoup
import re
import os
from concurrent.futures import ThreadPoolExecutor
import threading
from itertools import product
import multiprocessing as mp
from functools import partial
import google.generativeai as genai

# Config Gemini API
genai.configure(api_key="AIzaSyBK4Jep3kc--b0DDBIT8nWu2AZQIyK4as8")

# ===============================
# CẤU HÌNH SONG SONG VÀ EMAIL
# ===============================
MAX_WORKERS_SEARCH = 4
MAX_WORKERS_EMAIL = 8
BATCH_SIZE = 50
EMAIL_REGEX = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
TAGS_TO_EXCLUDE = ['input', 'form', 'textarea', 'button']
INVALID_EXTENSIONS = ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg', '.webp', '.pdf', '.doc', '.docx',
                      '.xls', '.xlsx', '.ppt', '.pptx', '.zip', '.rar', '.tar', '.gz', '.mp3', '.mp4',
                      '.avi', '.mov', '.wav', '.css', '.js', '.html', '.xml', '.json', '.exe', '.msi',
                      '.dmg', '.deb', '.txt', '.log', '.tmp']
INVALID_DOMAINS = ['example.com', 'test.com', 'localhost', 'domain.com', 'yoursite.com', 'website.com',
                   'company.com', 'email.com', 'sentry.io', 'facebook.com', 'twitter.com', 'instagram.com',
                   'linkedin.com', 'youtube.com', 'google.com', 'microsoft.com']
INVALID_PATTERNS = [r'.*@.*\.(png|jpg|jpeg|gif|bmp|svg|webp|pdf|doc|docx|css|js)$', r'.*@\d+\.\d+\.\d+\.\d+$',
                    r'^[^@]*@[^@]*@.*$', r'.*@.*\.$', r'.*@\.$', r'.*@.*\.\w{1}$', r'.*@.*\.\w{5,}$']
output_lock = threading.Lock()
progress_lock = threading.Lock()

def generate_places_and_provinces(country, max_places=10, max_provinces=20):
    prompt = f"""
List exactly {max_places} common types of investors or investment-related institutions 
(e.g. venture capital firms, angel investors, private equity firms, etc.)
and exactly {max_provinces} real provinces or cities in {country}. 
Return as 2 Python lists in valid code format:
PLACES = [...]
PROVINCES = [...]
"""
    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt)
    raw = response.text.strip()
    cleaned_code = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("```") )
    local_vars = {}
    try:
        exec(cleaned_code, {}, local_vars)
        return local_vars.get("PLACES", []), local_vars.get("PROVINCES", [])
    except:
        return [], []

def is_valid_email(email):
    email = email.lower().strip()
    if not email or '@' not in email or email.count('@') != 1:
        return False
    try:
        local_part, domain = email.split('@')
        if not local_part or not domain or '.' not in domain:
            return False
        if any(email.endswith(ext) for ext in INVALID_EXTENSIONS):
            return False
        if domain in INVALID_DOMAINS:
            return False
        for pattern in INVALID_PATTERNS:
            if re.match(pattern, email):
                return False
        domain_parts = domain.split('.')
        if len(domain_parts) < 2:
            return False
        last_part = domain_parts[-1]
        if not (2 <= len(last_part) <= 4) or not last_part.isalpha():
            return False
        invalid_chars = ['<', '>', '"', "'", '\\', '/', '?', '#', '%', '&', '=']
        if any(char in email for char in invalid_chars):
            return False
        return True
    except:
        return False

def driver_setup():
    options = webdriver.EdgeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    service = Service()
    driver = webdriver.Edge(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.get('https://www.google.com/maps')
    return driver

def tear_down(driver):
    try:
        driver.quit()
    except:
        pass

def scroll_results(driver, pause_time=1, max_scrolls=20):
    try:
        scrollable_div = driver.find_element(By.XPATH, '//div[@role="feed"]')
        for _ in range(max_scrolls):
            driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
            time.sleep(pause_time)
    except Exception as e:
        with progress_lock:
            print(f"⚠️ Scroll error: {e}")

def extract_websites_for_keyword(place, province, thread_id):
    keyword = f"{place} {province}"
    driver = None
    try:
        driver = driver_setup()
        driver.implicitly_wait(3)
        text_box = driver.find_element(By.ID, "searchboxinput")
        text_box.clear()
        text_box.send_keys(keyword)
        text_box.send_keys(Keys.ENTER)
        time.sleep(3)
        scroll_results(driver)
        selectors = ['a.lcr4fd.S9kvJb', 'a[data-value="Website"]', 'a[href^="http"]:not([href*="google"])']
        urls = set()
        for selector in selectors:
            try:
                for link in driver.find_elements(By.CSS_SELECTOR, selector):
                    href = link.get_attribute('href')
                    if href and href.startswith("http") and 'google' not in href:
                        urls.add(href.strip())
            except:
                continue
        return {'keyword': keyword, 'place': place, 'province': province, 'urls': list(urls)}
    except Exception as e:
        print(f"❌ Error in {keyword}: {e}")
        return {'keyword': keyword, 'place': place, 'province': province, 'urls': []}
    finally:
        if driver:
            tear_down(driver)

def extract_all_investor_websites():
    search_combinations = list(product(PLACES, PROVINCES))
    print(f"🔍 Total search: {len(search_combinations)}")
    all_urls = set()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_SEARCH) as executor:
        futures = [executor.submit(extract_websites_for_keyword, place, province, i) for i, (place, province) in enumerate(search_combinations)]
        for future in futures:
            try:
                result = future.result()
                all_urls.update(result['urls'])
            except:
                continue
    return list(all_urls)

def extract_emails_from_url_single(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return set()
        soup = BeautifulSoup(r.text, 'lxml')
        for tag in TAGS_TO_EXCLUDE:
            [e.decompose() for e in soup.find_all(tag)]
        combined = soup.get_text(separator=' ') + ' ' + str(soup)
        raw_emails = re.findall(EMAIL_REGEX, combined)
        return {e.lower() for e in raw_emails if is_valid_email(e)}
    except:
        return set()

def extract_emails_from_websites_parallel(urls):
    url_batches = [urls[i:i + BATCH_SIZE] for i in range(0, len(urls), BATCH_SIZE)]
    all_results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_EMAIL) as executor:
        futures = [executor.submit(lambda b: {u: extract_emails_from_url_single(u) for u in b}, batch) for batch in url_batches]
        for future in futures:
            try:
                all_results.update(future.result())
            except:
                continue
    return all_results

def extract_investor_name_from_url(url):
    try:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lower()
        parts = domain.split('.')
        name = parts[1] if parts[0] == 'www' else parts[0]
        name = name.replace('-', ' ').replace('_', ' ')
        name = ' '.join(word.capitalize() for word in name.split())
        if not any(k in name.lower() for k in ['capital', 'ventures', 'partners']):
            name += " Capital"
        return name
    except:
        return "Unknown Investor"

def save_emails_to_csv_parallel(results, filename="investor_emails_result.csv"):
    with output_lock:
        with open(filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Email", "Tên nhà đầu tư", "Location"])
            for url, emails in results.items():
                name = extract_investor_name_from_url(url)
                for email in sorted(emails):
                    writer.writerow([email, name])
    print(f"✅ Saved to {filename}")

def main():
    global PLACES, PROVINCES
    PLACES, PROVINCES = generate_places_and_provinces("Germany")
    print("\n🚀 INVESTOR EMAIL CRAWLER")
    urls = extract_all_investor_websites()
    print(f"🌐 Found {len(urls)} unique URLs")
    emails = extract_emails_from_websites_parallel(urls)
    print(f"📧 Found emails from {len(emails)} websites")
    save_emails_to_csv_parallel(emails)

if __name__ == "__main__":
    mp.freeze_support()
    main()
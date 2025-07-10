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
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import threading
from itertools import product
import multiprocessing as mp
from functools import partial
import google.generativeai as genai


genai.configure(api_key="AIzaSyBK4Jep3kc--b0DDBIT8nWu2AZQIyK4as8")
# ===============================
# CẤU HÌNH TÌM KIẾM SONG SONG
# ===============================
# PLACES = [
#     'hospital',
#     'clinic',
#     'medical center',
#     'healthcare',
#     'health center',
#     'urgent care'
# ]

# PROVINCES = [
#     'Arzberg (Bavaria)',
#     'Aschaffenburg (Bavaria)',
#     'Aschersleben (Saxony-Anhalt)',
#     'Asperg (Baden-Württemberg)',
#     'Aßlar (Hesse)',
#     'Attendorn (North Rhine-Westphalia)',
#     'Aub (Bavaria)',
#     'Aue-Bad Schlema (Saxony)'
# ]


def generate_places_and_provinces(country, max_places=10, max_provinces=20):
    prompt = f"""
List exactly {max_places} common types of medical facilities (like hospitals, clinics) and exactly {max_provinces} real provinces or cities in {country}. 
Return as 2 Python lists in valid code format:
PLACES = [...]
PROVINCES = [...]
"""

    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt)

    # Xử lý kết quả trả về từ Gemini
    raw = response.text.strip()

    # Bỏ markdown ``` nếu có
    cleaned_code = "\n".join(
        line for line in raw.splitlines() if not line.strip().startswith("```")
    )

    # Debug nếu cần
    print("📥 Kết quả từ Gemini:")
    print(cleaned_code)

    # Thực thi code để lấy 2 biến PLACES và PROVINCES
    local_vars = {}
    try:
        exec(cleaned_code, {}, local_vars)
        places = local_vars.get("PLACES", [])
        provinces = local_vars.get("PROVINCES", [])

        # Kiểm tra kết quả có đúng định dạng list không
        if not isinstance(places, list) or not isinstance(provinces, list):
            raise ValueError("PLACES hoặc PROVINCES không phải là danh sách")

        return places, provinces

    except Exception as e:
        print("❌ Lỗi khi thực thi kết quả từ Gemini:")
        print(e)
        return [], []


# Cấu hình song song hóa
MAX_WORKERS_SEARCH = 4  # Số luồng cho tìm kiếm Google Maps
MAX_WORKERS_EMAIL = 8   # Số luồng cho crawl email
BATCH_SIZE = 50         # Số URL xử lý trong 1 batch

# Regex và cấu hình email
EMAIL_REGEX = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
TAGS_TO_EXCLUDE = ['input', 'form', 'textarea', 'button']

# Danh sách các extension file cần loại bỏ
INVALID_EXTENSIONS = [
    '.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg', '.webp',  # Ảnh
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',  # Documents
    '.zip', '.rar', '.tar', '.gz',  # Archives
    '.mp3', '.mp4', '.avi', '.mov', '.wav',  # Media
    '.css', '.js', '.html', '.xml', '.json',  # Web files
    '.exe', '.msi', '.dmg', '.deb',  # Executables
    '.txt', '.log', '.tmp'  # Text files
]

# Danh sách các domain không hợp lệ
INVALID_DOMAINS = [
    'example.com', 'test.com', 'localhost', 'domain.com',
    'yoursite.com', 'website.com', 'company.com', 'email.com',
    'sentry.io', 'facebook.com', 'twitter.com', 'instagram.com',
    'linkedin.com', 'youtube.com', 'google.com', 'microsoft.com',
    'sentry.wixpress.com','sentry-next.wixpress.com'
]

# Các pattern email không hợp lệ
INVALID_PATTERNS = [
    r'.*@.*\.(png|jpg|jpeg|gif|bmp|svg|webp|pdf|doc|docx|css|js)$',
    r'.*@\d+\.\d+\.\d+\.\d+$',  # IP address
    r'^[^@]*@[^@]*@.*$',  # Nhiều hơn 1 ký tự @
    r'.*@.*\.$',  # Kết thúc bằng dấu chấm
    r'.*@\.$',  # @ theo sau bởi dấu chấm
    r'.*@.*\.\w{1}$',  # Domain chỉ có 1 ký tự
    r'.*@.*\.\w{5,}$',  # Domain extension quá dài (>4 ký tự)
]

# Lock cho thread-safe operations
output_lock = threading.Lock()
progress_lock = threading.Lock()

def is_valid_email(email):
    """Kiểm tra email có hợp lệ không"""
    email = email.lower().strip()
    
    # Kiểm tra format cơ bản
    if not email or '@' not in email or email.count('@') != 1:
        return False
    
    try:
        local_part, domain = email.split('@')
        
        # Kiểm tra local part
        if not local_part or len(local_part) < 1:
            return False
            
        # Kiểm tra domain
        if not domain or '.' not in domain:
            return False
            
        # Kiểm tra extension file
        if any(email.endswith(ext) for ext in INVALID_EXTENSIONS):
            return False
            
        # Kiểm tra domain không hợp lệ
        if domain in INVALID_DOMAINS:
            return False
            
        # Kiểm tra pattern không hợp lệ
        for pattern in INVALID_PATTERNS:
            if re.match(pattern, email):
                return False
                
        # Kiểm tra domain có ít nhất 1 dấu chấm và không bắt đầu/kết thúc bằng dấu chấm
        if domain.startswith('.') or domain.endswith('.') or '..' in domain:
            return False
            
        # Kiểm tra domain extension hợp lệ (2-4 ký tự)
        domain_parts = domain.split('.')
        if len(domain_parts) < 2:
            return False
            
        last_part = domain_parts[-1]
        if not (2 <= len(last_part) <= 4) or not last_part.isalpha():
            return False
            
        # Kiểm tra không chứa ký tự đặc biệt không hợp lệ
        invalid_chars = ['<', '>', '"', "'", '\\', '/', '?', '#', '%', '&', '=']
        if any(char in email for char in invalid_chars):
            return False
            
        return True
        
    except Exception:
        return False

def driver_setup():
    """Khởi tạo Edge WebDriver với cấu hình tối ưu"""
    options = webdriver.EdgeOptions()
    options.add_argument('--start-maximized')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-images')  # Tắt tải ảnh để tăng tốc
    options.add_argument('--disable-javascript')  # Tắt JS không cần thiết
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--headless=new')  # Chạy trình duyệt không giao diện

    service = Service()
    driver = webdriver.Edge(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.get('https://www.google.com/maps')
    return driver

def tear_down(driver):
    """Đóng WebDriver"""
    try:
        driver.quit()
    except:
        pass

def scroll_results(driver, pause_time=1, max_scrolls=20):
    """Cuộn để tải thêm kết quả trên Google Maps - tối ưu hóa"""
    scrollable_div_xpath = '//div[@role="feed"]'
    
    try:
        scrollable_div = driver.find_element(By.XPATH, scrollable_div_xpath)
        
        for i in range(max_scrolls):
            driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
            time.sleep(pause_time)
                
    except Exception as e:
        with progress_lock:
            print(f"⚠️ Lỗi khi cuộn: {e}")

def extract_websites_for_keyword(place, province, thread_id):
    """Lấy danh sách website cho một keyword cụ thể"""
    keyword = f"{place} {province}"
    thread_name = f"Thread-{thread_id}"
    
    with progress_lock:
        print(f"🔍 [{thread_name}] Đang tìm kiếm: {keyword}")
    
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

        # Tìm các thẻ chứa link website
        selectors = [
            'a.lcr4fd.S9kvJb',
            'a[data-value="Website"]',
            'a[href^="http"]:not([href*="google"])',
        ]
        
        urls = set()
        
        for selector in selectors:
            try:
                website_links = driver.find_elements(By.CSS_SELECTOR, selector)
                
                for link in website_links:
                    href = link.get_attribute('href')
                    if href and href.startswith("http") and 'google' not in href.lower():
                        urls.add(href.strip())
                        
            except Exception:
                continue

        with progress_lock:
            print(f"✅ [{thread_name}] {keyword}: {len(urls)} websites")
                        
        return {
            'keyword': keyword,
            'place': place,
            'province': province,
            'urls': list(urls),
            'count': len(urls)
        }
        
    except Exception as e:
        with progress_lock:
            print(f"❌ [{thread_name}] Lỗi với {keyword}: {e}")
        return {
            'keyword': keyword,
            'place': place,
            'province': province,
            'urls': [],
            'count': 0
        }
        
    finally:
        if driver:
            tear_down(driver)

def extract_all_hospital_websites():
    """Lấy tất cả website bệnh viện song song"""
    print("=" * 80)
    print("🏥 BƯỚC 1: LẤY WEBSITE BỆNH VIỆN SONG SONG TỪ GOOGLE MAPS")
    print("=" * 80)
    
    # Tạo tất cả combinations của place và province
    search_combinations = list(product(PLACES, PROVINCES))
    total_searches = len(search_combinations)
    
    print(f"📊 Tổng số tìm kiếm: {total_searches}")
    print(f"🔧 Số luồng song song: {MAX_WORKERS_SEARCH}")
    print(f"📍 Places: {len(PLACES)} | Provinces: {len(PROVINCES)}")
    
    all_results = []
    all_urls = set()
    
    # Sử dụng ThreadPoolExecutor để chạy song song
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_SEARCH) as executor:
        # Submit tất cả tasks
        future_to_combo = {
            executor.submit(extract_websites_for_keyword, place, province, i): (place, province)
            for i, (place, province) in enumerate(search_combinations, 1)
        }
        
        # Thu thập kết quả
        for future in future_to_combo:
            try:
                result = future.result(timeout=60)  # Timeout 60s cho mỗi tìm kiếm
                all_results.append(result)
                all_urls.update(result['urls'])
                
            except Exception as e:
                place, province = future_to_combo[future]
                with progress_lock:
                    print(f"❌ Timeout/Error cho {place} {province}: {e}")

    print(f"\n📊 THỐNG KÊ BƯỚC 1:")
    print(f"   • Tổng số tìm kiếm: {len(all_results)}")
    print(f"   • Tổng số URLs unique: {len(all_urls)}")
    
    return list(all_urls), all_results

def extract_hospital_name_from_url(url):
    """Trích xuất tên bệnh viện từ URL hoặc nội dung website"""
    try:
        # Thử lấy từ domain name trước
        from urllib.parse import urlparse
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        
        # Loại bỏ www. và subdomain
        domain_parts = domain.split('.')
        if len(domain_parts) >= 2:
            # Lấy phần chính của domain (bỏ www, subdomain)
            if domain_parts[0] == 'www':
                main_domain = domain_parts[1]
            else:
                main_domain = domain_parts[0]
            
            # Làm sạch và format tên
            hospital_name = main_domain.replace('-', ' ').replace('_', ' ')
            hospital_name = ' '.join(word.capitalize() for word in hospital_name.split())
            
            # Thêm từ khóa y tế nếu chưa có
            medical_keywords = ['hospital', 'medical', 'health', 'clinic', 'care']
            has_medical_keyword = any(keyword in hospital_name.lower() for keyword in medical_keywords)
            
            if not has_medical_keyword:
                hospital_name += " Medical Center"
            
            return hospital_name
    except:
        pass
    
    return "Unknown Hospital"

def extract_emails_from_url_batch(url_batch):
    """Trích xuất email từ một batch URLs"""
    batch_results = {}
    
    for url in url_batch:
        try:
            emails = extract_emails_from_url_single(url)
            if emails:
                batch_results[url] = emails
                
        except Exception as e:
            with progress_lock:
                print(f"⚠️ Lỗi với {url}: {e}")
            
        time.sleep(0.5)  # Giảm delay giữa requests
    
    return batch_results

def extract_emails_from_url_single(url):
    """Trích xuất email từ một URL đơn"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return set()
            
        soup = BeautifulSoup(response.text, 'lxml')

        # Loại bỏ các thẻ không cần thiết
        for tag in TAGS_TO_EXCLUDE:
            for element in soup.find_all(tag):
                element.decompose()

        # Tìm email trong text và HTML
        text_content = soup.get_text(separator=' ')
        html_content = str(soup)
        combined = text_content + ' ' + html_content
        
        # Tìm tất cả email matches
        raw_emails = re.findall(EMAIL_REGEX, combined)
        
        # Lọc email hợp lệ
        valid_emails = set()
        
        for email in raw_emails:
            if is_valid_email(email):
                valid_emails.add(email.lower())
        
        return valid_emails
        
    except Exception:
        return set()

def extract_emails_from_websites_parallel(urls):
    """Trích xuất email từ danh sách website song song"""
    print("\n" + "=" * 80)
    print("📧 BƯỚC 2: TRÍCH XUẤT EMAIL SONG SONG TỪ CÁC WEBSITE")
    print("=" * 80)
    
    if not urls:
        print("❌ Không có URL nào để crawl email!")
        return {}

    total_urls = len(urls)
    print(f"📊 Tổng số URLs cần crawl: {total_urls}")
    print(f"🔧 Số luồng song song: {MAX_WORKERS_EMAIL}")
    print(f"📦 Batch size: {BATCH_SIZE}")

    # Chia URLs thành batches
    url_batches = [urls[i:i + BATCH_SIZE] for i in range(0, len(urls), BATCH_SIZE)]
    
    all_results = {}
    processed_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_EMAIL) as executor:
        # Submit các batches
        future_to_batch = {
            executor.submit(extract_emails_from_url_batch, batch): batch
            for batch in url_batches
        }
        
        for future in future_to_batch:
            try:
                batch_results = future.result(timeout=120)  # Timeout 2 phút cho mỗi batch
                all_results.update(batch_results)
                
                batch = future_to_batch[future]
                processed_count += len(batch)
                
                with progress_lock:
                    print(f"✅ Processed {processed_count}/{total_urls} URLs. Found emails in {len(batch_results)} URLs.")
                    
            except Exception as e:
                batch = future_to_batch[future]
                processed_count += len(batch)
                with progress_lock:
                    print(f"❌ Batch failed: {e}")

    # Tính toán kết quả
    total_valid_emails = sum(len(emails) for emails in all_results.values())
    failed_count = total_urls - len(all_results)

    print(f"\n✅ HOÀN THÀNH BƯỚC 2:")
    print(f"   • URLs thành công: {len(all_results)}")
    print(f"   • URLs thất bại: {failed_count}")
    print(f"   • Tổng emails tìm được: {total_valid_emails}")

    return all_results

def save_emails_to_csv_parallel(results, filename="hospital_emails_result_parallel.csv"):
    """Lưu kết quả email vào file CSV với định dạng mới - thread-safe"""
    total_emails = sum(len(emails) for emails in results.values())
    print(f"\n📊 Tổng số emails để lưu: {total_emails}")

    if total_emails == 0:
        print(f"❌ Không có email nào để lưu")
        return

    with output_lock:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            # Header mới theo yêu cầu: Email, Tên bệnh viện, Location
            writer.writerow(["Email", "Tên bệnh viện", "Location"])

            for url, emails in results.items():
                # Trích xuất tên bệnh viện từ URL
                hospital_name = extract_hospital_name_from_url(url)
                
                for email in sorted(emails):
                    writer.writerow([email, hospital_name])

        print(f"✅ Đã lưu kết quả vào: {filename}")

def display_results_parallel(results):
    """Hiển thị kết quả cuối cùng với thống kê chi tiết"""
    print(f"\n" + "=" * 80)
    print(f"📊 KẾT QUẢ CUỐI CÙNG - SONG SONG HÓA")
    print("=" * 80)
    
    if not results:
        print("❌ KHÔNG TÌM ĐƯỢC EMAIL HỢP LỆ NÀO!")
        return
    
    total_valid_emails = sum(len(emails) for emails in results.values())
    unique_emails = set()
    
    for emails in results.values():
        unique_emails.update(emails)
    
    print(f"📊 Websites có email: {len(results)}")
    print(f"📊 Tổng số emails: {total_valid_emails}")
    print(f"📊 Emails duy nhất: {len(unique_emails)}")
    
    # Thống kê theo domain
    domains = {}
    for email in unique_emails:
        if '@' in email:
            domain = email.split('@')[1]
            domains[domain] = domains.get(domain, 0) + 1
    
    if domains:
        print(f"\n🔝 TOP 10 DOMAINS:")
        for domain, count in sorted(domains.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"   • {domain}: {count} emails")
    
    # Thống kê theo tỉnh (nếu có thể xác định từ domain)
    provinces_found = {}
    for email in unique_emails:
        if '@' in email:
            domain = email.split('@')[1].lower()
            for province in PROVINCES:
                if province.lower() in domain:
                    provinces_found[province] = provinces_found.get(province, 0) + 1
                    break
    
    if provinces_found:
        print(f"\n🗺️ EMAILS THEO TỈNH (từ domain):")
        for province, count in sorted(provinces_found.items(), key=lambda x: x[1], reverse=True):
            print(f"   • {province}: {count} emails")

def main():
    """Hàm chính chạy toàn bộ quy trình song song"""
    start_time = time.time()
    
    # Thêm dòng sau vào đầu hàm main()
    global PLACES, PROVINCES
    PLACES, PROVINCES = generate_places_and_provinces("Russian")

    print("=" * 80)
    print("🚀 HOSPITAL EMAIL CRAWLER - SONG SONG HÓA HOÀN TOÀN")
    print("=" * 80)
    print(f"🔧 Cấu hình:")
    print(f"   • Search threads: {MAX_WORKERS_SEARCH}")
    print(f"   • Email crawl threads: {MAX_WORKERS_EMAIL}")
    print(f"   • Batch size: {BATCH_SIZE}")
    print(f"   • Places: {len(PLACES)}")
    print(f"   • Provinces: {len(PROVINCES)}")
    print(f"   • Total combinations: {len(PLACES) * len(PROVINCES)}")
    
    try:
        # Bước 1: Lấy website bệnh viện song song
        urls, search_results = extract_all_hospital_websites()
        
        if not urls:
            print("❌ Không tìm được website nào. Dừng chương trình.")
            return
        
        # Bước 2: Trích xuất email song song
        email_results = extract_emails_from_websites_parallel(urls)
        
        # Bước 3: Hiển thị và lưu kết quả - CHỈ LUU 1 FILE DUY NHẤT
        display_results_parallel(email_results)
        save_emails_to_csv_parallel(email_results)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n🎉 HOÀN THÀNH!")
        print(f"⏱️ Tổng thời gian: {total_time:.2f} giây ({total_time/60:.2f} phút)")
        print(f"📁 Kiểm tra file kết quả:")
        print(f"   • hospital_emails_result_parallel.csv - Kết quả emails với định dạng mới")
        
    except KeyboardInterrupt:
        print(f"\n⚠️ Chương trình bị dừng bởi người dùng!")
    except Exception as e:
        print(f"\n❌ Lỗi không xác định: {e}")

if __name__ == "__main__":
    # Thiết lập cho multiprocessing trên Windows
    mp.freeze_support()
    main()
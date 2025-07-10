#!/usr/bin/env python3
"""
Setup script for Hospital Email Crawler
Tự động cài đặt tất cả dependencies và Edge WebDriver
"""

import subprocess
import sys
import os
import platform
from pathlib import Path

def run_command(command, description):
    """Chạy command và hiển thị kết quả"""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, 
                              capture_output=True, text=True)
        print(f"✅ {description} thành công!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} thất bại: {e}")
        print(f"Error output: {e.stderr}")
        return False

def check_python_version():
    """Kiểm tra phiên bản Python"""
    print("🐍 Kiểm tra phiên bản Python...")
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ Cần Python 3.8 trở lên!")
        print(f"Phiên bản hiện tại: {version.major}.{version.minor}.{version.micro}")
        return False
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} - OK")
    return True

def install_requirements():
    """Cài đặt các package từ requirements.txt"""
    requirements_file = Path("requirements.txt")
    
    if not requirements_file.exists():
        print("❌ Không tìm thấy file requirements.txt!")
        return False
    
    print("📦 Cài đặt Python packages...")
    
    # Upgrade pip trước
    if not run_command(f"{sys.executable} -m pip install --upgrade pip", 
                      "Nâng cấp pip"):
        return False
    
    # Cài đặt requirements
    if not run_command(f"{sys.executable} -m pip install -r requirements.txt", 
                      "Cài đặt dependencies"):
        return False
    
    return True

def check_edge_browser():
    """Kiểm tra Microsoft Edge có được cài đặt không"""
    print("🌐 Kiểm tra Microsoft Edge...")
    
    system = platform.system().lower()
    
    if system == "windows":
        edge_paths = [
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"
        ]
    elif system == "darwin":  # macOS
        edge_paths = ["/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"]
    else:  # Linux
        edge_paths = ["/usr/bin/microsoft-edge", "/opt/microsoft/msedge/msedge"]
    
    for path in edge_paths:
        if os.path.exists(path):
            print(f"✅ Tìm thấy Microsoft Edge tại: {path}")
            return True
    
    print("⚠️ Không tìm thấy Microsoft Edge!")
    print("📥 Tải Edge tại: https://www.microsoft.com/edge")
    return False

def setup_webdriver():
    """Cài đặt và cấu hình WebDriver"""
    print("🚗 Cài đặt WebDriver Manager...")
    
    # Test WebDriver
    test_code = """
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from webdriver_manager.microsoft import EdgeChromiumDriverManager

try:
    service = Service(EdgeChromiumDriverManager().install())
    print("✅ WebDriver setup thành công!")
except Exception as e:
    print(f"❌ WebDriver setup thất bại: {e}")
"""
    
    try:
        exec(test_code)
        return True
    except Exception as e:
        print(f"❌ Lỗi WebDriver: {e}")
        return False

def create_config_file():
    """Tạo file config mẫu"""
    config_content = """# Hospital Email Crawler Configuration
# Chỉnh sửa các thông số tùy theo nhu cầu

# Số luồng song song cho tìm kiếm Google Maps
MAX_WORKERS_SEARCH = 4

# Số luồng song song cho crawl email  
MAX_WORKERS_EMAIL = 8

# Số URL xử lý trong 1 batch
BATCH_SIZE = 50

# Timeout cho mỗi request (giây)
REQUEST_TIMEOUT = 10

# Delay giữa các request (giây)
REQUEST_DELAY = 0.5

# Số lần cuộn tối đa trên Google Maps
MAX_SCROLLS = 20

# Pause time giữa các lần cuộn (giây)
SCROLL_PAUSE_TIME = 1
"""
    
    try:
        with open("config.py", "w", encoding="utf-8") as f:
            f.write(config_content)
        print("✅ Tạo file config.py thành công!")
        return True
    except Exception as e:
        print(f"❌ Không thể tạo config.py: {e}")
        return False

def run_test():
    """Chạy test đơn giản để kiểm tra setup"""
    print("🧪 Chạy test cơ bản...")
    
    test_code = """
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor
print("✅ Tất cả modules import thành công!")

# Test regex
email_regex = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+'
test_email = "test@example.com"
if re.match(email_regex, test_email):
    print("✅ Regex email hoạt động!")

# Test threading
with ThreadPoolExecutor(max_workers=2) as executor:
    print("✅ ThreadPoolExecutor hoạt động!")

print("🎉 Setup hoàn tất và test thành công!")
"""
    
    try:
        exec(test_code)
        return True
    except Exception as e:
        print(f"❌ Test thất bại: {e}")
        return False

def main():
    """Hàm chính setup"""
    print("=" * 60)
    print("🚀 HOSPITAL EMAIL CRAWLER - SETUP SCRIPT")
    print("=" * 60)
    
    success_steps = 0
    total_steps = 6
    
    # 1. Kiểm tra Python version
    if check_python_version():
        success_steps += 1
    
    # 2. Cài đặt Python packages
    if install_requirements():
        success_steps += 1
    
    # 3. Kiểm tra Edge browser
    if check_edge_browser():
        success_steps += 1
    
    # 4. Setup WebDriver
    if setup_webdriver():
        success_steps += 1
    
    # 5. Tạo config file
    if create_config_file():
        success_steps += 1
    
    # 6. Chạy test
    if run_test():
        success_steps += 1
    
    # Kết quả
    print("\n" + "=" * 60)
    print(f"📊 SETUP HOÀN TẤT: {success_steps}/{total_steps} bước thành công")
    print("=" * 60)
    
    if success_steps == total_steps:
        print("🎉 Setup hoàn hảo! Bạn có thể chạy crawler ngay!")
        print("\n📝 Cách sử dụng:")
        print("   python hospital_crawler_parallel.py")
        print("\n📁 Files được tạo:")
        print("   • config.py - File cấu hình")
        print("   • requirements.txt - Danh sách dependencies")
    else:
        print("⚠️ Setup chưa hoàn hảo. Vui lòng kiểm tra lại các lỗi!")
        
        if success_steps < 2:
            print("\n🔧 Các bước khắc phục:")
            print("1. Cài đặt Python 3.8+ nếu chưa có")
            print("2. Chạy: pip install --upgrade pip")
            print("3. Chạy: pip install -r requirements.txt")
            print("4. Tải Microsoft Edge nếu chưa có")

if __name__ == "__main__":
    main()
# Hospital Email Crawler Configuration
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

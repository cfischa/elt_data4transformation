"""
Scrapy settings for web scraping spiders.
Configuration for web scraping components of the ELT pipeline.
"""

import os
from pathlib import Path

# Scrapy settings for bnb_data4transformation project
BOT_NAME = 'bnb_data4transformation'

SPIDER_MODULES = ['scraping.spiders']
NEWSPIDER_MODULE = 'scraping.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure delays for requests
DOWNLOAD_DELAY = 1  # 1 second delay between requests
RANDOMIZE_DOWNLOAD_DELAY = 0.5  # 0.5 * to 1.5 * DOWNLOAD_DELAY

# Concurrent requests settings
CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 8

# Enable autothrottling for respectful scraping
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0
AUTOTHROTTLE_DEBUG = False

# User agent
USER_AGENT = 'bnb_data4transformation (+http://www.yourdomain.com)'

# Configure pipelines
ITEM_PIPELINES = {
    'scraping.pipelines.ValidationPipeline': 300,
    'scraping.pipelines.DuplicatesPipeline': 400,
    'scraping.pipelines.StoragePipeline': 500,
}

# Configure middlewares
SPIDER_MIDDLEWARES = {
    'scraping.middlewares.ErrorHandlingMiddleware': 543,
}

DOWNLOADER_MIDDLEWARES = {
    'scraping.middlewares.ProxyMiddleware': 350,
    'scraping.middlewares.UserAgentMiddleware': 400,
    'scraping.middlewares.RetryMiddleware': 550,
}

# Enable and configure HTTP caching
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 3600  # 1 hour
HTTPCACHE_DIR = 'httpcache'
HTTPCACHE_IGNORE_HTTP_CODES = [503, 504, 505, 500, 408, 429]

# Configure request fingerprinting
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"

# Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False

# Logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE_PATH', './data/logs/scrapy.log')

# Custom settings
FEEDS = {
    'data/raw/scraped_data.json': {
        'format': 'json',
        'encoding': 'utf8',
        'store_empty': False,
        'item_export_kwargs': {
            'ensure_ascii': False,
        },
    },
    'data/raw/scraped_data.csv': {
        'format': 'csv',
        'encoding': 'utf8',
        'store_empty': False,
    },
}

# Database settings for storing scraped data
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///data/scraped_data.db')

# ClickHouse settings for direct storage
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '9000'))
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'analytics')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')

# Retry settings
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]

# Cookies and sessions
COOKIES_ENABLED = True
COOKIES_DEBUG = False

# Compression
COMPRESSION_ENABLED = True

# Memory usage
MEMUSAGE_ENABLED = True
MEMUSAGE_LIMIT_MB = 512
MEMUSAGE_WARNING_MB = 256

# Extensions
EXTENSIONS = {
    'scrapy.extensions.telnet.TelnetConsole': None,
    'scrapy.extensions.memusage.MemoryUsage': 500,
    'scrapy.extensions.logstats.LogStats': 500,
}

# Custom settings for different environments
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')

if ENVIRONMENT == 'production':
    # Production settings
    CONCURRENT_REQUESTS = 32
    DOWNLOAD_DELAY = 0.5
    AUTOTHROTTLE_TARGET_CONCURRENCY = 4.0
    MEMUSAGE_LIMIT_MB = 1024
    
elif ENVIRONMENT == 'development':
    # Development settings
    CONCURRENT_REQUESTS = 8
    DOWNLOAD_DELAY = 2
    AUTOTHROTTLE_DEBUG = True
    LOG_LEVEL = 'DEBUG'

# Proxy settings (if needed)
PROXY_ENABLED = os.getenv('PROXY_ENABLED', 'false').lower() == 'true'
PROXY_LIST = os.getenv('PROXY_LIST', '').split(',') if os.getenv('PROXY_LIST') else []

# User agent rotation
USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
]

# Custom headers
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# Scheduler settings
SCHEDULER_PRIORITY_QUEUE = 'scrapy.pqueues.DownloaderAwarePriorityQueue'
SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleFifoDiskQueue'
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.FifoMemoryQueue'

# Stats collection
STATS_CLASS = 'scrapy.statscollectors.MemoryStatsCollector'

# Item processing
ITEM_PROCESSOR = 'itemprocessors.TakeFirst'

# Dupefilter
DUPEFILTER_CLASS = 'scrapy.dupefilters.RFPDupeFilter'
DUPEFILTER_DEBUG = False

# Job directory for persistence
JOBDIR = 'data/scrapy_jobs'

# Custom settings for specific spiders
SPIDER_SETTINGS = {
    'news_spider': {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 4,
        'ROBOTSTXT_OBEY': True,
    },
    'social_media_spider': {
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 2,
        'ROBOTSTXT_OBEY': False,
    },
}

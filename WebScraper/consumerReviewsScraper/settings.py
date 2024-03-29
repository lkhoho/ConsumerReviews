# -*- coding: utf-8 -*-

# Scrapy settings for scrapy_reviews project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html

import os
import platform

BOT_NAME = 'consumerReviewsScraper'

SPIDER_MODULES = ['consumerReviewsScraper.spiders']
NEWSPIDER_MODULE = 'consumerReviewsScraper.spiders'

LOG_LEVEL = 'INFO'
LOG_DATEFORMAT = '%Y%m%d %H:%M:%S'

# custom de-dupe filter class
DUPEFILTER_CLASS = 'WebScraper.core_customized.RFPDupeFilterWithStatus'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'scrapy_reviews (+http://www.yourdomain.com)'
# USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False  # some website does not allow scrape user page

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 2
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 4
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'scrapy_reviews.middlewares.MyCustomSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    'scrapy_reviews.middlewares.MyCustomDownloaderMiddleware': 543,
# }

# Enable or disable extensions
# See http://scrapy.readthedocs.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Rotating proxies
ROTATING_PROXY_LIST_PATH = 'proxies.txt'

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'consumerReviewsScraper.pipelines.SqlItemPipeline': 100,
    # 'consumerReviewsScraper.pipelines.SaveExpediaItemsPipeline': 100,
    # 'consumerReviewsScraper.pipelines.SaveHyoubanItemsPipeline': 100,
    # 'consumerReviewsScraper.pipelines.SaveXcarItemsPipeline': 100,
    # 'consumerReviewsScraper.pipelines.SaveToSqlitePipeline': 100
    # 'consumerReviewsScraper.pipelines.SaveToMongoDb': 100,
    # 'scrapy_reviews.pipelines.SaveRawItemPipeline': 100,
    # 'scrapy_reviews.pipelines.RemoveStopwordsPipeline': 200,
    # 'scrapy_reviews.pipelines.TokenizationPipeline': 300,
    # 'scrapy_reviews.pipelines.StemmingReviewsPipeline': 400,
    # 'scrapy_reviews.pipelines.SaveToCsvPipeline': 500,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# FEED_EXPORT_FIELDS = ["content"]

DB_PROVIDER = 'mysql'
DB_NAME = 'consumer_reviews'

if DB_PROVIDER == 'mysql':
    DB_HOST = 'kesnas.local'
    DB_PORT = 3306
    DB_USER = os.getenv('mysql_user')
    DB_PASSWORD = os.getenv('mysql_password')
    DB_CONN_STR = '{driver}://{user}:{password}@{host}:{port}/{schema}?charset=utf8mb4'.format(
        driver=DB_PROVIDER, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, schema=DB_NAME)
elif DB_PROVIDER == 'sqlite':
    if platform.system() == 'Windows':
        DB_FULL_NAME = 'C:\\Users\\lkhoho\\OneDrive\\{}.db'.format(DB_NAME)
    elif platform.system() == 'Linux':
        DB_FULL_NAME = '/mnt/c/Users/lkhoho/OneDrive/{}.db'.format(DB_NAME)
    elif platform.system() == 'Darwin':
        DB_FULL_NAME = '/Users/keliu/OneDrive/{}.db'.format(DB_NAME)
    else:
        raise ValueError('Unsupported platform: ' + platform.system())
    DB_CONN_STR = '{driver}:///{name}'.format(driver=DB_PROVIDER, name=DB_FULL_NAME)
else:
    raise ValueError('Unsupported database: ' + DB_PROVIDER)

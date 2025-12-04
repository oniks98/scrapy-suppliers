"""
Чистий запуск Scrapy пауків без технічного сміття в логах
Використання: python clean_run.py viatec_retail
"""
import sys
import os
import logging


# КРИТИЧНО: Налаштовуємо логування ДО імпорту Scrapy
os.environ['SCRAPY_SETTINGS_MODULE'] = 'suppliers.settings'

# Налаштовуємо базове логування Python
logging.basicConfig(
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

# Відключаємо технічні логери ДО імпорту Scrapy
noisy_loggers = [
    'scrapy.utils.log',
    'scrapy.addons', 
    'scrapy.middleware',
    'scrapy.crawler',
    'scrapy.core.engine',
    'scrapy.core.scraper',
    'scrapy.extensions.logstats',
    'scrapy.extensions.telnet',
    'scrapy.statscollectors',
    'twisted',
]

for logger_name in noisy_loggers:
    logging.getLogger(logger_name).setLevel(logging.CRITICAL)

# Тільки ТЕПЕР імпортуємо Scrapy
from scrapy.cmdline import execute


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("❌ Використання: python clean_run.py <spider_name>")
        print("📝 Приклад: python clean_run.py viatec_retail")
        sys.exit(1)
    
    spider_name = sys.argv[1]
    
    # Запускаємо Scrapy
    sys.argv = ['scrapy', 'crawl', spider_name]
    execute()

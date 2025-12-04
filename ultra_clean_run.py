"""
УЛЬТРА-ЧИСТИЙ запуск Scrapy - патчимо логування на рівні коду
Використання: python ultra_clean_run.py viatec_retail
"""
import sys
import os
import warnings


# Встановлюємо environment variables ДО всього
os.environ['SCRAPY_SETTINGS_MODULE'] = 'suppliers.settings'

# Ігноруємо DeprecationWarning
warnings.filterwarnings('ignore', category=DeprecationWarning)


# Патчимо configure_logging ДО імпорту Scrapy
def silent_configure_logging(settings=None, install_root_handler=True):
    """Наша версія configure_logging яка приховує технічні логи"""
    import logging
    
    # Базова конфігурація
    logging.basicConfig(
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    
    # Відключаємо всі технічні Scrapy логери
    noisy = [
        'scrapy.utils.log',
        'scrapy.addons',
        'scrapy.middleware', 
        'scrapy.crawler',
        'scrapy.core.engine',
        'scrapy.core.scraper',
        'scrapy.extensions',
        'scrapy.statscollectors',
        'twisted',
        'filelock',
        'py.warnings',
    ]
    
    for name in noisy:
        logging.getLogger(name).setLevel(logging.ERROR)


# Патчимо Scrapy ДО імпорту
import scrapy.utils.log
scrapy.utils.log.configure_logging = silent_configure_logging

# Тепер імпортуємо решту
from scrapy.cmdline import execute


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("❌ Використання: python ultra_clean_run.py <spider_name>")
        print("📝 Приклад: python ultra_clean_run.py viatec_retail")
        sys.exit(1)
    
    spider_name = sys.argv[1]
    
    # Запускаємо
    sys.argv = ['scrapy', 'crawl', spider_name]
    execute()

"""
Custom Scrapy команда для чистого виводу
Використання: scrapy run viatec_retail
"""
import logging
from scrapy.commands.crawl import Command as CrawlCommand
from scrapy.exceptions import UsageError


class Command(CrawlCommand):
    """Запуск паука з чистим виводом без технічної інфи"""
    
    def short_desc(self):
        return "Запустити паука (чистий вивід)"
    
    def run(self, args, opts):
        if len(args) < 1:
            raise UsageError()
        
        # Налаштовуємо логування ДО запуску
        self._setup_clean_logging()
        
        # Запускаємо паука
        return super().run(args, opts)
    
    def _setup_clean_logging(self):
        """Налаштовує чисте логування - відключає технічні логи Scrapy"""
        # Відключаємо всі Scrapy логери
        scrapy_loggers = [
            'scrapy.utils.log',
            'scrapy.addons',
            'scrapy.middleware',
            'scrapy.crawler',
            'scrapy.core.engine',
            'scrapy.core.scraper',
            'scrapy.extensions.logstats',
            'scrapy.extensions.telnet',
            'scrapy.statscollectors',
            'py.warnings',
        ]
        
        for logger_name in scrapy_loggers:
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.CRITICAL)  # Показуємо тільки критичні помилки
            logger.propagate = False

stalk@DESKTOP-APMRUCI MINGW64 /c/FullStack/Scrapy/suppliers (main)
$ scrapy crawl secur_retail
2025-11-29 19:31:03 [scrapy.utils.log] INFO: Scrapy 2.13.4 started (bot: suppliers)
2025-11-29 19:31:03 [scrapy.utils.log] INFO: Versions:
{'lxml': '6.0.2',
'libxml2': '2.11.9',
'cssselect': '1.3.0',
'parsel': '1.10.0',
'w3lib': '2.3.1',
'Twisted': '25.5.0',
'Python': '3.12.6 (tags/v3.12.6:a4a2d2b, Sep 6 2024, 20:11:23) [MSC v.1940 '
'64 bit (AMD64)]',
'pyOpenSSL': '25.3.0 (OpenSSL 3.5.4 30 Sep 2025)',
'cryptography': '46.0.3',
'Platform': 'Windows-11-10.0.26100-SP0'}
2025-11-29 19:31:03 [secur_retail] INFO: ✅ Завантажено 11 категорій
2025-11-29 19:31:03 [scrapy.addons] INFO: Enabled addons:
[]
2025-11-29 19:31:03 [scrapy.middleware] INFO: Enabled extensions:
['scrapy.extensions.corestats.CoreStats',
'scrapy.extensions.logstats.LogStats',
'scrapy.extensions.throttle.AutoThrottle']
2025-11-29 19:31:03 [scrapy.crawler] INFO: Overridden settings:
{'AUTOTHROTTLE_ENABLED': True,
'AUTOTHROTTLE_MAX_DELAY': 5,
'AUTOTHROTTLE_START_DELAY': 1,
'BOT_NAME': 'suppliers',
'CONCURRENT_REQUESTS': 8,
'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
'DOWNLOAD_DELAY': 1,
'DOWNLOAD_TIMEOUT': 30,
'FEED_EXPORT_ENCODING': 'utf-8',
'LOG_LEVEL': 'INFO',
'NEWSPIDER_MODULE': 'suppliers.spiders',
'RETRY_TIMES': 3,
'ROBOTSTXT_OBEY': True,
'SPIDER_MODULES': ['suppliers.spiders'],
'TELNETCONSOLE_ENABLED': False,
'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
'(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'}
Unhandled error in Deferred:
2025-11-29 19:31:03 [twisted] CRITICAL: Unhandled error in Deferred:

Traceback (most recent call last):
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\twisted\internet\defer.py", line 1857, in _inlineCallbacks
result = context.run(gen.send, result)
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\crawler.py", line 156, in crawl
self.engine = self.\_create_engine()
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\crawler.py", line 169, in \_create_engine  
 return ExecutionEngine(self, lambda _: self.stop())
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\core\engine.py", line 113, in **init**  
 self.downloader: Downloader = downloader_cls(crawler)
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\core\downloader\_\_init**.py", line 109, in **init**
DownloaderMiddlewareManager.from_crawler(crawler)
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\middleware.py", line 77, in from_crawler  
 return cls.\_from_settings(crawler.settings, crawler)
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\middleware.py", line 86, in \_from_settings  
 mwcls = load_object(clspath)
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\utils\misc.py", line 71, in load_object  
 mod = import_module(module)
File "C:\Python312\Lib\importlib\_\_init**.py", line 90, in import_module
return \_bootstrap.\_gcd_import(name[level:], package, level)
File "<frozen importlib._bootstrap>", line 1387, in \_gcd_import

File "<frozen importlib._bootstrap>", line 1360, in \_find_and_load

File "<frozen importlib._bootstrap>", line 1324, in \_find_and_load_unlocked

builtins.ModuleNotFoundError: No module named 'scrapy_playwright.middleware'

2025-11-29 19:31:03 [twisted] CRITICAL:
Traceback (most recent call last):
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\twisted\internet\defer.py", line 1857, in _inlineCallbacks
result = context.run(gen.send, result)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\crawler.py", line 156, in crawl
self.engine = self.\_create_engine()
^^^^^^^^^^^^^^^^^^^^^
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\crawler.py", line 169, in \_create_engine  
 return ExecutionEngine(self, lambda _: self.stop())
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\core\engine.py", line 113, in **init**  
 self.downloader: Downloader = downloader_cls(crawler)
^^^^^^^^^^^^^^^^^^^^^^^
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\core\downloader\_\_init**.py", line 109, in **init**
DownloaderMiddlewareManager.from_crawler(crawler)
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\middleware.py", line 77, in from_crawler  
 return cls.\_from_settings(crawler.settings, crawler)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\middleware.py", line 86, in \_from_settings  
 mwcls = load_object(clspath)
^^^^^^^^^^^^^^^^^^^^
File "C:\Users\stalk\AppData\Roaming\Python\Python312\site-packages\scrapy\utils\misc.py", line 71, in load_object  
 mod = import_module(module)
^^^^^^^^^^^^^^^^^^^^^
File "C:\Python312\Lib\importlib\_\_init**.py", line 90, in import_module
return \_bootstrap.\_gcd_import(name[level:], package, level)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "<frozen importlib._bootstrap>", line 1387, in \_gcd_import
File "<frozen importlib._bootstrap>", line 1360, in \_find_and_load
File "<frozen importlib._bootstrap>", line 1324, in \_find_and_load_unlocked
ModuleNotFoundError: No module named 'scrapy_playwright.middleware'

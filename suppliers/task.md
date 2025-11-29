сделай в C:\FullStack\Scrapy\suppliers\suppliers\spiders\secur\retail.py

такое же логирование в терминал как в C:\FullStack\Scrapy\suppliers\suppliers\spiders\viatec\retail.py

вот ниже пример

2025-11-29 20:34:13 [viatec_retail] INFO: ✅ Pipeline відкрито для viatec_retail
2025-11-29 20:34:13 [viatec_retail] INFO: 📁 Вихідна директорія: C:\FullStack\Scrapy\output
2025-11-29 20:34:13 [viatec_retail] INFO: ✅ Мапінг особистих нотаток для viatec_retail завантажено: {'dealer': 'V', 'retail': 'PROM'}
2025-11-29 20:34:13 [viatec_retail] INFO: 📝 Створено файл з заголовком: C:\FullStack\Scrapy\output\viatec_retail.csv  
2025-11-29 20:34:13 [viatec_retail] INFO: ✅ Початковий код товару для viatec_retail завантажено з C:\FullStack\Scrapy\data\viatec\viatec_counter_product_code.csv: 200000
2025-11-29 20:34:13 [scrapy.extensions.logstats] INFO: Crawled 0 pages (at 0 pages/min), scraped 0 items (at 0 items/min)  
2025-11-29 20:34:13 [viatec_retail] INFO: 🚀 СТАРТ ПАРСИНГУ. Перша категорія [1/68]: https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision
2025-11-29 20:34:15 [viatec_retail] INFO: 📂 Обробляю категорію [1/68] сторінка 1: https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision
2025-11-29 20:34:15 [viatec_retail] INFO: 📦 Знайдено товарів на сторінці: 48
2025-11-29 20:34:15 [viatec_retail] INFO: 📄 Перехід на наступну сторінку пагінації (2): https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision;page:2
2025-11-29 20:34:16 [viatec_retail] INFO: 📂 Обробляю категорію [1/68] сторінка 2: https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision
2025-11-29 20:34:16 [viatec_retail] INFO: 📦 Знайдено товарів на сторінці: 48
2025-11-29 20:34:16 [viatec_retail] INFO: 📄 Перехід на наступну сторінку пагінації (3): https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision;page:3
2025-11-29 20:34:17 [viatec_retail] INFO: 📂 Обробляю категорію [1/68] сторінка 3: https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision
2025-11-29 20:34:17 [viatec_retail] INFO: 📦 Знайдено товарів на сторінці: 48
2025-11-29 20:34:17 [viatec_retail] INFO: 📄 Перехід на наступну сторінку пагінації (4): https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision;page:4
2025-11-29 20:34:18 [viatec_retail] INFO: 📂 Обробляю категорію [1/68] сторінка 4: https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision
2025-11-29 20:34:18 [viatec_retail] INFO: 📦 Знайдено товарів на сторінці: 48
2025-11-29 20:34:18 [viatec_retail] INFO: 📄 Перехід на наступну сторінку пагінації (5): https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision;page:5
2025-11-29 20:34:19 [viatec_retail] INFO: 📂 Обробляю категорію [1/68] сторінка 5: https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision
2025-11-29 20:34:19 [viatec_retail] INFO: 📦 Знайдено товарів на сторінці: 48
2025-11-29 20:34:19 [viatec_retail] INFO: 📄 Перехід на наступну сторінку пагінації (6): https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision;page:6
2025-11-29 20:34:21 [viatec_retail] INFO: 📂 Обробляю категорію [1/68] сторінка 6: https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision
2025-11-29 20:34:21 [viatec_retail] INFO: 📦 Знайдено товарів на сторінці: 48
2025-11-29 20:34:21 [viatec_retail] INFO: 📄 Перехід на наступну сторінку пагінації (7): https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision;page:7
2025-11-29 20:34:22 [viatec_retail] INFO: 📂 Обробляю категорію [1/68] сторінка 7: https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision
2025-11-29 20:34:22 [viatec_retail] INFO: 📦 Знайдено товарів на сторінці: 48
2025-11-29 20:34:22 [viatec_retail] INFO: 📄 Перехід на наступну сторінку пагінації (8): https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision;page:8
2025-11-29 20:34:24 [viatec_retail] INFO: 📂 Обробляю категорію [1/68] сторінка 8: https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision
2025-11-29 20:34:24 [viatec_retail] INFO: 📦 Знайдено товарів на сторінці: 48
2025-11-29 20:34:24 [viatec_retail] INFO: 📄 Перехід на наступну сторінку пагінації (9): https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision;page:9
2025-11-29 20:34:24 [viatec_retail] INFO: 📂 Обробляю категорію [1/68] сторінка 9: https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision
2025-11-29 20:34:24 [viatec_retail] INFO: 📦 Знайдено товарів на сторінці: 18
2025-11-29 20:34:24 [viatec_retail] INFO: ✅ ПАГІНАЦІЯ ЗАВЕРШЕНА [1/68]: накопичено 201 товарів
2025-11-29 20:34:24 [viatec_retail] INFO: 🔗 ЗАПУСК ланцюга продуктів. Перший: https://viatec.ua/product/DS-2CD1321G0-I-28. Залишилось: 200
2025-11-29 20:34:26 [viatec_retail] INFO: 🔗 Парсимо товар (UA): https://viatec.ua/product/DS-2CD1321G0-I-28
2025-11-29 20:34:26 [viatec_retail] INFO: Знайдено <p> теги в описі на https://viatec.ua/product/DS-2CD1321G0-I-28
2025-11-29 20:34:26 [viatec_retail] INFO: 📐 Характеристик (UA) знайдено: 38 шт.
2025-11-29 20:34:27 [viatec_retail] INFO: 🔗 Парсимо товар (RU): https://viatec.ua/ru/product/DS-2CD1321G0-I-28
2025-11-29 20:34:27 [viatec_retail] INFO: Знайдено <p> теги в описі на https://viatec.ua/ru/product/DS-2CD1321G0-I-28
2025-11-29 20:34:27 [viatec_retail] INFO: 📝 Опис RU: 345 символів
2025-11-29 20:34:27 [viatec_retail] INFO: 📝 Опис UA: 357 символів
2025-11-29 20:34:27 [viatec_retail] INFO: ✅ YIELD: IP видеокамера Hikvision DS-2CD1321G0-I 2МП (2.8мм) | Ціна: 2537.0 | Характеристик: 38
2025-11-29 20:34:27 [viatec_retail] INFO: ⏭️ Перехід до наступного товару. Залишилось: 199
2025-11-29 20:34:27 [viatec_retail] INFO: 🔍 ПРОВЕРКА НАЯВНОСТІ RAW: 'В наличии'
2025-11-29 20:34:27 [viatec_retail] INFO: 🔍 РЕЗУЛЬТАТ ПРОВЕРКИ: True
2025-11-29 20:34:28 [viatec_retail] INFO: 🔗 Парсимо товар (UA): https://viatec.ua/product/DS-2CD1341G0-I-28
2025-11-29 20:34:28 [viatec_retail] INFO: Знайдено <p> теги в описі на https://viatec.ua/product/DS-2CD1341G0-I-28
2025-11-29 20:34:28 [viatec_retail] INFO: 📐 Характеристик (UA) знайдено: 38 шт.
2025-11-29 20:34:29 [viatec_retail] INFO: 🔗 Парсимо товар (RU): https://viatec.ua/ru/product/DS-2CD1341G0-I-28
2025-11-29 20:34:29 [viatec_retail] INFO: Знайдено <p> теги в описі на https://viatec.ua/ru/product/DS-2CD1341G0-I-28
2025-11-29 20:34:29 [viatec_retail] INFO: 📝 Опис RU: 346 символів
2025-11-29 20:34:29 [viatec_retail] INFO: 📝 Опис UA: 358 символів
2025-11-29 20:34:29 [viatec_retail] INFO: ✅ YIELD: IP видеокамера Hikvision DS-2CD1341G0-I 4МП (2.8мм) | Ціна: 3827.0 | Характеристик: 38
2025-11-29 20:34:29 [viatec_retail] INFO: ⏭️ Перехід до наступного товару. Залишилось: 198
2025-11-29 20:34:29 [viatec_retail] INFO: 🔍 ПРОВЕРКА НАЯВНОСТІ RAW: 'В наличии'
2025-11-29 20:34:29 [viatec_retail] INFO: 🔍 РЕЗУЛЬТАТ ПРОВЕРКИ: True
2025-11-29 20:34:31 [viatec_retail] INFO: 🔗 Парсимо товар (UA): https://viatec.ua/product/DS-2CD1021G0-I-28
2025-11-29 20:34:31 [viatec_retail] INFO: Знайдено <p> теги в описі на https://viatec.ua/product/DS-2CD1021G0-I-28
2025-11-29 20:34:31 [viatec_retail] INFO: 📐 Характеристик (UA) знайдено: 38 шт.
2025-11-29 20:34:32 [viatec_retail] INFO: 🔗 Парсимо товар (RU): https://viatec.ua/ru/product/DS-2CD1021G0-I-28
2025-11-29 20:34:32 [viatec_retail] INFO: Знайдено <p> теги в описі на https://viatec.ua/ru/product/DS-2CD1021G0-I-28
2025-11-29 20:34:32 [viatec_retail] INFO: 📝 Опис RU: 344 символів
2025-11-29 20:34:32 [viatec_retail] INFO: 📝 Опис UA: 355 символів
2025-11-29 20:34:32 [viatec_retail] INFO: ✅ YIELD: IP видеокамера Hikvision DS-2CD1021G0-I 2МП (2.8мм) | Ціна: 2731.0 | Характеристик: 38

И после окончания

2025-11-29 20:35:27 [viatec_retail] INFO: ================================================================================
2025-11-29 20:35:27 [viatec_retail] INFO: 📊 СТАТИСТИКА PIPELINE
2025-11-29 20:35:27 [viatec_retail] INFO: ================================================================================
2025-11-29 20:35:27 [viatec_retail] INFO:
📄 Файл: viatec_retail.csv
2025-11-29 20:35:27 [viatec_retail] INFO: ✅ Товарів записано: 24
2025-11-29 20:35:27 [viatec_retail] INFO: ❌ Відфільтровано без ціни: 0
2025-11-29 20:35:27 [viatec_retail] INFO: ❌ Відфільтровано без наявності: 0
2025-11-29 20:35:27 [viatec_retail] INFO: ================================================================================
2025-11-29 20:35:27 [viatec_retail] INFO: ✅ Pipeline закрито
2025-11-29 20:35:27 [viatec_retail] INFO: 🎉 Паук viatec_retail завершено! Причина: shutdown
2025-11-29 20:35:27 [viatec_retail] INFO: ✅ Товарів з помилками завантаження не знайдено.
2025-11-29 20:35:28 [viatec_retail] INFO: 🔔 Звуковий сигнал відтворено!
2025-11-29 20:35:28 [scrapy.statscollectors] INFO: Dumping Scrapy stats:
{'downloader/request_bytes': 73417,
'downloader/request_count': 59,
'downloader/request_method_count/GET': 59,
'downloader/response_bytes': 4705131,
'downloader/response_count': 59,
'downloader/response_status_count/200': 59,
{'downloader/request_bytes': 73417,
'downloader/request_count': 59,
'downloader/request_method_count/GET': 59,
'downloader/response_bytes': 4705131,
'downloader/response_count': 59,
'downloader/response_status_count/200': 59,
'downloader/request_count': 59,
'downloader/request_method_count/GET': 59,
'downloader/response_bytes': 4705131,
'downloader/response_count': 59,
'downloader/response_status_count/200': 59,
'downloader/request_method_count/GET': 59,
'downloader/response_bytes': 4705131,
'downloader/response_count': 59,
'downloader/response_status_count/200': 59,
'downloader/response_bytes': 4705131,
'downloader/response_count': 59,
'downloader/response_status_count/200': 59,
'downloader/response_count': 59,
'downloader/response_status_count/200': 59,
'elapsed_time_seconds': 74.976199,
'finish_reason': 'shutdown',
'finish_time': datetime.datetime(2025, 11, 29, 18, 35, 28, 137395, tzinfo=datetime.timezone.utc),
'downloader/response_status_count/200': 59,
'elapsed_time_seconds': 74.976199,
'finish_reason': 'shutdown',
'finish_time': datetime.datetime(2025, 11, 29, 18, 35, 28, 137395, tzinfo=datetime.timezone.utc),
'elapsed_time_seconds': 74.976199,
'finish_reason': 'shutdown',
'finish_time': datetime.datetime(2025, 11, 29, 18, 35, 28, 137395, tzinfo=datetime.timezone.utc),
'finish_time': datetime.datetime(2025, 11, 29, 18, 35, 28, 137395, tzinfo=datetime.timezone.utc),
'httpcompression/response_bytes': 35732185,
'httpcompression/response_count': 58,
'item_scraped_count': 24,
'items_per_minute': 19.45945945945946,
'log_count/INFO': 323,
'log_count/WARNING': 1,
'request_depth_max': 58,
'response_received_count': 59,
'responses_per_minute': 47.83783783783784,
'robotstxt/request_count': 1,
'robotstxt/response_count': 1,
'robotstxt/response_status_count/200': 1,
'scheduler/dequeued': 58,
'scheduler/dequeued/memory': 58,
'scheduler/enqueued': 59,
'scheduler/enqueued/memory': 59,
'start_time': datetime.datetime(2025, 11, 29, 18, 34, 13, 161196, tzinfo=datetime.timezone.utc)}
2025-11-29 20:35:28 [scrapy.core.engine] INFO: Spider closed (shutdown)
(venv)

и ВОТ ЄТО НЕ НАДО - СПИСОК ХАРАКТЕРИСТИК В ТЕРМИНАЛ
2025-11-29 20:41:00 [secur_retail] INFO: ✅ Бренд: Ajax
2025-11-29 20:41:00 [secur_retail] INFO: ✅ Модель: Starterkit 2
2025-11-29 20:41:00 [secur_retail] INFO: ✅ Комплектація: Централь Hub 2 , Датчик руху MotionProtect , Датчи
2025-11-29 20:41:00 [secur_retail] INFO: ✅ Макс. кількість пристроїв: 100
2025-11-29 20:41:00 [secur_retail] INFO: ✅ Підтримка датчиків MotionCam: Немає
2025-11-29 20:41:00 [secur_retail] INFO: ✅ Канали зв'язку: Ethernet , 2G , 3G
2025-11-29 20:41:00 [secur_retail] INFO: ✅ Макс. кількість груп охорони: 25

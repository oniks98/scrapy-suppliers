"""
Spider для парсингу роздрібних цін з secur.ua (UAH)
Вигружає дані в: output/secur_retail.csv

ВИПРАВЛЕНО: Прибрано wait_for_selector що викликав timeout
"""
import scrapy
import csv
import re
from pathlib import Path
from scrapy_playwright.page import PageMethod
from suppliers.spiders.base import BaseRetailSpider


class SecurRetailSpider(BaseRetailSpider):
    name = "secur_retail"
    supplier_id = "secur"
    output_filename = "secur_retail.csv"
    allowed_domains = ["secur.ua"]
    
    custom_settings = {
        "ITEM_PIPELINES": {
            "suppliers.pipelines.SuppliersPipeline": 300,
        },
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "CONCURRENT_REQUESTS": 1,
        "DOWNLOAD_DELAY": 2,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category_mapping = self._load_category_mapping()
        self.category_urls = list(self.category_mapping.keys())
        self.current_category_index = 0
        self.products_from_pagination = []
    
    def _load_category_mapping(self):
        """Завантажує маппінг категорій з CSV"""
        mapping = {}
        csv_path = Path(r"C:\FullStack\Scrapy\data\secur\secur_category_retail.csv")
        
        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    url = row["Линк категории поставщика"].strip().strip('"')
                    
                    if not url or not url.startswith("http"):
                        continue
                    
                    mapping[url] = {
                        "category_ru": row["Категория на моем сайте_RU"],
                        "category_ua": row["Категория на моем сайте_UA"],
                        "group_number": row.get("Номер_групи", ""),
                        "subdivision_id": row.get("Ідентифікатор_підрозділу", ""),
                        "subdivision_link": row.get("Посилання_підрозділу", ""),
                    }
            self.logger.info(f"✅ Завантажено {len(mapping)} категорій")
        except Exception as e:
            self.logger.error(f"❌ Помилка завантаження категорій: {e}")
        
        return mapping
    
    def start_requests(self):
        """Стартуємо з першої категорії"""
        if self.category_urls:
            first_category_url = self.category_urls[0]
            self.logger.info(f"🚀 СТАРТ ПАРСИНГУ. Перша категорія [1/{len(self.category_urls)}]: {first_category_url}")
            yield scrapy.Request(
                url=first_category_url,
                callback=self.parse_category,
                meta={
                    "category_url": first_category_url,
                    "category_index": 0,
                    "page_number": 1,
                    "playwright": True,
                },
                dont_filter=True,
                errback=self.errback_httpbin,
            )
    
    def errback_httpbin(self, failure):
        """Обробка помилок"""
        self.logger.error(f"❌ ERRBACK: {failure.value}")
        self.logger.error(f"   URL: {failure.request.url}")
        
        # Отримуємо remaining_products з meta
        remaining = failure.request.meta.get("remaining_products", [])
        category_index = failure.request.meta.get("category_index", 0)
        
        # Якщо є ще товари - обробляємо їх
        if remaining:
            self.logger.info(f"⏭️ Пропускаємо товар з помилкою, обробляємо наступний ({len(remaining)} залишилось)")
            request_to_yield = self._process_next_item(remaining, category_index)
            if request_to_yield:
                yield request_to_yield
        else:
            # Інакше переходимо до наступної категорії
            next_cat = self._start_next_category(category_index)
            if next_cat:
                yield next_cat
    
    def parse_category(self, response):
        """Парсимо список товарів у категорії"""
        category_url = response.meta["category_url"]
        category_index = response.meta["category_index"]
        page_number = response.meta.get("page_number", 1)
        category_info = self.category_mapping.get(category_url, {})
        
        self.logger.info(f"📂 Обробляю категорію [{category_index + 1}/{len(self.category_urls)}] сторінка {page_number}")
        
        product_links = response.css('div.productsCardsSlider a::attr(href)').getall()
        
        if not product_links:
            self.logger.warning(f"⚠️ Не знайдено товарів на сторінці: {response.url}")
        else:
            self.logger.info(f"📦 Знайдено товарів на сторінці: {len(product_links)}")
            for link in product_links:
                product_url = response.urljoin(link)
                
                if product_url not in self.processed_products:
                    self.products_from_pagination.append({
                        "url": product_url,
                        "meta": {
                            "category_url": category_url,
                            "category_ru": category_info.get("category_ru", ""),
                            "category_ua": category_info.get("category_ua", ""),
                            "group_number": category_info.get("group_number", ""),
                            "subdivision_id": category_info.get("subdivision_id", ""),
                            "subdivision_link": category_info.get("subdivision_link", ""),
                        },
                    })
                    self.processed_products.add(product_url)
        
        next_page = response.css('a.next-button::attr(href)').get()
        
        if next_page:
            next_page_url = response.urljoin(next_page)
            self.logger.info(f"📄 Пагінація: сторінка {page_number + 1}")
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_category,
                meta={
                    "category_url": category_url,
                    "category_index": category_index,
                    "page_number": page_number + 1,
                    "playwright": True,
                },
                dont_filter=True,
                errback=self.errback_httpbin,
            )
        else:
            self.logger.info(f"✅ ПАГІНАЦІЯ ЗАВЕРШЕНА [{category_index + 1}/{len(self.category_urls)}]: накопичено {len(self.products_from_pagination)} товарів")
            
            if self.products_from_pagination:
                product_data = self.products_from_pagination.pop(0)
                product_data["meta"]["remaining_products"] = list(self.products_from_pagination)
                product_data["meta"]["category_index"] = category_index
                
                self.logger.info(f"🔗 ЗАПУСК ланцюга продуктів. Перший: {product_data['url']}. Залишилось: {len(self.products_from_pagination)}")
                
                # Просто чекаємо 3 секунди - без wait_for_selector
                yield scrapy.Request(
                    url=product_data["url"],
                    callback=self.parse_product_ua,
                    meta={
                        **product_data["meta"],
                        "playwright": True,
                        "playwright_page_methods": [
                            PageMethod("wait_for_timeout", 2000),  # Чекаємо 2 секунди для Vue.js
                        ],
                    },
                    dont_filter=True,
                    errback=self.errback_httpbin,
                )
            else:
                self.logger.warning(f"⚠️ У категорії {category_url} не знайдено товарів. Переходжу до наступної.")
                next_cat = self._start_next_category(category_index)
                if next_cat:
                    yield next_cat
    
    def parse_product_ua(self, response):
        """Парсим украинскую версию товара"""
        self.logger.info(f"🇺🇦 UA: {response.url}")
        
        name_ua = response.css('h1.title::text').get()
        price_raw = response.css('div.currentPrice span.bold::text').get()
        image_url = response.css('div.productsCardsSlider a img::attr(src)').get()
        product_code = response.css('div.productsCardsCode span::text').get()
        
        availability_raw = response.css('div.statusWrap::text').get()
        if availability_raw:
            availability_raw = availability_raw.strip()
        else:
            availability_raw = "В наявності"
        
        description_ua = response.css('div.content.descr div.item').get()
        if description_ua:
            description_ua = self._clean_html_description(description_ua)
        else:
            description_ua = ""
        
        specs_list = self._parse_specifications(response)
        
        self.logger.info(f"📊 UA: Знайдено характеристик: {len(specs_list)}")
        
        meta = response.meta.copy()
        meta.update({
            "name_ua": name_ua.strip() if name_ua else "",
            "price_raw": price_raw,
            "image_url": image_url,
            "product_code": product_code.strip() if product_code else "",
            "availability_raw": availability_raw,
            "description_ua": description_ua,
            "specs_list": specs_list,
        })
        
        ru_url = response.url.replace("secur.ua/", "secur.ua/ru/")
        
        yield scrapy.Request(
            url=ru_url,
            callback=self.parse_product_ru,
            meta={
                **meta,
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_timeout", 2000),
                ],
            },
            dont_filter=True,
            errback=self.errback_httpbin,
        )
    
    def parse_product_ru(self, response):
        """Парсим русскую версию товара"""
        self.logger.info(f"🇷🇺 RU: {response.url}")
        
        name_ru = response.css('h1.title::text').get()
        brand = response.xpath("//div[@class='subtitle' and text()='Бренд']/../div[@class='inner']//p/text()").get()
        
        description_ru = response.css('div.content.descr div.item').get()
        if description_ru:
            description_ru = self._clean_html_description(description_ru)
        else:
            description_ru = ""
        
        name_ua = response.meta.get("name_ua", "")
        name_ru = name_ru.strip() if name_ru else name_ua
        price_raw = response.meta.get("price_raw", "")
        image_url = response.meta.get("image_url", "")
        product_code = response.meta.get("product_code", "")
        availability_raw = response.meta.get("availability_raw", "")
        description_ua = response.meta.get("description_ua", "")
        specs_list = response.meta.get("specs_list", [])
        
        price = self._clean_price(price_raw) if price_raw else ""
        image_url = response.urljoin(image_url) if image_url else ""
        brand = brand.strip() if brand else ""
        quantity = self._extract_quantity(availability_raw)
        
        search_terms_ru = self._generate_search_terms(name_ru)
        search_terms_ua = self._generate_search_terms(name_ua)
        
        self.logger.info(f"📝 Опис RU: {len(description_ru)} символів")
        self.logger.info(f"📝 Опис UA: {len(description_ua)} символів")
        
        item = {
            "Код_товару": product_code,
            "Назва_позиції": name_ru,
            "Назва_позиції_укр": name_ua,
            "Пошукові_запити": search_terms_ru,
            "Пошукові_запити_укр": search_terms_ua,
            "Опис": description_ru,
            "Опис_укр": description_ua,
            "Тип_товару": "r",
            "Ціна": price,
            "Валюта": self.currency,
            "Одиниця_виміру": "шт.",
            "Посилання_зображення": image_url,
            "Наявність": availability_raw,
            "Кількість": quantity,
            "Назва_групи": response.meta.get("category_ru", ""),
            "Назва_групи_укр": response.meta.get("category_ua", ""),
            "Номер_групи": response.meta.get("group_number", ""),
            "Ідентифікатор_підрозділу": response.meta.get("subdivision_id", ""),
            "Посилання_підрозділу": response.meta.get("subdivision_link", ""),
            "Виробник": brand,
            "Країна_виробник": "",
            "price_type": self.price_type,
            "supplier_id": self.supplier_id,
            "output_file": self.output_filename,
            "Продукт_на_сайті": response.url.replace("/ru/", "/"),
            "specifications_list": specs_list,
        }
        
        self.logger.info(f"✅ YIELD: {item['Назва_позиції']} | Ціна: {item['Ціна']} | Характеристик: {len(specs_list)}")
        yield item
        
        # Обробляємо наступний товар
        remaining = response.meta.get("remaining_products", [])
        category_index = response.meta.get("category_index", 0)
        
        request_to_yield = self._process_next_item(remaining, category_index)
        if request_to_yield:
            yield request_to_yield
    
    def _process_next_item(self, remaining, category_index):
        """Обробляє наступний товар або переходить до наступної категорії"""
        if remaining:
            next_data = remaining.pop(0)
            next_data["meta"]["remaining_products"] = list(remaining)
            next_data["meta"]["category_index"] = category_index
            
            self.logger.info(f"⏭️ Наступний товар ({len(remaining)} залишилось)")
            
            return scrapy.Request(
                url=next_data["url"],
                callback=self.parse_product_ua,
                meta={
                    **next_data["meta"],
                    "playwright": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_timeout", 2000),
                    ],
                },
                dont_filter=True,
                errback=self.errback_httpbin,
            )
        else:
            self.logger.info(f"✅ ВСІ ТОВАРИ КАТЕГОРІЇ ОБРОБЛЕНІ")
            return self._start_next_category(category_index)
    
    def _parse_specifications(self, response):
        """
        Парсим характеристики товара
        Структура HTML: 
        <div class="item"><div class="subtitle">Назва</div><div class="inner"><div class="innerItem"><p>Значення</p></div></div></div>
        """
        specs_list = []
        
        # Знаходимо всі div.item які містять характеристики
        # Вони можуть бути в різних контейнерах, тому шукаємо глобально
        items = response.xpath('//div[@class="item"][.//div[@class="subtitle"]]')
        
        self.logger.info(f"🔍 Знайдено {len(items)} елементів div.item з характеристиками")
        
        for item in items:
            # Витягуємо назву характеристики
            characteristic = item.xpath('.//div[@class="subtitle"]/text()').get()
            
            if not characteristic:
                continue
            
            characteristic = characteristic.strip()
            
            # Витягуємо значення - всі текстові вузли в div.inner
            value_texts = item.xpath('.//div[@class="inner"]//text()').getall()
            value = ' '.join(t.strip() for t in value_texts if t.strip())
            
            if value:
                value = value.replace('\u00a0', ' ').strip()
                specs_list.append({
                    "name": characteristic,
                    "unit": "",
                    "value": value,
                })
        
        return specs_list
    
    def _clean_html_description(self, html_content):
        """Очищаем HTML описание, сохраняя форматирование"""
        if not html_content:
            return ""
        
        from scrapy.selector import Selector
        sel = Selector(text=html_content)
        
        description_html = sel.css('div.item').get()
        
        if not description_html:
            return ""
        
        description_html = re.sub(r'^<div[^>]*>', '', description_html)
        description_html = re.sub(r'</div>$', '', description_html)
        description_html = re.sub(r'\s*style="[^"]*"', '', description_html)
        description_html = re.sub(r'>\s+<', '><', description_html)
        
        if len(description_html) > 10000:
            description_html = description_html[:10000] + '...</p>'
        
        return description_html.strip()
    
    def closed(self, reason):
        """Викликається при завершенні паука"""
        self.logger.info(f"🎉 Паук {self.name} завершено! Причина: {reason}")
        
        if self.failed_products:
            self.logger.info("=" * 80)
            self.logger.info("📦 СПИСОК ТОВАРІВ З ПОМИЛКАМИ ЗАВАНТАЖЕННЯ")
            self.logger.info("=" * 80)
            for failed in self.failed_products:
                self.logger.error(f"- Товар: {failed['product_name']} | URL: {failed['url']} | Причина: {failed['reason']}")
            self.logger.info("=" * 80)
        else:
            self.logger.info("✅ Товарів з помилками завантаження не знайдено.")
        
        # Звуковий сигнал (опціонально, працює тільки на Windows)
        try:
            import winsound
            for _ in range(3):
                winsound.Beep(1000, 300)
            self.logger.info("🔔 Звуковий сигнал відтворено!")
        except Exception as e:
            self.logger.debug(f"Не вдалося відтворити звук: {e}")
    
    def _start_next_category(self, current_category_index):
        """Запуск следующей категории"""
        next_category_index = current_category_index + 1
        if next_category_index < len(self.category_urls):
            next_category_url = self.category_urls[next_category_index]
            self.logger.info(f"🚀 НАСТУПНА КАТЕГОРІЯ [{next_category_index + 1}/{len(self.category_urls)}]")
            self.products_from_pagination = []
            return scrapy.Request(
                url=next_category_url,
                callback=self.parse_category,
                meta={
                    "category_url": next_category_url,
                    "category_index": next_category_index,
                    "page_number": 1,
                    "playwright": True,
                },
                dont_filter=True,
                errback=self.errback_httpbin,
            )
        else:
            self.logger.info(f"🎉 ВСІ КАТЕГОРІЇ ОБРОБЛЕНІ 🎉")
            return None

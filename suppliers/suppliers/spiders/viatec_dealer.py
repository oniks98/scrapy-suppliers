r"""
Spider для парсинга дилерских цен с viatec.ua (USD)
Требует авторизации через форму логина
Выгружает данные в: C:\FullStack\Scrapy\output\prom_diler_import.csv

⚠️ ВАЖНО: Паук создаёт ТОЛЬКО файл дилера (prom_diler_import.csv)
Файл розницы НЕ создаётся при запуске этого паука

ПОСЛЕДОВАТЕЛЬНАЯ ОБРАБОТКА: категория → все страницы пагинации → следующая категория
ХАРАКТЕРИСТИКИ: парсятся на УКРАИНСКОМ (UA) языке
"""
import scrapy
from scrapy.http import FormRequest
import csv
import re
from pathlib import Path
from urllib.parse import urljoin
import os
from dotenv import load_dotenv
import winsound


class ViatecDealerSpider(scrapy.Spider):
    name = "viatec_dealer"
    allowed_domains = ["viatec.ua"]
    
    custom_settings = {
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 60,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0,
        "COOKIES_ENABLED": True,
        "HTTPERROR_ALLOWED_CODES": [404, 500, 502, 503],
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Загружаем учётные данные из .env
        load_dotenv(Path(r"C:\FullStack\Scrapy\suppliers\.env"))
        self.email = os.getenv("VIATEC_EMAIL")
        self.password = os.getenv("VIATEC_PASSWORD")
        
        if not self.email or not self.password:
            raise ValueError("❌ Отсутствуют учётные данные VIATEC_EMAIL и VIATEC_PASSWORD в .env")
        
        self.category_mapping = self._load_category_mapping()
        self.category_urls = list(self.category_mapping.keys())
        self.current_category_index = 0
        self.products_from_pagination = []
        self.processed_products = set()
    
    def _load_category_mapping(self):
        """Загружает маппинг категорий из CSV"""
        mapping = {}
        csv_path = Path(r"C:\FullStack\Scrapy\data\category_matching_dealer_viatec.csv")
        
        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    url = row["Линк категории поставщика"].strip().strip('"')
                    
                    if not url or url == "" or not url.startswith("http"):
                        continue
                    
                    mapping[url] = {
                        "category_ru": row["Категория на моем сайте_RU"],
                        "category_ua": row["Категория на моем сайте_UA"],
                        "group_number": row.get("Номер_групи", ""),
                        "subdivision_id": row.get("Ідентифікатор_підрозділу", ""),
                        "subdivision_link": row.get("Посилання_підрозділу", ""),
                    }
            self.logger.info(f"✅ Загружено {len(mapping)} категорий")
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки категорий: {e}")
        
        return mapping
    
    def start_requests(self):
        # сначала GET /login → получим cookies и csrf
        yield scrapy.Request(
            "https://viatec.ua/login",
            callback=self.parse_login_page,
            dont_filter=True
        )


    def parse_login_page(self, response):
        # извлекаем CSRF
        csrf = response.css("input[name=_token]::attr(value)").get()

        if not csrf:
            self.logger.error("Не найден CSRF (_token) на странице логина!")
            return

        self.logger.info(f"Найден CSRF: {csrf}")

        yield scrapy.FormRequest(
            url="https://viatec.ua/login",
            method="POST",
            formdata={
                "_token": csrf,
                "email": self.email,
                "password": self.password,
            },
            callback=self.after_login,
            dont_filter=True
        )


    def after_login(self, response):
        if b"viatec_session" not in b" ".join(response.headers.getlist("Set-Cookie")):
            self.logger.error("Авторизация не выполнена!")
            return

        self.logger.info("УСПЕШНЫЙ ЛОГИН")

        if not self.category_urls:
            self.logger.error("Нет категорий для парсинга.")
            return

        first = self.category_urls[0]

        yield scrapy.Request(
            url=first,
            callback=self.parse_category,
            meta={
                "category_url": first,
                "category_index": 0,
                "page_number": 1
            },
            dont_filter=True
        )

    def parse_category(self, response):
        """Парсим список товаров в категории и страницы пагинации"""
        category_url = response.meta["category_url"]
        category_index = response.meta["category_index"]
        page_number = response.meta.get("page_number", 1)
        category_info = self.category_mapping.get(category_url, {})

        self.logger.info(f"📂 Обрабатываю категорию [{category_index + 1}/{len(self.category_urls)}] страница {page_number}: {category_url}")

        product_links = response.css("a[href*='/product/']::attr(href)").getall()

        if not product_links:
            self.logger.warning(f"⚠️ Не найдены товары на странице: {response.url}")
        else:
            self.logger.info(f"📦 Найдено товаров на странице: {len(product_links)}")
            for link in product_links:
                product_url = response.urljoin(link)
                normalized_url = product_url.replace("/ru/", "/")
                
                if normalized_url not in self.processed_products:
                    self.products_from_pagination.append({
                        "url": normalized_url,
                        "meta": {
                            "category_url": category_url,
                            "category_ru": category_info.get("category_ru", ""),
                            "category_ua": category_info.get("category_ua", ""),
                            "group_number": category_info.get("group_number", ""),
                            "subdivision_id": category_info.get("subdivision_id", ""),
                            "subdivision_link": category_info.get("subdivision_link", ""),
                        },
                    })
                    self.processed_products.add(normalized_url)

        next_page_link = response.css("a.paggination__next::attr(href)").get()
        if not next_page_link:
            all_pages = response.css("a.paggination__page::attr(href)").getall()
            active_page_nodes = response.css("a.paggination__page--active")
            if all_pages and active_page_nodes:
                try:
                    active_page_text = active_page_nodes[0].css("::text").get()
                    all_page_texts = [a.css("::text").get() for a in response.css("a.paggination__page")]
                    current_idx = all_page_texts.index(active_page_text)
                    
                    if current_idx >= 0 and current_idx + 1 < len(all_pages):
                        next_page_link = all_pages[current_idx + 1]
                except (ValueError, IndexError):
                    pass

        if next_page_link:
            self.logger.info(f"📄 Переход на следующую страницу пагинации ({page_number + 1}): {next_page_link}")
            yield scrapy.Request(
                url=urljoin(response.url, next_page_link),
                callback=self.parse_category,
                meta={
                    "category_url": response.meta["category_url"],
                    "category_index": response.meta["category_index"],
                    "page_number": response.meta["page_number"] + 1
                },
                dont_filter=True
            )
        else:
            self.logger.info(f"✅ ПАГИНАЦИЯ ЗАВЕРШЕНА [{category_index + 1}/{len(self.category_urls)}]: накоплено {len(self.products_from_pagination)} товаров")
            
            if self.products_from_pagination:
                product_data = self.products_from_pagination.pop(0)
                
                product_data["meta"]["remaining_products"] = self.products_from_pagination
                product_data["meta"]["category_index"] = category_index
                
                self.logger.info(f"🔗 ЗАПУСК цепочки продуктов. Первый: {product_data['url']}. Осталось: {len(self.products_from_pagination)}")
                
                yield scrapy.Request(
                    url=product_data["url"],
                    callback=self.parse_product,
                    meta=product_data["meta"],
                    dont_filter=True,
                )
            else:
                self.logger.warning(f"⚠️ В категории {category_url} не найдено товаров. Перехожу к следующей.")
                next_idx = response.meta["category_index"] + 1

                if next_idx < len(self.category_urls):
                    next_cat = self.category_urls[next_idx]

                    yield scrapy.Request(
                        url=next_cat,
                        callback=self.parse_category,
                        meta={
                            "category_url": next_cat,
                            "category_index": next_idx,
                            "page_number": 1
                        },
                        dont_filter=True
                    )

            self.products_from_pagination = []

    def parse_product(self, response):
        """Парсим страницу товара (украинская версия) - НАЗВАНИЕ, ОПИСАНИЕ, ХАРАКТЕРИСТИКИ"""
        self.logger.info(f"🔗 Парсим товар (UA): {response.url}")
        
        name_ua = response.css("h1::text").get()
        name_ua = name_ua.strip() if name_ua else ""
        
        description_ua = self._extract_description_with_br(response)
        
        specs_list_ua = self._extract_specifications(response)
        
        self.logger.info(f"📐 Характеристик (UA) найдено: {len(specs_list_ua)} шт.")
        
        ru_url = self._convert_to_ru_url(response.url)
        
        yield scrapy.Request(
            url=ru_url,
            callback=self.parse_product_ru,
            meta={
                **response.meta,
                "name_ua": name_ua,
                "description_ua": description_ua,
                "specifications_list": specs_list_ua,
                "original_url": response.url,
            },
            dont_filter=True,
        )
    
    def parse_product_ru(self, response):
        """Парсим страницу товара (русская версия) и продолжаем цепочку"""
        self.logger.info(f"🔗 Парсим товар (RU): {response.url}")
        
        name_ru = response.css("h1::text").get()
        name_ru = name_ru.strip() if name_ru else ""
        
        description_ru = self._extract_description_with_br(response)
        
        name_ua = response.meta.get("name_ua", "")
        description_ua = response.meta.get("description_ua", "")
        specs_list = response.meta.get("specifications_list", [])
        
        code = ""
        
        # ДИЛЕРСКАЯ ЦЕНА В USD (селектор тот же, но валюта USD)
        price_raw = response.css("div.card-header__card-price-new::text").get()
        price_raw = price_raw.strip().replace("&nbsp;", "").replace(" ", "") if price_raw else ""
        price = self._clean_price(price_raw) if price_raw else ""
        currency = "USD"
        
        self.logger.info(f"📝 Описание RU: {len(description_ru)} символов")
        self.logger.info(f"📝 Описание UA: {len(description_ua)} символов")
        
        images = response.css("img.card-header__card-images-image::attr(src)").getall()
        image_url = response.urljoin(images[0]) if images else ""
        
        availability_raw_text = response.css("div.card-header__card-status-badge::text").get()
        availability_status = self._normalize_availability(availability_raw_text)
        quantity = self._extract_quantity(availability_raw_text)
        
        manufacturer = self._extract_manufacturer(name_ru)
        
        search_terms_ru = self._generate_search_terms(name_ru)
        search_terms_ua = self._generate_search_terms(name_ua)
        
        item = {
            "Код_товару": code,
            "Назва_позиції": name_ru,
            "Назва_позиції_укр": name_ua,
            "Пошукові_запити": search_terms_ru,
            "Пошукові_запити_укр": search_terms_ua,
            "Опис": description_ru,
            "Опис_укр": description_ua,
            "Тип_товару": "r",
            "Ціна": price,
            "Валюта": currency,
            "Одиниця_виміру": "шт.",
            "Посилання_зображення": image_url,
            "Наявність": availability_status,
            "Кількість": quantity,
            "Назва_групи": response.meta.get("category_ru", ""),
            "Назва_групи_укр": response.meta.get("category_ua", ""),
            "Номер_групи": response.meta.get("group_number", ""),
            "Ідентифікатор_підрозділу": response.meta.get("subdivision_id", ""),
            "Посилання_підрозділу": response.meta.get("subdivision_link", ""),
            "Виробник": manufacturer,
            "Країна_виробник": "",
            "price_type": "dealer",
            "Продукт_на_сайті": response.meta.get("original_url", response.url),
            "specifications_list": specs_list,
        }
        
        self.logger.info(f"✅ YIELD: {item['Назва_позиції']} | Ціна: {item['Ціна']} USD | Характеристик: {len(specs_list)}")
        yield item
        
        remaining_products = response.meta.get("remaining_products", [])
        category_index = response.meta.get("category_index")

        if remaining_products:
            next_product_data = remaining_products.pop(0)
            next_product_data["meta"]["remaining_products"] = remaining_products
            next_product_data["meta"]["category_index"] = category_index

            self.logger.info(f"🔗 Продолжаем цепочку продуктов. Осталось: {len(remaining_products)}")
            yield scrapy.Request(
                url=next_product_data["url"],
                callback=self.parse_product,
                meta=next_product_data["meta"],
                dont_filter=True,
            )
        else:
            self.logger.info(f"✅ Все продукты категории [{category_index + 1}] обработаны.")
            next_request = self._start_next_category(category_index)
            if next_request:
                yield next_request

    def _start_next_category(self, current_category_index):
        """Вспомогательный метод для запуска следующей категории"""
        next_category_index = current_category_index + 1
        if next_category_index < len(self.category_urls):
            next_category_url = self.category_urls[next_category_index]
            self.logger.info(f"🚀 СТАРТ СЛЕДУЮЩЕЙ КАТЕГОРИИ [{next_category_index + 1}/{len(self.category_urls)}]: {next_category_url}")
            return scrapy.Request(
                url=next_category_url,
                callback=self.parse_category,
                meta={
                    "category_url": next_category_url,
                    "category_index": next_category_index,
                    "page_number": 1,
                },
                dont_filter=True,
            )
        else:
            self.logger.info(f"🎉🎉🎉 ВСЕ КАТЕГОРИИ И ПРОДУКТЫ ОБРАБОТАНЫ 🎉🎉🎉")
            return None
    
    def _clean_price(self, price_str):
        """Очистка цены от лишних символов"""
        if not price_str:
            return ""
        
        price_str = price_str.replace(" ", "").replace("у.е.", "").replace("$", "").replace("USD", "")
        price_str = price_str.replace(",", ".")
        
        try:
            cleaned = "".join(c for c in price_str if c.isdigit() or c == ".")
            return str(float(cleaned)) if cleaned else ""
        except ValueError:
            return ""
    
    def _normalize_availability(self, availability):
        """Нормализация статуса наличия"""
        if not availability:
            return "Уточняйте"
        
        availability_lower = availability.lower()
        
        if any(word in availability_lower for word in ["є в наявності", "в наличии", "есть", "заканчивается", "закінчується"]):
            return "В наличии"
        elif any(word in availability_lower for word in ["під замовлення", "под заказ"]):
            return "Под заказ"
        elif any(word in availability_lower for word in ["немає", "нет"]):
            return "Нет в наличии"
        else:
            return "Уточняйте"

    def _extract_quantity(self, text):
        """Извлекает количество из текста наличия."""
        if not text:
            return ""
        
        quantity_match = re.search(r'\d+', text)
        if quantity_match:
            return quantity_match.group(0)
        
        return ""
    
    def _generate_search_terms(self, product_name):
        """Генерация поисковых запросов: 'Название продукта, Слово1, Слово2, Слово3, ...'"""
        if not product_name:
            return ""
        
        words = product_name.replace(",", " ").split()
        
        unique_words = []
        seen = set()
        for word in words:
            word_clean = word.strip().lower()
            if len(word_clean) > 2 and word_clean not in seen:
                unique_words.append(word)
                seen.add(word_clean)
        
        search_terms = f"{product_name}, {', '.join(unique_words)}"
        
        return search_terms
    
    def _extract_specifications(self, response):
        """
        Извлечение характеристик товара из таблицы (УКРАИНСКИЕ названия)
        Возвращает список триплетов: [{'name': '...', 'value': '...', 'unit': ''}, ...]
        """
        specs_list = []
        
        spec_rows = response.css("li.card-tabs__item.active div.card-tabs__characteristic-content table tr")
        
        if not spec_rows:
            spec_rows = response.css("div.card-tabs__characteristic-content table tr")
        
        if not spec_rows:
            spec_rows = response.css("ul.card-tabs__list table tr")
        
        for row in spec_rows[:60]:
            name = row.css("th::text").get()
            value = row.css("td::text").get()
            
            if name and value:
                specs_list.append({
                    "name": name.strip(),
                    "value": value.strip(),
                    "unit": ""
                })
        
        return specs_list
    
    def _convert_to_ru_url(self, url):
        """Конвертирует украинский URL в русский"""
        if "/ru/" not in url:
            url = url.replace("viatec.ua/", "viatec.ua/ru/")
        return url
    
    def _extract_manufacturer(self, product_name):
        """Определяет производителя из названия товара и маппинга CSV"""
        if not product_name:
            return ""
        
        product_name_lower = product_name.lower()
        
        priority_patterns = {
            "hikvision": "Hikvision",
            "dahua": "Dahua Technology",
            "axis": "Axis",
            "uniview": "UniView",
            "imou": "Imou",
            "ezviz": "Ezviz",
            "unv": "UNV",
            "hiwatch": "HiWatch",
            "ajax": "Ajax",
            "tp-link": "TP-Link",
            "mikrotik": "MikroTik",
            "ubiquiti": "Ubiquiti",
        }
        
        for pattern, name in priority_patterns.items():
            if pattern in product_name_lower:
                return name
        
        code_patterns = {
            "ds-": "Hikvision",
            "dh-": "Dahua Technology",
            "dhi-": "Dahua Technology",
            "vto-": "Dahua Technology",
            "vtm-": "Dahua Technology",
        }
        
        for pattern, name in code_patterns.items():
            if pattern in product_name_lower:
                return name
        
        if not hasattr(self, "_manufacturers_cache"):
            self._manufacturers_cache = self._load_manufacturers_from_csv()
        
        sorted_manufacturers = sorted(
            self._manufacturers_cache.items(),
            key=lambda x: len(x[0]),
            reverse=True
        )
        
        for keyword, manufacturer in sorted_manufacturers:
            keyword_lower = keyword.lower()
            if len(keyword) <= 2:
                pattern = r'\b' + re.escape(keyword_lower) + r'\b'
                if re.search(pattern, product_name_lower):
                    return manufacturer
            else:
                if keyword_lower in product_name_lower:
                    return manufacturer
        
        return ""
    
    def _load_manufacturers_from_csv(self):
        """Загружает маппинг производителей из CSV файла"""
        mapping = {}
        try:
            csv_path = Path(r"C:\FullStack\Scrapy\data\manufacturers_viatec.csv")
            if csv_path.exists():
                with open(csv_path, encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f, delimiter=";")
                    for row in reader:
                        keyword = row.get("Слово в названии продукта", "").strip()
                        manufacturer = row.get("Производитель (виробник)", "").strip()
                        if keyword and manufacturer:
                            mapping[keyword] = manufacturer
                self.logger.info(f"✅ Загружено {len(mapping)} производителей из CSV")
        except Exception as e:
            self.logger.warning(f"⚠️  Ошибка загрузки производителей: {e}")
        
        return mapping
    
    def _extract_description_with_br(self, response):
        """
        Извлечение описания с сохранением переносов <br> и обработкой списков <ul>.
        Возвращает текст с HTML тегами <br> для переносов (для PROM).
        """
        description_container = response.css("div.card-header__card-info-text")
        if not description_container:
            self.logger.warning(f"Не найден контейнер описания 'div.card-header__card-info-text' на {response.url}")
            return ""

        ul_list = description_container.css("ul")
        if ul_list:
            self.logger.info(f"Найден <ul> список в описании на {response.url}")
            list_items = ul_list.css("li")
            
            description_parts = []
            for item in list_items:
                inner_content = item.get()
                inner_content = re.sub(r'</?li[^>]*>', '', inner_content).strip()
                if not inner_content.startswith('●'):
                    description_parts.append(f"● {inner_content}")
                else:
                    description_parts.append(inner_content)
            
            return "<br>".join(description_parts)

        p_tags = description_container.css("p")
        if p_tags:
            self.logger.info(f"Найдены <p> теги в описании на {response.url}")
            result_parts = []
            for p in p_tags:
                if p.css("::attr(class)").get() == "card-header__analog-link":
                    continue
                
                p_html = p.get()
                inner_html = re.sub(r'^<p[^>]*>|</p>$', '', p_html).strip()
                
                if inner_html:
                    inner_html = inner_html.replace("<br/>", "<br>").replace("<br />", "<br>")
                    result_parts.append(inner_html)
            
            return "<br>".join(result_parts)
        
        self.logger.warning(f"В контейнере описания не найдены ни <ul>, ни <p> на {response.url}")
        return ""
    
    def closed(self, reason):
        """Вызывается при завершении паука - издаём звуковой сигнал"""
        self.logger.info(f"🎉 Паук {self.name} завершён! Причина: {reason}")
        
        # Воспроизводим 3 коротких сигнала
        try:
            for _ in range(3):
                winsound.Beep(1000, 300)  # Частота 1000 Hz, длительность 300 мс
            self.logger.info("🔔 Звуковой сигнал воспроизведён!")
        except Exception as e:
            self.logger.warning(f"⚠️ Не удалось воспроизвести звук: {e}")

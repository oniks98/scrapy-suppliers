"""
Spider для парсинга розничных цен с viatec.ua (UAH)
Выгружает данные в: C:\FullStack\Scrapy\output\prom_import.csv
ПОСЛЕДОВАТЕЛЬНАЯ ОБРАБОТКА: категория → все страницы пагинации → следующая категория
ХАРАКТЕРИСТИКИ: парсятся на УКРАИНСКОМ (UA) языке
"""
import scrapy
import csv
import re
from pathlib import Path
from urllib.parse import urljoin
from scrapy import Selector


class ViatecRetailSpider(scrapy.Spider):
    name = "viatec_retail"
    allowed_domains = ["viatec.ua"]
    
    custom_settings = {
        "CONCURRENT_REQUESTS": 1,  # Один запрос за раз (последовательно)
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 2,
        "SCHEDULER_PRIORITY_QUEUE": "scrapy.pqueues.ScrapyPriorityQueue",  # Уважать приоритеты
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category_mapping = self._load_category_mapping()
        self.category_urls = list(self.category_mapping.keys())
        self.current_category_index = 0
    
    def _load_category_mapping(self):
        """Загружает маппинг категорий из CSV"""
        mapping = {}
        csv_path = Path(r"C:\FullStack\Scrapy\data\category_matching_viatec.csv")
        
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
                    }
            self.logger.info(f"✅ Загружено {len(mapping)} категорий")
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки категорий: {e}")
        
        return mapping
    
    def start_requests(self):
        """Стартуем с первой категории"""
        if self.category_urls:
            first_category = self.category_urls[0]
            self.logger.info(f"🚀 СТАРТ КАТЕГОРИИ [1/{len(self.category_urls)}]: {first_category}")
            yield scrapy.Request(
                url=first_category,
                callback=self.parse_category,
                meta={
                    "category_url": first_category,
                    "category_index": 0,
                },
                dont_filter=True,
                priority=1000,
            )
    
    def parse_category(self, response):
        """Парсим список товаров в категории"""
        category_url = response.meta["category_url"]
        category_index = response.meta["category_index"]
        category_info = self.category_mapping.get(category_url, {})
        
        self.logger.info(f"📂 Обрабатываю категорию [{category_index + 1}/{len(self.category_urls)}]: {category_url}")
        
        product_links = response.css("a[href*='/product/']::attr(href)").getall()
        
        if not product_links:
            self.logger.warning(f"⚠️ Не найдены товары в категории: {category_url}")
        else:
            self.logger.info(f"📦 Найдено товаров на странице: {len(product_links)}")
        
        # Парсим товары текущей страницы
        for link in product_links:
            product_url = response.urljoin(link)
            yield scrapy.Request(
                url=product_url,
                callback=self.parse_product,
                meta={
                    "category_url": category_url,
                    "category_ru": category_info.get("category_ru", ""),
                    "category_ua": category_info.get("category_ua", ""),
                    "group_number": category_info.get("group_number", ""),
                    "subdivision_id": category_info.get("subdivision_id", ""),
                },
                priority=900,
                dont_filter=True,
            )
        
        next_page_link = response.css("a.paggination__next::attr(href)").get()
        
        if not next_page_link:
            all_pages = response.css("a.paggination__page::attr(href)").getall()
            active_page = response.css("a.paggination__page--active::text").get()
            if all_pages and active_page:
                try:
                    current_idx = response.css("a.paggination__page").index(
                        response.css("a.paggination__page--active")[0]
                    ) if hasattr(response.css("a.paggination__page"), "index") else -1
                    if current_idx >= 0 and current_idx + 1 < len(all_pages):
                        next_page_link = all_pages[current_idx + 1]
                except:
                    pass
        
        if next_page_link:
            self.logger.info(f"📄 Переход на следующую страницу пагинации: {next_page_link}")
            yield response.follow(
                next_page_link,
                callback=self.parse_category,
                meta={
                    "category_url": category_url,
                    "category_index": category_index,
                },
                priority=950,
                dont_filter=True,
            )
        else:
            self.logger.info(f"✅ КАТЕГОРИЯ ЗАВЕРШЕНА [{category_index + 1}/{len(self.category_urls)}]: {category_url}")
            
            next_category_index = category_index + 1
            if next_category_index < len(self.category_urls):
                next_category_url = self.category_urls[next_category_index]
                self.logger.info(f"🚀 СТАРТ КАТЕГОРИИ [{next_category_index + 1}/{len(self.category_urls)}]: {next_category_url}")
                yield scrapy.Request(
                    url=next_category_url,
                    callback=self.parse_category,
                    meta={
                        "category_url": next_category_url,
                        "category_index": next_category_index,
                    },
                    priority=1000,
                    dont_filter=True,
                )
            else:
                self.logger.info(f"🎉 ВСЕ КАТЕГОРИИ ОБРАБОТАНЫ ({len(self.category_urls)} шт.)")
    
    def parse_product(self, response):
        """Парсим страницу товара (украинская версия) - НАЗВАНИЕ, ОПИСАНИЕ, ХАРАКТЕРИСТИКИ"""
        self.logger.info(f"🔗 Парсим товар (UA): {response.url}")
        
        name_ua = response.css("h1::text").get()
        name_ua = name_ua.strip() if name_ua else ""
        
        description_ua = self._extract_description_with_br(response)
        
        # ⚠️ ВАЖНО: Парсим характеристики с УКРАИНСКОЙ версии
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
                "specifications_list": specs_list_ua,  # Передаём украинские характеристики
                "original_url": response.url,
            },
            priority=900,
            dont_filter=True,
        )
    
    def parse_product_ru(self, response):
        """Парсим страницу товара (русская версия) - НАЗВАНИЕ, ОПИСАНИЕ"""
        self.logger.info(f"🔗 Парсим товар (RU): {response.url}")
        
        name_ru = response.css("h1::text").get()
        name_ru = name_ru.strip() if name_ru else ""
        
        description_ru = self._extract_description_with_br(response)
        
        name_ua = response.meta.get("name_ua", "")
        description_ua = response.meta.get("description_ua", "")
        specs_list = response.meta.get("specifications_list", [])  # Украинские характеристики из meta
        
        code = ""
        
        price_raw = response.css("div.card-header__card-price-new::text").get()
        if price_raw:
            price_raw = price_raw.strip().replace("&nbsp;", "").replace(" ", "")
        else:
            price_raw = ""
        
        price = self._clean_price(price_raw) if price_raw else ""
        
        currency = "UAH"
        
        self.logger.info(f"📝 Описание RU: {len(description_ru)} символов")
        self.logger.info(f"📝 Описание UA: {len(description_ua)} символов")
        
        images = response.css("img.card-header__card-images-image::attr(src)").getall()
        image_url = response.urljoin(images[0]) if images else ""
        
        availability = response.css("div.card-header__card-status-badge::text").get()
        availability = self._normalize_availability(availability)
        
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
            "Наявність": availability,
            "Назва_групи": response.meta.get("category_ru", ""),
            "Назва_групи_укр": response.meta.get("category_ua", ""),
            "Номер_групи": response.meta.get("group_number", ""),
            "Ідентифікатор_підрозділу": response.meta.get("subdivision_id", ""),
            "Виробник": manufacturer,
            "Країна_виробник": "",
            "price_type": "retail",
            "Продукт_на_сайті": response.meta.get("original_url", response.url),
            "specifications_list": specs_list,  # Украинские характеристики
        }
        
        self.logger.info(f"✅ YIELD: {item['Назва_позиції']} | Ціна: {item['Ціна']} | Характеристик: {len(specs_list)}")
        yield item
    
    def _clean_price(self, price_str):
        """Очистка цены от лишних символов"""
        if not price_str:
            return ""
        
        price_str = price_str.replace(" ", "").replace("грн", "").replace("₴", "")
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
        
        if any(word in availability_lower for word in ["є в наявності", "в наличии", "есть"]):
            return "В наличии"
        elif any(word in availability_lower for word in ["під замовлення", "под заказ"]):
            return "Под заказ"
        elif any(word in availability_lower for word in ["немає", "нет"]):
            return "Нет в наличии"
        else:
            return "Уточняйте"
    
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
        
        # Попытка 1: Активная вкладка
        spec_rows = response.css("li.card-tabs__item.active div.card-tabs__characteristic-content table tr")
        
        # Попытка 2: Любая вкладка с характеристиками
        if not spec_rows:
            spec_rows = response.css("div.card-tabs__characteristic-content table tr")
        
        # Попытка 3: Общий селектор таблицы
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
        
        if not hasattr(self, "_manufacturers_cache"):
            self._manufacturers_cache = self._load_manufacturers_from_csv()
        
        for keyword, manufacturer in self._manufacturers_cache.items():
            if keyword.lower() in product_name_lower:
                return manufacturer
        
        name_patterns = {
            "hikvision": "Hikvision",
            "dahua": "Dahua Technology",
            "axis": "Axis",
            "uniview": "UniView",
            "imou": "Imou",
            "ezviz": "Ezviz",
            "unv": "UNV",
            "hiwatch": "HiWatch",
            "ds-": "Hikvision",
            "dh-": "Dahua Technology",
            "dhi-": "Dahua Technology",
        }
        
        for pattern, name in name_patterns.items():
            if pattern in product_name_lower:
                return name
        
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
        Извлечение описания с сохранением переносов <br>
        Возвращает текст с HTML тегами <br> для переносов (для PROM)
        """
        description_html = response.css("div.card-header__card-info-text").get()
        
        if not description_html:
            return ""
        
        desc_selector = Selector(text=description_html)
        paragraphs = desc_selector.css("p")
        
        result_parts = []
        for p in paragraphs:
            if p.css("::attr(class)").get() == "card-header__analog-link":
                continue
            
            p_html = p.get()
            p_html = p_html.replace("<br/>", "<br>").replace("<br />", "<br>")
            
            text_selector = Selector(text=p_html)
            inner_html = text_selector.css("p").get()
            
            if inner_html:
                inner_html = re.sub(r'^<p[^>]*>|</p>$', '', inner_html)
                inner_html = inner_html.strip()
                
                if inner_html:
                    result_parts.append(inner_html)
        
        return "<br>".join(result_parts)

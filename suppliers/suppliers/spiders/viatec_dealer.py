"""
Spider для парсинга дилерских цен с viatec.ua (USD)
Требует авторизации с использованием .env
Выгружает данные в: C:\\Users\\stalk\\Documents\\Prom\\prom_diler_import.csv
"""
import scrapy
import csv
from pathlib import Path
from urllib.parse import urljoin
import os
from dotenv import load_dotenv


class ViatecDealerSpider(scrapy.Spider):
    name = "viatec_dealer"
    allowed_domains = ["viatec.ua"]
    
    # URL для авторизации (нужно уточнить реальный URL)
    login_url = "https://viatec.ua/login"  # Замени на реальный URL авторизации
    
    custom_settings = {
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 2,
        "COOKIES_ENABLED": True,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Загружаем учётные данные из .env
        load_dotenv(Path(r"C:\FullStack\Scrapy\suppliers\.env"))
        self.email = os.getenv("VIATEC_EMAIL")
        self.password = os.getenv("VIATEC_PASSWORD")
        
        if not self.email or not self.password:
            raise ValueError("❌ Отсутствуют учётные данные в .env")
        
        self.category_mapping = self._load_category_mapping()
        self.start_urls = list(self.category_mapping.keys())
    
    def _load_category_mapping(self):
        """Загружает маппинг категорий из CSV"""
        mapping = {}
        csv_path = Path(r"C:\FullStack\Scrapy\data\category_matching_viatec.csv")
        
        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    url = row["Линк категории поставщика"].strip().strip('"')
                    mapping[url] = {
                        "category_ru": row["Категория на моем сайте_RU"],
                        "category_ua": row["Категория на моем сайте_UA"],
                    }
            self.logger.info(f"✅ Загружено {len(mapping)} категорий")
        except Exception as e:
            self.logger.error(f"❌ Ошибка загрузки категорий: {e}")
        
        return mapping
    
    def start_requests(self):
        """Начинаем с авторизации"""
        self.logger.info("🔐 Начинаем авторизацию...")
        
        yield scrapy.Request(
            url=self.login_url,
            callback=self.login,
            dont_filter=True,
        )
    
    def login(self, response):
        """Авторизация на сайте"""
        # Ищем CSRF токен, если требуется
        csrf_token = response.css("input[name='csrf_token']::attr(value)").get()
        
        # Формируем данные для авторизации
        formdata = {
            "email": self.email,
            "password": self.password,
        }
        
        # Если есть CSRF токен
        if csrf_token:
            formdata["csrf_token"] = csrf_token
        
        # Отправляем POST запрос
        yield scrapy.FormRequest.from_response(
            response,
            formdata=formdata,
            callback=self.after_login,
            dont_filter=True,
        )
    
    def after_login(self, response):
        """Проверяем успешность авторизации"""
        # Проверяем, есть ли признак успешной авторизации
        if "logout" in response.text.lower() or "выход" in response.text.lower():
            self.logger.info("✅ Авторизация успешна!")
            
            # Начинаем парсинг категорий
            for url in self.start_urls:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_category,
                    meta={"category_url": url},
                    dont_filter=True,
                )
        else:
            self.logger.error("❌ Авторизация не удалась!")
            self.logger.debug(f"Response URL: {response.url}")
    
    def parse_category(self, response):
        """Парсим список товаров в категории"""
        category_url = response.meta["category_url"]
        category_info = self.category_mapping.get(category_url, {})
        
        # Селекторы для товаров (проверено test_selectors.py)
        product_links = response.css("a[href*='/product/']::attr(href)").getall()
        
        if not product_links:
            self.logger.warning(f"⚠️ Не найдены товары в категории: {category_url}")
        
        for link in product_links:
            product_url = response.urljoin(link)
            yield scrapy.Request(
                url=product_url,
                callback=self.parse_product,
                meta={
                    "category_url": category_url,
                    "category_ru": category_info.get("category_ru", ""),
                    "category_ua": category_info.get("category_ua", ""),
                },
            )
        
        # Пагинация (несколько вариантов селекторов)
        next_page = (
            response.css("a.next-page::attr(href)").get() or
            response.css("a[rel='next']::attr(href)").get() or
            response.css("li.pagination-next a::attr(href)").get() or
            response.css("a.pagination__next::attr(href)").get() or
            response.css("a:contains('Далее')::attr(href)").get() or
            response.css("a:contains('→')::attr(href)").get()
        )
        
        if next_page:
            self.logger.info(f"📄 Найдена следующая страница: {next_page}")
            yield response.follow(
                next_page,
                callback=self.parse_category,
                meta={"category_url": category_url},
            )
        else:
            self.logger.info(f"✅ Пагинация завершена для категории: {category_url}")
    
    def parse_product(self, response):
        """Парсим страницу товара (дилерская цена в USD)"""
        # Извлекаем данные товара
        name_ru = response.css("h1::text").get()
        
        name_ru = name_ru.strip() if name_ru else ""
        name_ua = name_ru
        
        # Код товара
        code = response.css("span.product-code::text").get()
        if not code:
            code = response.url.split("/")[-1]
        
        # ДИЛЕРСКАЯ ЦЕНА (селектор может отличаться)
        dealer_price = response.css("span.dealer-price::text").get()
        if not dealer_price:
            # Альтернативный селектор для дилерской цены
            dealer_price = response.css("div.price-dealer::text").get()
        
        dealer_price = self._clean_price(dealer_price) if dealer_price else ""
        
        # Валюта для дилеров - USD
        currency = "USD"
        
        # Описание
        description_ru = response.css("div.description::text").getall()
        description_ru = " ".join([d.strip() for d in description_ru if d.strip()])
        description_ua = description_ru
        
        # Изображения
        images = response.css("img.product-image::attr(src)").getall()
        image_url = response.urljoin(images[0]) if images else ""
        
        # Наличие
        availability = response.css("span.availability::text").get()
        availability = self._normalize_availability(availability)
        
        # Производитель
        manufacturer = response.css("span.manufacturer::text").get()
        manufacturer = manufacturer.strip() if manufacturer else ""
        
        # Формируем поисковые запросы
        search_terms_ru = self._generate_search_terms(name_ru)
        search_terms_ua = self._generate_search_terms(name_ua)
        
        # Характеристики
        specs = self._extract_specifications(response)
        
        yield {
            "Код_товару": code,
            "Назва_позиції": name_ru,
            "Назва_позиції_укр": name_ua,
            "Пошукові_запити": search_terms_ru,
            "Пошукові_запити_укр": search_terms_ua,
            "Опис": description_ru,
            "Опис_укр": description_ua,
            "Ціна": dealer_price,
            "Валюта": currency,
            "Одиниця_виміру": "шт.",
            "Посилання_зображення": image_url,
            "Наявність": availability,
            "Назва_групи": response.meta.get("category_ru", ""),
            "Назва_групи_укр": response.meta.get("category_ua", ""),
            "Виробник": manufacturer,
            "Країна_виробник": "",
            "price_type": "dealer",  # Маркер для pipeline
            "Продукт_на_сайті": response.url,
            **specs,
        }
    
    def _clean_price(self, price_str):
        """Очистка цены от лишних символов"""
        if not price_str:
            return ""
        
        # Удаляем всё кроме цифр, точки и запятой
        price_str = price_str.replace(" ", "").replace("$", "").replace("USD", "")
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
        """
        Генерация поисковых запросов:
        'Название продукта, Слово1, Слово2, Слово3, ...'
        """
        if not product_name:
            return ""
        
        # Полное название + слова через запятую
        words = product_name.replace(",", " ").split()
        
        # Убираем дубликаты и короткие слова
        unique_words = []
        seen = set()
        for word in words:
            word_clean = word.strip().lower()
            if len(word_clean) > 2 and word_clean not in seen:
                unique_words.append(word)
                seen.add(word_clean)
        
        # Формат: "Полное название, Слово1, Слово2, ..."
        search_terms = f"{product_name}, {', '.join(unique_words)}"
        
        return search_terms
    
    def _extract_specifications(self, response):
        """Извлечение характеристик товара"""
        specs = {}
        
        # Селекторы для характеристик (адаптировать под viatec.ua)
        spec_rows = response.css("table.specifications tr")
        
        for i, row in enumerate(spec_rows[:30], 1):  # Максимум 30 характеристик
            name = row.css("td:first-child::text").get()
            value = row.css("td:last-child::text").get()
            
            if name and value:
                specs[f"Назва_Характеристики_{i}"] = name.strip()
                specs[f"Значення_Характеристики_{i}"] = value.strip()
                specs[f"Одиниця_виміру_Характеристики_{i}"] = ""
        
        return specs

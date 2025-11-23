"""
Spider для парсинга розничных цен с viatec.ua (UAH)
Выгружает данные в: C:\\Users\\stalk\\Documents\\Prom\\prom_import.csv
"""
import scrapy
import csv
from pathlib import Path
from urllib.parse import urljoin


class ViatecRetailSpider(scrapy.Spider):
    name = "viatec_retail"
    allowed_domains = ["viatec.ua"]
    
    # Загружаем маппинг категорий из CSV
    custom_settings = {
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 2,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
        """Стартуем парсинг с каждой категории"""
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_category,
                meta={"category_url": url},
                dont_filter=True,
            )
    
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
        
        # Пагинация - селектор: <a href="https://viatec.ua/catalog/cameras/proizvoditel:hikvision;page:2" class="paggination__page">2</a>
        next_page_link = response.css("a.paggination__next::attr(href)").get()
        
        # Если не найдена следующая кнопка, ищем ссылку на следующую страницу в нумерации
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
        
        next_page = next_page_link
        
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
        """Парсим страницу товара"""
        self.logger.info(f"🔗 Парсим товар: {response.url}")
        # Извлекаем данные товара
        name_ru = response.css("h1::text").get()
        name_ru = name_ru.strip() if name_ru else ""
        name_ua = name_ru
        
        # Код товара - генерируем статически, начиная с 200000
        code = self._generate_product_code(response)
        
        # Цена - селектор из HTML: <div class="card-header__card-price-new">2&nbsp;537 <span>грн</span></div>
        price_raw = response.css("div.card-header__card-price-new::text").get()
        if price_raw:
            price_raw = price_raw.strip().replace("&nbsp;", "").replace(" ", "")
        else:
            price_raw = ""
        
        price = self._clean_price(price_raw) if price_raw else ""
        
        # Валюта
        currency = "UAH"
        
        # Описание - селектор: <p>● Роздільна здатність...</p>
        description_raw = response.css("div.card-header__card-description p::text, div.card-header__card-description p *::text").getall()
        if not description_raw:
            description_raw = response.css("div.card-header__card-description::text").getall()
        description_ru = " ".join([d.strip() for d in description_raw if d.strip()])
        description_ua = description_ru
        
        self.logger.info(f"📝 Описание найдено: {len(description_ru)} символов")
        self.logger.debug(f"📄 Текст описания: {description_ru[:100]}...")
        
        # Изображения - селектор: <img src="/upload/images/prod/2024-06/DS-2CD1321G0-I.webp" class="card-header__card-images-image">
        images = response.css("img.card-header__card-images-image::attr(src)").getall()
        image_url = response.urljoin(images[0]) if images else ""
        
        # Наличие - селектор: <div class="card-header__card-status-badge">В наявності</div>
        availability = response.css("div.card-header__card-status-badge::text").get()
        availability = self._normalize_availability(availability)
        
        # Производитель - определяем из маппинга по URL
        manufacturer = self._extract_manufacturer_from_url(response.url)
        
        # Формируем поисковые запросы
        search_terms_ru = self._generate_search_terms(name_ru)
        search_terms_ua = self._generate_search_terms(name_ua)
        
        # Характеристики
        specs = self._extract_specifications(response)
        self.logger.info(f"📐 Характеристики: {len([k for k in specs.keys() if 'Назва_Характеристики' in k])} шт.")
        
        item = {
            "Код_товару": code,
            "Назва_позиції": name_ru,
            "Назва_позиції_укр": name_ua,
            "Пошукові_запити": search_terms_ru,
            "Пошукові_запити_укр": search_terms_ua,
            "Опис": description_ru,
            "Опис_укр": description_ua,
            "Ціна": price,
            "Валюта": currency,
            "Одиниця_виміру": "шт.",
            "Посилання_зображення": image_url,
            "Наявність": availability,
            "Назва_групи": response.meta.get("category_ru", ""),
            "Назва_групи_укр": response.meta.get("category_ua", ""),
            "Виробник": manufacturer,
            "Країна_виробник": "",
            "price_type": "retail",  # Маркер для pipeline
            "Продукт_на_сайті": response.url,
            **specs,  # Добавляем характеристики
        }
        
        self.logger.info(f"✅ YIELD: {item['Назва_позиції']} | Ціна: {item['Ціна']} | Наявність: {item['Наявність']}")
        yield item
    
    def _clean_price(self, price_str):
        """Очистка цены от лишних символов"""
        if not price_str:
            return ""
        
        # Удаляем всё кроме цифр, точки и запятой
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
        """Извлечение характеристик товара из таблицы"""
        specs = {}
        
        # Селектор для характеристик из документа:
        # <table><tbody><tr><th>Матриця</th><td>1/2.9" Progressive Scan CMOS</td></tr>...</table>
        spec_rows = response.css("div.card-tabs__characteristic-content table tbody tr")
        
        for i, row in enumerate(spec_rows[:30], 1):  # Максимум 30 характеристик
            # Названия в <th>, значения в <td>
            name = row.css("th::text").get()
            value = row.css("td::text").get()
            
            if name and value:
                specs[f"Назва_Характеристики_{i}"] = name.strip()
                specs[f"Значення_Характеристики_{i}"] = value.strip()
                specs[f"Одиниця_виміру_Характеристики_{i}"] = ""
        
        return specs
    
    def _generate_product_code(self, response):
        """Генерация статического кода товара, начиная с 200000"""
        # Получаем счетчик из spider атрибута
        if not hasattr(self, "_product_counter"):
            self._product_counter = 200000
        
        code = str(self._product_counter)
        self._product_counter += 1
        return code
    
    def _extract_manufacturer_from_url(self, url):
        """Определяет производителя из URL категории и маппинга CSV"""
        url_lower = url.lower()
        
        # Кэш производителей для избежания повторного парсинга CSV
        if not hasattr(self, "_manufacturers_cache"):
            self._manufacturers_cache = self._load_manufacturers_from_csv()
        
        # Ищем производителя в CSV по совпадению с URL
        for keyword, manufacturer in self._manufacturers_cache.items():
            if keyword.lower() in url_lower:
                return manufacturer
        
        # Встроенный маппинг для стандартных производителей
        url_patterns = {
            "hikvision": "Hikvision",
            "dahua": "Dahua Technology",
            "axis": "Axis",
            "uniview": "UniView",
            "imou": "Imou",
            "ezviz": "Ezviz",
            "unv": "UNV",
        }
        
        for pattern, name in url_patterns.items():
            if pattern in url_lower:
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
                        # CSV структура: 'Слово в названии продукта;Производитель (виробник)'
                        keyword = row.get("Слово в названии продукта", "").strip()
                        manufacturer = row.get("Производитель (виробник)", "").strip()
                        if keyword and manufacturer:
                            mapping[keyword] = manufacturer
                self.logger.info(f"✅ Загружено {len(mapping)} производителей из CSV")
        except Exception as e:
            self.logger.warning(f"⚠️  Ошибка загрузки производителей: {e}")
        
        return mapping

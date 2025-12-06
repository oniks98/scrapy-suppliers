"""
Spider для парсингу роздрібних цін з e-server.com.ua (UAH)
Вигружає дані в: output/eserver_retail.csv

ПОСЛІДОВНА ОБРОБКА: категорія → всі сторінки пагінації → наступна категорія
ХАРАКТЕРИСТИКИ: парсяться УКРАЇНСЬКОЮ (UA) та РОСІЙСЬКОЮ (RU) з окремих URL
ПАГІНАЦІЯ: Підтримка параметрів ?only-inStock та &page=N
"""
import scrapy
import csv
import re
from pathlib import Path
from suppliers.spiders.base import EserverBaseSpider, BaseRetailSpider


class EserverRetailSpider(EserverBaseSpider, BaseRetailSpider):
    name = "eserver_retail"
    supplier_id = "eserver"
    output_filename = "eserver_retail.csv"
    
    custom_settings = {
        **EserverBaseSpider.custom_settings,
        "ITEM_PIPELINES": {
            "suppliers.pipelines.SuppliersPipeline": 300,
        },
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category_mapping = self._load_category_mapping()
        self.category_urls = list(self.category_mapping.keys())
        self.current_category_index = 0
        self.keywords_mapping = self._load_keywords_mapping_eserver()
    
    def _load_category_mapping(self):
        """Завантажує маппінг категорій з CSV"""
        mapping = {}
        csv_path = Path(r"C:\FullStack\Scrapy\data\eserver\eserver_category_retail.csv")
        
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
                },
                dont_filter=True,
            )
    
    def parse_category(self, response):
        """Парсимо список товарів у категорії та сторінки пагінації"""
        category_url = response.meta["category_url"]
        category_index = response.meta["category_index"]
        page_number = response.meta.get("page_number", 1)
        category_info = self.category_mapping.get(category_url, {})
        
        self.logger.info(f"📂 Обробляю категорію [{category_index + 1}/{len(self.category_urls)}] сторінка {page_number}: {response.url}")
        
        # Посилання на товари - використовуємо універсальний селектор карточок
        # Сайт використовує різні URL структури: з -detail та без
        product_links = response.css("div[class*='card'] a[href*='/uk/']::attr(href)").getall()
        
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
        
        # ПАГІНАЦІЯ
        next_page_link = response.css("li.next a::attr(href)").get()
        
        if not next_page_link and len(product_links) > 0:
            next_page_link = self._build_next_page_url(category_url, page_number, len(product_links))
        
        if next_page_link:
            self.logger.info(f"📄 Перехід на наступну сторінку пагінації ({page_number + 1}): {next_page_link}")
            
            yield response.follow(
                url=next_page_link,
                callback=self.parse_category,
                meta={
                    "category_url": category_url,
                    "category_index": category_index,
                    "page_number": page_number + 1,
                },
                dont_filter=True,
            )
        else:
            self.logger.info(f"✅ ПАГІНАЦІЯ ЗАВЕРШЕНА [{category_index + 1}/{len(self.category_urls)}]: накопичено {len(self.products_from_pagination)} товарів")
            
            if self.products_from_pagination:
                product_data = self.products_from_pagination.pop(0)
                product_data["meta"]["remaining_products"] = self.products_from_pagination
                product_data["meta"]["category_index"] = category_index
                
                self.logger.info(f"🔗 ЗАПУСК ланцюга продуктів. Перший: {product_data['url']}. Залишилось: {len(self.products_from_pagination)}")
                
                yield scrapy.Request(
                    url=product_data["url"],
                    callback=self.parse_product,
                    errback=self.parse_product_error,
                    meta=product_data["meta"],
                    dont_filter=True,
                )
            else:
                self.logger.warning(f"⚠️ У категорії {category_url} не знайдено товарів. Переходжу до наступної.")
                yield self._start_next_category(category_index)
            
            self.products_from_pagination = []
    
    def _build_next_page_url(self, category_url, current_page, products_count):
        """Будує URL наступної сторінки"""
        if products_count == 0:
            return None
        
        next_page_number = current_page + 1
        
        if '/page/' in category_url:
            return re.sub(r'/page/\d+', f'/page/{next_page_number}', category_url)
        else:
            clean_url = category_url.rstrip('/')
            return f"{clean_url}/page/{next_page_number}"
    
    def parse_product(self, response):
        """Парсимо сторінку товару - шукаємо посилання на обидві мови через перемикач"""
        try:
            self.logger.info(f"🔗 Парсимо товар (пошук мов): {response.url}")
            
            # Шукаємо перемикач мови
            # Селектор: <a href="/uk/..."><div>Укр</div></a>
            # Селектор: <a href="/servernye-shkafy/..."><div>Рус</div></a>
            ua_link = response.css("div.langs_langs__QyR6J a[href*='/uk/']::attr(href)").get()
            ru_link = response.css("div.langs_langs__QyR6J a:not([href*='/uk/'])::attr(href)").get()
            
            if not ua_link or not ru_link:
                self.logger.error(f"❌ Не знайдено посилань на мови: UA={ua_link}, RU={ru_link}")
                yield from self._skip_product(response.meta)
                return
            
            # Нормалізуємо URL
            ua_url = response.urljoin(ua_link)
            ru_url = response.urljoin(ru_link)
            
            self.logger.info(f"🌐 Знайдено мови: UA={ua_url}, RU={ru_url}")
            
            # Переходимо на українську версію
            yield scrapy.Request(
                url=ua_url,
                callback=self.parse_product_ua,
                errback=self.parse_product_error,
                meta={
                    **response.meta,
                    "ru_url": ru_url,
                    "original_url": response.url,
                },
                dont_filter=True,
            )
            
        except Exception as e:
            self.logger.error(f"❌ Помилка парсингу перемикача мов: {response.url} | {e}")
            yield from self._skip_product(response.meta)
            return
    
    def parse_product_ua(self, response):
        """Парсимо українську версію товару"""
        try:
            self.logger.info(f"🔗 Парсимо товар (UA): {response.url}")
            
            # Селектор для h1 з класом es-h1
            name_ua = response.css("h1.es-h1::text").get()
            if not name_ua:
                name_ua = response.css("h1::text").get()
            name_ua = name_ua.strip() if name_ua else ""
            
            description_ua = self._extract_description_from_html(response)
            specs_list_ua = self._extract_specifications_eserver(response)
            
            self.logger.info(f"📊 Характеристик (UA) знайдено: {len(specs_list_ua)} шт.")
            
            ru_url = response.meta.get("ru_url")
            
            # Переходимо на російську версію
            yield scrapy.Request(
                url=ru_url,
                callback=self.parse_product_ru,
                errback=self.parse_product_error,
                meta={
                    **response.meta,
                    "name_ua": name_ua,
                    "description_ua": description_ua,
                    "specifications_list": specs_list_ua,
                },
                dont_filter=True,
            )
            
        except Exception as e:
            self.logger.error(f"❌ Помилка парсингу продукту (UA): {response.url} | {e}")
            yield from self._skip_product(response.meta)
            return
    
    def parse_product_ru(self, response):
        """Парсимо російську версію товару та продовжуємо ланцюг"""
        try:
            self.logger.info(f"🔗 Парсимо товар (RU): {response.url}")
            
            # Селектор для h1 з класом es-h1
            name_ru = response.css("h1.es-h1::text").get()
            if not name_ru:
                name_ru = response.css("h1::text").get()
            name_ru = name_ru.strip() if name_ru else ""
            
            description_ru = self._extract_description_from_html(response)
            
            name_ua = response.meta.get("name_ua", "")
            description_ua = response.meta.get("description_ua", "")
            specs_list = response.meta.get("specifications_list", [])
            
            # Ціна
            price_raw = response.css("div.flex.items-end.font-bold.text-23px::text").get()
            if not price_raw:
                price_raw = response.css("div[class*='price']::text").get()
            price = self._clean_price(price_raw) if price_raw else ""
            
            # Наявність - РОЗШИРЕНЕ ВИТЯГУВАННЯ З ЛОГУВАННЯМ
            availability_raw = ""
            
            # Спробуємо різні селектори
            availability_element = response.css("div.product_ag-sts__x60QA")
            if availability_element:
                availability_text = availability_element.css("::text").getall()
                availability_raw = " ".join([t.strip() for t in availability_text if t.strip()])
                self.logger.info(f"📦 Наявність (селектор 1): '{availability_raw}'")
            
            # Альтернативний селектор 1: загальний пошук тексту з "наявності" або "наличии"
            if not availability_raw:
                all_text = response.css("*::text").getall()
                for text in all_text:
                    text_lower = text.lower().strip()
                    if "наявност" in text_lower or "налич" in text_lower:
                        availability_raw = text.strip()
                        self.logger.info(f"📦 Наявність (селектор 2 - пошук): '{availability_raw}'")
                        break
            
            # Альтернативний селектор 2: шукаємо в div з класами що містять "status", "stock", "available"
            if not availability_raw:
                status_divs = response.css("div[class*='status'], div[class*='stock'], div[class*='available']")
                for div in status_divs:
                    text = " ".join(div.css("::text").getall()).strip()
                    if text:
                        availability_raw = text
                        self.logger.info(f"📦 Наявність (селектор 3 - div): '{availability_raw}'")
                        break
            
            # Якщо нічого не знайдено - логуємо попередження з HTML
            if not availability_raw:
                self.logger.warning(f"⚠️ НЕ ЗНАЙДЕНО наявності для: {response.url}")
                # Логуємо фрагмент HTML для дебагу
                product_section = response.css("div[class*='product']").get()
                if product_section:
                    self.logger.warning(f"HTML фрагмент: {product_section[:500]}...")
                # За замовчуванням вважаємо В НАЯВНОСТІ (бо в категорії фільтр only-inStock)
                availability_raw = "В наявності"
            
            # Зображення
            image_url = self._extract_image_from_srcset(response)
            
            # Виробник
            manufacturer = self._extract_manufacturer(name_ru)
            
            # Пошукові запити з урахуванням ключових слів
            subdivision_id = response.meta.get("subdivision_id", "")
            search_terms_ru = self._generate_search_terms(name_ru, subdivision_id, lang="ru")
            search_terms_ua = self._generate_search_terms(name_ua, subdivision_id, lang="ua")
            
            # Кількість
            quantity = self._extract_quantity(availability_raw)
            
            self.logger.info(f"📝 Опис RU: {len(description_ru)} символів")
            self.logger.info(f"📝 Опис UA: {len(description_ua)} символів")
            
            item = {
                "Код_товару": "",
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
                "Виробник": manufacturer,
                "Країна_виробник": "",
                "price_type": self.price_type,
                "supplier_id": self.supplier_id,
                "output_file": self.output_filename,
                "Продукт_на_сайті": response.meta.get("original_url", response.url),
                "specifications_list": specs_list,
            }
            
            self.logger.info(f"✅ YIELD: {item['Назва_позиції']} | Ціна: {item['Ціна']} | Характеристик: {len(specs_list)}")
            yield item
            
            yield from self._skip_product(response.meta)
        
        except Exception as e:
            self.logger.error(f"❌ Помилка парсингу продукту (RU): {response.url} | {e}")
            yield from self._skip_product(response.meta)
            return
    
    def parse_product_error(self, failure):
        """Обробка помилок завантаження товару"""
        url = failure.request.url
        reason = failure.value
        product_name = failure.request.meta.get("name_ua", "Назва не знайдена")
        
        self.logger.error(f"❌ Помилка завантаження товару: {product_name} ({url}). Причина: {reason}")
        self.failed_products.append({"url": url, "reason": str(reason), "product_name": product_name})
        
        meta = failure.request.meta
        remaining = meta.get("remaining_products", [])
        category_index = meta.get("category_index")
        
        if remaining:
            next_data = remaining.pop(0)
            next_data["meta"]["remaining_products"] = remaining
            next_data["meta"]["category_index"] = category_index
            
            self.logger.info(f"⏭️ Пропускаю товар. Залишилось: {len(remaining)}")
            yield scrapy.Request(
                url=next_data["url"],
                callback=self.parse_product,
                errback=self.parse_product_error,
                meta=next_data["meta"],
                dont_filter=True,
            )
        else:
            self.logger.info(f"⏭️ Всі товари категорії оброблені (з помилками).")
            next_cat = self._start_next_category(category_index)
            if next_cat:
                yield next_cat
    
    def _skip_product(self, meta):
        """Перехід до наступного товару в ланцюгу"""
        remaining = meta.get("remaining_products", [])
        category_index = meta.get("category_index")
        
        if remaining:
            next_data = remaining.pop(0)
            next_data["meta"]["remaining_products"] = remaining
            next_data["meta"]["category_index"] = category_index
            
            self.logger.info(f"⏭️ Перехід до наступного товару. Залишилось: {len(remaining)}")
            yield scrapy.Request(
                url=next_data["url"],
                callback=self.parse_product,
                errback=self.parse_product_error,
                meta=next_data["meta"],
                dont_filter=True,
            )
        else:
            self.logger.info(f"⏭️ Товари категорії закінчились.")
            next_cat = self._start_next_category(category_index)
            if next_cat:
                yield next_cat
    
    def _start_next_category(self, current_category_index):
        """Допоміжний метод для запуску наступної категорії"""
        next_category_index = current_category_index + 1
        if next_category_index < len(self.category_urls):
            next_category_url = self.category_urls[next_category_index]
            self.logger.info(f"🚀 СТАРТ НАСТУПНОЇ КАТЕГОРІЇ [{next_category_index + 1}/{len(self.category_urls)}]: {next_category_url}")
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
            self.logger.info(f"🎉🎉🎉 ВСІ КАТЕГОРІЇ ТА ПРОДУКТИ ОБРОБЛЕНІ 🎉🎉🎉")
            return None
    
    def _extract_image_from_srcset(self, response):
        """Витягує найбільше зображення з srcset"""
        srcset = response.css("img[alt*='фото']::attr(srcset)").get()
        
        if srcset:
            urls = re.findall(r'(https?://[^\s]+)\s+\d+w', srcset)
            if urls:
                return urls[-1]
        
        # Fallback на src
        image_url = response.css("img[alt*='фото']::attr(src)").get()
        if not image_url:
            image_url = response.css("img[src*='storage']::attr(src)").get()
        
        if image_url and not image_url.startswith('http'):
            image_url = response.urljoin(image_url)
        
        return image_url or ""
    
    def _extract_specifications_eserver(self, response):
        """Екстракт характеристик з таблиці e-server"""
        specs = []
        
        spec_container = response.css("div.bg-white")
        if not spec_container:
            self.logger.warning(f"⚠️ Не знайдено контейнер характеристик: {response.url}")
            return specs
        
        spec_rows = spec_container.css("div.flex.justify-between.mx-3")
        
        for row in spec_rows:
            name_element = row.css("div.font-semibold::text").get()
            name = name_element.strip() if name_element else ""
            
            # Витягуємо ВСІ текстові вузли зі значення (включаючи багаторядкові)
            value_elements = row.css("div.text-right::text, div.whitespace-pre-line::text").getall()
            if not value_elements:
                value_elements = row.css("div.font-medium::text").getall()
            
            # Об'єднуємо всі текстові вузли, замінюючи переноси на <br> для збереження форматування
            value = "<br>".join([v.strip() for v in value_elements if v.strip()])
            
            if name and value:
                specs.append({
                    "name": name,
                    "unit": "",
                    "value": value,
                })
        
        return specs
    
    def _extract_description_from_html(self, response):
        """Екстракт тексту опису з HTML"""
        description_container = response.css("div.product_pg-dsc__h3fai")
        
        if not description_container:
            return ""
        
        paragraphs = description_container.css("p::text").getall()
        
        if paragraphs:
            return "\n".join([p.strip() for p in paragraphs if p.strip()])
        
        all_text = description_container.css("::text").getall()
        return " ".join([t.strip() for t in all_text if t.strip()])
    
    def _load_keywords_mapping_eserver(self):
        """Завантажує маппінг ключових слів для eserver з CSV"""
        import csv
        mapping = {}
        csv_path = Path(r"C:\FullStack\Scrapy\data\eserver\eserver_keywords.csv")
        
        if not csv_path.exists():
            self.logger.warning("eserver_keywords.csv not found")
            return mapping
        
        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    subdivision_id = row["Ідентифікатор_підрозділу"].strip()
                    mapping[subdivision_id] = {
                        "keywords_ru": [w.strip() for w in row["keywords_ru"].strip('"').split(",") if w.strip()],
                        "keywords_ua": [w.strip() for w in row["keywords_ua"].strip('"').split(",") if w.strip()],
                        "characteristics_ru": [w.strip() for w in row["characteristics_ru"].strip('"').split(",") if w.strip()],
                        "characteristics_ua": [w.strip() for w in row["characteristics_ua"].strip('"').split(",") if w.strip()],
                    }
            self.logger.info(f"✅ Завантажено {len(mapping)} підрозділів з ключовими словами для eserver")
        except Exception as e:
            self.logger.warning(f"⚠️ Помилка завантаження eserver_keywords.csv: {e}")
        
        return mapping
    
    def _generate_search_terms(self, title: str, subdivision_id: str = "", lang: str = "ua") -> str:
        """Генерує пошукові терміни з назви товару та ключових слів"""
        if not title:
            return ""
        
        components = self._extract_model_components(title, lang)
        
        # Додаємо ключові слова з CSV (БЛОК 2)
        if subdivision_id and subdivision_id in self.keywords_mapping:
            keywords_key = f"keywords_{lang}"
            characteristics_key = f"characteristics_{lang}"
            
            category_keywords = self.keywords_mapping[subdivision_id].get(keywords_key, [])
            characteristics = self.keywords_mapping[subdivision_id].get(characteristics_key, [])
            
            # Об'єднуємо ключові слова та характеристики
            all_keywords = category_keywords + characteristics
            
            # Обираємо максимум 12 ключових слів
            components.extend(all_keywords[:12])
        
        # Видаляємо дублікати
        seen = set()
        unique_terms = []
        for term in components:
            term_lower = term.lower()
            if term_lower not in seen:
                unique_terms.append(term)
                seen.add(term_lower)
        
        return ", ".join(unique_terms[:20])  # Обмежуємо до 20 термінів

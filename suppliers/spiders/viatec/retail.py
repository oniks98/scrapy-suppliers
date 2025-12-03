"""
Spider для парсингу роздрібних цін з viatec.ua (UAH)
Вигружає дані в: output/viatec_retail.csv

ПОСЛІДОВНА ОБРОБКА: категорія → всі сторінки пагінації → наступна категорія
ХАРАКТЕРИСТИКИ: парсяться УКРАЇНСЬКОЮ (UA) мовою
"""
import scrapy
import csv
from pathlib import Path
from suppliers.spiders.base import ViatecBaseSpider, BaseRetailSpider


class ViatecRetailSpider(ViatecBaseSpider, BaseRetailSpider):
    name = "viatec_retail"
    supplier_id = "viatec"
    output_filename = "viatec_retail.csv"
    
    custom_settings = {
        **ViatecBaseSpider.custom_settings,
        "ITEM_PIPELINES": {
            "suppliers.pipelines.SuppliersPipeline": 300,
        }
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category_mapping = self._load_category_mapping()
        self.category_urls = list(self.category_mapping.keys())
        self.current_category_index = 0
    
    def _load_category_mapping(self):
        """Завантажує маппінг категорій з CSV"""
        mapping = {}
        csv_path = Path(r"C:\FullStack\Scrapy\data\viatec\viatec_category_retail.csv")
        
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
        
        self.logger.info(f"📂 Обробляю категорію [{category_index + 1}/{len(self.category_urls)}] сторінка {page_number}: {category_url}")
        
        product_links = response.css("a[href*='/product/']::attr(href)").getall()
        
        if not product_links:
            self.logger.warning(f"⚠️ Не знайдено товарів на сторінці: {response.url}")
        else:
            self.logger.info(f"📦 Знайдено товарів на сторінці: {len(product_links)}")
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
            self.logger.info(f"📄 Перехід на наступну сторінку пагінації ({page_number + 1}): {next_page_link}")
            yield response.follow(
                next_page_link,
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
    
    def parse_product(self, response):
        """Парсимо сторінку товару (українська версія) - НАЗВА, ОПИС, ХАРАКТЕРИСТИКИ"""
        try:
            self.logger.info(f"🔗 Парсимо товар (UA): {response.url}")
            
            name_ua = response.css("h1::text").get()
            name_ua = name_ua.strip() if name_ua else ""
            
            description_ua = self._extract_description_with_br(response)
            specs_list_ua = self._extract_specifications(response)
            
            self.logger.info(f"📐 Характеристик (UA) знайдено: {len(specs_list_ua)} шт.")
            
            ru_url = self._convert_to_ru_url(response.url)
            
            yield scrapy.Request(
                url=ru_url,
                callback=self.parse_product_ru,
                errback=self.parse_product_error,
                meta={
                    **response.meta,
                    "name_ua": name_ua,
                    "description_ua": description_ua,
                    "specifications_list": specs_list_ua,
                    "original_url": response.url,
                },
                dont_filter=True,
            )
        except Exception as e:
            self.logger.error(f"❌ Помилка парсингу продукту (UA): {response.url} | {e}")
            yield from self._skip_product(response.meta)
            return
    
    def parse_product_ru(self, response):
        """Парсимо сторінку товару (російська версія) та продовжуємо ланцюг"""
        try:
            self.logger.info(f"🔗 Парсимо товар (RU): {response.url}")
            
            name_ru = response.css("h1::text").get()
            name_ru = name_ru.strip() if name_ru else ""
            
            description_ru = self._extract_description_with_br(response)
            
            name_ua = response.meta.get("name_ua", "")
            description_ua = response.meta.get("description_ua", "")
            specs_list = response.meta.get("specifications_list", [])
            
            code = ""
            price_raw = response.css("div.card-header__card-price-new::text").get()
            price_raw = price_raw.strip().replace("&nbsp;", "").replace(" ", "") if price_raw else ""
            price = self._clean_price(price_raw) if price_raw else ""
            
            self.logger.info(f"📝 Опис RU: {len(description_ru)} символів")
            self.logger.info(f"📝 Опис UA: {len(description_ua)} символів")
            
            images = response.css("img.card-header__card-images-image::attr(src)").getall()
            image_url = response.urljoin(images[0]) if images else ""
            
            availability_raw_text = response.css("div.card-header__card-status-badge::text").get()
            availability_status = self._normalize_availability(availability_raw_text)
            quantity = self._extract_quantity(availability_raw_text)
            
            manufacturer = self._extract_manufacturer(name_ru)
            
            group_number = response.meta.get("group_number", "")
            search_terms_ru = self._generate_search_terms(name_ru, group_number, lang="ru")
            search_terms_ua = self._generate_search_terms(name_ua, group_number, lang="ua")
            
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
                "Валюта": self.currency,
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
        url = failure.request.url
        reason = failure.value
        product_name = failure.request.meta.get("name_ru", "Назва не знайдена")
        
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

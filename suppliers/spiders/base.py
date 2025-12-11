"""
Базові класи для всіх пауків-постачальників.
Мінімізує дублювання коду та забезпечує уніфікований підхід.
"""
import scrapy
import re
from pathlib import Path
from typing import Optional, Dict, List


class BaseSupplierSpider(scrapy.Spider):
    """Базовий клас для всіх пауків постачальників"""
    
    # Налаштування за замовчуванням (можна перевизначити в дочірніх класах)
    custom_settings = {
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 60,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2.0,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_products = set()
        self.failed_products = []
    
    def _clean_price(self, price_str: str) -> str:
        """Очищення ціни від зайвих символів"""
        if not price_str:
            return ""
        
        price_str = price_str.replace(" ", "").replace("грн", "").replace("₴", "")
        price_str = price_str.replace("у.е.", "").replace("$", "").replace("USD", "")
        price_str = price_str.replace(",", ".")
        
        try:
            cleaned = "".join(c for c in price_str if c.isdigit() or c == ".")
            return str(float(cleaned)) if cleaned else ""
        except ValueError:
            return ""
    
    def _normalize_availability(self, availability: Optional[str]) -> str:
        """Нормалізація статусу наявності"""
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
    
    def _extract_quantity(self, text: Optional[str]) -> str:
        """Витягує кількість з тексту наявності"""
        if not text:
            return ""
        
        quantity_match = re.search(r'\d+', text)
        if quantity_match:
            return quantity_match.group(0)
        
        return ""
    
    def _sanitize_image_url(self, url: str) -> str:
        """Екранує спеціальні символи в URL зображень для PROM
        
        PROM не приймає URL із запятими - потрібно замінити на %2C
        """
        if not url:
            return ""
        
        # Замінюємо запятую на %2C
        url = url.replace(",", "%2C")
        
        return url
    
    def _load_keywords_mapping(self) -> Dict[str, Dict[str, List[str]]]:
        """Завантажує маппінг ключових слів з CSV за Ідентифікатор_підрозділу
        
        Структура:
        {
            "301105": {
                "keywords_ru": [...],  # Категорійні ключі
                "keywords_ua": [...],
                "characteristics_ru": [...],  # Характеристичні ключі
                "characteristics_ua": [...]
            }
        }
        """
        import csv
        mapping = {}
        csv_path = Path(r"C:\FullStack\Scrapy\data\viatec\viatec_keywords.csv")
        if not csv_path.exists():
            self.logger.warning("viatec_keywords.csv not found")
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
            self.logger.info(f"✅ Завантажено {len(mapping)} підрозділів з ключовими словами")
        except Exception as e:
            self.logger.warning(f"⚠️ Помилка завантаження viatec_keywords.csv: {e}")
        return mapping
    
    def _extract_model_components(self, title: str, lang: str = "ua") -> List[str]:
        """Витягує ключові компоненти з назви товару (БЛОК 1)
        
        Приклад входу: "Turbo HD відеокамера Hikvision DS-2CE16H0T-ITF(С) 5МП (2.8мм)"
        Результат:
        - DS-2CE16H0T-ITF
        - Hikvision DS-2CE16H0T-ITF
        - Turbo HD Hikvision
        - 5MP Hikvision
        - Hikvision 2.8mm
        """
        if not title:
            return []
        
        components = []
        title_lower = title.lower()
        
        # 1. Витягуємо бренди
        brands = ["hikvision", "dahua", "ezviz", "imou", "uniview", "axis", "tp-link", "mikrotik", 
                  "ajax", "ubiquiti", "wd", "western digital", "seagate", "pulsar", "infiray", "dji"]
        
        detected_brand = None
        for brand in brands:
            if brand in title_lower:
                # Знаходимо бренд з оригінальним регістром
                brand_idx = title_lower.find(brand)
                detected_brand = title[brand_idx:brand_idx+len(brand)]
                break
        
        # 2. Витягуємо код моделі (зазвичай з дефісами та цифрами)
        model_pattern = re.compile(r'[A-Z]{2,}-[A-Z0-9-]+[A-Z0-9](?:\([A-Zа-яА-Я]\))?', re.IGNORECASE)
        model_matches = model_pattern.findall(title)
        
        model_code = None
        if model_matches:
            # Беремо перше знайдене значення
            model_code = model_matches[0]
            components.append(model_code)
        
        # 3. Бренд + модель
        if detected_brand and model_code:
            components.append(f"{detected_brand} {model_code}")
        
        # 4. Витягуємо технологію (Turbo HD, IP, AHD, TVI, тощо)
        technologies = ["turbo hd", "ip", "ahd", "tvi", "cvi", "analog", "nvr", "dvr", "hybrid"]
        for tech in technologies:
            if tech in title_lower and detected_brand:
                tech_idx = title_lower.find(tech)
                tech_original = title[tech_idx:tech_idx+len(tech)]
                components.append(f"{tech_original} {detected_brand}")
                break
        
        # 5. Роздільність (2MP, 4MP, 5MP, 8MP, тощо)
        resolution_pattern = re.compile(r'\d+\s*[Mm][Pp]|д+\s*МП', re.IGNORECASE)
        resolution_match = resolution_pattern.search(title)
        if resolution_match and detected_brand:
            resolution = resolution_match.group(0)
            components.append(f"{resolution} {detected_brand}")
        
        # 6. Фокусна відстань (2.8mm, 3.6mm, тощо)
        focal_pattern = re.compile(r'\d+\.\d+\s*мм', re.IGNORECASE)
        focal_match = focal_pattern.search(title)
        if focal_match and detected_brand:
            focal = focal_match.group(0)
            components.append(f"{detected_brand} {focal}")
        
        # 7. Канали (для реєстраторів: 4, 8, 16, 32 каналів/каналов)
        if lang == "ua" and "канал" in title_lower:
            channels_pattern = re.compile(r'(\d+)\s*канал', re.IGNORECASE)
            channels_match = channels_pattern.search(title)
            if channels_match:
                components.append(f"реєстратор {channels_match.group(1)} каналів")
        elif lang == "ru" and "канал" in title_lower:
            channels_pattern = re.compile(r'(\d+)\s*канал', re.IGNORECASE)
            channels_match = channels_pattern.search(title)
            if channels_match:
                components.append(f"регистратор {channels_match.group(1)} каналов")
        
        # 8. Ємність (1TB, 2TB, тощо - для HDD)
        capacity_pattern = re.compile(r'\d+\s*[TtГг][BbБб]', re.IGNORECASE)
        capacity_match = capacity_pattern.search(title)
        if capacity_match:
            capacity = capacity_match.group(0).upper()
            components.append(f"HDD {capacity}")
        
        # Видаляємо дублікати
        seen = set()
        unique_components = []
        for comp in components:
            comp_lower = comp.lower()
            if comp_lower not in seen:
                unique_components.append(comp)
                seen.add(comp_lower)
        
        return unique_components[:8]  # Обмежуємо до 8 компонентів
    
    def _generate_search_terms(self, product_name: str, subdivision_id: str = "", lang: str = "ua") -> str:
        """Генерує пошукові запити за логікою:
        
        БЛОК 1: Модельні ключі (5-8 шт.) - з назви товару
        БЛОК 2: Характеристичні ключі - з characteristics, яких НЕМАЄ в назві (до 18 разом з БЛОК 1)
        БЛОК 3: Категорійні ключі - всі доступні з keywords
        
        Мінімум: 8 ключів (з попередженням якщо менше)
        Максимум: без обмежень
        """
        if not product_name:
            return ""
        
        # Завантажуємо ключі за Ідентифікатор_підрозділу один раз
        if not hasattr(self, "_keywords_cache"):
            self._keywords_cache = self._load_keywords_mapping()
        
        result = []
        seen = set()
        product_name_lower = product_name.lower()
        
        # БЛОК 1: Модельні ключі (5-8 шт.) - витягуємо з назви
        model_components = self._extract_model_components(product_name, lang)
        for comp in model_components:
            comp_lower = comp.lower()
            if comp_lower not in seen:
                result.append(comp)
                seen.add(comp_lower)
        
        # БЛОК 2: Характеристичні ключі (6-10 шт.) - з CSV, яких НЕМАЄ в назві
        if subdivision_id and subdivision_id in self._keywords_cache:
            lang_key = f"characteristics_{lang}" if lang in ["ua", "ru"] else "characteristics_ua"
            characteristics = self._keywords_cache[subdivision_id].get(lang_key, [])
            
            for char in characteristics:
                char_lower = char.lower()
                
                # Перевірка 1: Чи не є ця фраза повністю підфразою назви?
                if char_lower in product_name_lower:
                    continue  # Пропускаємо, бо точно є в назві
                
                # Перевірка 2: Чи не співпадають всі ключові слова?
                char_words = [w for w in char_lower.split() if len(w) > 2]
                if not char_words:  # Якщо немає значущих слів
                    continue
                
                # Рахуємо скільки слів є в назві
                words_in_title = sum(1 for word in char_words if word in product_name_lower)
                
                # Якщо більше 70% слів є в назві - пропускаємо
                if len(char_words) > 0 and words_in_title / len(char_words) > 0.7:
                    continue
                
                # Додаємо якщо ще не було
                if char_lower not in seen:
                    result.append(char)
                    seen.add(char_lower)
                    if len(result) >= 18:  # Обмежуємо БЛОК 1 + БЛОК 2 до 18 ключів
                        break
        
        # БЛОК 3: Категорійні ключі (всі доступні) - завжди додаємо
        if subdivision_id and subdivision_id in self._keywords_cache:
            lang_key = f"keywords_{lang}" if lang in ["ua", "ru"] else "keywords_ua"
            category_keywords = self._keywords_cache[subdivision_id].get(lang_key, [])
            
            for kw in category_keywords:  # Додаємо всі категорійні ключі
                kw_lower = kw.lower()
                if kw_lower not in seen:
                    result.append(kw)
                    seen.add(kw_lower)
        
        # Гарантуємо мінімум 8 ключів
        if len(result) < 8:
            # Якщо менше 8 ключів, логуємо попередження
            self.logger.warning(f"Недостатньо ключових слів для товару. Знайдено: {len(result)} (мінімум: 8)")
        
        return ", ".join(result)


class BaseRetailSpider(BaseSupplierSpider):
    """Базовий клас для роздрібних пауків"""
    
    price_type = "retail"
    currency = "UAH"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Ініціалізація специфічних для роздрібу властивостей
        if not hasattr(self, 'supplier_id'):
            raise ValueError(f"Spider {self.name} must define 'supplier_id' attribute")
        
        if not hasattr(self, 'output_filename'):
            self.output_filename = f"{self.supplier_id}_retail.csv"


class BaseDealerSpider(BaseSupplierSpider):
    """Базовий клас для дилерських пауків"""
    
    price_type = "dealer"
    currency = "USD"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Ініціалізація специфічних для дилерів властивостей
        if not hasattr(self, 'supplier_id'):
            raise ValueError(f"Spider {self.name} must define 'supplier_id' attribute")
        
        if not hasattr(self, 'output_filename'):
            self.output_filename = f"{self.supplier_id}_dealer.csv"


class EserverBaseSpider(BaseSupplierSpider):
    """Базовий клас для пауків E-Server (загальна логіка для retail і dealer)"""
    
    allowed_domains = ["e-server.com.ua"]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category_urls = []
        self.products_from_pagination = []
    
    def _extract_manufacturer(self, product_name: str) -> str:
        """Визначає виробника з назви товару"""
        if not product_name:
            return ""
        
        product_name_lower = product_name.lower()
        
        # ПРІОРИТЕТ 1: Явні згадки брендів
        priority_patterns = {
            "eserver": "EServer",
            "e-server": "EServer",
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
        
        return ""
    
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


class ViatecBaseSpider(BaseSupplierSpider):
    """Базовий клас для пауків Viatec (загальна логіка для retail і dealer)"""
    
    allowed_domains = ["viatec.ua"]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category_urls = []
        self.products_from_pagination = []
    
    def _extract_manufacturer(self, product_name: str) -> str:
        """Визначає виробника з назви товару"""
        if not product_name:
            return ""
        
        product_name_lower = product_name.lower()
        
        # ПРІОРИТЕТ 1: Явні згадки брендів
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
        
        # ПРІОРИТЕТ 2: Коди продуктів з дефісом
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
        
        # ПРІОРИТЕТ 3: Маппінг з CSV
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
    
    def _load_manufacturers_from_csv(self) -> Dict[str, str]:
        """Завантажує маппінг виробників з CSV"""
        import csv
        mapping = {}
        try:
            csv_path = Path(r"C:\FullStack\Scrapy\data\viatec\viatec_manufacturers.csv")
            if csv_path.exists():
                with open(csv_path, encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f, delimiter=";")
                    for row in reader:
                        keyword = row.get("Слово в названии продукта", "").strip()
                        manufacturer = row.get("Производитель (виробник)", "").strip()
                        if keyword and manufacturer:
                            mapping[keyword] = manufacturer
                self.logger.info(f"✅ Завантажено {len(mapping)} виробників з CSV")
        except Exception as e:
            self.logger.warning(f"⚠️ Помилка завантаження виробників: {e}")
        
        return mapping
    
    def _extract_description_with_br(self, response) -> str:
        """
        Витягує опис зі збереженням переносів <br> та обробкою списків <ul>
        """
        description_container = response.css("div.card-header__card-info-text")
        if not description_container:
            self.logger.warning(f"Не знайдено контейнер опису на {response.url}")
            return ""
        
        # Перевірка на наявність <ul>
        ul_list = description_container.css("ul")
        if ul_list:
            self.logger.info(f"Знайдено <ul> список в описі на {response.url}")
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
        
        # Обробка <p> тегів
        p_tags = description_container.css("p")
        if p_tags:
            self.logger.info(f"Знайдено <p> теги в описі на {response.url}")
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
        
        self.logger.warning(f"В контейнері опису не знайдено ні <ul>, ні <p> на {response.url}")
        return ""
    
    def _extract_specifications(self, response) -> List[Dict[str, str]]:
        """
        Витягує характеристики товару з таблиці (українські назви)
        """
        specs_list = []
        
        # Спроба 1: Активна вкладка
        spec_rows = response.css("li.card-tabs__item.active div.card-tabs__characteristic-content table tr")
        
        # Спроба 2: Будь-яка вкладка з характеристиками
        if not spec_rows:
            spec_rows = response.css("div.card-tabs__characteristic-content table tr")
        
        # Спроба 3: Загальний селектор таблиці
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
    
    def _convert_to_ru_url(self, url: str) -> str:
        """Конвертує український URL в російський"""
        if "/ru/" not in url:
            url = url.replace("viatec.ua/", "viatec.ua/ru/")
        return url
    
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

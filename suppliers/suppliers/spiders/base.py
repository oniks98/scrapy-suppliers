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
    
    def _generate_search_terms(self, product_name: str) -> str:
        """Генерація пошукових запитів"""
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
        
        return f"{product_name}, {', '.join(unique_words)}"


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

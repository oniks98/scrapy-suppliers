import csv
import re
from pathlib import Path
from itemadapter import ItemAdapter


class SuppliersPipeline:
    """
    Pipeline для записи данных в два CSV файла:
    - prom_import.csv (розничные цены UAH)
    - prom_diler_import.csv (дилерские цены USD)
    
    ФИЛЬТРАЦИЯ: 
    - Пропускает товары БЕЗ цены
    - Пропускает товары НЕ В НАЛИЧИИ
    - "В наличии" → "+"
    - "В наличии 5 шт" → Наявність: "+", Кількість: "5"
    
    ХАРАКТЕРИСТИКИ:
    - Формат PROM: повторяющиеся триплеты БЕЗ нумерации
    - Назва_Характеристики;Одиниця_виміру_Характеристики;Значення_Характеристики (x60 раз)
    """
    
    def __init__(self):
        self.retail_file = None
        self.dealer_file = None
        self.retail_writer = None
        self.dealer_writer = None
        
        # Поля CSV согласно формату PROM
        self.fieldnames_base = [
            "Код_товару",
            "Назва_позиції",
            "Назва_позиції_укр",
            "Пошукові_запити",
            "Пошукові_запити_укр",
            "Опис",
            "Опис_укр",
            "Тип_товару",
            "Ціна",
            "Валюта",
            "Одиниця_виміру",
            "Мінімальний_обсяг_замовлення",
            "Оптова_ціна",
            "Мінімальне_замовлення_опт",
            "Посилання_зображення",
            "Наявність",
            "Кількість",
            "Номер_групи",
            "Назва_групи",
            "Посилання_підрозділу",
            "Можливість_поставки",
            "Термін_поставки",
            "Спосіб_пакування",
            "Спосіб_пакування_укр",
            "Унікальний_ідентифікатор",
            "Ідентифікатор_товару",
            "Ідентифікатор_підрозділу",
            "Ідентифікатор_групи",
            "Виробник",
            "Країна_виробник",
            "Знижка",
            "ID_групи_різновидів",
            "Особисті_нотатки",
            "Продукт_на_сайті",
            "Термін_дії_знижки_від",
            "Термін_дії_знижки_до",
            "Ціна_від",
            "Ярлик",
            "HTML_заголовок",
            "HTML_заголовок_укр",
            "HTML_опис",
            "HTML_опис_укр",
            "Код_маркування_(GTIN)",
            "Номер_пристрою_(MPN)",
            "Вага,кг",
            "Ширина,см",
            "Висота,см",
            "Довжина,см",
            "Де_знаходиться_товар",
        ]
        
        # Путь к директории
        self.output_dir = Path(r"C:\FullStack\Scrapy\output")
        
        # Счетчики для последовательной нумерации
        self.retail_product_counter = 200000
        self.dealer_product_counter = 200000
        
        # Статистика
        self.retail_count = 0
        self.dealer_count = 0
        self.filtered_no_price = 0
        self.filtered_no_stock = 0
    
    def open_spider(self, spider):
        """Создаём файлы с РУЧНЫМ УПРАВЛЕНИЕМ записью заголовков"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        retail_path = self.output_dir / "prom_import.csv"
        dealer_path = self.output_dir / "prom_diler_import.csv"
        
        self.retail_file = open(retail_path, "w", encoding="utf-8", newline="", buffering=1)
        self.dealer_file = open(dealer_path, "w", encoding="utf-8", newline="", buffering=1)
        
        # Создаём заголовки ВРУЧНУЮ (без DictWriter для заголовков)
        self._write_header(self.retail_file)
        self._write_header(self.dealer_file)
        
        spider.logger.info(f"📝 Создан файл розницы: {retail_path}")
        spider.logger.info(f"📝 Создан файл дилера: {dealer_path}")
    
    def _write_header(self, file_obj):
        """Пишет заголовок с повторяющимися триплетами (БЕЗ нумерации)"""
        header_parts = self.fieldnames_base.copy()
        
        # Добавляем 60 повторяющихся триплетов характеристик
        for _ in range(60):
            header_parts.extend([
                "Назва_Характеристики",
                "Одиниця_виміру_Характеристики",
                "Значення_Характеристики",
            ])
        
        file_obj.write(";".join(header_parts) + "\n")
    
    def close_spider(self, spider):
        """Закрываем файлы и выводим статистику"""
        if self.retail_file:
            self.retail_file.close()
        
        if self.dealer_file:
            self.dealer_file.close()
        
        spider.logger.info("=" * 80)
        spider.logger.info("📊 СТАТИСТИКА ФИЛЬТРАЦИИ")
        spider.logger.info("=" * 80)
        spider.logger.info(f"✅ Розничных товаров записано: {self.retail_count}")
        spider.logger.info(f"✅ Дилерских товаров записано: {self.dealer_count}")
        spider.logger.info(f"❌ Отфильтровано без цены: {self.filtered_no_price}")
        spider.logger.info(f"❌ Отфильтровано без наличия: {self.filtered_no_stock}")
        spider.logger.info("=" * 80)
    
    def process_item(self, item, spider):
        """Обрабатываем каждый item с ФИЛЬТРАЦИЕЙ"""
        adapter = ItemAdapter(item)
        
        # ========== ФИЛЬТР 1: Проверка цены ==========
        price = adapter.get("Ціна", "")
        if not price or not self._is_valid_price(price):
            self.filtered_no_price += 1
            spider.logger.debug(
                f"⚠️ Пропущен товар без цены: {adapter.get('Назва_позиції', 'Unknown')}"
            )
            raise ValueError("Товар без цены")
        
        # ========== ФИЛЬТР 2: Проверка наличия ==========
        availability_raw = adapter.get("Наявність", "")
        availability_status = self._check_availability(availability_raw)
        
        if not availability_status:
            self.filtered_no_stock += 1
            spider.logger.debug(
                f"⚠️ Пропущен товар не в наличии: {adapter.get('Назва_позиції', 'Unknown')} [{availability_raw}]"
            )
            raise ValueError("Товар не в наличии")
        
        # ========== ОБРАБОТКА НАЛИЧИЯ ==========
        # ВАЖНО: используем количество ИЗ SPIDER, не пересчитываем!
        quantity = adapter.get("Кількість", "")
        spider.logger.debug(f"🔢 Quantity из spider: '{quantity}' | Availability raw: '{availability_raw}'")
        
        # Очистка и нормализация данных
        cleaned_item = self._clean_item(adapter, spider)
        
        # Обновляем поля наличия (ВАЖНО: после clean_item!)
        cleaned_item["Наявність"] = "+"
        cleaned_item["Кількість"] = quantity  # Используем значение из spider
        
        # ========== ГЕНЕРАЦИЯ ПОСЛЕДОВАТЕЛЬНОГО КОДА ==========
        price_type = adapter.get("price_type", "retail")
        
        if price_type == "dealer":
            cleaned_item["Код_товару"] = str(self.dealer_product_counter)
            self.dealer_product_counter += 1
            cleaned_item["Особисті_нотатки"] = "V"
        else:
            cleaned_item["Код_товару"] = str(self.retail_product_counter)
            self.retail_product_counter += 1
            cleaned_item["Особисті_нотатки"] = "PROM"
        
        # ========== ОБРАБОТКА ОПИСАНИЯ ==========
        cleaned_item["Опис"] = self._clean_description(cleaned_item.get("Опис", ""))
        cleaned_item["Опис_укр"] = self._clean_description(cleaned_item.get("Опис_укр", ""))
        
        # ========== ОБРАБОТКА ХАРАКТЕРИСТИК ==========
        specs_list = adapter.get("specifications_list", [])
        
        # Создаём ROW с базовыми полями + характеристиками
        row_parts = []
        
        # Базовые поля
        for field in self.fieldnames_base:
            value = cleaned_item.get(field, "")
            # Экранируем точку с запятой и кавычки
            value_str = str(value).replace(";", ",").replace('"', '""')
            row_parts.append(value_str)
        
        # Характеристики (60 триплетов)
        for i in range(60):
            if i < len(specs_list):
                spec = specs_list[i]
                row_parts.append(str(spec.get("name", "")).replace(";", ",").replace('"', '""'))
                row_parts.append(str(spec.get("unit", "")).replace(";", ",").replace('"', '""'))
                row_parts.append(str(spec.get("value", "")).replace(";", ",").replace('"', '""'))
            else:
                # Пустые триплеты
                row_parts.extend(["", "", ""])
        
        # Записываем строку в нужный файл
        row_line = ";".join(row_parts) + "\n"
        
        if price_type == "dealer":
            self.dealer_file.write(row_line)
            self.dealer_count += 1
            spider.logger.debug(
                f"💰 Дилер: {cleaned_item.get('Назва_позиції')} | Цена: {cleaned_item.get('Ціна')} | Характеристик: {len(specs_list)}"
            )
        else:
            self.retail_file.write(row_line)
            self.retail_count += 1
            spider.logger.debug(
                f"🛒 Розница: {cleaned_item.get('Назва_позиції')} | Цена: {cleaned_item.get('Ціна')} | Характеристик: {len(specs_list)}"
            )
        
        return item
    
    def _is_valid_price(self, price):
        """Проверка валидности цены"""
        if not price:
            return False
        
        try:
            price_float = float(str(price).replace(",", ".").replace(" ", ""))
            return price_float > 0
        except (ValueError, TypeError):
            return False
    
    def _check_availability(self, availability_str):
        """
        Проверка наличия товара
        Возвращает True если товар В НАЛИЧИИ, False если нет
        """
        if not availability_str:
            return False
        
        availability_lower = str(availability_str).lower()
        
        in_stock_keywords = [
            "є в наявності",
            "в наявності",
            "в наличии",
            "есть",
            "доступно",
            "available",
            "in stock",
        ]
        
        for keyword in in_stock_keywords:
            if keyword in availability_lower:
                return True
        
        out_of_stock_keywords = [
            "немає",
            "нет в наличии",
            "відсутній",
            "закінчився",
            "out of stock",
            "unavailable",
        ]
        
        for keyword in out_of_stock_keywords:
            if keyword in availability_lower:
                return False
        
        return False
    
    def _extract_quantity(self, availability_str):
        """Извлекает количество из строки вида 'В наличии 5 шт'"""
        if not availability_str:
            return ""
        
        match = re.search(r'\d+', str(availability_str))
        
        if match:
            return match.group()
        
        return ""
    
    def _clean_description(self, description):
        """Очищает описание от текста про аналоги и сохраняет переносы"""
        if not description:
            return ""
        
        patterns_to_remove = [
            r"Є товари з аналогічними характеристиками\s*→",
            r"Есть товары с аналогичными характеристиками\s*→",
        ]
        
        for pattern in patterns_to_remove:
            description = re.sub(pattern, "", description, flags=re.IGNORECASE)
        
        description = re.sub(r'\s+', ' ', description)
        
        return description.strip()
    
    def _clean_item(self, adapter, spider):
        """Очистка и нормализация данных"""
        cleaned = {}
        
        for field in self.fieldnames_base:
            value = adapter.get(field, "")
            
            if isinstance(value, str):
                value = value.strip()
            
            if field == "Ціна":
                value = self._clean_price(value)
            elif field == "Валюта":
                value = value.upper() if value else "UAH"
            elif field == "Одиниця_виміру":
                value = value if value else "шт."
            
            cleaned[field] = value
        
        return cleaned
    
    def _clean_price(self, price):
        """
        Очистка цены от лишних символов
        ЗАМЕНЯЕТ ТОЧКУ НА ЗАПЯТУЮ в цене
        """
        if not price:
            return ""
        
        price_str = str(price).replace(",", ".").replace(" ", "")
        price_str = price_str.replace("грн", "").replace("₴", "").replace("$", "").replace("USD", "")
        
        try:
            cleaned = "".join(c for c in price_str if c.isdigit() or c == ".")
            if cleaned:
                price_float = float(cleaned)
                return str(price_float).replace(".", ",")
            return ""
        except ValueError:
            return ""


class ValidationPipeline:
    """Дополнительный pipeline для валидации"""
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        if not adapter.get("Назва_позиції"):
            raise ValueError("Отсутствует название товара")
        
        if not adapter.get("Ціна"):
            raise ValueError("Отсутствует цена")
        
        try:
            float(str(adapter.get("Ціна")).replace(",", "."))
        except (ValueError, TypeError):
            raise ValueError(f"Некорректная цена: {adapter.get('Ціна')}")
        
        return item

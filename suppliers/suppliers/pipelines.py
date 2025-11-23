import csv
from pathlib import Path
from itemadapter import ItemAdapter


class SuppliersPipeline:
    """
    Pipeline для записи данных в два CSV файла:
    - prom_import.csv (розничные цены UAH)
    - prom_diler_import.csv (дилерские цены USD)
    
    ФИЛЬТРАЦИЯ: Выводит только товары с ценой и в наличии
    """
    
    def __init__(self):
        self.retail_file = None
        self.dealer_file = None
        self.retail_writer = None
        self.dealer_writer = None
        
        # Поля CSV согласно формату PROM (порядок важен!)
        self.fieldnames = [
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
        
        # Добавляем поля характеристик (30 штук по 3 поля каждая)
        for i in range(1, 31):
            self.fieldnames.extend([
                f"Назва_Характеристики_{i}",
                f"Одиниця_виміру_Характеристики_{i}",
                f"Значення_Характеристики_{i}",
            ])
        
        # Путь к директории для сохранения CSV
        self.output_dir = Path(r"C:\Users\stalk\Documents\Prom")
        
        # Счетчики успешных записей
        self.retail_count = 0
        self.dealer_count = 0
        self.filtered_count = 0
    
    def open_spider(self, spider):
        """Вызывается при запуске паука - создаём файлы и writers"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        retail_path = self.output_dir / "prom_import.csv"
        dealer_path = self.output_dir / "prom_diler_import.csv"
        
        # buffering=1 - построчная буферизация для реального времени
        self.retail_file = open(retail_path, "w", encoding="utf-8", newline="", buffering=1)
        self.dealer_file = open(dealer_path, "w", encoding="utf-8", newline="", buffering=1)
        
        self.retail_writer = csv.DictWriter(
            self.retail_file,
            fieldnames=self.fieldnames,
            delimiter=";",
            extrasaction="ignore",
        )
        self.dealer_writer = csv.DictWriter(
            self.dealer_file,
            fieldnames=self.fieldnames,
            delimiter=";",
            extrasaction="ignore",
        )
        
        self.retail_writer.writeheader()
        self.dealer_writer.writeheader()
        
        spider.logger.info(f"📝 Файл розницы: {retail_path}")
        spider.logger.info(f"📝 Файл дилера: {dealer_path}")
    
    def close_spider(self, spider):
        """Вызывается при завершении паука - закрываем файлы и логируем статистику"""
        if self.retail_file:
            self.retail_file.close()
        if self.dealer_file:
            self.dealer_file.close()
        
        spider.logger.info(f"✅ Записано розницы: {self.retail_count} товаров")
        spider.logger.info(f"✅ Записано дилера: {self.dealer_count} товаров")
        spider.logger.info(f"⏭️  Отфильтровано: {self.filtered_count} товаров")
    
    def process_item(self, item, spider):
        """
        Обрабатываем каждый item и записываем в CSV
        ФИЛЬТРАЦИЯ ВРЕМЕННО ОТКЛЮЧЕНА ДЛЯ ОТЛАДКИ!
        """
        adapter = ItemAdapter(item)
        
        # Обязательные поля
        name = adapter.get("Назва_позиції")
        price = adapter.get("Ціна")
        availability = adapter.get("Наявність")
        
        # Фильтрация #1: проверка названия
        if not name:
            spider.logger.warning(f"⏭️  Нет названия товара, пропускаем")
            self.filtered_count += 1
            return item
        
        # ⚠️ ВРЕМЕННО ОТКЛЮЧЕНО - ЗАПИСЫВАЕМ ВСЁ!
        spider.logger.info(f"💾 Записываем: {name} | Цена: {price} | Наличие: {availability}")
        
        # Очистка и нормализация данных
        cleaned_item = self._clean_item(adapter, spider)
        
        # Определяем тип цены и пишем в соответствующий файл
        price_type = adapter.get("price_type", "retail")
        
        if price_type == "dealer":
            self.dealer_writer.writerow(cleaned_item)
            self.dealer_file.flush()  # Принудительная запись на диск
            self.dealer_count += 1
            spider.logger.debug(f"💰 Дилер: {cleaned_item.get('Назва_позиції')}")
        else:
            self.retail_writer.writerow(cleaned_item)
            self.retail_file.flush()  # Принудительная запись на диск
            self.retail_count += 1
            spider.logger.debug(f"🛒 Розница: {cleaned_item.get('Назва_позиції')}")
        
        return item
    
    def _clean_item(self, adapter, spider):
        """Очистка и нормализация данных"""
        cleaned = {}
        
        for field in self.fieldnames:
            value = adapter.get(field, "")
            
            if isinstance(value, str):
                value = value.strip()
            
            # Обработка специфичных полей
            if field == "Ціна":
                value = self._clean_price(value)
            elif field == "Наявність":
                value = self._normalize_availability(value)
            elif field == "Валюта":
                value = value.upper() if value else "UAH"
            elif field == "Одиниця_виміру":
                value = value if value else "шт."
            
            cleaned[field] = value
        
        return cleaned
    
    def _clean_price(self, price):
        """Очистка цены: удаляем все кроме цифр и точки"""
        if not price:
            return ""
        
        price_str = str(price).replace(",", ".").replace(" ", "")
        
        try:
            cleaned = "".join(c for c in price_str if c.isdigit() or c == ".")
            return str(float(cleaned)) if cleaned else ""
        except ValueError:
            return ""
    
    def _normalize_availability(self, availability):
        """Нормализация наличия товара"""
        if not availability:
            return "Уточняйте"
        
        availability_lower = str(availability).lower()
        
        if any(word in availability_lower for word in ["є в наявності", "в наличии", "есть", "доступно", "в наявності"]):
            return "В наличии"
        elif any(word in availability_lower for word in ["під замовлення", "під заказ", "под заказ"]):
            return "Под заказ"
        elif any(word in availability_lower for word in ["немає", "нет", "відсутній"]):
            return "Нет в наличии"
        else:
            return "Уточняйте"


class ValidationPipeline:
    """
    Дополнительный pipeline для валидации (опционально)
    """
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        required_fields = ["Назва_позиції", "Ціна"]
        
        for field in required_fields:
            if not adapter.get(field):
                raise ValueError(f"❌ Отсутствует обязательное поле: {field}")
        
        return item

"""
Універсальний Pipeline для всіх постачальників.
Один pipeline керує записом у різні CSV файли на основі supplier_id та output_file.

ФІЛЬТРАЦІЯ:
- Пропускає товари БЕЗ ціни
- Пропускає товари НЕ В НАЯВНОСТІ
- "В наличии" → "+"
- "В наличии 5 шт" → Наявність: "+", Кількість: "5"

ХАРАКТЕРИСТИКИ:
- Формат PROM: повторювані триплети БЕЗ нумерації
- Назва_Характеристики;Одиниця_виміру_Характеристики;Значення_Характеристики (x60 разів)
"""
import re
import csv
from pathlib import Path
from itemadapter import ItemAdapter


class SuppliersPipeline:
    """Один pipeline для всіх постачальників"""
    
    def __init__(self):
        self.files = {}
        self.writers = {}
        self.viatec_dealer_coefficient = None
        self.personal_notes_mapping = {}
        
        # Базові поля CSV згідно формату PROM
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
        
        # Директорія для вихідних файлів (абсолютний шлях)
        self.output_dir = Path(r"C:\FullStack\Scrapy\output")
        
        # Лічильники для послідовної нумерації продуктів
        self.product_counters = {}
        
        # Статистика
        self.stats = {}
    
    def open_spider(self, spider):
        """Створюємо директорію output та файл при відкритті паука"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        spider.logger.info(f"✅ Pipeline відкрито для {spider.name}")
        spider.logger.info(f"📁 Вихідна директорія: {self.output_dir}")

        # --- Завантаження коефіцієнту (тільки для viatec_dealer) ---
        if spider.name == 'viatec_dealer':
            coefficient_path = r"C:\FullStack\Scrapy\data\viatec\viatec_coefficient_dealer.csv"
            try:
                with open(coefficient_path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f, delimiter=';')
                    row = next(reader)
                    coefficient_str = row[1].strip('"')
                    self.viatec_dealer_coefficient = float(coefficient_str.replace(',', '.'))
                    spider.logger.info(f"✅ Коефіцієнт для viatec_dealer завантажено: {self.viatec_dealer_coefficient}")
            except Exception as e:
                spider.logger.error(f"❌ Помилка завантаження коефіцієнту для viatec_dealer: {e}")

        # --- Універсальне завантаження особистих нотаток ---
        supplier_name = spider.name.split('_')[0]
        personal_notes_path = Path(r"C:\FullStack\Scrapy\data") / supplier_name / f"{supplier_name}_personal_notes.csv"
        
        try:
            with open(personal_notes_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=';')
                next(reader)  # Skip header
                for row in reader:
                    if len(row) >= 2:
                        price_type_key = row[0].strip()
                        personal_note_value = row[1].strip()
                        self.personal_notes_mapping[price_type_key] = personal_note_value
            spider.logger.info(f"✅ Мапінг особистих нотаток для {spider.name} завантажено: {self.personal_notes_mapping}")
        except FileNotFoundError:
            spider.logger.warning(f"⚠️  Файл особистих нотаток не знайдено для {spider.name} за шляхом: {personal_notes_path}")
        except Exception as e:
            spider.logger.error(f"❌ Помилка завантаження мапінгу особистих нотаток для {spider.name}: {e}")

        # Отримуємо ім'я файлу з атрибутів паука
        output_file = getattr(spider, 'output_filename', f"{spider.name}.csv")
        filepath = self.output_dir / output_file
        
        # Перевіряємо чи файл не відкритий в іншій програмі
        try:
            # Спочатку пробуємо відкрити у режимі read-write для перевірки
            test_file = open(filepath, "a", encoding="utf-8")
            test_file.close()
        except PermissionError:
            spider.logger.error(f"❌ ПОМИЛКА: Файл {filepath} відкритий в іншій програмі!")
            spider.logger.error(f"   Закрийте файл в Excel або іншому редакторі і спробуйте знову.")
            raise PermissionError(
                f"Неможливо записати у файл {filepath}. "
                f"Файл відкритий в іншій програмі. Закрийте його і спробуйте знову."
            )
        
        # Створюємо файл і пишемо заголовок ВІДРАЗУ
        try:
            self.files[output_file] = open(filepath, "w", encoding="utf-8", newline="", buffering=1)
            self._write_header(self.files[output_file])
            spider.logger.info(f"📝 Створено файл з заголовком: {filepath}")
        except Exception as e:
            spider.logger.error(f"❌ Помилка створення файлу {filepath}: {e}")
            raise
        
        # Ініціалізуємо лічильник і статистику
        self.product_counters[output_file] = 200000
        self.stats[output_file] = {
            "count": 0,
            "filtered_no_price": 0,
            "filtered_no_stock": 0,
        }
    
    def process_item(self, item, spider):
        """Обробляємо кожен item з ФІЛЬТРАЦІЄЮ"""
        adapter = ItemAdapter(item)
        
        # Отримуємо ідентифікатор файлу
        output_file = adapter.get("output_file") or f"{adapter.get('supplier_id', 'unknown')}.csv"
        filepath = self.output_dir / output_file
        
        # ========== ФІЛЬТР 1: Перевірка ціни ==========
        price = adapter.get("Ціна", "")
        if not price or not self._is_valid_price(price):
            self._increment_stat(output_file, "filtered_no_price")
            spider.logger.debug(
                f"⚠️ Пропущено товар без ціни: {adapter.get('Назва_позиції', 'Unknown')}"
            )
            raise ValueError("Товар без ціни")
        
        # ========== ФІЛЬТР 2: Перевірка наявності ==========
        availability_raw = adapter.get("Наявність", "")
        availability_status = self._check_availability(availability_raw)
        
        if not availability_status:
            self._increment_stat(output_file, "filtered_no_stock")
            spider.logger.debug(
                f"⚠️ Пропущено товар не в наявності: {adapter.get('Назва_позиції', 'Unknown')} [{availability_raw}]"
            )
            raise ValueError("Товар не в наявності")
        
        # Очищення та нормалізація даних
        cleaned_item = self._clean_item(adapter, spider)
        
        # ========== РОЗРАХУНОК ЦІНИ З КОЕФІЦІЄНТОМ (ЯКЩО ПОТРІБНО) ==========
        if spider.name == 'viatec_dealer' and self.viatec_dealer_coefficient:
            try:
                price_float = float(cleaned_item["Ціна"].replace(',', '.'))
                multiplied_price = price_float * self.viatec_dealer_coefficient
                cleaned_item["Ціна"] = f"{multiplied_price:.2f}".replace('.', ',')
                spider.logger.debug(f"Ціна для {cleaned_item['Назва_позиції']} помножена на {self.viatec_dealer_coefficient} -> {cleaned_item['Ціна']}")
            except (ValueError, TypeError) as e:
                spider.logger.error(f"❌ Помилка при множенні ціни для {cleaned_item['Назва_позиції']}: {e}")

        # Оновлюємо поля наявності
        cleaned_item["Наявність"] = "+"
        quantity = adapter.get("Кількість", "")
        cleaned_item["Кількість"] = quantity
        
        # ========== ГЕНЕРАЦІЯ ПОСЛІДОВНОГО КОДУ ==========
        price_type = adapter.get("price_type", "retail")
        
        # Ініціалізуємо лічільник для цього файлу якщо немає
        if output_file not in self.product_counters:
            self.product_counters[output_file] = 200000
        
        cleaned_item["Код_товару"] = str(self.product_counters[output_file])
        self.product_counters[output_file] += 1
        
        # Встановлюємо Особисті_нотатки
        cleaned_item["Особисті_нотатки"] = self.personal_notes_mapping.get(price_type, "PROM")
        
        # ========== ОБРОБКА ОПИСУ ==========
        cleaned_item["Опис"] = self._clean_description(cleaned_item.get("Опис", ""))
        cleaned_item["Опис_укр"] = self._clean_description(cleaned_item.get("Опис_укр", ""))
        
        # ========== ОБРОБКА ХАРАКТЕРИСТИК ==========
        specs_list = adapter.get("specifications_list", [])
        
        # Файл вже створений в open_spider(), просто використовуємо
        if output_file not in self.files:
            spider.logger.error(f"❌ Файл {output_file} не знайдено! Це помилка.")
            raise ValueError(f"File {output_file} was not initialized in open_spider")
        
        # Створюємо ROW з базовими полями + характеристиками
        row_parts = []
        
        # Базові поля
        for field in self.fieldnames_base:
            value = cleaned_item.get(field, "")
            # Екрануємо крапку з комою та лапки
            value_str = str(value).replace(";", ",").replace('"', '""')
            row_parts.append(value_str)
        
        # Характеристики (60 триплетів)
        for i in range(60):
            if i < len(specs_list):
                spec = specs_list[i]
                row_parts.append(str(spec.get("name", "")).replace(";", ",").replace('"', '""'))
                row_parts.append(str(spec.get("unit", "")).replace(";", ",").replace('"', '""'))
                row_parts.append(str(spec.get("value", "")).replace(";", ",").replace('"', '""'))
            else:
                # Порожні триплети
                row_parts.extend(["", "", ""])
        
        # Записуємо рядок у файл
        row_line = ";".join(row_parts) + "\n"
        self.files[output_file].write(row_line)
        
        # Оновлюємо статистику
        self.stats[output_file]["count"] += 1
        
        spider.logger.debug(
            f"✅ Записано: {cleaned_item.get('Назва_позиції')} | Ціна: {cleaned_item.get('Ціна')} | Характеристик: {len(specs_list)}"
        )
        
        return item
    
    def close_spider(self, spider):
        """Закриваємо файли та виводимо статистику"""
        for f in self.files.values():
            f.close()
        
        spider.logger.info("=" * 80)
        spider.logger.info("📊 СТАТИСТИКА PIPELINE")
        spider.logger.info("=" * 80)
        
        for output_file, stats in self.stats.items():
            spider.logger.info(f"\n📄 Файл: {output_file}")
            spider.logger.info(f"  ✅ Товарів записано: {stats['count']}")
            spider.logger.info(f"  ❌ Відфільтровано без ціни: {stats['filtered_no_price']}")
            spider.logger.info(f"  ❌ Відфільтровано без наявності: {stats['filtered_no_stock']}")
        
        spider.logger.info("=" * 80)
        spider.logger.info(f"✅ Pipeline закрито")
    
    def _write_header(self, file_obj):
        """Пише заголовок з повторюваними триплетами (БЕЗ нумерації)"""
        header_parts = self.fieldnames_base.copy()
        
        # Додаємо 60 повторюваних триплетів характеристик
        for _ in range(60):
            header_parts.extend([
                "Назва_Характеристики",
                "Одиниця_виміру_Характеристики",
                "Значення_Характеристики",
            ])
        
        file_obj.write(";".join(header_parts) + "\n")
    
    def _is_valid_price(self, price):
        """Перевірка валідності ціни"""
        if not price:
            return False
        
        try:
            price_float = float(str(price).replace(",", ".").replace(" ", ""))
            return price_float > 0
        except (ValueError, TypeError):
            return False
    
    def _check_availability(self, availability_str):
        """
        Перевірка наявності товару
        Повертає True якщо товар В НАЯВНОСТІ, False якщо немає
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
    
    def _clean_description(self, description):
        """Очищає опис від тексту про аналоги та зберігає переноси"""
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
        """Очищення та нормалізація даних"""
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
        Очищення ціни від зайвих символів
        ЗАМІНЮЄ ТОЧКУ НА КОМУ в ціні
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
    
    def _increment_stat(self, output_file, stat_key):
        """Допоміжний метод для інкрементування статистики"""
        if output_file not in self.stats:
            self.stats[output_file] = {
                "count": 0,
                "filtered_no_price": 0,
                "filtered_no_stock": 0,
            }
        self.stats[output_file][stat_key] += 1


class ValidationPipeline:
    """Додатковий pipeline для валідації (опціонально)"""
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        if not adapter.get("Назва_позиції"):
            raise ValueError("Відсутня назва товару")
        
        if not adapter.get("Ціна"):
            raise ValueError("Відсутня ціна")
        
        try:
            float(str(adapter.get("Ціна")).replace(",", "."))
        except (ValueError, TypeError):
            raise ValueError(f"Некоректна ціна: {adapter.get('Ціна')}")
        
        return item

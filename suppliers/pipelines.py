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
from scrapy.exceptions import DropItem


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
                with open(coefficient_path, 'r', encoding='utf-8-sig') as f:
                    content = f.read().strip()
                    spider.logger.debug(f"Вміст файлу коефіцієнту: '{content}'")
                    
                    # Спробуємо різні формати
                    coefficient_str = None
                    
                    # Варіант 1: Файл містить тільки число (наприклад: "1,2" або "1.2")
                    if ';' not in content and '\n' not in content:
                        coefficient_str = content.strip('"').strip()
                        spider.logger.debug(f"Формат 1: просте число '{coefficient_str}'")
                    else:
                        # Варіант 2: CSV з роздільником ; (наприклад: "coefficient;1,2")
                        f.seek(0)
                        reader = csv.reader(f, delimiter=';')
                        row = next(reader)
                        spider.logger.debug(f"Формат 2: CSV рядок {row}")
                        
                        if len(row) >= 2:
                            coefficient_str = row[1].strip('"').strip()
                        elif len(row) == 1:
                            coefficient_str = row[0].strip('"').strip()
                        else:
                            raise ValueError(f"Некоректний формат CSV: {row}")
                    
                    if coefficient_str:
                        # Конвертуємо кому на крапку для float
                        self.viatec_dealer_coefficient = float(coefficient_str.replace(',', '.'))
                        spider.logger.info(f"✅ Коефіцієнт для viatec_dealer завантажено: {self.viatec_dealer_coefficient}")
                    else:
                        raise ValueError("Не вдалося визначити коефіцієнт")
                        
            except FileNotFoundError:
                spider.logger.error(f"❌ Файл коефіцієнту не знайдено: {coefficient_path}")
            except Exception as e:
                spider.logger.error(f"❌ Помилка завантаження коефіцієнту для viatec_dealer: {e}")
                spider.logger.error(f"   Перевірте формат файлу {coefficient_path}")

        # --- Універсальне завантаження особистих нотаток ---
        supplier_name = spider.name.split('_')[0]
        personal_notes_path = Path(r"C:\FullStack\Scrapy\data") / supplier_name / f"{supplier_name}_personal_notes.csv"
        
        try:
            with open(personal_notes_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig видаляє BOM
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
        self.product_counters[output_file] = self._load_initial_product_code(spider.name, spider.logger)
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
            product_name = adapter.get('Назва_позиції', 'Невідомий')[:60]
            product_url = adapter.get('Продукт_на_сайті', 'N/A')
            spider.logger.warning(f"❌ Товар без ціни: {product_name}... | {product_url}")
            raise DropItem(f"Товар без ціни")
        
        # ========== ФІЛЬТР 2: Перевірка наявності ==========
        availability_raw = adapter.get("Наявність", "")
        spider.logger.info(f"🔍 ПРОВЕРКА НАЯВНОСТІ RAW: '{availability_raw}'")
        availability_status = self._check_availability(availability_raw)
        spider.logger.info(f"🔍 РЕЗУЛЬТАТ ПРОВЕРКИ: {availability_status}")
        
        if not availability_status:
            self._increment_stat(output_file, "filtered_no_stock")
            product_name = adapter.get('Назва_позиції', 'Невідомий')[:60]
            product_url = adapter.get('Продукт_на_сайті', 'N/A')
            spider.logger.warning(f"❌ Товар не в наявності: {product_name}... | {product_url}")
            raise DropItem(f"Товар не в наявності")
        
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
        # Якщо кількість не вказана постачальником, ставимо 100 за замовчуванням
        cleaned_item["Кількість"] = quantity if quantity else "100"
        
        # ========== ГЕНЕРАЦІЯ ПОСЛІДОВНОГО КОДУ ==========
        # Ініціалізуємо лічільник для цього файлу якщо немає
        if output_file not in self.product_counters:
            self.product_counters[output_file] = 200000
        
        cleaned_item["Код_товару"] = str(self.product_counters[output_file])
        self.product_counters[output_file] += 1
        
        # Встановлюємо Особисті_нотатки за Номер_групи
        group_number = adapter.get("Номер_групи", "")
        personal_note = self.personal_notes_mapping.get(group_number, "PROM")
        spider.logger.debug(f"📝 Особиста нотатка для Номер_групи='{group_number}': '{personal_note}'")
        cleaned_item["Особисті_нотатки"] = personal_note
        
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
            # Екрануємо ; на кому, " на подвійні лапки, \n та \r на <br> для збереження форматування
            value_str = str(value).replace(";", ",").replace('"', '""').replace("\n", "<br>").replace("\r", "")
            row_parts.append(value_str)
        
        # Характеристики (160 триплетів)
        for i in range(160):
            if i < len(specs_list):
                spec = specs_list[i]
                # Замінюємо ; на кому, " на подвійні лапки, \n та \r на <br> для збереження форматування
                name = str(spec.get("name", "")).replace(";", ",").replace('"', '""').replace("\n", "<br>").replace("\r", "")
                unit = str(spec.get("unit", "")).replace(";", ",").replace('"', '""').replace("\n", "<br>").replace("\r", "")
                value = str(spec.get("value", "")).replace(";", ",").replace('"', '""').replace("\n", "<br>").replace("\r", "")
                row_parts.append(name)
                row_parts.append(unit)
                row_parts.append(value)
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
        
        # Додаємо 160 повторюваних триплетів характеристик
        for _ in range(160):
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
        
        ВАЖЛИВО: За замовчуванням вважаємо товар В НАЯВНОСТІ,
        якщо явно не вказано що його немає
        """
        if not availability_str:
            return True  # Змінено з False на True - за замовчуванням В НАЯВНОСТІ
        
        availability_lower = str(availability_str).lower().strip()
        
        # Спочатку перевіряємо на відсутність (явні негативні маркери)
        out_of_stock_keywords = [
            "немає",
            "нет в наличии",
            "відсутній",
            "закінчився",
            "out of stock",
            "unavailable",
            "немає в наявності",
            "нет на складе",
        ]
        
        for keyword in out_of_stock_keywords:
            if keyword in availability_lower:
                return False
        
        # Позитивні маркери наявності
        in_stock_keywords = [
            "є в наявності",
            "в наявності",
            "в наличии",
            "есть",
            "доступно",
            "available",
            "in stock",
            "наявності",
            "наявност",
            "є",
        ]
        
        for keyword in in_stock_keywords:
            if keyword in availability_lower:
                return True
        
        # За замовчуванням вважаємо товар В НАЯВНОСТІ
        return True
    
    def _clean_description(self, description):
        """Очищає опис від тексту про аналоги та замінює \n на <br>"""
        if not description:
            return ""
        
        patterns_to_remove = [
            r"Є товари з аналогічними характеристиками\s*→",
            r"Есть товары с аналогичными характеристиками\s*→",
        ]
        
        for pattern in patterns_to_remove:
            description = re.sub(pattern, "", description, flags=re.IGNORECASE)
        
        # Замінюємо \n на <br> для збереження переносів рядків
        description = description.replace("\n", "<br>")
        # Видаляємо зайві пробіли, але зберігаємо <br>
        description = re.sub(r' +', ' ', description)
        
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

    def _load_initial_product_code(self, spider_name, logger):
        """
        Завантажує початковий код товару з CSV файлу.
        Формат файлу: один рядок, одне число.
        """
        # Визначаємо шлях до файлу лічильника на основі імені павука
        # Приклад: C:\FullStack\Scrapy\data\viatec\viatec_counter_product_code.csv
        # Приклад: C:\FullStack\Scrapy\data\eserver\eserver_counter_product_code.csv
        
        # Розділяємо ім'я павука, щоб отримати назву постачальника (наприклад, 'viatec' з 'viatec_retail' або 'viatec_dealer')
        supplier_prefix = spider_name.split('_')[0]
        
        counter_file_path = Path(r"C:\FullStack\Scrapy\data") / supplier_prefix / f"{supplier_prefix}_counter_product_code.csv"
        
        try:
            with open(counter_file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if row:
                        try:
                            # Використовуємо регулярний вираз для пошуку першого числа в рядку
                            match = re.search(r'(\d+)', row[0])
                            if match:
                                initial_code = int(match.group(1))
                                logger.info(f"✅ Початковий код товару для {spider_name} завантажено з {counter_file_path}: {initial_code}")
                                return initial_code
                            else:
                                logger.warning(f"⚠️ Не знайдено числа у файлі лічильника {counter_file_path}. Використовуємо значення за замовчуванням.")
                                return 200000
                        except ValueError:
                            logger.warning(f"⚠️ Некоректний формат числа у файлі лічильника {counter_file_path}. Використовуємо значення за замовчуванням.")
                            return 200000
            logger.warning(f"⚠️ Файл лічильника {counter_file_path} порожній. Використовуємо значення за замовчуванням.")
            return 200000
        except FileNotFoundError:
            logger.warning(f"⚠️ Файл лічильника не знайдено для {spider_name} за шляхом: {counter_file_path}. Використовуємо значення за замовчуванням.")
            return 200000
        except Exception as e:
            logger.error(f"❌ Помилка завантаження початкового коду товару для {spider_name} з {counter_file_path}: {e}. Використовуємо значення за замовчуванням.")
            return 200000


class ValidationPipeline:
    """Додатковий pipeline для валідації (опціонально)"""
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        if not adapter.get("Назва_позиції"):
            raise DropItem("Відсутня назва товару")
        
        if not adapter.get("Ціна"):
            raise DropItem("Відсутня ціна")
        
        try:
            float(str(adapter.get("Ціна")).replace(",", "."))
        except (ValueError, TypeError):
            raise DropItem(f"Некоректна ціна: {adapter.get('Ціна')}")
        
        return item

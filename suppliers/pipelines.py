"""
Універсальний Pipeline для всіх постачальників.
Один pipeline керує записом у різні CSV файли на основі supplier_id та output_file.

ФІЛЬТРАЦІЯ:
- Пропускає товари БЕЗ ціни
- Пропускає товари НЕ В НАЯВНОСТІ
- "В наличии" → "+"
- "В наличии 5 шт" → Наявність: "+", Кількість: "5"

ХАРАКТЕРИСТИКИ з підтримкою rule_kind:
- extract: основне правило витягування (пріоритет по priority)
- normalize: нормалізація формату (пріоритет по priority)
- derive: логічний вивід (НЕ перезаписує extract/normalize)
- fallback: використовується тільки якщо значення відсутнє
- skip: пропустити цю характеристику

Формат PROM: повторювані триплети БЕЗ нумерації
- Назва_Характеристики;Одиниця_виміру_Характеристики;Значення_Характеристики (x160 разів)
"""
import re
import csv
from pathlib import Path
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from suppliers.attribute_mapper import AttributeMapper


class SuppliersPipeline:
    """Один pipeline для всіх постачальників з підтримкою rule_kind"""
    
    def __init__(self):
        self.files = {}
        self.writers = {}
        self.viatec_dealer_coefficient_mapping = {}
        self.personal_notes_mapping = {}
        self.label_mapping = {}
        self.attribute_mapper = None
        
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
        
        self.output_dir = Path(r"C:\FullStack\Scrapy\output")
        self.product_counters = {}
        self.stats = {}
    
    def open_spider(self, spider):
        """Створюємо директорію output та файл при відкритті паука"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        spider.logger.info(f"✅ Pipeline відкрито для {spider.name}")
        spider.logger.info(f"📁 Вихідна директорія: {self.output_dir}")

        # Завантаження мапінгу коефіцієнтів (тільки для viatec_dealer)
        if spider.name == 'viatec_dealer':
            coefficient_path = r"C:\FullStack\Scrapy\data\viatec\viatec_coefficient_dealer.csv"
            try:
                with open(coefficient_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.reader(f, delimiter=';')
                    next(reader)  # Пропускаємо заголовок
                    
                    for row in reader:
                        if len(row) >= 3:
                            url = row[1].strip()
                            coefficient_str = row[2].strip().replace(',', '.')
                            try:
                                coefficient = float(coefficient_str)
                                self.viatec_dealer_coefficient_mapping[url] = coefficient
                                spider.logger.debug(f"Мапінг: {url} → {coefficient}")
                            except ValueError:
                                spider.logger.warning(f"⚠️ Некоректний коефіцієнт для {url}: {coefficient_str}")
                
                spider.logger.info(
                    f"✅ Мапінг коефіцієнтів для viatec_dealer завантажено: "
                    f"{len(self.viatec_dealer_coefficient_mapping)} URL"
                )
                        
            except FileNotFoundError:
                spider.logger.error(f"❌ Файл коефіцієнту не знайдено: {coefficient_path}")
            except Exception as e:
                spider.logger.error(f"❌ Помилка завантаження коефіцієнтів для viatec_dealer: {e}")

        # Завантаження особистих нотаток та ярликів
        supplier_name = spider.name.split('_')[0]
        personal_notes_path = Path(r"C:\FullStack\Scrapy\data") / supplier_name / f"{supplier_name}_personal_notes.csv"
        
        try:
            with open(personal_notes_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f, delimiter=';')
                next(reader)
                for row in reader:
                    if len(row) >= 2:
                        group_number = row[0].strip()
                        personal_note_value = row[1].strip()
                        label_value = row[2].strip() if len(row) >= 3 else ""
                        
                        self.personal_notes_mapping[group_number] = personal_note_value
                        self.label_mapping[group_number] = label_value
            
            spider.logger.info(f"✅ Мапінг особистих нотаток: {len(self.personal_notes_mapping)} записів")
            spider.logger.info(f"✅ Мапінг ярликів: {len(self.label_mapping)} записів")
        except FileNotFoundError:
            spider.logger.warning(f"⚠️  Файл особистих нотаток не знайдено: {personal_notes_path}")
        except Exception as e:
            spider.logger.error(f"❌ Помилка завантаження мапінгу: {e}")

        # Ініціалізація маппера характеристик
        rules_path = Path(r"C:\FullStack\Scrapy\data") / supplier_name / f"{supplier_name}_mapping_rules.csv"
        if rules_path.exists():
            try:
                self.attribute_mapper = AttributeMapper(str(rules_path), spider.logger)
                spider.logger.info(f"✅ AttributeMapper ініціалізовано")
            except Exception as e:
                spider.logger.error(f"❌ Помилка ініціалізації AttributeMapper: {e}")
                self.attribute_mapper = None
        else:
            spider.logger.warning(f"⚠️  Маппінг характеристик відключено")
            self.attribute_mapper = None

        output_file = getattr(spider, 'output_filename', f"{spider.name}.csv")
        filepath = self.output_dir / output_file
        
        # Перевірка доступності файлу
        try:
            test_file = open(filepath, "a", encoding="utf-8")
            test_file.close()
        except PermissionError:
            spider.logger.error(f"❌ Файл {filepath} відкритий в іншій програмі!")
            raise PermissionError(f"Неможливо записати у файл {filepath}")
        
        # Створення файлу
        try:
            self.files[output_file] = open(filepath, "w", encoding="utf-8-sig", newline="", buffering=1)
            self._write_header(self.files[output_file])
            spider.logger.info(f"📝 Створено файл: {filepath}")
        except Exception as e:
            spider.logger.error(f"❌ Помилка створення файлу: {e}")
            raise
        
        self.product_counters[output_file] = self._load_initial_product_code(spider.name, spider.logger)
        self.stats[output_file] = {
            "count": 0,
            "filtered_no_price": 0,
            "filtered_no_stock": 0,
        }
    
    def process_item(self, item, spider):
        """Обробляємо кожен item з ФІЛЬТРАЦІЄЮ"""
        adapter = ItemAdapter(item)
        output_file = adapter.get("output_file") or f"{adapter.get('supplier_id', 'unknown')}.csv"
        
        # ФІЛЬТР 1: Ціна
        price = adapter.get("Ціна", "")
        if not price or not self._is_valid_price(price):
            self._increment_stat(output_file, "filtered_no_price")
            product_name = adapter.get('Назва_позиції', 'Невідомий')[:60]
            spider.logger.warning(f"❌ Товар без ціни: {product_name}...")
            raise DropItem("Товар без ціни")
        
        # ФІЛЬТР 2: Наявність
        availability_raw = adapter.get("Наявність", "")
        if not self._check_availability(availability_raw):
            self._increment_stat(output_file, "filtered_no_stock")
            product_name = adapter.get('Назва_позиції', 'Невідомий')[:60]
            spider.logger.warning(f"❌ Товар не в наявності: {product_name}...")
            raise DropItem("Товар не в наявності")
        
        cleaned_item = self._clean_item(adapter, spider)
        
        # Множення ціни для viatec_dealer
        if spider.name == 'viatec_dealer' and self.viatec_dealer_coefficient_mapping:
            category_url = adapter.get('category_url', '')
            coefficient = self.viatec_dealer_coefficient_mapping.get(category_url)
            
            if coefficient:
                try:
                    price_float = float(cleaned_item["Ціна"].replace(',', '.'))
                    multiplied_price = price_float * coefficient
                    cleaned_item["Ціна"] = f"{multiplied_price:.2f}".replace('.', ',')
                except (ValueError, TypeError) as e:
                    spider.logger.error(f"❌ Помилка множення ціни: {e}")

        cleaned_item["Наявність"] = "+"
        cleaned_item["Кількість"] = adapter.get("Кількість", "") or "100"
        
        # Генерація коду товару
        if output_file not in self.product_counters:
            self.product_counters[output_file] = self._load_initial_product_code(spider.name, spider.logger)
        
        cleaned_item["Код_товару"] = str(self.product_counters[output_file])
        self.product_counters[output_file] += 1
        
        cleaned_item["Ідентифікатор_товару"] = adapter.get("Ідентифікатор_товару", "").strip()
        
        # Особисті нотатки та ярлики
        group_number = adapter.get("Номер_групи", "")
        cleaned_item["Особисті_нотатки"] = self.personal_notes_mapping.get(group_number, "PROM")
        cleaned_item["Ярлик"] = self.label_mapping.get(group_number, "")
        
        # Опис
        cleaned_item["Опис"] = self._clean_description(cleaned_item.get("Опис", ""))
        cleaned_item["Опис_укр"] = self._clean_description(cleaned_item.get("Опис_укр", ""))
        
        # Санітизація URL зображень
        image_url = cleaned_item.get("Посилання_зображення", "")
        if image_url:
            urls = [u.strip() for u in image_url.split(", ") if u.strip()]
            sanitized_urls = [url.replace(",", "%2C") if ',' in url else url for url in urls]
            cleaned_item["Посилання_зображення"] = ", ".join(sanitized_urls)
        
        # Обробка характеристик
        specs_list_original = adapter.get("specifications_list", [])
        
        if self.attribute_mapper:
            category_id = adapter.get("Ідентифікатор_підрозділу", "")
            product_name = cleaned_item.get('Назва_позиції', '')
            
            # Мапінг з назви товару
            name_mapped = []
            if product_name:
                name_mapped = self.attribute_mapper.map_product_name(product_name, category_id)
            
            # Мапінг з характеристик
            mapping_result = {'supplier': [], 'mapped': [], 'unmapped': []}
            if specs_list_original:
                mapping_result = self.attribute_mapper.map_attributes(specs_list_original, category_id)
            
            # Об'єднання з дедуплікацією
            specs_dict = {}
            
            for spec in mapping_result['supplier']:
                key = spec['name'].lower().strip()
                if key not in specs_dict:
                    specs_dict[key] = {**spec, 'rule_priority': 9999, 'rule_kind': 'supplier'}
            
            for spec in mapping_result['mapped']:
                rule_kind = spec.get('rule_kind', 'extract')
                if rule_kind == 'skip':
                    continue
                
                key = spec['name'].lower().strip()
                if key not in specs_dict or self._should_replace_attribute(
                    rule_kind, spec.get('rule_priority', 999),
                    specs_dict[key].get('rule_kind', 'extract'),
                    specs_dict[key].get('rule_priority', 999)
                ):
                    specs_dict[key] = spec
            
            for spec in name_mapped:
                rule_kind = spec.get('rule_kind', 'extract')
                if rule_kind == 'skip':
                    continue
                
                key = spec['name'].lower().strip()
                if key not in specs_dict or self._should_replace_attribute(
                    rule_kind, spec.get('rule_priority', 999),
                    specs_dict[key].get('rule_kind', 'extract'),
                    specs_dict[key].get('rule_priority', 999)
                ):
                    specs_dict[key] = spec
            
            specs_list = list(specs_dict.values())
            
            # Постобробка
            specs_list = self._postprocess_weight_in_specs(specs_list, spider)
            specs_list = self._postprocess_hdd_capacity_in_specs(specs_list, spider)
            specs_list = self._postprocess_battery_capacity_in_specs(specs_list, spider)
        else:
            specs_list = specs_list_original
        
        # Витягуємо габарити з характеристик для колонок PROM (після всіх постпроцесів)
        dimensions = self._extract_dimensions_from_specs(specs_list, spider)
        cleaned_item.update(dimensions)
        
        # Запис у файл
        if output_file not in self.files:
            raise ValueError(f"File {output_file} was not initialized")
        
        row_parts = []
        for field in self.fieldnames_base:
            value = cleaned_item.get(field, "")
            value_str = str(value).replace(";", ",").replace('"', '″').replace("\n", "<br>").replace("\r", "")
            row_parts.append(value_str)
        
        for i in range(160):
            if i < len(specs_list):
                spec = specs_list[i]
                name = str(spec.get("name", "")).replace(";", ",").replace('"', '″').replace("\n", "<br>").replace("\r", "")
                unit = str(spec.get("unit", "")).replace(";", ",").replace('"', '″').replace("\n", "<br>").replace("\r", "")
                value = str(spec.get("value", "")).replace(";", ",").replace('"', '″').replace("\n", "<br>").replace("\r", "")
                row_parts.extend([name, unit, value])
            else:
                row_parts.extend(["", "", ""])
        
        self.files[output_file].write(";".join(row_parts) + "\n")
        self.stats[output_file]["count"] += 1
        
        return item
    
    def _should_replace_attribute(self, new_kind, new_priority, current_kind, current_priority):
        """Визначає чи треба замінити характеристику"""
        if new_kind in ['skip', 'fallback']:
            return False
        if new_kind == 'derive':
            return current_kind == 'derive' and new_priority < current_priority
        return new_priority < current_priority
    
    def close_spider(self, spider):
        """Закриття файлів та статистика"""
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
    
    def _write_header(self, file_obj):
        """Запис заголовку"""
        header_parts = self.fieldnames_base.copy()
        for _ in range(160):
            header_parts.extend([
                "Назва_Характеристики",
                "Одиниця_виміру_Характеристики",
                "Значення_Характеристики",
            ])
        file_obj.write(";".join(header_parts) + "\n")
    
    def _is_valid_price(self, price):
        """Перевірка ціни"""
        if not price:
            return False
        try:
            price_float = float(str(price).replace(",", ".").replace(" ", ""))
            return price_float > 0
        except (ValueError, TypeError):
            return False
    
    def _check_availability(self, availability_str):
        """Перевірка наявності"""
        if not availability_str:
            return True
        
        availability_lower = str(availability_str).lower().strip()
        
        out_of_stock_keywords = [
            "немає", "немає в наявності", "нет в наличии", "нет на складе",
            "відсутній", "відсутня", "закінчився", "закінчилась",
            "out of stock", "unavailable", "под заказ", "під замовлення",
        ]
        
        for keyword in out_of_stock_keywords:
            if keyword in availability_lower:
                return False
        
        return True
    
    def _clean_description(self, description):
        """Очищення опису"""
        if not description:
            return ""
        
        patterns_to_remove = [
            r"Є товари з аналогічними характеристиками\s*→",
            r"Есть товары с аналогичными характеристиками\s*→",
        ]
        
        for pattern in patterns_to_remove:
            description = re.sub(pattern, "", description, flags=re.IGNORECASE)
        
        description = description.replace("\n", "<br>")
        description = re.sub(r' +', ' ', description)
        
        return description.strip()
    
    def _clean_item(self, adapter, spider):
        """Очищення даних"""
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
            elif field == "Вага,кг":
                value = self._convert_weight_to_grams(value)
            
            cleaned[field] = value
        
        return cleaned
    
    def _clean_price(self, price):
        """Очищення ціни"""
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
    
    def _convert_weight_to_grams(self, weight_str):
        """Конвертація ваги в грами"""
        if not weight_str:
            return ""
        
        weight_str = str(weight_str).strip()
        
        match_g = re.search(r'(?:Вага\s+)?([0-9\.]+)\s*г', weight_str, re.IGNORECASE)
        if match_g:
            return match_g.group(1)
        
        match_kg = re.search(r'(?:Вага\s+)?([0-9\.]+)\s*кг', weight_str, re.IGNORECASE)
        if match_kg:
            kg = float(match_kg.group(1))
            grams = kg * 1000
            return str(int(grams)) if grams == int(grams) else str(grams)
        
        return weight_str
    
    def _postprocess_weight_in_specs(self, specs_list, spider):
        """Постобробка ваги в характеристиках"""
        if not specs_list:
            return specs_list
        
        weight_names = ['вага', 'вага брутто', 'вага нетто', 'weight', 'gross weight', 'net weight']
        
        for spec in specs_list:
            if spec.get('name', '').lower().strip() in weight_names:
                original_value = spec.get('value', '')
                converted_value = self._convert_weight_to_grams(original_value)
                if converted_value != original_value:
                    spec['value'] = converted_value
                    spider.logger.info(f"⚖️ Конвертація ваги: {spec['name']} = '{original_value}' → '{converted_value}'")
        
        return specs_list
    
    def _postprocess_hdd_capacity_in_specs(self, specs_list, spider):
        """Постобробка ємності HDD"""
        if not specs_list:
            return specs_list
        
        hdd_names = ['суммарная емкость hdd', 'total hdd capacity', 'загальна ємність hdd']
        disk_names = ['об\'єм накопичувача', 'disk capacity', 'ємність диска']
        
        for spec in specs_list:
            spec_name_lower = spec.get('name', '').lower().strip()
            original_value = spec.get('value', '')
            
            if spec_name_lower in hdd_names:
                match = re.search(r'(\d+)\s*SATA\s*(\d+)\s*Тб', original_value, re.IGNORECASE)
                if match:
                    try:
                        num_sata = int(match.group(1))
                        max_tb = int(match.group(2))
                        total_gb = num_sata * max_tb * 1024
                        spec['value'] = str(total_gb)
                        spider.logger.info(f"💾 HDD: '{original_value}' → '{total_gb} GB'")
                    except (ValueError, AttributeError) as e:
                        spider.logger.warning(f"⚠️ Помилка HDD: {e}")
            
            elif spec_name_lower in disk_names:
                match = re.search(r'(\d+)\s*[Тт][БбBb]', original_value, re.IGNORECASE)
                if match:
                    try:
                        tb_value = int(match.group(1))
                        gb_value = tb_value * 1024
                        spec['value'] = str(gb_value)
                        spider.logger.info(f"💾 Диск: '{original_value}' → '{gb_value} GB'")
                    except (ValueError, AttributeError) as e:
                        spider.logger.warning(f"⚠️ Помилка диск: {e}")
        
        return specs_list
    
    def _postprocess_battery_capacity_in_specs(self, specs_list, spider):
        """Постобробка ємності батареї"""
        if not specs_list:
            return specs_list
        
        battery_names = ['ємність акумулятору', 'battery capacity', 'емкость аккумулятора']
        
        for spec in specs_list:
            if spec.get('name', '').lower().strip() in battery_names:
                original_value = spec.get('value', '')
                match = re.search(r'([\d\.]+)\s*[АA](?:•|·|г)?[гч]?', original_value, re.IGNORECASE)
                if match:
                    try:
                        ah_value = float(match.group(1))
                        mah_value = int(ah_value * 1000)
                        spec['value'] = str(mah_value)
                        spider.logger.info(f"🔋 Батарея: '{original_value}' → '{mah_value} мА·г'")
                    except (ValueError, AttributeError) as e:
                        spider.logger.warning(f"⚠️ Помилка батарея: {e}")
        
        return specs_list
    
    def _extract_dimensions_from_specs(self, specs_list, spider):
        """
        Витягує габарити з характеристик для заповнення колонок PROM.
        Конвертує мм → см, г → кг
        
        Шукає:
        - Вага (вже в г після постпроцесу) → переводить в кг
        - Ширина (мм) → см
        - Висота (мм) → см
        - Довжина (мм) → см
        """
        dimensions = {
            "Вага,кг": "",
            "Ширина,см": "",
            "Висота,см": "",
            "Довжина,см": ""
        }
        
        if not specs_list:
            return dimensions
        
        # Мапінг назв характеристик → колонки PROM
        weight_keys = ['вага', 'вага брутто', 'вага нетто', 'weight', 'gross weight', 'net weight']
        width_keys = ['ширина', 'width']
        height_keys = ['висота', 'высота', 'height']
        length_keys = ['довжина', 'длина', 'length', 'глибина', 'глубина', 'depth']
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower().strip()
            spec_value = spec.get('value', '').strip()
            spec_unit = spec.get('unit', '').lower().strip()
            
            if not spec_value:
                continue
            
            # 1. ВАГА: з г → кг (після постпроцесу вже в грамах)
            if spec_name in weight_keys:
                match_g = re.search(r'([0-9\.]+)', spec_value)
                if match_g:
                    try:
                        grams = float(match_g.group(1))
                        kg = grams / 1000
                        dimensions["Вага,кг"] = f"{kg:.3f}".replace('.', ',')
                        spider.logger.debug(f"⚖️ Вага: {grams}г → {kg}кг")
                    except ValueError:
                        pass
            
            # 2. ШИРИНА: з мм → см (перевіряємо unit або value)
            elif spec_name in width_keys:
                if spec_unit == 'мм' or 'мм' in spec_value:
                    match_num = re.search(r'([0-9\.]+)', spec_value)
                    if match_num:
                        try:
                            mm = float(match_num.group(1))
                            cm = mm / 10
                            dimensions["Ширина,см"] = f"{cm:.1f}".replace('.', ',')
                            spider.logger.debug(f"📏 Ширина: {mm}мм → {cm}см")
                        except ValueError:
                            pass
            
            # 3. ВИСОТА: з мм → см (перевіряємо unit або value)
            elif spec_name in height_keys:
                if spec_unit == 'мм' or 'мм' in spec_value:
                    match_num = re.search(r'([0-9\.]+)', spec_value)
                    if match_num:
                        try:
                            mm = float(match_num.group(1))
                            cm = mm / 10
                            dimensions["Висота,см"] = f"{cm:.1f}".replace('.', ',')
                            spider.logger.debug(f"📏 Висота: {mm}мм → {cm}см")
                        except ValueError:
                            pass
            
            # 4. ДОВЖИНА: з мм → см (перевіряємо unit або value)
            elif spec_name in length_keys:
                if spec_unit == 'мм' or 'мм' in spec_value:
                    match_num = re.search(r'([0-9\.]+)', spec_value)
                    if match_num:
                        try:
                            mm = float(match_num.group(1))
                            cm = mm / 10
                            dimensions["Довжина,см"] = f"{cm:.1f}".replace('.', ',')
                            spider.logger.debug(f"📏 Довжина: {mm}мм → {cm}см")
                        except ValueError:
                            pass
        
        return dimensions
    
    def _increment_stat(self, output_file, stat_key):
        """Інкремент статистики"""
        if output_file not in self.stats:
            self.stats[output_file] = {
                "count": 0,
                "filtered_no_price": 0,
                "filtered_no_stock": 0,
            }
        self.stats[output_file][stat_key] += 1

    def _load_initial_product_code(self, spider_name, logger):
        """Завантаження початкового коду товару"""
        supplier_prefix = spider_name.split('_')[0]
        counter_file_path = Path(r"C:\FullStack\Scrapy\data") / supplier_prefix / f"{supplier_prefix}_counter_product_code.csv"
        
        try:
            with open(counter_file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if row:
                        match = re.search(r'(\d+)', row[0])
                        if match:
                            initial_code = int(match.group(1))
                            logger.info(f"✅ Початковий код: {initial_code}")
                            return initial_code
            return 200000
        except FileNotFoundError:
            logger.warning(f"⚠️ Файл лічильника не знайдено: {counter_file_path}")
            return 200000
        except Exception as e:
            logger.error(f"❌ Помилка завантаження коду: {e}")
            return 200000


class ValidationPipeline:
    """Додатковий pipeline для валідації"""
    
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

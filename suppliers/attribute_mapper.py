"""
Маппінг характеристик постачальника → портальні характеристики PROM
Використовує словник правил з pattern matching (exact, contains, regex)
"""
import re
import csv
from pathlib import Path
from typing import List, Dict, Optional


class AttributeMapper:
    """Клас для маппінгу характеристик постачальника на портальні"""
    
    def __init__(self, rules_path: str, logger=None):
        """
        Args:
            rules_path: Шлях до CSV з правилами маппінгу
            logger: Scrapy logger для логування
        """
        self.logger = logger
        self.rules = []
        self.regex_cache = {}
        self._load_rules(rules_path)
    
    def _load_rules(self, rules_path: str):
        """Завантажує правила з CSV"""
        try:
            with open(rules_path, encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                for row in reader:
                    # Пропускаємо неактивні правила
                    if row.get('is_active', 'true').lower() != 'true':
                        continue
                    
                    rule = {
                        'supplier_name_substring': row.get('supplier_name_substring', '').strip(),  # ПЕРША колонка!
                        'supplier_attribute': row['supplier_attribute'].strip(),
                        'supplier_value_pattern': row['supplier_value_pattern'].strip(),
                        'pattern_type': row['pattern_type'].strip(),
                        'prom_attribute': row['prom_attribute'].strip(),
                        'prom_value_template': row['prom_value_template'].strip(),
                        'priority': int(row.get('priority', 100)),
                        'category_id': row.get('category_id', '').strip(),
                        'notes': row.get('notes', '').strip()
                    }
                    
                    # Прекомпілюємо regex для швидкості
                    if rule['pattern_type'] == 'regex':
                        # Regex для supplier_value_pattern
                        if row['supplier_value_pattern']:
                            try:
                                self.regex_cache[row['supplier_value_pattern']] = re.compile(
                                    row['supplier_value_pattern'], 
                                    re.IGNORECASE | re.UNICODE
                                )
                            except re.error as e:
                                if self.logger:
                                    self.logger.error(f"❌ Невалідний regex (value): {row['supplier_value_pattern']} | {e}")
                                continue
                        
                        # Regex для supplier_name_substring
                        name_pattern = row.get('supplier_name_substring', '').strip()
                        if name_pattern:
                            try:
                                cache_key = f"name:{name_pattern}"
                                self.regex_cache[cache_key] = re.compile(
                                    name_pattern,
                                    re.IGNORECASE | re.UNICODE
                                )
                            except re.error as e:
                                if self.logger:
                                    self.logger.error(f"❌ Невалідний regex (name): {name_pattern} | {e}")
                                continue
                    
                    self.rules.append(rule)
            
            # Сортуємо за пріоритетом (менше = раніше)
            self.rules.sort(key=lambda x: x['priority'])
            
            if self.logger:
                # Підрахуємо category_id
                category_counts = {}
                for rule in self.rules:
                    cat = rule.get('category_id', '').strip()
                    cat_key = cat if cat else 'universal'
                    category_counts[cat_key] = category_counts.get(cat_key, 0) + 1
                
                self.logger.info(f"✅ Завантажено {len(self.rules)} правил маппінгу")
                self.logger.info(f"   Категорії: {category_counts}")
        
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Помилка завантаження правил маппінгу: {e}")
            self.rules = []
    
    def _normalize_attribute_name(self, name: str) -> str:
        """Нормалізує назву атрибута для порівняння"""
        if not name:
            return ""
        return name.lower().strip()
    
    def _apply_rule(self, rule: Dict, value: str) -> Optional[str]:
        """
        Застосовує правило до значення
        Повертає змапене значення або None якщо не підходить
        """
        if not value:
            return None
        
        pattern_type = rule['pattern_type']
        pattern = rule['supplier_value_pattern']
        template = rule['prom_value_template']
        
        # Exact match
        if pattern_type == 'exact':
            if not pattern:  # Порожній паттерн = будь-яке значення
                return template if template else value
            return template if value.lower().strip() == pattern.lower().strip() else None
        
        # Contains
        elif pattern_type == 'contains':
            if pattern.lower() in value.lower():
                return template if template else value
            return None
        
        # Regex
        elif pattern_type == 'regex':
            regex = self.regex_cache.get(pattern)
            if not regex:
                return None
            
            match = regex.search(value)
            if not match:
                return None
            
            # Замінюємо $1, $2 тощо на capture groups
            result = template
            for i, group in enumerate(match.groups(), start=1):
                if group:
                    result = result.replace(f'${i}', group)
            
            return result if result else value
        
        return None
    
    def map_single_attribute(self, spec: Dict, category_id: Optional[str] = None) -> List[Dict]:
        """
        Мапить одну характеристику
        
        Args:
            spec: {'name': 'Тип', 'unit': '', 'value': 'UTP CAT5e'}
        
        Returns:
            Список змаплених характеристик (може бути більше однієї!)
            [
                {'name': 'Категорія витої пари', 'unit': '', 'value': 'САТ5е'},
                {'name': 'Тип витої пари', 'unit': '', 'value': 'UTP'}
            ]
        """
        supplier_name = spec.get('name', '').strip()
        supplier_value = spec.get('value', '').strip()
        supplier_unit = spec.get('unit', '').strip()
        
        if not supplier_name or not supplier_value:
            return []
        
        normalized_name = self._normalize_attribute_name(supplier_name)
        mapped_attributes = []
        seen_attributes = {}  # Дедуплікація: ім'я атрибута → найкращий пріоритет
        
        # Шукаємо підходящі правила
        for rule in self.rules:
            # Фільтр по категорії (СТРОГА перевірка!)
            rule_category = rule.get('category_id', '').strip()
            
            # Якщо в правилі вказано category_id - перевіряємо строгий збіг
            if rule_category:
                # Строга перевірка: тільки точний збіг
                if not category_id or str(rule_category) != str(category_id):
                    if self.logger:
                        self.logger.debug(
                            f"⏭️ Пропускаю правило: rule_category='{rule_category}' != category_id='{category_id}' | "
                            f"Атрибут: {rule['supplier_attribute']} → {rule['prom_attribute']}"
                        )
                    continue  # Правило для іншої категорії
            # Якщо category_id порожній - правило універсальне (для всіх категорій)
            
            rule_name_normalized = self._normalize_attribute_name(rule['supplier_attribute'])
            
            # Перевіряємо чи правило підходить до цього атрибута
            if not rule_name_normalized:  # Порожнє ім'я = будь-який атрибут
                pass
            elif rule_name_normalized not in normalized_name:
                continue
            
            # Застосовуємо правило
            mapped_value = self._apply_rule(rule, supplier_value)
            
            if mapped_value:
                prom_attribute = rule['prom_attribute']
                
                # Спеціальний маркер "Пропустити"
                if prom_attribute == 'Пропустити':
                    if self.logger:
                        self.logger.debug(f"⏭️ Пропускаю: {supplier_name} = {supplier_value}")
                    return []  # Не додаємо цю характеристику взагалі
                
                # ДЕДУПЛІКАЦІЯ: Перевіряємо чи цей атрибут вже є
                attr_key = prom_attribute.lower().strip()
                if attr_key in seen_attributes:
                    # Атрибут вже існує - порівнюємо пріоритети
                    existing_priority = seen_attributes[attr_key]['rule_priority']
                    current_priority = rule['priority']
                    
                    if current_priority < existing_priority:
                        # Поточне правило має вищий пріоритет (менше число) - оновлюємо
                        if self.logger:
                            self.logger.warning(
                                f"⚠️ ДУБЛЮВАННЯ: '{prom_attribute}' вже є з priority={existing_priority}, "
                                f"оновлюю на priority={current_priority}: {supplier_name}={supplier_value} → {mapped_value}"
                            )
                        # Знаходимо і оновлюємо існуючий запис
                        for attr in mapped_attributes:
                            if attr['name'].lower().strip() == attr_key:
                                attr['value'] = mapped_value
                                attr['unit'] = supplier_unit
                                attr['rule_priority'] = current_priority
                                seen_attributes[attr_key] = attr
                                break
                    else:
                        # Існуючий пріоритет кращий - пропускаємо
                        if self.logger:
                            self.logger.debug(
                                f"⏭️ Пропускаю дублікат '{prom_attribute}': існуючий priority={existing_priority} кращий за {current_priority}"
                            )
                    continue
                
                # Додаємо нову змаплену характеристику
                new_attr = {
                    'name': prom_attribute,
                    'unit': supplier_unit,  # Зберігаємо одиницю виміру
                    'value': mapped_value,
                    'rule_priority': rule['priority']
                }
                mapped_attributes.append(new_attr)
                seen_attributes[attr_key] = new_attr
                
                if self.logger:
                    rule_cat_info = f" [cat={rule['category_id']}]" if rule.get('category_id') else " [universal]"
                    self.logger.debug(
                        f"✅ Змапилось{rule_cat_info}: {supplier_name}={supplier_value} → "
                        f"{prom_attribute}={mapped_value} (priority {rule['priority']})"
                    )
        
        return mapped_attributes
    
    def map_product_name(self, product_name: str, category_id: Optional[str] = None) -> List[Dict]:
        """
        Мапить характеристики з назви товару
        
        Args:
            product_name: Назва товару (наприклад, "Hikvision DS-2CE16D0T-IT3F 2MP HD-TVI 2.8mm")
            category_id: ID категорії
        
        Returns:
            Список змаплених характеристик (БЕЗ дублікатів)
        """
        if not product_name:
            return []
        
        mapped_attributes = []
        seen_attributes = {}  # Дедуплікація: ім'я атрибута → найкращий пріоритет
        
        # Шукаємо правила з supplier_name_substring
        for rule in self.rules:
            # Пропускаємо правила без name pattern
            name_pattern = rule.get('supplier_name_substring', '').strip()
            if not name_pattern:
                continue
            
            # Фільтр по категорії
            rule_category = rule.get('category_id', '').strip()
            if rule_category:
                if not category_id or str(rule_category) != str(category_id):
                    continue
            
            # Перевіряємо regex зі списку name
            if rule['pattern_type'] == 'regex':
                cache_key = f"name:{name_pattern}"
                regex = self.regex_cache.get(cache_key)
                
                if regex and regex.search(product_name):
                    prom_attribute = rule['prom_attribute']
                    prom_value = rule['prom_value_template']
                    
                    # Спеціальний маркер "Пропустити"
                    if prom_attribute == 'Пропустити':
                        continue
                    
                    # ДЕДУПЛІКАЦІЯ: Перевіряємо чи цей атрибут вже є
                    attr_key = prom_attribute.lower().strip()
                    if attr_key in seen_attributes:
                        # Атрибут вже існує - порівнюємо пріоритети
                        existing_priority = seen_attributes[attr_key]['rule_priority']
                        current_priority = rule['priority']
                        
                        if current_priority < existing_priority:
                            # Поточне правило має вищий пріоритет - оновлюємо
                            if self.logger:
                                self.logger.warning(
                                    f"⚠️ ДУБЛЮВАННЯ в назві: '{prom_attribute}' вже є з priority={existing_priority}, "
                                    f"оновлюю на priority={current_priority}: '{product_name}' → {prom_value}"
                                )
                            # Знаходимо і оновлюємо існуючий запис
                            for attr in mapped_attributes:
                                if attr['name'].lower().strip() == attr_key:
                                    attr['value'] = prom_value
                                    attr['rule_priority'] = current_priority
                                    seen_attributes[attr_key] = attr
                                    break
                        else:
                            # Існуючий пріоритет кращий - пропускаємо
                            if self.logger:
                                self.logger.debug(
                                    f"⏭️ Пропускаю дублікат з назви '{prom_attribute}': "
                                    f"існуючий priority={existing_priority} кращий за {current_priority}"
                                )
                        continue
                    
                    # Додаємо нову характеристику
                    new_attr = {
                        'name': prom_attribute,
                        'unit': '',
                        'value': prom_value,
                        'rule_priority': rule['priority'],
                        'source': 'product_name'  # Позначаємо джерело
                    }
                    mapped_attributes.append(new_attr)
                    seen_attributes[attr_key] = new_attr
                    
                    if self.logger:
                        self.logger.debug(
                            f"✅ Змаплено з назви: '{product_name}' → {prom_attribute} = {prom_value} [priority={rule['priority']}]"
                        )
        
        return mapped_attributes
    
    def map_attributes(self, specifications_list: List[Dict], category_id: Optional[str] = None) -> Dict:
        """
        Мапить список характеристик
        
        Args:
            specifications_list: [
                {'name': 'Тип', 'unit': '', 'value': 'UTP CAT5e'},
                {'name': 'Довжина кабеля', 'unit': '', 'value': '305 м'},
                ...
            ]
        
        Returns:
            {
                'supplier': [...],  # Оригінальні характеристики
                'mapped': [...],    # Портальні характеристики
                'unmapped': [...]   # Що не змапилось
            }
        """
        result = {
            'supplier': specifications_list.copy(),
            'mapped': [],
            'unmapped': []
        }
        
        for spec in specifications_list:
            mapped_list = self.map_single_attribute(spec, category_id)
            
            if mapped_list:
                # Може бути декілька змаплених характеристик з одної
                result['mapped'].extend(mapped_list)
            else:
                # Не змапилось - додаємо в unmapped
                if spec.get('name') and spec.get('value'):
                    result['unmapped'].append(spec)
                    if self.logger:
                        self.logger.debug(
                            f"❌ Не змапилось: {spec['name']} = {spec['value']}"
                        )
        
        if self.logger:
            self.logger.info(
                f"📊 Маппінг: {len(specifications_list)} вхідних → "
                f"{len(result['mapped'])} змаплених + "
                f"{len(result['unmapped'])} не змаплених"
            )
        
        return result


def test_mapper():
    """Тестування маппера"""
    import logging
    
    # Створюємо простий logger
    logger = logging.getLogger('test')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    
    # Тестові дані
    test_specs = [
        {'name': 'Тип', 'unit': '', 'value': 'UTP CAT5e'},
        {'name': 'Оболонка', 'unit': '', 'value': 'Полівінілхлорид (PVC)'},
        {'name': 'Довжина кабеля', 'unit': '', 'value': '305 м'},
        {'name': 'Матеріал жили (провідника)', 'unit': '', 'value': 'мідь'},
        {'name': 'Кількість жил', 'unit': '', 'value': '8'},
        {'name': 'Переріз', 'unit': '', 'value': '0.5 мм'},
    ]
    
    # Створюємо маппер
    rules_path = r"C:\FullStack\Scrapy\data\viatec\viatec_mapping_rules.csv"
    mapper = AttributeMapper(rules_path, logger)
    
    # Мапимо
    result = mapper.map_attributes(test_specs)
    
    print("\n" + "="*80)
    print("РЕЗУЛЬТАТ МАППІНГУ:")
    print("="*80)
    
    print(f"\n📥 Оригінальні ({len(result['supplier'])}):")
    for spec in result['supplier']:
        print(f"  • {spec['name']}: {spec['value']}")
    
    print(f"\n✅ Змаплені ({len(result['mapped'])}):")
    for spec in result['mapped']:
        print(f"  • {spec['name']}: {spec['value']}")
    
    print(f"\n❌ Не змаплені ({len(result['unmapped'])}):")
    for spec in result['unmapped']:
        print(f"  • {spec['name']}: {spec['value']}")


if __name__ == '__main__':
    test_mapper()

"""
Маппінг характеристик постачальника → портальні характеристики PROM
Використовує словник правил з pattern matching (exact, contains, regex)

ПІДТРИМКА rule_kind:
- extract: основне правило (пріоритет по priority)
- normalize: нормалізація формату (пріоритет по priority)
- derive: логічний вивід (НЕ перезаписує extract/normalize)
- fallback: використовується тільки якщо значення відсутнє
- skip: пропустити цю характеристику
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
                        'supplier_name_substring': row.get('supplier_name_substring', '').strip(),
                        'supplier_attribute': row['supplier_attribute'].strip(),
                        'supplier_value_pattern': row['supplier_value_pattern'].strip(),
                        'pattern_type': row['pattern_type'].strip(),
                        'prom_attribute': row['prom_attribute'].strip(),
                        'prom_attribute_unit_template': row.get('prom_attribute_unit_template', '').strip(),  # НОВА КОЛОНКА
                        'prom_value_template': row['prom_value_template'].strip(),
                        'priority': int(row.get('priority', 100)),
                        'category_id': row.get('category_id', '').strip(),
                        'rule_kind': row.get('rule_kind', 'extract').strip(),  # NOWE POLE
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
                # Підрахуємо category_id та rule_kind
                category_counts = {}
                kind_counts = {}
                for rule in self.rules:
                    cat = rule.get('category_id', '').strip()
                    cat_key = cat if cat else 'universal'
                    category_counts[cat_key] = category_counts.get(cat_key, 0) + 1
                    
                    kind = rule.get('rule_kind', 'extract')
                    kind_counts[kind] = kind_counts.get(kind, 0) + 1
                
                self.logger.info(f"✅ Завантажено {len(self.rules)} правил маппінгу")
                self.logger.info(f"   Категорії: {category_counts}")
                self.logger.info(f"   Типи правил: {kind_counts}")
        
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Помилка завантаження правил маппінгу: {e}")
            self.rules = []
    
    def _normalize_attribute_name(self, name: str) -> str:
        """Нормалізує назву атрибута для порівняння"""
        if not name:
            return ""
        return name.lower().strip()
    
    def _apply_rule(self, rule: Dict, value: str) -> tuple[Optional[str], Optional[str]]:
        """
        Застосовує правило до значення
        Повертає (mapped_value, mapped_unit) або (None, None) якщо не підходить
        """
        if not value:
            return None, None
        
        pattern_type = rule['pattern_type']
        pattern = rule['supplier_value_pattern']
        value_template = rule['prom_value_template']
        unit_template = rule.get('prom_attribute_unit_template', '')
        
        # Exact match
        if pattern_type == 'exact':
            if not pattern:  # Порожній паттерн = будь-яке значення
                return (value_template if value_template else value, unit_template)
            if value.lower().strip() == pattern.lower().strip():
                return (value_template if value_template else value, unit_template)
            return None, None
        
        # Contains
        elif pattern_type == 'contains':
            if pattern.lower() in value.lower():
                return (value_template if value_template else value, unit_template)
            return None, None
        
        # Regex
        elif pattern_type == 'regex':
            regex = self.regex_cache.get(pattern)
            if not regex:
                return None, None
            
            match = regex.search(value)
            if not match:
                return None, None
            
            # Замінюємо $1, $2 тощо на capture groups для value
            result_value = value_template
            for i, group in enumerate(match.groups(), start=1):
                if group:
                    result_value = result_value.replace(f'${i}', group)
            
            # Замінюємо $1, $2 тощо на capture groups для unit
            result_unit = unit_template
            if result_unit:
                for i, group in enumerate(match.groups(), start=1):
                    if group:
                        result_unit = result_unit.replace(f'${i}', group)
            
            return (result_value if result_value else value, result_unit)
        
        return None, None
    
    def _should_apply_rule(self, rule: Dict, current_value: Optional[str], current_kind: Optional[str], 
                          current_priority: int) -> bool:
        """
        Визначає чи треба застосовувати правило з урахуванням rule_kind
        
        Args:
            rule: Правило що застосовується
            current_value: Поточне значення атрибута (None якщо відсутнє)
            current_kind: Тип поточного правила ('extract', 'derive', тощо)
            current_priority: Пріоритет поточного правила
        
        Returns:
            True якщо правило треба застосувати, False якщо пропустити
        """
        rule_kind = rule.get('rule_kind', 'extract')
        rule_priority = rule['priority']
        
        # skip - завжди пропускаємо
        if rule_kind == 'skip':
            return False
        
        # fallback - тільки якщо значення відсутнє
        if rule_kind == 'fallback':
            return current_value is None or current_value == ''
        
        # derive - НЕ перезаписує extract/normalize
        if rule_kind == 'derive':
            # Застосовуємо тільки якщо:
            # 1. Значення відсутнє АБО
            # 2. Поточне теж derive І новий пріоритет вищий
            if current_value is None or current_value == '':
                return True
            if current_kind == 'derive' and rule_priority < current_priority:
                return True
            return False
        
        # extract/normalize - основна логіка
        # Застосовуємо якщо:
        # 1. Значення відсутнє АБО
        # 2. Новий пріоритет вищий (менше число)
        if current_value is None or current_value == '':
            return True
        if rule_priority < current_priority:
            return True
        
        return False
    
    def map_single_attribute(self, spec: Dict, category_id: Optional[str] = None) -> List[Dict]:
        """
        Мапить одну характеристику з урахуванням rule_kind
        
        Args:
            spec: {'name': 'Тип', 'unit': '', 'value': 'UTP CAT5e'}
        
        Returns:
            Список змаплених характеристик
        """
        supplier_name = spec.get('name', '').strip()
        supplier_value = spec.get('value', '').strip()
        supplier_unit = spec.get('unit', '').strip()
        
        if not supplier_name or not supplier_value:
            return []
        
        normalized_name = self._normalize_attribute_name(supplier_name)
        mapped_attributes = []
        seen_attributes = {}  # Дедуплікація: ім'я атрибута → {value, unit, priority, kind}
        
        # Шукаємо підходящі правила
        for rule in self.rules:
            # Фільтр по категорії (global = застосовується до всіх)
            rule_category = rule.get('category_id', '').strip()
            if rule_category and rule_category.lower() != 'global':
                if not category_id or str(rule_category) != str(category_id):
                    continue
            
            rule_name_normalized = self._normalize_attribute_name(rule['supplier_attribute'])
            
            # Перевіряємо чи правило підходить до цього атрибута
            if not rule_name_normalized:
                pass
            elif rule_name_normalized not in normalized_name:
                continue
            
            # Застосовуємо правило
            mapped_value, mapped_unit = self._apply_rule(rule, supplier_value)
            
            if mapped_value:
                prom_attribute = rule['prom_attribute']
                rule_kind = rule.get('rule_kind', 'extract')
                
                # Спеціальний маркер "Пропустити"
                if prom_attribute == 'Пропустити' or rule_kind == 'skip':
                    if self.logger:
                        self.logger.debug(f"⏭️ Пропускаю: {supplier_name} = {supplier_value}")
                    return []
                
                # Перевіряємо чи цей атрибут вже є
                attr_key = prom_attribute.lower().strip()
                
                if attr_key in seen_attributes:
                    current_data = seen_attributes[attr_key]
                    current_value = current_data['value']
                    current_kind = current_data.get('kind', 'extract')
                    current_priority = current_data['priority']
                    
                    # Перевіряємо чи треба застосувати це правило
                    if self._should_apply_rule(rule, current_value, current_kind, current_priority):
                        if self.logger:
                            self.logger.debug(
                                f"🔄 Оновлюю '{prom_attribute}': {current_kind}[{current_priority}] → "
                                f"{rule_kind}[{rule['priority']}]: {mapped_value}"
                            )
                        # Оновлюємо існуючий запис
                        for attr in mapped_attributes:
                            if attr['name'].lower().strip() == attr_key:
                                attr['value'] = mapped_value
                                # Використовуємо mapped_unit, якщо є, інакше supplier_unit
                                attr['unit'] = mapped_unit if mapped_unit else supplier_unit
                                attr['rule_priority'] = rule['priority']
                                attr['rule_kind'] = rule_kind
                                seen_attributes[attr_key] = {
                                    'value': mapped_value,
                                    'priority': rule['priority'],
                                    'kind': rule_kind
                                }
                                break
                    else:
                        if self.logger:
                            self.logger.debug(
                                f"⏭️ Пропускаю '{prom_attribute}': rule_kind={rule_kind}, "
                                f"current={current_kind}[{current_priority}], new=[{rule['priority']}]"
                            )
                    continue
                
                # Додаємо нову характеристику
                new_attr = {
                    'name': prom_attribute,
                    # Використовуємо mapped_unit, якщо є, інакше supplier_unit
                    'unit': mapped_unit if mapped_unit else supplier_unit,
                    'value': mapped_value,
                    'rule_priority': rule['priority'],
                    'rule_kind': rule_kind
                }
                mapped_attributes.append(new_attr)
                seen_attributes[attr_key] = {
                    'value': mapped_value,
                    'priority': rule['priority'],
                    'kind': rule_kind
                }
                
                if self.logger:
                    rule_cat_info = f" [cat={rule['category_id']}]" if rule.get('category_id') else " [universal]"
                    self.logger.debug(
                        f"✅ Змапилось{rule_cat_info}: {supplier_name}={supplier_value} → "
                        f"{prom_attribute}={mapped_value} ({rule_kind}[{rule['priority']}])"
                    )
        
        return mapped_attributes
    
    def map_product_name(self, product_name: str, category_id: Optional[str] = None) -> List[Dict]:
        """
        Мапить характеристики з назви товару з урахуванням rule_kind
        
        Args:
            product_name: Назва товару
            category_id: ID категорії
        
        Returns:
            Список змаплених характеристик
        """
        if not product_name:
            return []
        
        mapped_attributes = []
        seen_attributes = {}
        
        for rule in self.rules:
            name_pattern = rule.get('supplier_name_substring', '').strip()
            if not name_pattern:
                continue
            
            # Фільтр по категорії (global = застосовується до всіх)
            rule_category = rule.get('category_id', '').strip()
            if rule_category and rule_category.lower() != 'global':
                if not category_id or str(rule_category) != str(category_id):
                    continue
            
            # Перевіряємо regex
            if rule['pattern_type'] == 'regex':
                cache_key = f"name:{name_pattern}"
                regex = self.regex_cache.get(cache_key)
                
                if regex and regex.search(product_name):
                    prom_attribute = rule['prom_attribute']
                    prom_value_template = rule['prom_value_template']
                    prom_unit_template = rule.get('prom_attribute_unit_template', '')
                    rule_kind = rule.get('rule_kind', 'extract')
                    
                    # Замінюємо $1, $2 на capture groups якщо є
                    match = regex.search(product_name)
                    prom_value = prom_value_template
                    prom_unit = prom_unit_template
                    if match:
                        for i, group in enumerate(match.groups(), start=1):
                            if group:
                                prom_value = prom_value.replace(f'${i}', group)
                                if prom_unit:
                                    prom_unit = prom_unit.replace(f'${i}', group)
                    
                    if prom_attribute == 'Пропустити' or rule_kind == 'skip':
                        continue
                    
                    attr_key = prom_attribute.lower().strip()
                    
                    if attr_key in seen_attributes:
                        current_data = seen_attributes[attr_key]
                        current_value = current_data['value']
                        current_kind = current_data.get('kind', 'extract')
                        current_priority = current_data['priority']
                        
                        if self._should_apply_rule(rule, current_value, current_kind, current_priority):
                            if self.logger:
                                self.logger.debug(
                                    f"🔄 Оновлюю з назви '{prom_attribute}': {current_kind}[{current_priority}] → "
                                    f"{rule_kind}[{rule['priority']}]"
                                )
                            for attr in mapped_attributes:
                                if attr['name'].lower().strip() == attr_key:
                                    attr['value'] = prom_value
                                    attr['unit'] = prom_unit if prom_unit else attr.get('unit', '')
                                    attr['rule_priority'] = rule['priority']
                                    attr['rule_kind'] = rule_kind
                                    seen_attributes[attr_key] = {
                                        'value': prom_value,
                                        'priority': rule['priority'],
                                        'kind': rule_kind
                                    }
                                    break
                        continue
                    
                    new_attr = {
                        'name': prom_attribute,
                        'unit': prom_unit if prom_unit else '',
                        'value': prom_value,
                        'rule_priority': rule['priority'],
                        'rule_kind': rule_kind,
                        'source': 'product_name'
                    }
                    mapped_attributes.append(new_attr)
                    seen_attributes[attr_key] = {
                        'value': prom_value,
                        'priority': rule['priority'],
                        'kind': rule_kind
                    }
                    
                    if self.logger:
                        self.logger.debug(
                            f"✅ З назви: '{product_name}' → {prom_attribute}={prom_value} ({rule_kind}[{rule['priority']}])"
                        )
        
        return mapped_attributes
    
    def map_attributes(self, specifications_list: List[Dict], category_id: Optional[str] = None) -> Dict:
        """
        Мапить список характеристик
        
        Returns:
            {
                'supplier': [...],
                'mapped': [...],
                'unmapped': [...]
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
                result['mapped'].extend(mapped_list)
            else:
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
    
    logger = logging.getLogger('test')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    
    test_specs = [
        {'name': 'Тип', 'unit': '', 'value': 'UTP CAT5e'},
        {'name': 'Довжина кабеля', 'unit': '', 'value': '305 м'},
    ]
    
    rules_path = r"C:\FullStack\Scrapy\data\viatec\viatec_mapping_rules.csv"
    mapper = AttributeMapper(rules_path, logger)
    
    result = mapper.map_attributes(test_specs, category_id="301105")
    
    print("\n" + "="*80)
    print("РЕЗУЛЬТАТ МАППІНГУ:")
    print("="*80)
    
    print(f"\n✅ Змаплені ({len(result['mapped'])}):")
    for spec in result['mapped']:
        kind = spec.get('rule_kind', 'extract')
        priority = spec.get('rule_priority', 999)
        print(f"  • {spec['name']}: {spec['value']} [{kind}, priority={priority}]")


if __name__ == '__main__':
    test_mapper()

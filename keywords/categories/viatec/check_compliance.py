"""
Скрипт для проверки строгости соблюдения allowed_specs в категорийных обработчиках.
Проверяет, что обработчики используют ТОЛЬКО те характеристики, которые указаны в viatec_keywords.csv.
"""

import ast
import re
from pathlib import Path
from typing import Set, List, Dict


def extract_spec_checks_from_file(file_path: Path) -> Set[str]:
    """
    Извлекает все названия характеристик, которые проверяет обработчик.
    
    Args:
        file_path: Путь к файлу обработчика
        
    Returns:
        Множество названий проверяемых характеристик
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    checked_specs = set()
    
    # Паттерн 1: is_spec_allowed("Название", allowed)
    pattern1 = r'is_spec_allowed\(["\']([^"\']+)["\']\s*,\s*allowed\)'
    for match in re.finditer(pattern1, content):
        checked_specs.add(match.group(1))
    
    # Паттерн 2: accessor.value("Название")
    pattern2 = r'accessor\.value\(["\']([^"\']+)["\']\)'
    for match in re.finditer(pattern2, content):
        checked_specs.add(match.group(1))
    
    # Паттерн 3: extract_*(..., "Название")
    pattern3 = r'extract_\w+\([^,]+,\s*["\']([^"\']+)["\']\)'
    for match in re.finditer(pattern3, content):
        checked_specs.add(match.group(1))
    
    return checked_specs


def parse_csv_allowed_specs() -> Dict[str, Set[str]]:
    """
    Парсит viatec_keywords.csv и извлекает allowed_specs для каждой категории.
    
    Returns:
        Словарь {category_id: set(allowed_specs)}
    """
    csv_path = Path(__file__).parent.parent.parent / "data" / "viatec" / "viatec_keywords.csv"
    
    category_specs = {}
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        # Пропускаем заголовок
        next(f)
        
        for line in f:
            parts = line.strip().split(';')
            if len(parts) < 7:
                continue
            
            category_id = parts[0]
            allowed_specs_str = parts[6]
            
            # Разбиваем на отдельные характеристики
            allowed_specs = {spec.strip() for spec in allowed_specs_str.split(',')}
            
            category_specs[category_id] = allowed_specs
    
    return category_specs


def check_handler_compliance():
    """
    Проверяет соответствие обработчиков allowed_specs из CSV.
    """
    # Маппинг категорий → файлы обработчиков
    handlers = {
        "70704": "hdd.py",
        "63705": "sd_card.py",
        "70501": "usb_flash.py",
        "301112": "mounts.py",
        "5092913": "boxes.py",
    }
    
    # Получаем allowed_specs из CSV
    csv_specs = parse_csv_allowed_specs()
    
    # Проверяем каждый обработчик
    base_path = Path(__file__).parent
    
    print("=" * 80)
    print("ПРОВЕРКА СТРОГОСТИ СОБЛЮДЕНИЯ allowed_specs")
    print("=" * 80)
    print()
    
    all_compliant = True
    
    for category_id, handler_file in handlers.items():
        handler_path = base_path / handler_file
        
        if not handler_path.exists():
            print(f"⚠️  Категория {category_id}: файл {handler_file} не найден")
            continue
        
        # Получаем allowed_specs из CSV
        allowed_in_csv = csv_specs.get(category_id, set())
        
        # Извлекаем проверяемые характеристики из кода
        checked_in_code = extract_spec_checks_from_file(handler_path)
        
        # Проверяем соответствие
        print(f"📁 Категория {category_id} ({handler_file}):")
        print(f"   Allowed в CSV: {sorted(allowed_in_csv)}")
        print(f"   Проверяется в коде: {sorted(checked_in_code)}")
        
        # Находим нарушения
        violations = checked_in_code - allowed_in_csv
        
        if violations:
            print(f"   ❌ НАРУШЕНИЯ! Проверяются характеристики, не указанные в allowed_specs:")
            for violation in sorted(violations):
                print(f"      - {violation}")
            all_compliant = False
        else:
            print(f"   ✅ Все проверки соответствуют allowed_specs")
        
        print()
    
    print("=" * 80)
    if all_compliant:
        print("✅ ВСЕ ОБРАБОТЧИКИ СТРОГО СОБЛЮДАЮТ allowed_specs")
    else:
        print("❌ НАЙДЕНЫ НАРУШЕНИЯ! Обработчики проверяют характеристики вне allowed_specs")
    print("=" * 80)


if __name__ == "__main__":
    check_handler_compliance()

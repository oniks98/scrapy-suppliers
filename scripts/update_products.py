#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Універсальний скрипт порівняння та оновлення товарів для всіх постачальників.
Підтримує типи: dealer, retail
"""

import csv
import os
import sys
from typing import Dict, List, Set


SUPPLIERS = ['viatec', 'secur', 'neolight', 'lun', 'eserver']
TYPES = ['dealer', 'retail']


def read_csv_as_rows(file_path: str) -> tuple[List[List[str]], List[str]]:
    """Читає CSV як список рядків."""
    rows = []
    headers = []
    
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f, delimiter=';')
            headers = next(reader)
            for row in reader:
                rows.append(row)
        
        print(f"✅ Прочитано {len(rows)} товарів з {os.path.basename(file_path)}")
        return rows, headers
        
    except FileNotFoundError:
        print(f"❌ Файл не знайдено: {file_path}")
        return [], []
    except Exception as e:
        print(f"❌ Помилка читання: {e}")
        return [], []


def get_field_index(headers: List[str], field_name: str) -> int:
    """Повертає індекс поля або -1."""
    try:
        return headers.index(field_name)
    except ValueError:
        return -1


def normalize_name(name: str) -> str:
    """Нормалізує назву: прибирає зайві пробіли, lowercase."""
    return ' '.join(name.split()).strip().lower()


def get_max_product_code(rows: List[List[str]], code_idx: int) -> int:
    """Повертає максимальний код товару."""
    max_code = 0
    for row in rows:
        if code_idx < len(row):
            try:
                code = int(row[code_idx])
                if code > max_code:
                    max_code = code
            except (ValueError, IndexError):
                continue
    return max_code


def get_characteristics_start_index(headers: List[str]) -> int:
    """Індекс початку характеристик (після "Де_знаходиться_товар")."""
    try:
        return headers.index("Де_знаходиться_товар") + 1
    except ValueError:
        return len(headers)


def merge_rows(old_row: List[str], new_row: List[str], 
               old_headers: List[str], availability_idx: int, 
               quantity_idx: int, chars_start_idx: int) -> List[str]:
    """Об'єднує рядки: базові поля зі старого, наявність/кількість/характеристики з нового."""
    merged = old_row.copy()
    
    # Оновлюємо Наявність та Кількість
    if availability_idx < len(new_row) and availability_idx < len(merged):
        merged[availability_idx] = new_row[availability_idx]
    
    if quantity_idx < len(new_row) and quantity_idx < len(merged):
        merged[quantity_idx] = new_row[quantity_idx]
    
    # Заміняємо характеристики
    merged = merged[:chars_start_idx]
    if chars_start_idx < len(new_row):
        merged.extend(new_row[chars_start_idx:])
    
    # Доповнюємо до потрібної довжини
    while len(merged) < len(old_headers):
        merged.append("")
    
    return merged


def process_supplier(supplier: str, product_type: str) -> None:
    """Обробляє одного постачальника з вказаним типом."""
    print(f"\n{'='*60}")
    print(f"🔄 {supplier.upper()} - {product_type.upper()}")
    print(f"{'='*60}")
    
    base_path = r"C:\FullStack\Scrapy"
    
    # Шляхи до файлів
    export_file = os.path.join(base_path, "data", supplier, "export-products.csv")
    new_file = os.path.join(base_path, "output", f"{supplier}_{product_type}.csv")
    import_file = os.path.join(base_path, "data", supplier, "import_products.csv")
    
    # Перевіряємо існування файлів
    if not os.path.exists(export_file):
        print(f"❌ Export файл не знайдено: {export_file}")
        return
    
    if not os.path.exists(new_file):
        print(f"❌ {product_type.capitalize()} файл не знайдено: {new_file}")
        return
    
    # Читаємо файли
    old_rows, old_headers = read_csv_as_rows(export_file)
    new_rows, new_headers = read_csv_as_rows(new_file)
    
    if not old_rows or not new_rows:
        print("❌ Не вдалося прочитати файли")
        return
    
    # Індекси полів
    name_idx = get_field_index(old_headers, "Назва_позиції")
    code_idx = get_field_index(old_headers, "Код_товару")
    availability_idx = get_field_index(old_headers, "Наявність")
    quantity_idx = get_field_index(old_headers, "Кількість")
    chars_start_idx = get_characteristics_start_index(old_headers)
    
    if name_idx == -1:
        print("❌ Не знайдено колонку 'Назва_позиції'")
        return
    
    # Створюємо словники
    old_products_dict: Dict[str, List[str]] = {}
    for row in old_rows:
        if name_idx < len(row):
            name = normalize_name(row[name_idx])
            if name:
                old_products_dict[name] = row
    
    new_products_dict: Dict[str, List[str]] = {}
    for row in new_rows:
        if name_idx < len(row):
            name = normalize_name(row[name_idx])
            if name:
                new_products_dict[name] = row
    
    print(f"📊 Старих товарів: {len(old_products_dict)}")
    print(f"📊 Нових товарів:  {len(new_products_dict)}")
    
    # Список для імпорту
    import_rows: List[List[str]] = []
    processed_names: Set[str] = set()
    
    stats = {
        'unchanged': 0,
        'qty_changed': 0,
        'availability_changed': 0,
        'both_changed': 0,
        'not_in_new': 0,
        'new_products': 0
    }
    
    # Обробка існуючих товарів
    for old_name, old_row in old_products_dict.items():
        processed_names.add(old_name)
        
        if old_name in new_products_dict:
            new_row = new_products_dict[old_name]
            
            old_availability = old_row[availability_idx] if availability_idx < len(old_row) else ""
            new_availability = new_row[availability_idx] if availability_idx < len(new_row) else ""
            old_quantity = old_row[quantity_idx] if quantity_idx < len(old_row) else ""
            new_quantity = new_row[quantity_idx] if quantity_idx < len(new_row) else ""
            
            availability_changed = old_availability.strip() != new_availability.strip()
            quantity_changed = old_quantity.strip() != new_quantity.strip()
            
            if not availability_changed and not quantity_changed:
                stats['unchanged'] += 1
                continue
            
            updated_row = merge_rows(old_row, new_row, old_headers, 
                                    availability_idx, quantity_idx, chars_start_idx)
            
            if availability_changed and quantity_changed:
                stats['both_changed'] += 1
            elif quantity_changed:
                stats['qty_changed'] += 1
            elif availability_changed:
                stats['availability_changed'] += 1
            
            import_rows.append(updated_row)
            
        else:
            # Товар відсутній у новому - видаляємо
            updated_row = old_row.copy()
            if availability_idx < len(updated_row):
                updated_row[availability_idx] = "-"
            if quantity_idx < len(updated_row):
                updated_row[quantity_idx] = "0"
            import_rows.append(updated_row)
            stats['not_in_new'] += 1
    
    # Обробка нових товарів
    new_product_names = set(new_products_dict.keys()) - processed_names
    
    if new_product_names:
        max_code = get_max_product_code(old_rows, code_idx)
        next_code = max_code + 1
        
        for new_name in sorted(new_product_names):
            new_row = new_products_dict[new_name].copy()
            
            if code_idx < len(new_row):
                new_row[code_idx] = str(next_code)
            
            while len(new_row) < len(old_headers):
                new_row.append("")
            
            import_rows.append(new_row)
            next_code += 1
            stats['new_products'] += 1
    
    # Запис результату
    try:
        os.makedirs(os.path.dirname(import_file), exist_ok=True)
        
        with open(import_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(old_headers)
            
            for row in import_rows:
                row = row[:len(old_headers)]
                writer.writerow(row)
        
        print(f"\n✅ Файл створено: {import_file}")
        
    except Exception as e:
        print(f"❌ Помилка запису: {e}")
        return
    
    # Статистика
    print(f"\n{'='*60}")
    print("📈 СТАТИСТИКА:")
    print(f"{'='*60}")
    print(f"  Без змін:                {stats['unchanged']}")
    print(f"  Змінилася кількість:     {stats['qty_changed']}")
    print(f"  Змінилася наявність:     {stats['availability_changed']}")
    print(f"  Змінилося обидва:        {stats['both_changed']}")
    print(f"  Відсутні в новому:       {stats['not_in_new']}")
    print(f"  Нові товари:             {stats['new_products']}")
    print(f"{'-'*60}")
    print(f"  ВСЬОГО для імпорту:      {len(import_rows)}")
    print(f"{'='*60}")


def main():
    """Головна функція."""
    print("="*60)
    print("🚀 УНІВЕРСАЛЬНИЙ СКРИПТ ОНОВЛЕННЯ ТОВАРІВ")
    print("="*60)
    
    # Без аргументів - обробити всіх
    if len(sys.argv) == 1:
        print("\n📦 Обробка всіх постачальників...")
        for supplier in SUPPLIERS:
            for product_type in TYPES:
                try:
                    process_supplier(supplier, product_type)
                except Exception as e:
                    print(f"❌ Помилка {supplier} {product_type}: {e}")
        print("\n✅ ВСІ ПОСТАЧАЛЬНИКИ ОБРОБЛЕНО")
        return
    
    # Перевірка аргументів
    if len(sys.argv) < 3:
        print("\n❌ Використання: python update_products.py <supplier> <type>")
        print(f"\nПостачальники: {', '.join(SUPPLIERS)}")
        print(f"Типи: {', '.join(TYPES)}")
        print("\nПриклади:")
        print("  python update_products.py                  # Всі постачальники")
        print("  python update_products.py viatec dealer")
        print("  python update_products.py viatec retail")
        sys.exit(1)
    
    supplier = sys.argv[1].lower()
    product_type = sys.argv[2].lower()
    
    if supplier not in SUPPLIERS:
        print(f"❌ Невідомий постачальник: {supplier}")
        print(f"Доступні: {', '.join(SUPPLIERS)}")
        sys.exit(1)
    
    if product_type not in TYPES:
        print(f"❌ Невідомий тип: {product_type}")
        print(f"Доступні: {', '.join(TYPES)}")
        sys.exit(1)
    
    process_supplier(supplier, product_type)
    print("\n✅ ЗАВЕРШЕНО")


if __name__ == "__main__":
    main()

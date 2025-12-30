#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Універсальний скрипт порівняння та оновлення товарів для всіх постачальників.
Підтримує типи: dealer, retail

ОНОВЛЕНО: Порівняння товарів по Ідентифікатор_товару (артикул постачальника)
"""

import csv
import os
import sys
from typing import Dict, List, Set


SUPPLIERS = ['viatec', 'secur', 'neolight', 'lun', 'eserver']
TYPES = ['dealer', 'retail']


def detect_encoding(file_path: str) -> str:
    """Автоматично визначає кодування файлу."""
    # Спробуємо спочатку прочитати перші байти для визначення
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)  # Перші 10KB
        
        # Перевірка на BOM UTF-8
        if raw_data.startswith(b'\xef\xbb\xbf'):
            return 'utf-8-sig'
        
        # Спробуємо різні кодування
        encodings_to_try = [
            'utf-8',
            'utf-8-sig', 
            'windows-1251',
            'cp1251',
            'latin-1'
        ]
        
        for encoding in encodings_to_try:
            try:
                raw_data.decode(encoding)
                # Перевіряємо чи є заголовок з кирилицею
                try:
                    text = raw_data.decode(encoding)
                    if 'Назва_позиції' in text or 'Код_товару' in text:
                        return encoding
                except:
                    pass
                # Якщо декодування пройшло без помилок, використовуємо це кодування
                return encoding
            except (UnicodeDecodeError, LookupError):
                continue
                
    except Exception as e:
        print(f"⚠️  Помилка визначення кодування: {e}")
    
    # Fallback
    return 'utf-8-sig'


def read_csv_as_rows(file_path: str) -> tuple[List[List[str]], List[str]]:
    """Читає CSV як список рядків з автоматичним визначенням кодування."""
    rows = []
    headers = []
    
    try:
        encoding = detect_encoding(file_path)
        print(f"🔍 Кодування: {encoding}")
        
        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            reader = csv.reader(f, delimiter=';')
            headers = next(reader)
            
            # Виводимо перші 3 колонки заголовка для діагностики
            print(f"📋 Заголовки: {headers[:3]}...")
            
            for row in reader:
                rows.append(row)
        
        print(f"✅ Прочитано {len(rows)} товарів з {os.path.basename(file_path)}")
        return rows, headers
        
    except FileNotFoundError:
        print(f"❌ Файл не знайдено: {file_path}")
        return [], []
    except Exception as e:
        print(f"❌ Помилка читання: {e}")
        import traceback
        traceback.print_exc()
        return [], []


def get_field_index(headers: List[str], field_name: str) -> int:
    """Повертає індекс поля або -1."""
    try:
        return headers.index(field_name)
    except ValueError:
        print(f"⚠️  Не знайдено колонку '{field_name}'")
        print(f"⚠️  Доступні колонки: {headers[:10]}...")
        return -1


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
    print("\n📂 Читаємо export-products.csv...")
    old_rows, old_headers = read_csv_as_rows(export_file)
    
    print(f"\n📂 Читаємо {product_type}.csv...")
    new_rows, new_headers = read_csv_as_rows(new_file)
    
    if not old_rows or not new_rows:
        print("❌ Не вдалося прочитати файли")
        return
    
    # Індекси полів
    name_idx = get_field_index(old_headers, "Назва_позиції")
    code_idx = get_field_index(old_headers, "Код_товару")
    availability_idx = get_field_index(old_headers, "Наявність")
    quantity_idx = get_field_index(old_headers, "Кількість")
    identifier_idx = get_field_index(old_headers, "Ідентифікатор_товару")
    chars_start_idx = get_characteristics_start_index(old_headers)
    
    if name_idx == -1:
        print("❌ Не знайдено колонку 'Назва_позиції'")
        return
    
    if identifier_idx == -1:
        print("❌ Не знайдено колонку 'Ідентифікатор_товару'")
        return
    
    # Створюємо словники по Ідентифікатор_товару (артикул постачальника)
    old_products_dict: Dict[str, List[str]] = {}  # {identifier: row}
    old_no_identifier: List[str] = []
    old_duplicates: List[tuple[str, str]] = []
    
    for row in old_rows:
        if identifier_idx < len(row):
            identifier = row[identifier_idx].strip()
            
            if not identifier:
                product_name = row[name_idx].strip() if name_idx < len(row) else 'N/A'
                old_no_identifier.append(f"{product_name[:40]}... | Код: {row[code_idx] if code_idx < len(row) else 'N/A'}")
            elif identifier in old_products_dict:
                product_name = row[name_idx].strip() if name_idx < len(row) else 'N/A'
                old_duplicates.append((product_name, identifier))
            else:
                old_products_dict[identifier] = row
    
    new_products_dict: Dict[str, List[str]] = {}  # {identifier: row}
    new_no_identifier: List[str] = []
    new_duplicates: List[tuple[str, str]] = []
    
    for row in new_rows:
        if identifier_idx < len(row):
            identifier = row[identifier_idx].strip()
            
            if not identifier:
                product_name = row[name_idx].strip() if name_idx < len(row) else 'N/A'
                new_no_identifier.append(f"{product_name[:40]}... | Код: {row[code_idx] if code_idx < len(row) else 'N/A'}")
            elif identifier in new_products_dict:
                product_name = row[name_idx].strip() if name_idx < len(row) else 'N/A'
                new_duplicates.append((product_name, identifier))
            else:
                new_products_dict[identifier] = row
    
    print(f"\n📊 Старих товарів (з ідентифікатором): {len(old_products_dict)}")
    print(f"📊 Нових товарів (з ідентифікатором):  {len(new_products_dict)}")
    
    # Виводимо інформацію про фільтрацію
    if old_no_identifier or old_duplicates or new_no_identifier or new_duplicates:
        print(f"\n{'-'*60}")
        print("⚠️  ФІЛЬТРАЦІЯ ТОВАРІВ:")
        print(f"{'-'*60}")
        
        if old_no_identifier:
            print(f"\n🚫 Без ідентифікатора в export-products.csv: {len(old_no_identifier)}")
            for item in old_no_identifier[:5]:
                print(f"   - {item}")
            if len(old_no_identifier) > 5:
                print(f"   ... та ще {len(old_no_identifier) - 5}")
        
        if old_duplicates:
            print(f"\n🔁 Дублікати ідентифікаторів в export-products.csv: {len(old_duplicates)}")
            for name, identifier in old_duplicates[:5]:
                print(f"   - '{name}' | ID: '{identifier}'")
            if len(old_duplicates) > 5:
                print(f"   ... та ще {len(old_duplicates) - 5}")
        
        if new_no_identifier:
            print(f"\n🚫 Без ідентифікатора в {product_type}.csv: {len(new_no_identifier)}")
            for item in new_no_identifier[:5]:
                print(f"   - {item}")
            if len(new_no_identifier) > 5:
                print(f"   ... та ще {len(new_no_identifier) - 5}")
        
        if new_duplicates:
            print(f"\n🔁 Дублікати ідентифікаторів в {product_type}.csv: {len(new_duplicates)}")
            for name, identifier in new_duplicates[:5]:
                print(f"   - '{name}' | ID: '{identifier}'")
            if len(new_duplicates) > 5:
                print(f"   ... та ще {len(new_duplicates) - 5}")
        
        print(f"{'-'*60}")
    
    # Список для імпорту
    import_rows: List[List[str]] = []
    processed_identifiers: Set[str] = set()  # Відстежуємо ідентифікатори
    
    stats = {
        'unchanged': 0,
        'qty_changed': 0,
        'availability_changed': 0,
        'both_changed': 0,
        'not_in_new': 0,
        'already_unavailable': 0,
        'new_products': 0
    }
    
    # Обробка існуючих товарів (порівнюємо по Ідентифікатор_товару)
    for old_identifier, old_row in old_products_dict.items():
        processed_identifiers.add(old_identifier)
        
        if old_identifier in new_products_dict:
            new_row = new_products_dict[old_identifier]
            
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
            # Товар відсутній у новому - перевіряємо чи він вже був відсутній
            old_availability = old_row[availability_idx] if availability_idx < len(old_row) else ""
            old_quantity = old_row[quantity_idx] if quantity_idx < len(old_row) else ""
            
            # Якщо товар УЖЕ був відсутній - пропускаємо (не потрібно оновлювати)
            if old_availability.strip() == "-" and old_quantity.strip() == "0":
                stats['already_unavailable'] += 1
                continue
            
            # Товар був в наявності, але зник - позначаємо як відсутній
            updated_row = old_row.copy()
            if availability_idx < len(updated_row):
                updated_row[availability_idx] = "-"
            if quantity_idx < len(updated_row):
                updated_row[quantity_idx] = "0"
            import_rows.append(updated_row)
            stats['not_in_new'] += 1
    
    # Обробка нових товарів (порівнюємо по Ідентифікатор_товару)
    new_product_identifiers = set(new_products_dict.keys()) - processed_identifiers
    
    if new_product_identifiers:
        max_code = get_max_product_code(old_rows, code_idx)
        next_code = max_code + 1
        
        for new_identifier in sorted(new_product_identifiers):
            new_row = new_products_dict[new_identifier].copy()
            
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
    print(f"  Вже були відсутні:      {stats['already_unavailable']}")
    print(f"  Нові товари:             {stats['new_products']}")
    print(f"{'-'*60}")
    print(f"  ВСЬОГО для імпорту:      {len(import_rows)}")
    print(f"{'='*60}")


def main():
    """Головна функція."""
    print("="*60)
    print("🚀 УНІВЕРСАЛЬНИЙ СКРИПТ ОНОВЛЕННЯ ТОВАРІВ")
    print("   (Порівняння по Ідентифікатор_товару)")
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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для оновлення товарів v2.1.
Порівнює старий і новий список товарів та створює файл для імпорту.

ОНОВЛЕНО: Характеристики тепер коректно копіюються з нового файлу!
"""

import csv
import os
from typing import Dict, List, Set


def read_csv_file_as_rows(file_path: str) -> tuple[List[List[str]], List[str]]:
    """
    Читає CSV файл як список рядків (не словників).
    Це необхідно для роботи з повторюваними заголовками характеристик.
    
    Args:
        file_path: Шлях до CSV файлу
        
    Returns:
        tuple: (список рядків, список заголовків)
    """
    rows = []
    headers = []
    
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f, delimiter=';')
            headers = next(reader)  # Перший рядок - заголовки
            for row in reader:
                rows.append(row)
        
        print(f"✅ Прочитано {len(rows)} товарів з {os.path.basename(file_path)}")
        print(f"   Колонок: {len(headers)}")
        return rows, headers
        
    except FileNotFoundError:
        print(f"❌ Файл не знайдено: {file_path}")
        return [], []
    except Exception as e:
        print(f"❌ Помилка читання файлу {file_path}: {e}")
        return [], []


def get_field_index(headers: List[str], field_name: str) -> int:
    """
    Знаходить індекс поля в заголовках.
    
    Args:
        headers: Список заголовків
        field_name: Назва поля
        
    Returns:
        int: Індекс поля або -1 якщо не знайдено
    """
    try:
        return headers.index(field_name)
    except ValueError:
        return -1


def normalize_name(name: str) -> str:
    """
    Нормалізує назву товару для порівняння.
    - Прибирає зайві пробіли
    - Переводить у нижній регістр для порівняння
    
    Приклад:
        "антивандальний ящик IPCOM БК-400-З-2" == 
        "Антивандальний ящик IPCOM БК-400-З-2"
    
    Args:
        name: Назва товару
        
    Returns:
        str: Нормалізована назва (lowercase, без зайвих пробілів)
    """
    # Прибираємо зайві пробіли та переводимо у нижній регістр
    normalized = ' '.join(name.split()).strip().lower()
    return normalized


def get_max_product_code(rows: List[List[str]], code_idx: int) -> int:
    """
    Повертає максимальний код товару зі списку.
    
    Args:
        rows: Список рядків товарів
        code_idx: Індекс колонки з кодом товару
        
    Returns:
        int: Максимальний код товару
    """
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
    """
    Знаходить індекс початку характеристик (після "Де_знаходиться_товар").
    
    Args:
        headers: Список заголовків
        
    Returns:
        int: Індекс початку характеристик
    """
    try:
        base_end_idx = headers.index("Де_знаходиться_товар")
        return base_end_idx + 1
    except ValueError:
        # Якщо не знайдено, повертаємо довжину заголовків
        return len(headers)


def merge_rows(old_row: List[str], new_row: List[str], 
               old_headers: List[str], new_headers: List[str],
               availability_idx: int, quantity_idx: int,
               chars_start_idx: int) -> List[str]:
    """
    Об'єднує дані старого та нового рядка.
    Базові поля зі старого, наявність/кількість/характеристики з нового.
    
    Args:
        old_row: Старий рядок
        new_row: Новий рядок
        old_headers: Заголовки старого файлу
        new_headers: Заголовки нового файлу
        availability_idx: Індекс колонки "Наявність"
        quantity_idx: Індекс колонки "Кількість"
        chars_start_idx: Індекс початку характеристик
        
    Returns:
        List[str]: Об'єднаний рядок
    """
    merged = old_row.copy()
    
    # Оновлюємо Наявність
    if availability_idx < len(new_row):
        if availability_idx < len(merged):
            merged[availability_idx] = new_row[availability_idx]
    
    # Оновлюємо Кількість
    if quantity_idx < len(new_row):
        if quantity_idx < len(merged):
            merged[quantity_idx] = new_row[quantity_idx]
    
    # 🔥 КЛЮЧОВЕ: Заміняємо ВСІ характеристики з нового файлу
    # Видаляємо старі характеристики
    merged = merged[:chars_start_idx]
    
    # Додаємо нові характеристики з нового файлу
    if chars_start_idx < len(new_row):
        merged.extend(new_row[chars_start_idx:])
    
    # Доповнюємо до потрібної довжини якщо треба
    while len(merged) < len(old_headers):
        merged.append("")
    
    return merged


def create_import_file(old_file: str, new_file: str, output_file: str) -> None:
    """
    Створює файл для імпорту на основі порівняння старого та нового файлів.
    
    Логіка:
    1. Якщо Наявність ТА Кількість однакові - НЕ додаємо в імпорт
    2. Якщо щось змінилося - додаємо зі старого з оновленими даними + характеристики з НОВОГО
    3. Якщо товар є в новому, але немає в старому - додаємо з новим кодом
    4. Якщо товар є в старому, але немає в новому - додаємо з Наявність="-" та Кількість="0"
    
    Args:
        old_file: Шлях до старого файлу
        new_file: Шлях до нового файлу
        output_file: Шлях до вихідного файлу
    """
    # Читаємо файли як рядки
    old_rows, old_headers = read_csv_file_as_rows(old_file)
    new_rows, new_headers = read_csv_file_as_rows(new_file)
    
    if not old_rows or not new_rows:
        print("❌ Не вдалося прочитати файли. Перевірте шляхи.")
        return
    
    # Знаходимо індекси важливих полів
    name_idx = get_field_index(old_headers, "Назва_позиції")
    code_idx = get_field_index(old_headers, "Код_товару")
    availability_idx = get_field_index(old_headers, "Наявність")
    quantity_idx = get_field_index(old_headers, "Кількість")
    chars_start_idx = get_characteristics_start_index(old_headers)
    
    if name_idx == -1:
        print("❌ Не знайдено колонку 'Назва_позиції'")
        return
    
    print(f"📊 Індекси полів:")
    print(f"   Назва_позиції: {name_idx}")
    print(f"   Код_товару: {code_idx}")
    print(f"   Наявність: {availability_idx}")
    print(f"   Кількість: {quantity_idx}")
    print(f"   Початок характеристик: {chars_start_idx}")
    print(f"   Кількість полів характеристик: {len(old_headers) - chars_start_idx}")
    
    # Створюємо словники для швидкого пошуку
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
    
    print(f"\n📊 Статистика:")
    print(f"   Старих товарів: {len(old_products_dict)}")
    print(f"   Нових товарів: {len(new_products_dict)}")
    
    # Показуємо приклад нормалізації
    if old_rows and name_idx < len(old_rows[0]):
        example_original = old_rows[0][name_idx]
        example_normalized = normalize_name(example_original)
        print(f"\n🔤 Приклад нормалізації назв:")
        print(f"   Оригінал:      '{example_original}'")
        print(f"   Нормалізовано: '{example_normalized}'")
        print(f"   Це означає що регістр не впливає на порівняння")
    
    # Список рядків для імпорту
    import_rows: List[List[str]] = []
    
    # Множина оброблених товарів
    processed_names: Set[str] = set()
    
    # Статистика
    stats = {
        'unchanged': 0,
        'qty_changed': 0,
        'availability_changed': 0,
        'both_changed': 0,
        'not_in_new': 0,
        'new_products': 0
    }
    
    # 1. Обробляємо товари зі старого файлу
    print("\n🔄 Обробка існуючих товарів...")
    
    for old_name, old_row in old_products_dict.items():
        processed_names.add(old_name)
        
        if old_name in new_products_dict:
            new_row = new_products_dict[old_name]
            
            # Порівнюємо Наявність та Кількість
            old_availability = old_row[availability_idx] if availability_idx < len(old_row) else ""
            new_availability = new_row[availability_idx] if availability_idx < len(new_row) else ""
            
            old_quantity = old_row[quantity_idx] if quantity_idx < len(old_row) else ""
            new_quantity = new_row[quantity_idx] if quantity_idx < len(new_row) else ""
            
            availability_changed = old_availability.strip() != new_availability.strip()
            quantity_changed = old_quantity.strip() != new_quantity.strip()
            
            if not availability_changed and not quantity_changed:
                stats['unchanged'] += 1
                continue
            
            # 🔥 Об'єднуємо: базові поля зі старого + оновлені дані + характеристики з НОВОГО
            updated_row = merge_rows(old_row, new_row, old_headers, new_headers,
                                    availability_idx, quantity_idx, chars_start_idx)
            
            if quantity_changed:
                stats['qty_changed'] += 1
            
            if availability_changed:
                stats['availability_changed'] += 1
            
            if availability_changed and quantity_changed:
                stats['both_changed'] += 1
                stats['qty_changed'] -= 1
                stats['availability_changed'] -= 1
            
            import_rows.append(updated_row)
            
        else:
            # Товар є в старому, але немає в новому
            updated_row = old_row.copy()
            if availability_idx < len(updated_row):
                updated_row[availability_idx] = "-"
            if quantity_idx < len(updated_row):
                updated_row[quantity_idx] = "0"
            import_rows.append(updated_row)
            stats['not_in_new'] += 1
    
    # 2. Обробляємо нові товари
    print("➕ Обробка нових товарів...")
    
    new_product_names = set(new_products_dict.keys()) - processed_names
    
    if new_product_names:
        max_code = get_max_product_code(old_rows, code_idx)
        next_code = max_code + 1
        
        for new_name in sorted(new_product_names):
            new_row = new_products_dict[new_name].copy()
            
            # Встановлюємо новий код
            if code_idx < len(new_row):
                new_row[code_idx] = str(next_code)
            
            # Доповнюємо до потрібної довжини
            while len(new_row) < len(old_headers):
                new_row.append("")
            
            import_rows.append(new_row)
            next_code += 1
            stats['new_products'] += 1
    
    # 3. Записуємо результат
    print(f"\n💾 Запис результатів у {os.path.basename(output_file)}...")
    
    try:
        with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            
            # Записуємо заголовки
            writer.writerow(old_headers)
            
            # Записуємо всі рядки
            for row in import_rows:
                # Обрізаємо до довжини заголовків
                row = row[:len(old_headers)]
                writer.writerow(row)
        
        print(f"✅ Файл успішно створено!")
        
    except Exception as e:
        print(f"❌ Помилка запису файлу: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 4. Виводимо статистику
    print("\n" + "="*60)
    print("📈 ПІДСУМКОВА СТАТИСТИКА:")
    print("="*60)
    print(f"  Без змін (не додано):           {stats['unchanged']}")
    print(f"  Змінилася кількість:            {stats['qty_changed']}")
    print(f"  Змінилася наявність:            {stats['availability_changed']}")
    print(f"  Змінилося обидва параметри:     {stats['both_changed']}")
    print(f"  Відсутні в новому файлі:        {stats['not_in_new']}")
    print(f"  Нові товари:                    {stats['new_products']}")
    print("-"*60)
    print(f"  ВСЬОГО для імпорту:             {len(import_rows)}")
    print("="*60)


def main():
    """Головна функція скрипта."""
    print("="*60)
    print("🚀 СКРИПТ ОНОВЛЕННЯ ТОВАРІВ v2.1")
    print("="*60)
    print("✨ Характеристики тепер коректно копіюються!")
    
    # Шляхи до файлів
    base_path = r"C:\FullStack\Scrapy\data\viatec"
    old_file = os.path.join(base_path, "old_products.csv")
    new_file = os.path.join(base_path, "new_products.csv")
    output_file = os.path.join(base_path, "import_products.csv")
    
    print(f"\n📁 Файли:")
    print(f"   Старий: {old_file}")
    print(f"   Новий:  {new_file}")
    print(f"   Вихід:  {output_file}")
    print()
    
    # Перевіряємо існування вхідних файлів
    if not os.path.exists(old_file):
        print(f"❌ Старий файл не знайдено: {old_file}")
        return
    
    if not os.path.exists(new_file):
        print(f"❌ Новий файл не знайдено: {new_file}")
        return
    
    # Створюємо файл для імпорту
    create_import_file(old_file, new_file, output_file)
    
    print("\n✅ Готово!")


if __name__ == "__main__":
    main()

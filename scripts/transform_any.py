"""
УНІВЕРСАЛЬНИЙ скрипт для трансформації файлів зі структурою як invertor.csv
Структура: Назва;Одиниця;Значення повторюються

Використання:
  python transform_any.py <input_file> [output_file]

Приклади:
  python transform_any.py data/viatec/invertor.csv
  python transform_any.py data/viatec/solar.csv data/viatec/solar_transformed.csv
"""
import csv
from collections import OrderedDict
import sys
import os

def transform_csv(input_file, output_file=None):
    """
    Трансформує CSV файл зі структурою триплетів
    
    Args:
        input_file: Шлях до вхідного файлу
        output_file: Шлях до вихідного файлу (якщо None, додасть _transformed)
    """
    # Генеруємо ім'я вихідного файлу
    if output_file is None:
        base = os.path.splitext(input_file)[0]
        output_file = f"{base}_transformed.csv"
    
    # Читаємо вхідний файл
    with open(input_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
        rows = list(reader)
    
    print(f"📊 Файл: {os.path.basename(input_file)}")
    print(f"📊 Зчитано {len(rows)} рядків")
    
    # Збираємо всі характеристики та їх значення
    specs_dict = OrderedDict()
    
    for row in rows:
        for i in range(0, len(row), 3):
            if i+2 < len(row):
                name = row[i].strip()
                unit = row[i+1].strip()
                value = row[i+2].strip()
                
                if name and value:
                    key = name
                    
                    if key not in specs_dict:
                        specs_dict[key] = []
                    
                    if value not in specs_dict[key]:
                        specs_dict[key].append(value)
    
    print(f"✅ Знайдено {len(specs_dict)} унікальних характеристик")
    
    # Сортуємо характеристики по назві
    sorted_specs = sorted(specs_dict.items(), key=lambda x: x[0].lower())
    
    # Знаходимо максимальну кількість значень
    max_values = max(len(values) for _, values in sorted_specs) if sorted_specs else 0
    print(f"📈 Максимум значень у однієї характеристики: {max_values}")
    
    # Формуємо заголовок
    header_row = ['Назва_Характеристики'] + ['Значення_Характеристики'] * max_values
    
    # Формуємо рядки
    output_rows = []
    for name, values in sorted_specs:
        row = [name]
        row.extend(values)
        while len(row) < len(header_row):
            row.append('')
        output_rows.append(row)
        
        if len(values) > 5:
            print(f"  • {name}: {len(values)} значень")
    
    # Записуємо у файл
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(header_row)
        writer.writerows(output_rows)
    
    print(f"\n✅ Готово!")
    print(f"📄 Збережено: {output_file}")
    print(f"📊 Характеристик: {len(output_rows)}")
    print(f"📏 Колонок: {len(header_row)}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Використовуємо аргументи командного рядка
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
    else:
        # За замовчуванням
        input_file = r"C:\FullStack\Scrapy\data\viatec\data\data.csv"
        output_file = r"C:\FullStack\Scrapy\data\viatec\data\data_transformed.csv"
    
    transform_csv(input_file, output_file)

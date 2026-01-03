"""
Скрипт для трансформації data_prom.csv
Особливості:
- Структура: Назва;Одиниця;Значення (триплети)
- Значення можуть містити "|" (pipe) - розбиваємо їх на окремі значення
- Приклад: "Звукова|Світлова|Вібро" → 3 окремі значення

Результат: data_prom_transformed.csv
"""
import csv
from collections import OrderedDict

input_file = r"C:\FullStack\Scrapy\data\viatec\data\data_prom.csv"
output_file = r"C:\FullStack\Scrapy\data\viatec\data\data_prom_transformed.csv"

# Читаємо вхідний файл
with open(input_file, 'r', encoding='utf-8-sig') as f:
    reader = csv.reader(f, delimiter=';')
    header = next(reader)
    rows = list(reader)

print(f"📊 Файл: data_prom.csv")
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
                
                # РОЗБИВАЄМО значення по "|" (pipe)
                if '|' in value:
                    split_values = [v.strip() for v in value.split('|') if v.strip()]
                    for split_val in split_values:
                        if split_val not in specs_dict[key]:
                            specs_dict[key].append(split_val)
                else:
                    # Звичайне значення без pipe
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
    
    # Показуємо характеристики з багатьма значеннями
    if len(values) > 5:
        print(f"  • {name}: {len(values)} значень")
    elif len(values) > 1:
        print(f"  • {name}: {values}")

# Записуємо у файл
with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.writer(f, delimiter=';')
    writer.writerow(header_row)
    writer.writerows(output_rows)

print(f"\n✅ Готово!")
print(f"📄 Збережено: {output_file}")
print(f"📊 Характеристик: {len(output_rows)}")
print(f"📏 Колонок: {len(header_row)}")
print("\n💡 Значення з '|' були розбиті на окремі значення")

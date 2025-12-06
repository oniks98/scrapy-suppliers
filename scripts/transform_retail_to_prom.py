"""
Трансформує eserver_retail.csv в eserver_prom.csv
ПОСТРОКОВЕ КОПІЮВАННЯ: Копіює файл рядок за рядком і змінює тільки потрібні колонки
"""
import sys
from pathlib import Path
from decimal import Decimal, InvalidOperation


def normalize_price(price_str: str) -> str:
    """Нормалізує ціну: замінює кому на крапку"""
    return price_str.replace(",", ".").replace(" ", "").strip()


def load_mappings(data_dir: Path):
    """Завантажує всі маппінги з CSV файлів"""
    
    # 1. Коефіцієнт
    coefficient = Decimal("1.05")
    try:
        with open(data_dir / "eserver_coefficient_prom.csv", encoding="utf-8-sig") as f:
            for line in f:
                if line.strip() and "coefficient" not in line.lower():
                    coefficient = Decimal(line.strip())
                    break
        print(f"📊 Коефіцієнт: {coefficient}")
    except Exception as e:
        print(f"❌ Помилка завантаження коефіцієнта: {e}")
    
    # 2. Retail категорії (Номер_групи -> Линк)
    retail_categories = {}
    try:
        with open(data_dir / "eserver_category_retail.csv", encoding="utf-8-sig") as f:
            next(f)  # Skip header
            for line in f:
                parts = line.strip().split(";")
                if len(parts) >= 5:
                    # ИСПРАВЛЕНО: Линк в колонке 1, Номер_групи в колонке 4
                    link = parts[1].strip().strip('"')
                    group_number = parts[4].strip()
                    retail_categories[group_number] = link
        print(f"📂 Retail категорій: {len(retail_categories)}")
        print(f"   Приклад: {list(retail_categories.items())[:2]}")
    except Exception as e:
        print(f"❌ Помилка завантаження retail категорій: {e}")
    
    # 3. PROM категорії (Линк -> Номер_групи, Назва)
    prom_categories = {}
    try:
        with open(data_dir / "eserver_category_prom.csv", encoding="utf-8-sig") as f:
            next(f)  # Skip header
            for line in f:
                parts = line.strip().split(";")
                if len(parts) >= 5:
                    # ИСПРАВЛЕНО: Линк в колонке 1, Номер_групи в колонке 4, Назва в колонке 3
                    link = parts[1].strip().strip('"')
                    group_number = parts[4].strip()
                    category_name = parts[3].strip()  # Категория на моем сайте_UA
                    prom_categories[link] = {
                        "Номер_групи": group_number,
                        "Назва_групи": category_name,
                    }
        print(f"📂 PROM категорій: {len(prom_categories)}")
        print(f"   Приклад: {list(prom_categories.items())[:2]}")
    except Exception as e:
        print(f"❌ Помилка завантаження PROM категорій: {e}")
    
    # 4. Особисті нотатки (Номер_групи -> Нотатка)
    personal_notes = {}
    try:
        with open(data_dir / "eserver_personal_notes.csv", encoding="utf-8-sig") as f:
            next(f)  # Skip header
            for line in f:
                parts = line.strip().split(";")
                if len(parts) >= 2:
                    group_number = parts[0].strip()
                    note = parts[1].strip()
                    personal_notes[group_number] = note
        print(f"📝 Особистих нотаток: {len(personal_notes)}")
        print(f"   Приклад: {list(personal_notes.items())[:2]}")
    except Exception as e:
        print(f"❌ Помилка завантаження особистих нотаток: {e}")
    
    return coefficient, retail_categories, prom_categories, personal_notes


def transform_line(line: str, header: list, coefficient, retail_categories, prom_categories, personal_notes, line_num: int):
    """Трансформує один рядок даних"""
    parts = line.split(";")
    
    if len(parts) < len(header):
        # Якщо рядок коротший за заголовок, додаємо порожні поля
        parts.extend([""] * (len(header) - len(parts)))
    
    try:
        # Індекси колонок
        price_idx = header.index("Ціна")
        group_number_idx = header.index("Номер_групи")
        group_name_idx = header.index("Назва_групи")
        notes_idx = header.index("Особисті_нотатки")
        
        # 1. Ціна: множимо і округлюємо
        price_str = parts[price_idx].strip()
        if price_str:
            try:
                normalized_price = normalize_price(price_str)
                price = Decimal(normalized_price)
                new_price = price * coefficient
                parts[price_idx] = str(int(new_price.quantize(Decimal("1"))))
            except:
                pass
        
        # 2-3. Категорія: змінюємо Номер_групи та Назва_групи
        old_group_number = parts[group_number_idx].strip()
        category_link = retail_categories.get(old_group_number)
        
        # ДОБАВЛЕНА ОТЛАДКА
        if line_num <= 3:
            print(f"   [Рядок {line_num}] old_group_number={old_group_number}, link={category_link}")
        
        if category_link and category_link in prom_categories:
            prom_data = prom_categories[category_link]
            old_name = parts[group_name_idx]
            parts[group_number_idx] = prom_data["Номер_групи"]
            parts[group_name_idx] = prom_data["Назва_групи"]
            
            # ДОБАВЛЕНА ОТЛАДКА
            if line_num <= 3:
                print(f"   [Рядок {line_num}] ✅ ЗАМІНЕНО: {old_group_number} → {prom_data['Номер_групи']}")
                print(f"                  {old_name} → {prom_data['Назва_групи']}")
        else:
            if line_num <= 3:
                print(f"   [Рядок {line_num}] ⚠️ НЕ ЗНАЙДЕНО в маппінгу")
        
        # 4. Особисті нотатки
        new_group_number = parts[group_number_idx].strip()
        if new_group_number in personal_notes:
            parts[notes_idx] = personal_notes[new_group_number]
        else:
            parts[notes_idx] = ""
        
    except ValueError as e:
        print(f"⚠️ Помилка обробки рядка {line_num}: {e}")
    
    return ";".join(parts)


def main():
    """Головна функція"""
    print("ЗАПУСК ПОСТРОКОВОЇ ТРАНСФОРМАЦІЇ: RETAIL → PROM")
    print("=" * 80)
    
    base_dir = Path(r"C:\FullStack\Scrapy")
    data_dir = base_dir / "data" / "eserver"
    output_dir = base_dir / "output"
    
    input_file = output_dir / "eserver_retail.csv"
    output_file = output_dir / "eserver_prom.csv"
    
    if not input_file.exists():
        print(f"❌ Вхідний файл не знайдено: {input_file}")
        return False
    
    # Завантажуємо маппінги
    coefficient, retail_categories, prom_categories, personal_notes = load_mappings(data_dir)
    
    print(f"\n🔄 КОПІЮВАННЯ: {input_file.name} → {output_file.name}")
    
    rows_processed = 0
    rows_written = 0
    rows_transformed = 0
    header = []
    
    try:
        with open(input_file, "r", encoding="utf-8-sig") as infile, \
             open(output_file, "w", encoding="utf-8-sig", newline="") as outfile:
            
            # Читаємо і записуємо заголовок
            header_line = infile.readline()
            header = header_line.strip().split(";")
            outfile.write(header_line)
            
            print(f"\n🔍 ПЕРШІ 3 РЯДКИ (DEBUG):")
            
            # Обробляємо кожен рядок
            for line in infile:
                rows_processed += 1
                
                if line.strip():  # Пропускаємо порожні рядки
                    old_line = line.strip()
                    transformed_line = transform_line(old_line, header, coefficient, retail_categories, prom_categories, personal_notes, rows_processed)
                    
                    if old_line != transformed_line:
                        rows_transformed += 1
                    
                    outfile.write(transformed_line + "\n")
                    rows_written += 1
        
        print(f"\n✅ ТРАНСФОРМАЦІЯ ЗАВЕРШЕНА:")
        print(f"   📥 Оброблено рядків: {rows_processed}")
        print(f"   🔄 Трансформовано рядків: {rows_transformed}")
        print(f"   📤 Записано рядків: {rows_written}")
        print(f"   💾 Результат: {output_file}")
        return True
        
    except Exception as e:
        print(f"❌ КРИТИЧНА ПОМИЛКА: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

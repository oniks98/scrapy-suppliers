"""
Трансформує eserver_retail.csv в eserver_prom.csv
Застосовує коефіцієнт до ціни, змінює категорії та особисті нотатки
ЗБЕРІГАЄ ВСІ ХАРАКТЕРИСТИКИ
"""
import csv
import sys
from pathlib import Path
from decimal import Decimal, InvalidOperation


def load_coefficient(csv_path: Path) -> Decimal:
    """Завантажує коефіцієнт з CSV"""
    try:
        with open(csv_path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            row = next(reader)
            return Decimal(row["coefficient"].strip())
    except Exception as e:
        print(f"❌ Помилка завантаження коефіцієнта: {e}")
        return Decimal("1.05")


def load_prom_categories(csv_path: Path) -> dict:
    """Завантажує маппінг категорій для PROM"""
    mapping = {}
    try:
        with open(csv_path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                link = row["Линк категории поставщика"].strip().strip('"')
                mapping[link] = {
                    "Номер_групи": row["Номер_групи"].strip(),
                    "Назва_групи": row["Категория на моем сайте_RU"].strip(),
                }
    except Exception as e:
        print(f"❌ Помилка завантаження категорій PROM: {e}")
    return mapping


def load_personal_notes(csv_path: Path) -> dict:
    """Завантажує особисті нотатки"""
    mapping = {}
    try:
        with open(csv_path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                group_number = row["Номер_групи"].strip()
                personal_note = row["Особисті_нотатки"].strip()
                mapping[group_number] = personal_note
    except Exception as e:
        print(f"❌ Помилка завантаження особистих нотаток: {e}")
    return mapping


def load_retail_categories(csv_path: Path) -> dict:
    """Завантажує маппінг retail категорій (для зворотного пошуку)"""
    mapping = {}
    try:
        with open(csv_path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                link = row["Линк категории поставщика"].strip().strip('"')
                group_number = row["Номер_групи"].strip()
                mapping[group_number] = link
    except Exception as e:
        print(f"❌ Помилка завантаження retail категорій: {e}")
    return mapping


def normalize_price(price_str: str) -> str:
    """
    Нормалізує ціну: замінює кому на крапку та видаляє зайві пробіли
    """
    return price_str.replace(",", ".").replace(" ", "").strip()


def transform_retail_to_prom(
    input_csv: Path,
    output_csv: Path,
    coefficient_csv: Path,
    prom_category_csv: Path,
    retail_category_csv: Path,
    personal_notes_csv: Path,
):
    """Трансформує retail CSV в prom версію зі збереженням характеристик"""
    
    print(f"🔄 СТАРТ ТРАНСФОРМАЦІЇ: {input_csv.name} → {output_csv.name}")
    
    # Завантажуємо дані
    coefficient = load_coefficient(coefficient_csv)
    prom_categories = load_prom_categories(prom_category_csv)
    retail_categories = load_retail_categories(retail_category_csv)
    personal_notes = load_personal_notes(personal_notes_csv)
    
    print(f"📊 Коефіцієнт: {coefficient}")
    print(f"📂 Категорій PROM: {len(prom_categories)}")
    print(f"📝 Особистих нотаток: {len(personal_notes)}")
    
    if not input_csv.exists():
        print(f"❌ Вхідний файл не знайдено: {input_csv}")
        return False
    
    # Читаємо вхідний CSV
    rows_processed = 0
    rows_written = 0
    price_errors = 0
    
    try:
        with open(input_csv, encoding="utf-8-sig") as infile, \
             open(output_csv, "w", encoding="utf-8-sig", newline="") as outfile:
            
            reader = csv.DictReader(infile, delimiter=";")
            
            # Зберігаємо оригінальні заголовки (включаючи всі характеристики)
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=";")
            writer.writeheader()
            
            for row in reader:
                rows_processed += 1
                
                # 1. Множимо ціну на коефіцієнт
                price_str = row.get("Ціна", "").strip()
                if price_str:
                    try:
                        # Нормалізуємо ціну: замінюємо кому на крапку
                        normalized_price = normalize_price(price_str)
                        price = Decimal(normalized_price)
                        new_price = price * coefficient
                        row["Ціна"] = str(new_price.quantize(Decimal("0.01")))
                    except (InvalidOperation, ValueError) as e:
                        price_errors += 1
                        print(f"⚠️ Помилка перетворення ціни '{price_str}' (рядок {rows_processed}): {e}")
                        # Залишаємо оригінальну ціну
                
                # 2. Змінюємо Номер_групи та Назва_групи
                retail_group_number = row.get("Номер_групи", "").strip()
                
                # Шукаємо відповідний лінк категорії в retail
                category_link = retail_categories.get(retail_group_number)
                
                if category_link and category_link in prom_categories:
                    prom_data = prom_categories[category_link]
                    row["Номер_групи"] = prom_data["Номер_групи"]
                    row["Назва_групи"] = prom_data["Назва_групи"]
                else:
                    if rows_processed <= 5:  # Показуємо тільки перші 5 попереджень
                        print(f"⚠️ Не знайдено PROM категорію для групи {retail_group_number}")
                
                # 3. Змінюємо Особисті_нотатки
                new_group_number = row.get("Номер_групи", "").strip()
                if new_group_number in personal_notes:
                    row["Особисті_нотатки"] = personal_notes[new_group_number]
                else:
                    # Якщо немає маппінгу, залишаємо порожнім
                    row["Особисті_нотатки"] = ""
                
                # ВАЖЛИВО: Записуємо весь row зі ВСІМА полями, включаючи характеристики
                writer.writerow(row)
                rows_written += 1
        
        print(f"\n✅ ТРАНСФОРМАЦІЯ ЗАВЕРШЕНА:")
        print(f"   📥 Оброблено рядків: {rows_processed}")
        print(f"   📤 Записано рядків: {rows_written}")
        if price_errors > 0:
            print(f"   ⚠️ Помилок конвертації ціни: {price_errors}")
        print(f"   💾 Результат: {output_csv}")
        return True
        
    except Exception as e:
        print(f"❌ КРИТИЧНА ПОМИЛКА: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Головна функція"""
    print("ЗАПУСК АВТОМАТИЧНОЇ ТРАНСФОРМАЦІЇ: RETAIL → PROM")
    print("=" * 80)
    
    # Шляхи до файлів
    base_dir = Path(r"C:\FullStack\Scrapy")
    data_dir = base_dir / "data" / "eserver"
    output_dir = base_dir / "output"
    
    input_csv = output_dir / "eserver_retail.csv"
    output_csv = output_dir / "eserver_prom.csv"
    
    coefficient_csv = data_dir / "eserver_coefficient_prom.csv"
    prom_category_csv = data_dir / "eserver_category_prom.csv"
    retail_category_csv = data_dir / "eserver_category_retail.csv"
    personal_notes_csv = data_dir / "eserver_personal_notes.csv"
    
    # Перевірка наявності всіх файлів
    required_files = [
        coefficient_csv,
        prom_category_csv,
        retail_category_csv,
        personal_notes_csv,
    ]
    
    missing_files = [f for f in required_files if not f.exists()]
    if missing_files:
        print("❌ Відсутні необхідні файли:")
        for f in missing_files:
            print(f"   - {f}")
        return False
    
    # Запуск трансформації
    success = transform_retail_to_prom(
        input_csv=input_csv,
        output_csv=output_csv,
        coefficient_csv=coefficient_csv,
        prom_category_csv=prom_category_csv,
        retail_category_csv=retail_category_csv,
        personal_notes_csv=personal_notes_csv,
    )
    
    if success:
        print("\n🎉 УСПІХ! Файл eserver_prom.csv створено з усіма характеристиками.")
        return True
    else:
        print("\n❌ ПОМИЛКА! Трансформація не виконана.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

"""
Тестовий скрипт для перевірки генерації ключових слів
З використанням Номер_групи замість category_url
"""
import re
from typing import List, Dict
import csv
from pathlib import Path


def load_keywords_mapping() -> Dict[str, Dict[str, List[str]]]:
    """Завантажує маппінг ключових слів з CSV за Номер_групи"""
    mapping = {}
    csv_path = Path(r"C:\FullStack\Scrapy\data\viatec\keywords.csv")
    
    if not csv_path.exists():
        print("⚠️ keywords.csv not found")
        return mapping
    
    try:
        with open(csv_path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                group_number = row["Номер_групи"].strip()
                mapping[group_number] = {
                    "ru": [w.strip() for w in row["keywords_ru"].strip('"').split(",") if w.strip()],
                    "ua": [w.strip() for w in row["keywords_ua"].strip('"').split(",") if w.strip()],
                }
        print(f"✅ Завантажено {len(mapping)} груп з ключовими словами\n")
    except Exception as e:
        print(f"⚠️ Помилка завантаження keywords.csv: {e}")
    
    return mapping


def generate_keywords_from_title(title: str, lang: str = "ua") -> List[str]:
    """Генерує ключові слова з назви товару (біграми та триграми)"""
    # Стоп-слова для фільтрації
    stop_words_ua = {"до", "з", "на", "в", "у", "і", "та", "для", "від", "по"}
    stop_words_ru = {"до", "с", "на", "в", "у", "и", "для", "от", "по", "к"}
    stop_words = stop_words_ua if lang == "ua" else stop_words_ru
    
    if not title:
        return []
    
    words = title.split()
    keywords = [title]  # Повна назва завжди перша
    
    # Фільтруємо значущі слова
    meaningful_words = []
    for word in words:
        cleaned = re.sub(r'[^\wа-яіїєґА-ЯІЇЄҐ0-9]', '', word.lower())
        if len(cleaned) >= 2 and cleaned not in stop_words:
            meaningful_words.append(word)
    
    # Біграми (2 слова)
    for i in range(len(meaningful_words) - 1):
        bigram = f"{meaningful_words[i]} {meaningful_words[i + 1]}"
        keywords.append(bigram)
    
    # Триграми (3 слова) - обмежуємо до 5 штук
    for i in range(min(len(meaningful_words) - 2, 5)):
        trigram = f"{meaningful_words[i]} {meaningful_words[i + 1]} {meaningful_words[i + 2]}"
        keywords.append(trigram)
    
    # Видаляємо дублікати зберігаючи порядок
    seen = set()
    unique_keywords = []
    for kw in keywords:
        kw_lower = kw.lower()
        if kw_lower not in seen:
            unique_keywords.append(kw)
            seen.add(kw_lower)
    
    return unique_keywords


def generate_search_terms(product_name: str, group_number: str, keywords_cache: Dict, lang: str = "ua") -> str:
    """Генерує пошукові запити з назви товару та ключів за Номер_групи"""
    if not product_name:
        return ""
    
    # 1. Генеруємо ключі з назви товару
    keywords_from_title = generate_keywords_from_title(product_name, lang)
    
    # 2. Додаємо ключі за Номер_групи
    result = list(keywords_from_title)
    seen = {kw.lower() for kw in result}
    
    if group_number and group_number in keywords_cache:
        lang_key = "ua" if lang == "ua" else "ru"
        category_keywords = keywords_cache[group_number].get(lang_key, [])
        
        for kw in category_keywords:
            kw_lower = kw.lower()
            if kw_lower not in seen:
                result.append(kw)
                seen.add(kw_lower)
    
    # 3. Гарантуємо мінімум 8 ключів (вимога PROM)
    if len(result) < 8:
        filler_ua = ["товар", "обладнання", "продукція", "техніка", "пристрій"]
        filler_ru = ["товар", "оборудование", "продукция", "техника", "устройство"]
        filler = filler_ua if lang == "ua" else filler_ru
        
        for f in filler:
            if len(result) >= 8:
                break
            if f not in seen:
                result.append(f)
                seen.add(f)
    
    # 4. Обмежуємо до 20 ключів
    result = result[:20]
    
    return ", ".join(result)


# Тестування
if __name__ == "__main__":
    # Завантажуємо маппінг ключових слів
    keywords_cache = load_keywords_mapping()
    
    print("=" * 80)
    print("ТЕСТ 1: Dealer - Відеокамера (Номер_групи: 8950011)")
    print("=" * 80)
    
    title_ua = "Відеокамера Hikvision DS-2CD2143G2-IU 4MP"
    group_number = "8950011"  # Камери відеоспостереження (dealer)
    
    result = generate_search_terms(title_ua, group_number, keywords_cache, lang="ua")
    print(f"\nНазва товару: {title_ua}")
    print(f"Номер_групи: {group_number}")
    if group_number in keywords_cache:
        print(f"Ключові слова групи: {', '.join(keywords_cache[group_number]['ua'][:3])}...")
    print(f"\nРезультат ({len(result.split(', '))} ключів):")
    for i, keyword in enumerate(result.split(", "), 1):
        print(f"{i}. {keyword}")
    
    print("\n" + "=" * 80)
    print("ТЕСТ 2: Dealer - Відеореєстратор (Номер_групи: 8950007)")
    print("=" * 80)
    
    title_ua2 = "Відеореєстратор Hikvision DS-7608NI-K2"
    group_number2 = "8950007"  # Відеореєстратори (dealer)
    
    result2 = generate_search_terms(title_ua2, group_number2, keywords_cache, lang="ua")
    print(f"\nНазва товару: {title_ua2}")
    print(f"Номер_групи: {group_number2}")
    if group_number2 in keywords_cache:
        print(f"Ключові слова групи: {', '.join(keywords_cache[group_number2]['ua'][:3])}...")
    print(f"\nРезультат ({len(result2.split(', '))} ключів):")
    for i, keyword in enumerate(result2.split(", "), 1):
        print(f"{i}. {keyword}")
    
    print("\n" + "=" * 80)
    print("ТЕСТ 3: Retail - Універсальна група (Номер_групи: 140905382)")
    print("=" * 80)
    
    title_ua3 = "Камера Wi-Fi Imou"
    group_number3 = "140905382"  # Універсальна група для retail
    
    result3 = generate_search_terms(title_ua3, group_number3, keywords_cache, lang="ua")
    print(f"\nНазва товару: {title_ua3}")
    print(f"Номер_групи: {group_number3}")
    if group_number3 in keywords_cache:
        print(f"⚠️ Група {group_number3} не знайдена в keywords.csv (для retail може використовуватись інша група)")
    else:
        print(f"ℹ️ Для цієї групи немає специфічних ключів, використовуються тільки ключі з назви")
    print(f"\nРезультат ({len(result3.split(', '))} ключів):")
    for i, keyword in enumerate(result3.split(", "), 1):
        print(f"{i}. {keyword}")
    
    print("\n" + "=" * 80)
    print("ТЕСТ 4: Російська мова (Номер_групи: 8950011)")
    print("=" * 80)
    
    title_ru = "Видеокамера Hikvision DS-2CD2143G2-IU 4MP"
    
    result_ru = generate_search_terms(title_ru, group_number, keywords_cache, lang="ru")
    print(f"\nНазва товару: {title_ru}")
    print(f"Номер_групи: {group_number}")
    if group_number in keywords_cache:
        print(f"Ключові слова групи: {', '.join(keywords_cache[group_number]['ru'][:3])}...")
    print(f"\nРезультат ({len(result_ru.split(', '))} ключів):")
    for i, keyword in enumerate(result_ru.split(", "), 1):
        print(f"{i}. {keyword}")
    
    print("\n" + "=" * 80)
    print("ТЕСТ 5: Довга назва товару")
    print("=" * 80)
    
    title_long = "Підсилювач т2 Eurosky 99999 сигналу до 100км з блоком живлення регульованим"
    group_number_long = "4321341"  # Комплектуючі для відеоспостереження
    
    result_long = generate_search_terms(title_long, group_number_long, keywords_cache, lang="ua")
    print(f"\nНазва товару: {title_long}")
    print(f"Номер_групи: {group_number_long}")
    if group_number_long in keywords_cache:
        print(f"Ключові слова групи: {', '.join(keywords_cache[group_number_long]['ua'][:3])}...")
    print(f"\nРезультат ({len(result_long.split(', '))} ключів - обмежено до 20):")
    for i, keyword in enumerate(result_long.split(", "), 1):
        print(f"{i}. {keyword}")
    
    print("\n" + "=" * 80)
    print("\n✅ Всі тести завершено!")
    print("\nПеревірте що:")
    print("1. Перша фраза завжди - повна назва товару")
    print("2. Мінімум 8 ключів (доповнюється filler-словами якщо потрібно)")
    print("3. Максимум 20 ключів")
    print("4. Ключі з keywords.csv додаються за Номер_групи")
    print("5. Немає дублікатів")

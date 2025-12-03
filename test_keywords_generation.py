"""
Тестовий скрипт для перевірки генерації ключових слів
З використанням Ідентифікатор_підрозділу замість Номер_групи
"""
import re
from typing import List, Dict
import csv
from pathlib import Path


def load_keywords_mapping() -> Dict[str, Dict[str, List[str]]]:
    """Завантажує маппінг ключових слів з CSV за Ідентифікатор_підрозділу"""
    mapping = {}
    csv_path = Path(r"C:\FullStack\Scrapy\data\viatec\keywords.csv")
    
    if not csv_path.exists():
        print("⚠️ keywords.csv not found")
        return mapping
    
    try:
        with open(csv_path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                subdivision_id = row["Ідентифікатор_підрозділу"].strip()
                mapping[subdivision_id] = {
                    "ru": [w.strip() for w in row["keywords_ru"].strip('"').split(",") if w.strip()],
                    "ua": [w.strip() for w in row["keywords_ua"].strip('"').split(",") if w.strip()],
                }
        print(f"✅ Завантажено {len(mapping)} підрозділів з ключовими словами\n")
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


def generate_search_terms(product_name: str, subdivision_id: str, keywords_cache: Dict, lang: str = "ua") -> str:
    """Генерує пошукові запити з назви товару та ключів за Ідентифікатор_підрозділу"""
    if not product_name:
        return ""
    
    # 1. Генеруємо ключі з назви товару
    keywords_from_title = generate_keywords_from_title(product_name, lang)
    
    # 2. Додаємо ключі за Ідентифікатор_підрозділу
    result = list(keywords_from_title)
    seen = {kw.lower() for kw in result}
    
    if subdivision_id and subdivision_id in keywords_cache:
        lang_key = "ua" if lang == "ua" else "ru"
        category_keywords = keywords_cache[subdivision_id].get(lang_key, [])
        
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
    print("ТЕСТ 1: Відеокамера (Ідентифікатор_підрозділу: 301105)")
    print("=" * 80)
    
    title_ua = "Відеокамера Hikvision DS-2CD2143G2-IU 4MP"
    subdivision_id = "301105"  # Камери відеоспостереження
    
    result = generate_search_terms(title_ua, subdivision_id, keywords_cache, lang="ua")
    print(f"\nНазва товару: {title_ua}")
    print(f"Ідентифікатор_підрозділу: {subdivision_id}")
    if subdivision_id in keywords_cache:
        print(f"Ключові слова підрозділу: {', '.join(keywords_cache[subdivision_id]['ua'][:3])}...")
    print(f"\nРезультат ({len(result.split(', '))} ключів):")
    for i, keyword in enumerate(result.split(", "), 1):
        print(f"{i}. {keyword}")
    
    print("\n" + "=" * 80)
    print("ТЕСТ 2: Відеореєстратор (Ідентифікатор_підрозділу: 301101)")
    print("=" * 80)
    
    title_ua2 = "Відеореєстратор Hikvision DS-7608NI-K2"
    subdivision_id2 = "301101"  # Відеореєстратори
    
    result2 = generate_search_terms(title_ua2, subdivision_id2, keywords_cache, lang="ua")
    print(f"\nНазва товару: {title_ua2}")
    print(f"Ідентифікатор_підрозділу: {subdivision_id2}")
    if subdivision_id2 in keywords_cache:
        print(f"Ключові слова підрозділу: {', '.join(keywords_cache[subdivision_id2]['ua'][:3])}...")
    print(f"\nРезультат ({len(result2.split(', '))} ключів):")
    for i, keyword in enumerate(result2.split(", "), 1):
        print(f"{i}. {keyword}")
    
    print("\n" + "=" * 80)
    print("ТЕСТ 3: Жорсткий диск (Ідентифікатор_підрозділу: 70704)")
    print("=" * 80)
    
    title_ua3 = "Жорсткий диск WD Purple 2TB"
    subdivision_id3 = "70704"  # HDD для відеоспостереження
    
    result3 = generate_search_terms(title_ua3, subdivision_id3, keywords_cache, lang="ua")
    print(f"\nНазва товару: {title_ua3}")
    print(f"Ідентифікатор_підрозділу: {subdivision_id3}")
    if subdivision_id3 in keywords_cache:
        print(f"Ключові слова підрозділу: {', '.join(keywords_cache[subdivision_id3]['ua'][:3])}...")
    print(f"\nРезультат ({len(result3.split(', '))} ключів):")
    for i, keyword in enumerate(result3.split(", "), 1):
        print(f"{i}. {keyword}")
    
    print("\n" + "=" * 80)
    print("ТЕСТ 4: Російська мова (Ідентифікатор_підрозділу: 301105)")
    print("=" * 80)
    
    title_ru = "Видеокамера Hikvision DS-2CD2143G2-IU 4MP"
    
    result_ru = generate_search_terms(title_ru, subdivision_id, keywords_cache, lang="ru")
    print(f"\nНазва товару: {title_ru}")
    print(f"Ідентифікатор_підрозділу: {subdivision_id}")
    if subdivision_id in keywords_cache:
        print(f"Ключові слова підрозділу: {', '.join(keywords_cache[subdivision_id]['ru'][:3])}...")
    print(f"\nРезультат ({len(result_ru.split(', '))} ключів):")
    for i, keyword in enumerate(result_ru.split(", "), 1):
        print(f"{i}. {keyword}")
    
    print("\n" + "=" * 80)
    print("ТЕСТ 5: Домофон (Ідентифікатор_підрозділу: 3029)")
    print("=" * 80)
    
    title_long = "Відеодомофон Dahua VTH5221D"
    subdivision_id_long = "3029"  # Відеодомофони
    
    result_long = generate_search_terms(title_long, subdivision_id_long, keywords_cache, lang="ua")
    print(f"\nНазва товару: {title_long}")
    print(f"Ідентифікатор_підрозділу: {subdivision_id_long}")
    if subdivision_id_long in keywords_cache:
        print(f"Ключові слова підрозділу: {', '.join(keywords_cache[subdivision_id_long]['ua'][:3])}...")
    print(f"\nРезультат ({len(result_long.split(', '))} ключів - обмежено до 20):")
    for i, keyword in enumerate(result_long.split(", "), 1):
        print(f"{i}. {keyword}")
    
    print("\n" + "=" * 80)
    print("\n✅ Всі тести завершено!")
    print("\nПеревірте що:")
    print("1. Перша фраза завжди - повна назва товару")
    print("2. Мінімум 8 ключів (доповнюється filler-словами якщо потрібно)")
    print("3. Максимум 20 ключів")
    print("4. Ключі з keywords.csv додаються за Ідентифікатор_підрозділу")
    print("5. Немає дублікатів")

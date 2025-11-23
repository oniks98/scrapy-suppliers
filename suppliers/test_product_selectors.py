"""
Тест селекторов для страницы товара viatec.ua
ИСПРАВЛЕННАЯ ВЕРСИЯ - используем Scrapy напрямую
"""
from scrapy.http import TextResponse
import requests

# Тестовый URL товара
test_url = "https://viatec.ua/product/DS-2CD1321G0-I-28"

print("\n" + "="*80)
print("ТЕСТ СЕЛЕКТОРОВ ДЛЯ ТОВАРА")
print("="*80)
print(f"URL: {test_url}\n")

# Получаем HTML с правильными заголовками
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

try:
    response_raw = requests.get(test_url, headers=headers, timeout=10)
    print(f"✅ HTTP статус: {response_raw.status_code}")
    print(f"✅ Размер HTML: {len(response_raw.content)} байт\n")
    
    # Создаём Scrapy Response
    response = TextResponse(
        url=test_url,
        body=response_raw.content,
        encoding="utf-8"
    )
    
    # Название
    name = response.css("h1::text").get()
    print(f"📦 Название: {name}\n")
    
    # Цена - ОСНОВНОЙ СЕЛЕКТОР
    price_new = response.css("div.card-header__card-price-new::text").get()
    print(f"💰 Цена (новая): '{price_new}'")
    
    # Цена - АЛЬТЕРНАТИВНЫЕ СЕЛЕКТОРЫ
    price_alt1 = response.css(".card-header__card-price-new::text").getall()
    print(f"💰 Цена (все текстовые узлы): {price_alt1}")
    
    # Проверяем весь блок цены
    price_block = response.css("div.card-header__card-price-new").get()
    if price_block:
        print(f"💰 Блок цены HTML:\n{price_block[:200]}")
    else:
        print(f"❌ Блок цены не найден!")
    
    # Наличие
    availability = response.css("div.card-header__card-status-badge::text").get()
    print(f"\n📊 Наличие: '{availability}'")
    
    # Изображения
    images = response.css("img.card-header__card-images-image::attr(src)").getall()
    print(f"\n🖼️  Изображения ({len(images)} шт): {images[:2]}")
    
    # Описание - проверим разные варианты
    print("\n" + "="*80)
    print("ОПИСАНИЕ - ТЕСТИРОВАНИЕ СЕЛЕКТОРОВ")
    print("="*80)
    
    desc1 = response.css("div.card-header__card-description").get()
    print(f"1️⃣ Весь блок описания найден: {bool(desc1)}")
    if desc1:
        print(f"   HTML: {desc1[:150]}...")
    
    desc2 = response.css("div.card-header__card-description p::text").getall()
    print(f"2️⃣ Параграфы (p::text): {len(desc2)} шт")
    if desc2:
        print(f"   Первый: {desc2[0][:100]}")
    
    desc3 = response.css("div.card-header__card-description::text").getall()
    print(f"3️⃣ Все текстовые узлы: {len(desc3)} шт")
    
    desc4 = response.css("div.card-header__card-description *::text").getall()
    print(f"4️⃣ Все дочерние текстовые узлы: {len(desc4)} шт")
    
    # Характеристики
    print("\n" + "="*80)
    print("ХАРАКТЕРИСТИКИ - ТЕСТИРОВАНИЕ СЕЛЕКТОРОВ")
    print("="*80)
    
    specs_table = response.css("div.card-tabs__characteristic-content table").get()
    print(f"1️⃣ Таблица характеристик найдена: {bool(specs_table)}")
    if specs_table:
        print(f"   HTML: {specs_table[:200]}...")
    
    specs_rows = response.css("div.card-tabs__characteristic-content table tbody tr")
    print(f"2️⃣ Строки характеристик (tbody tr): {len(specs_rows)} шт")
    
    # АЛЬТЕРНАТИВА: без tbody
    specs_rows_alt = response.css("div.card-tabs__characteristic-content table tr")
    print(f"2️⃣б Строки характеристик (table tr): {len(specs_rows_alt)} шт")
    
    # АЛЬТЕРНАТИВА: просто table tr
    specs_simple = response.css("table tr")
    print(f"2️⃣в Все строки таблиц (table tr): {len(specs_simple)} шт")
    
    for i, row in enumerate(specs_rows_alt[:3], 1):
        name_spec = row.css("th::text").get()
        value_spec = row.css("td::text").get()
        print(f"   {i}. {name_spec}: {value_spec}")
    
    # Проверка: может быть другой селектор для табов?
    all_tabs = response.css("div.card-tabs").get()
    print(f"\n3️⃣ Блок табов найден: {bool(all_tabs)}")
    
    # Альтернативный селектор
    specs_alt = response.css("table.characteristics-table tr")
    print(f"4️⃣ Альтернативный селектор (table.characteristics-table): {len(specs_alt)} шт")
    
    # Проверим, вообще есть ли таблицы на странице
    all_tables = response.css("table")
    print(f"5️⃣ Всего таблиц на странице: {len(all_tables)} шт")
    
    # ДОПОЛНИТЕЛЬНО: Попробуем найти описание по другим селекторам
    print(f"\n6️⃣ Ищем описание альтернативными способами:")
    
    # Поиск по классу с 'description'
    desc_blocks = response.css("div[class*='description']").getall()
    print(f"   - Блоки с 'description' в классе: {len(desc_blocks)} шт")
    
    # Поиск по id
    desc_by_id = response.css("#description, #opisanie, #opys").get()
    print(f"   - По ID (#description): {bool(desc_by_id)}")
    
    # Поиск текста вокруг слова "Опис" или "Описание"
    if 'опис' in response.text.lower() or 'описание' in response.text.lower():
        print(f"   - Слово 'опис/описание' найдено в HTML ✅")
        # Попробуем найти ближайший div после заголовка
        desc_section = response.xpath("//h2[contains(text(), 'Опис') or contains(text(), 'Описание')]/following-sibling::div[1]").get()
        if desc_section:
            print(f"   - Нашли через XPath: {desc_section[:100]}...")
    
    print("\n" + "="*80)
    print("АНАЛИЗ HTML - ИЩЕМ КЛЮЧЕВЫЕ БЛОКИ")
    print("="*80)
    
    # Ищем блоки с ключевыми словами
    html_text = response.text.lower()
    
    keywords = [
        "card-header__card-description",
        "card-tabs__characteristic",
        "характеристики",
        "опис",
        "description",
        "specifications",
    ]
    
    for keyword in keywords:
        count = html_text.count(keyword.lower())
        print(f"🔍 '{keyword}': найдено {count} раз")
    
    # Проверим, есть ли хоть какой-то контент
    if len(html_text) < 5000:
        print(f"\n⚠️  ВНИМАНИЕ: HTML слишком маленький ({len(html_text)} символов)")
        print("   Возможно, контент загружается через JavaScript!")
    
    print("\n" + "="*80)
    print("ПРОВЕРКА ЗАВЕРШЕНА")
    print("="*80 + "\n")

except requests.RequestException as e:
    print(f"❌ Ошибка запроса: {e}")
except Exception as e:
    print(f"❌ Ошибка парсинга: {e}")
    import traceback
    traceback.print_exc()

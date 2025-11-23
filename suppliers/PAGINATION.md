# 📄 Пагинация в Viatec Spider'ах

## ✅ Что реализовано

Оба spider'а (`viatec_retail` и `viatec_dealer`) имеют **усиленную пагинацию** с проверкой нескольких вариантов селекторов.

---

## 🔍 Как работает пагинация

### Код (в обоих spider'ах):

```python
# Пагинация (несколько вариантов селекторов)
next_page = (
    response.css("a.next-page::attr(href)").get() or
    response.css("a[rel='next']::attr(href)").get() or
    response.css("li.pagination-next a::attr(href)").get() or
    response.css("a.pagination__next::attr(href)").get() or
    response.css("a:contains('Далее')::attr(href)").get() or
    response.css("a:contains('→')::attr(href)").get()
)

if next_page:
    self.logger.info(f"📄 Найдена следующая страница: {next_page}")
    yield response.follow(
        next_page,
        callback=self.parse_category,
        meta={"category_url": category_url},
    )
else:
    self.logger.info(f"✅ Пагинация завершена для категории: {category_url}")
```

### Что это значит:

Spider проверяет **6 вариантов** селекторов для кнопки "Следующая страница":

1. `a.next-page` - класс "next-page"
2. `a[rel='next']` - атрибут rel="next"
3. `li.pagination-next a` - ссылка внутри li.pagination-next
4. `a.pagination__next` - БЭМ-стиль класса
5. `a:contains('Далее')` - текст "Далее"
6. `a:contains('→')` - символ стрелки

Если **хотя бы один** селектор найдёт ссылку → spider переходит на следующую страницу.

---

## 📊 Как проверить, работает ли пагинация

### Вариант 1: Через test_selectors.py

```bash
python test_selectors.py
```

Вывод покажет:
```
4️⃣ Тест: Пагинация
   Вариант 1 (a.next-page): None
   Вариант 2 (a[rel='next']): /catalog/cameras/?page=2  ✅
   Вариант 3 (li.pagination-next a): None
```

→ Значит, работает `a[rel='next']`

---

### Вариант 2: В логах spider'а

```bash
scrapy crawl viatec_retail -s LOG_LEVEL=INFO
```

Ищи в логах:
```
[viatec_retail] INFO: 📄 Найдена следующая страница: /catalog/cameras/?page=2
[viatec_retail] INFO: 📄 Найдена следующая страница: /catalog/cameras/?page=3
[viatec_retail] INFO: ✅ Пагинация завершена для категории: https://viatec.ua/catalog/cameras/
```

---

### Вариант 3: Вручную через Scrapy Shell

```bash
scrapy shell "https://viatec.ua/catalog/cameras/"
```

В shell:
```python
# Попробуй все варианты
>>> response.css("a.next-page::attr(href)").get()
None

>>> response.css("a[rel='next']::attr(href)").get()
'/catalog/cameras/?page=2'  # ✅ Работает!

>>> response.css("li.pagination-next a::attr(href)").get()
None
```

---

## 🛠️ Если пагинация НЕ работает

### Причина 1: Неправильные селекторы

**Решение:** Найди правильный селектор через браузер

1. Открой категорию viatec.ua
2. Прокрути вниз до пагинации
3. F12 → Inspect Element на кнопке "Далее"
4. Посмотри HTML

**Примеры HTML:**

#### Вариант A: Bootstrap
```html
<ul class="pagination">
  <li class="page-item">
    <a class="page-link" href="?page=2">Следующая</a>
  </li>
</ul>
```
→ Селектор: `li.page-item a.page-link::attr(href)`

#### Вариант B: Custom класс
```html
<div class="pagination-wrapper">
  <a href="?page=2" class="btn-next">→</a>
</div>
```
→ Селектор: `a.btn-next::attr(href)`

#### Вариант C: Data-атрибут
```html
<button data-next-url="/catalog/cameras/?page=2">Далее</button>
```
→ Селектор: `button[data-next-url]::attr(data-next-url)`

**Добавь найденный селектор** в список (строка 82 в `viatec_retail.py`):

```python
next_page = (
    response.css("a.next-page::attr(href)").get() or
    response.css("a[rel='next']::attr(href)").get() or
    response.css("li.page-item a.page-link::attr(href)").get() or  # Твой селектор
    response.css("a.pagination__next::attr(href)").get() or
    response.css("a:contains('Далее')::attr(href)").get() or
    response.css("a:contains('→')::attr(href)").get()
)
```

---

### Причина 2: JavaScript-пагинация (AJAX)

Если viatec.ua использует **динамическую подгрузку** товаров (без перезагрузки страницы), обычный Scrapy не сработает.

**Признаки:**
- Нет ссылок "Далее" в HTML
- Есть кнопка "Загрузить ещё"
- URL не меняется при переходе на следующую страницу

**Решение 1: Найти API запрос**

1. F12 → Network → XHR
2. Нажми "Загрузить ещё"
3. Найди запрос к API (например: `/api/products?page=2`)
4. Парси этот API вместо HTML

```python
def parse_category(self, response):
    # Вместо парсинга HTML делаем JSON запрос
    api_url = "https://viatec.ua/api/products"
    
    for page in range(1, 100):  # Максимум 100 страниц
        yield scrapy.Request(
            url=f"{api_url}?page={page}",
            callback=self.parse_json_products,
        )

def parse_json_products(self, response):
    data = response.json()
    
    if not data.get("products"):
        return  # Нет товаров - выходим
    
    for product in data["products"]:
        yield {
            "Назва_позиції": product["name"],
            "Ціна": product["price"],
            # ...
        }
```

**Решение 2: Использовать Selenium/Playwright**

Если API нет, нужен браузер:

```bash
pip install scrapy-playwright
```

Конфигурация в `settings.py`:
```python
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
```

Spider с Playwright:
```python
def start_requests(self):
    for url in self.start_urls:
        yield scrapy.Request(
            url=url,
            callback=self.parse_category,
            meta={"playwright": True},
        )
```

---

### Причина 3: Бесконечная пагинация

Если spider крутится в цикле (одна и та же страница повторяется), добавь защиту:

```python
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.visited_pages = set()  # Храним посещённые URL
    self.category_mapping = self._load_category_mapping()
    self.start_urls = list(self.category_mapping.keys())

def parse_category(self, response):
    # Проверяем, не посещали ли уже эту страницу
    if response.url in self.visited_pages:
        self.logger.warning(f"⚠️ Страница уже посещена: {response.url}")
        return
    
    self.visited_pages.add(response.url)
    
    # ... остальной код ...
    
    if next_page:
        full_url = response.urljoin(next_page)
        
        # Проверяем, не зациклились ли мы
        if full_url not in self.visited_pages:
            yield response.follow(next_page, callback=self.parse_category)
```

---

## 🔢 Ограничение количества страниц (опционально)

Если категория слишком большая (1000+ товаров), можно ограничить:

```python
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.max_pages_per_category = 10  # Максимум 10 страниц
    self.category_pages = {}  # Счётчик страниц

def parse_category(self, response):
    category_url = response.meta["category_url"]
    
    # Считаем страницы
    if category_url not in self.category_pages:
        self.category_pages[category_url] = 0
    
    self.category_pages[category_url] += 1
    
    # Проверяем лимит
    if self.category_pages[category_url] > self.max_pages_per_category:
        self.logger.warning(f"⚠️ Достигнут лимит {self.max_pages_per_category} страниц для: {category_url}")
        return
    
    # ... остальной код пагинации ...
```

---

## 📊 Статистика пагинации

В конце парсинга Scrapy покажет:
```
2024-11-23 15:30:45 [scrapy.statscollectors] INFO: Dumping Scrapy stats:
{
    'downloader/request_count': 342,      # Всего запросов
    'downloader/response_count': 342,     # Всего ответов
    'item_scraped_count': 1256,           # Спаршено товаров
}
```

Если `request_count` примерно равен количеству категорий → **пагинация НЕ работает!**

Если `request_count` >> категорий → **пагинация работает** ✅

---

## ✅ Чек-лист пагинации

- [ ] Запустил `test_selectors.py` и проверил секцию "Пагинация"
- [ ] Хотя бы один селектор нашёл ссылку на следующую страницу
- [ ] В логах spider'а вижу сообщения "📄 Найдена следующая страница"
- [ ] Количество запросов `request_count` > количества категорий
- [ ] CSV файл содержит товары со всех страниц категорий

---

## 🆘 Помощь

Если пагинация не работает:

1. **Запусти тест:**
   ```bash
   python test_selectors.py
   ```

2. **Проверь HTML вручную:**
   - Открой категорию в браузере
   - F12 → Inspect кнопки "Далее"
   - Найди селектор

3. **Проверь логи spider'а:**
   ```bash
   scrapy crawl viatec_retail -s LOG_LEVEL=DEBUG
   ```

4. **Используй Scrapy Shell:**
   ```bash
   scrapy shell "https://viatec.ua/catalog/cameras/"
   ```
   
   Тестируй селекторы:
   ```python
   >>> response.css("твой_селектор::attr(href)").get()
   ```

Нужна помощь с конкретной реализацией?

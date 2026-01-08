# Швидкий старт: Додавання нової категорії

## 📝 3 простих кроки

### Крок 1: Створити файл категорії

```python
# keywords/categories/viatec/monitors.py

from typing import List, Set
from keywords.core.helpers import SpecAccessor
from keywords.utils.spec_helpers import is_spec_allowed

def generate(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """
    Генерація ключових слів для моніторів.
    
    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово (з CSV)
        allowed: Множина дозволених характеристик (з CSV)
    
    Returns:
        Список ключових слів
    """
    keywords = []
    
    # Перевірка дозволеної характеристики
    if not is_spec_allowed("Діагональ екрану", allowed):
        return keywords
    
    # Витягування характеристики
    diagonal = accessor.value("Діагональ екрану")
    if not diagonal:
        return keywords
    
    # Генерація ключових слів
    if lang == "ru":
        keywords.extend([
            f"{base} {diagonal}",
            f"монитор {diagonal} дюймов",
            f"{diagonal}\" монитор"
        ])
    else:
        keywords.extend([
            f"{base} {diagonal}",
            f"монітор {diagonal} дюймів",
            f"{diagonal}\" монітор"
        ])
    
    return keywords
```

### Крок 2: Зареєструвати в роутері

```python
# keywords/categories/viatec/router.py

from keywords.categories.viatec import hdd, sd_card, usb_flash, monitors  # ✅ Імпорт

CATEGORY_HANDLERS = {
    "70704": hdd.generate,
    "63705": sd_card.generate,
    "70501": usb_flash.generate,
    "12345": monitors.generate,  # ✅ Додати тут
}
```

### Крок 3: Експортувати в __init__

```python
# keywords/categories/viatec/__init__.py

from keywords.categories.viatec import hdd, sd_card, usb_flash, monitors  # ✅ Імпорт

__all__ = [
    "hdd",
    "sd_card",
    "usb_flash",
    "monitors",  # ✅ Додати тут
    "get_category_handler",
]
```

## ✅ Готово!

Ядро автоматично підхопить нову категорію.

---

## 🛠️ Корисні утиліти

### Витягування об'єму (HDD/SD/USB)
```python
from keywords.utils import extract_capacity

capacity_info = extract_capacity(accessor, "Об'єм накопичувача")
# Повертає: {"formatted": "128gb", "size_gb": 128}
```

### Витягування швидкості
```python
from keywords.utils import extract_speed

speed = extract_speed(accessor, "Швидкість зчитування")
# Повертає: "90" (рядок)
```

### Витягування інтерфейсу
```python
from keywords.utils import extract_interface

interface = extract_interface(accessor, "Інтерфейс")
# Повертає: "sata", "usb type-c", "usb 3.0" тощо
```

### Перевірка дозволеної характеристики
```python
from keywords.utils import is_spec_allowed

if is_spec_allowed("Діагональ екрану", allowed):
    # Характеристика дозволена в CSV
    diagonal = accessor.value("Діагональ екрану")
```

---

## 📚 Приклади складніших категорій

### Приклад 1: З множинними характеристиками

```python
def generate(accessor, lang, base, allowed):
    keywords = []
    
    # Характеристика 1: Діагональ
    if is_spec_allowed("Діагональ екрану", allowed):
        diagonal = accessor.value("Діагональ екрану")
        if diagonal:
            keywords.append(f"{base} {diagonal}")
    
    # Характеристика 2: Роздільна здатність
    if is_spec_allowed("Роздільна здатність", allowed):
        resolution = accessor.value("Роздільна здатність")
        if resolution and "4k" in resolution.lower():
            keywords.append(f"4k {base}")
    
    # Характеристика 3: Частота оновлення
    if is_spec_allowed("Частота оновлення", allowed):
        refresh_rate = accessor.value("Частота оновлення")
        if refresh_rate:
            match = re.search(r"(\d+)", refresh_rate)
            if match and int(match.group(1)) >= 144:
                keywords.append(f"ігровий {base}" if lang == "ua" else f"игровой {base}")
    
    return keywords
```

### Приклад 2: З умовною логікою

```python
def generate(accessor, lang, base, allowed):
    keywords = []
    
    # Витягуємо розмір
    size = accessor.value("Розмір")
    if not size:
        return keywords
    
    # Логіка залежить від розміру
    if "compact" in size.lower() or "компакт" in size.lower():
        if lang == "ru":
            keywords.extend([
                f"компактный {base}",
                f"мини {base}",
                f"портативный {base}"
            ])
        else:
            keywords.extend([
                f"компактний {base}",
                f"міні {base}",
                f"портативний {base}"
            ])
    else:
        if lang == "ru":
            keywords.extend([
                f"стандартный {base}",
                f"{base} полноразмерный"
            ])
        else:
            keywords.extend([
                f"стандартний {base}",
                f"{base} повнорозмірний"
            ])
    
    return keywords
```

### Приклад 3: З використанням regex

```python
import re

def generate(accessor, lang, base, allowed):
    keywords = []
    
    # Витягуємо потужність
    power = accessor.value("Потужність")
    if not power:
        return keywords
    
    # Шукаємо число (наприклад, "500 Вт" → "500")
    match = re.search(r"(\d+)", power)
    if not match:
        return keywords
    
    power_value = int(match.group(1))
    
    # Генеруємо ключові слова залежно від потужності
    if power_value >= 1000:
        keywords.append(f"потужний {base}" if lang == "ua" else f"мощный {base}")
    elif power_value >= 500:
        keywords.append(f"середній {base}" if lang == "ua" else f"средний {base}")
    else:
        keywords.append(f"компактний {base}" if lang == "ua" else f"компактный {base}")
    
    keywords.append(f"{base} {power_value}w")
    
    return keywords
```

---

## 🎯 Рекомендації

1. **Тримайте функції короткими** (50-100 рядків)
2. **Використовуйте is_spec_allowed()** перед кожним accessor.value()
3. **Повертайте порожній список** якщо характеристики немає
4. **Додавайте коментарі** до складної логіки
5. **Тестуйте на реальних даних** перед коммітом

---

## 📞 Підтримка

Якщо виникли питання:
1. Перегляньте існуючі категорії (`hdd.py`, `sd_card.py`, `usb_flash.py`)
2. Прочитайте `README.md`
3. Запустіть `example_usage.py` для тестів

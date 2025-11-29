"""
ВИПРАВЛЕНА ФУНКЦІЯ _check_availability

Замініть цю функцію в файлі pipelines.py
"""

def _check_availability(self, availability_str):
    """
    Перевірка наявності товару
    Повертає True якщо товар В НАЯВНОСТІ, False якщо немає
    
    ВАЖЛИВО: За замовчуванням вважаємо товар В НАЯВНОСТІ,
    якщо явно не вказано що його немає
    """
    if not availability_str:
        return True  # Змінено з False на True - за замовчуванням В НАЯВНОСТІ
    
    availability_lower = str(availability_str).lower().strip()
    
    # Спочатку перевіряємо на відсутність (явні негативні маркери)
    out_of_stock_keywords = [
        "немає",
        "нет в наличии",
        "відсутній",
        "закінчився",
        "out of stock",
        "unavailable",
        "немає в наявності",
        "нет на складе",
    ]
    
    for keyword in out_of_stock_keywords:
        if keyword in availability_lower:
            return False
    
    # Позитивні маркери наявності
    in_stock_keywords = [
        "є в наявності",
        "в наявності",
        "в наличии",
        "есть",
        "доступно",
        "available",
        "in stock",
        "наявності",
        "наявност",
        "є",
    ]
    
    for keyword in in_stock_keywords:
        if keyword in availability_lower:
            return True
    
    # За замовчуванням вважаємо товар В НАЯВНОСТІ
    return True

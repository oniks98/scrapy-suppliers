"""
Генератор ключових слів для SD-карт.
Категорія: 63705
"""

from typing import List, Set

from keywords.core.helpers import SpecAccessor
from keywords.utils.spec_helpers import (
    extract_capacity,
    extract_speed,
    is_spec_allowed
)


def generate(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """
    Генерація ключових слів для SD-карт.

    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово
        allowed: Множина дозволених характеристик

    Returns:
        Список ключових слів
    """
    keywords = []

    if not is_spec_allowed("Об'єм пам'яті", allowed):
        return keywords

    # 1. Об'єм пам'яті
    capacity_info = extract_capacity(accessor, "Об'єм пам'яті")
    if not capacity_info:
        return keywords

    capacity = capacity_info["formatted"]

    if lang == "ru":
        keywords.extend([
            f"сд карта {capacity}",
            f"micro sd {capacity}",
            f"sd карта {capacity}",
            f"карта памяти {capacity}",
            f"{capacity} sd карта",
            "сд карта для видеонаблюдения",
            "карта памяти для камеры"
        ])
    else:
        keywords.extend([
            f"сд карта {capacity}",
            f"micro sd {capacity}",
            f"sd карта {capacity}",
            f"карта пам'яті {capacity}",
            f"{capacity} sd карта",
            "сд карта для відеоспостереження",
            "карта пам'яті для камери"
        ])

    # 2. Тип карти (microSD / SD)
    if is_spec_allowed("Тип карти пам'яті", allowed):
        card_type = accessor.value("Тип карти пам'яті")
        if card_type:
            card_type_lower = card_type.lower()
            if "microsd" in card_type_lower or "micro sd" in card_type_lower:
                keywords.append(f"microsd {capacity}")
            elif "sd" in card_type_lower:
                keywords.append(f"sd {capacity}")

    # 3. Швидкість зчитування (якщо висока швидкість)
    if is_spec_allowed("Швидкість зчитування", allowed):
        read_speed = extract_speed(accessor, "Швидкість зчитування")
        if read_speed and int(read_speed) >= 90:
            if lang == "ru":
                keywords.append("быстрая sd карта")
            else:
                keywords.append("швидка sd карта")

    return keywords

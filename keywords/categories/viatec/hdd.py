"""
Генератор ключових слів для жорстких дисків (HDD/SSD).
Категорія: 70704
"""

import re
from typing import List, Set

from keywords.core.helpers import SpecAccessor
from keywords.utils.spec_helpers import (
    extract_capacity,
    extract_interface,
    extract_rpm,
    is_spec_allowed
)


def generate(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """
    Генерація ключових слів для жорстких дисків.

    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово
        allowed: Множина дозволених характеристик

    Returns:
        Список ключових слів
    """
    keywords = []

    if not is_spec_allowed("Об'єм накопичувача", allowed):
        return keywords

    # 1. Об'єм накопичувача
    capacity_info = extract_capacity(accessor, "Об'єм накопичувача")
    if not capacity_info:
        return keywords

    capacity = capacity_info["formatted"]

    keywords.extend([
        f"{base} {capacity}",
        f"{capacity} {base}"
    ])

    # 2. Інтерфейс
    if is_spec_allowed("Інтерфейс", allowed):
        interface = extract_interface(accessor, "Інтерфейс")
        if interface:
            keywords.append(f"{base} {interface}")

    # 3. Форм-фактор
    if is_spec_allowed("Форм-фактор", allowed):
        form_factor = accessor.value("Форм-фактор")
        if form_factor:
            match = re.search(r"(\d\.\d)[\"\']?", form_factor)
            if match:
                keywords.append(f"{base} {match.group(1)}\"")

    # 4. Швидкість обертання (якщо є - HDD, якщо немає - SSD)
    if is_spec_allowed("Швидкість обертання", allowed):
        rpm = extract_rpm(accessor, "Швидкість обертання")
        if rpm:
            if lang == "ru":
                keywords.append(f"{base} {rpm} об/мин")
            else:
                keywords.append(f"{base} {rpm} об/хв")
        else:
            # Якщо немає швидкості обертання, можливо це SSD
            keywords.append(f"ssd {base}")

    # 5. Для відеоспостереження
    if lang == "ru":
        keywords.extend([
            f"{base} для видеонаблюдения",
            f"{base} для регистратора",
            f"hdd {base}"
        ])
    else:
        keywords.extend([
            f"{base} для відеоспостереження",
            f"{base} для реєстратора",
            f"hdd {base}"
        ])

    return keywords

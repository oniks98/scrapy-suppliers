"""
Роутер категорій для постачальника Viatec.
"""

from typing import Optional, Callable, List, Set

from keywords.core.helpers import SpecAccessor
from keywords.categories.viatec import hdd, sd_card, usb_flash


# Реєстр обробників категорій
CATEGORY_HANDLERS = {
    "70704": hdd.generate,      # Жорсткі диски
    "63705": sd_card.generate,  # SD-карти
    "70501": usb_flash.generate, # USB-флешки
}


def get_category_handler(
    category_id: str
) -> Optional[Callable[[SpecAccessor, str, str, Set[str]], List[str]]]:
    """
    Отримати обробник для категорії.

    Args:
        category_id: ID категорії

    Returns:
        Функція-обробник або None
    """
    return CATEGORY_HANDLERS.get(category_id)

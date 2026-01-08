"""
Утиліти для витягування інформації з назв товарів.
"""

import re
from typing import Optional, Dict

from keywords.core.models import CAMERA_TECHNOLOGIES


def extract_brand(text: str, manufacturers: Dict[str, str]) -> Optional[str]:
    """
    Витягування бренду з назви.

    Args:
        text: Текст назви товару
        manufacturers: Словник виробників

    Returns:
        Бренд або None
    """
    text_lower = text.lower()

    # Пошук по маппінгу виробників
    for keyword, manufacturer in manufacturers.items():
        if keyword in text_lower:
            return manufacturer

    return None


def extract_model(text: str) -> Optional[str]:
    """
    Витягування моделі з назви (патерн: XX-XXXX).

    Args:
        text: Текст назви товару

    Returns:
        Модель або None
    """
    match = re.search(r"\b[A-Z]{2,5}-[A-Z0-9-]{3,}\b", text, re.I)
    return match.group(0).upper() if match else None


def extract_technology(text: str) -> Optional[str]:
    """
    Витягування технології з назви.

    Args:
        text: Текст назви товару

    Returns:
        Технологія або None
    """
    text_lower = text.lower()
    for tech in CAMERA_TECHNOLOGIES:
        if tech in text_lower:
            return tech.upper()
    return None


def check_wifi(name: str, specs: list) -> bool:
    """
    Перевірка наявності WiFi.

    Args:
        name: Назва товару
        specs: Список характеристик

    Returns:
        True, якщо є WiFi
    """
    # Перевірка в назві
    if re.search(r"wi[- ]?fi", name, re.I):
        return True

    # Перевірка в характеристиках
    for spec in specs:
        spec_text = f"{spec.get('name', '')} {spec.get('value', '')}"
        if re.search(r"wi[- ]?fi", spec_text, re.I):
            return True

    return False

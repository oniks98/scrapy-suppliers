"""
Моделі даних для генератора ключових слів.
"""

from dataclasses import dataclass
from enum import Enum
from typing import TypedDict, List, Set


class ProcessorType(str, Enum):
    """Типи процесорів для обробки категорій"""
    CAMERA = "camera"
    DVR = "dvr"
    GENERIC = "generic"


class Spec(TypedDict):
    """Структура характеристики товару"""
    name: str
    value: str


@dataclass
class CategoryConfig:
    """Конфігурація категорії з CSV"""
    category_id: str
    base_keyword_ru: str
    base_keyword_ua: str
    universal_phrases_ru: List[str]
    universal_phrases_ua: List[str]
    allowed_specs: Set[str]
    processor_type: ProcessorType


# Константи лімітів
MAX_MODEL_KEYWORDS = 10
MAX_SPEC_KEYWORDS = 15
MAX_UNIVERSAL_KEYWORDS = 10
MAX_TOTAL_KEYWORDS = 30

# Технології камер
CAMERA_TECHNOLOGIES = {"ip", "ahd", "hdcvi", "tvi", "cvi"}

# Терміни для різних мов
SPEC_TERMS = {
    "channel": {"ru": "канал", "ua": "канал"},
    "manufacturer": {"ru": "виробник", "ua": "виробник"},
    "resolution": {"ru": "роздільна здатність", "ua": "роздільна здатність"},
    "focal": {"ru": "фокусна відстань", "ua": "фокусна відстань"},
    "camera_type": {"ru": "тип камери", "ua": "тип камери"},
    "ip_rating": {"ru": "захист обладнання", "ua": "захист обладнання"},
    "dvr_type": {"ru": "тип відеореєстратора", "ua": "тип відеореєстратора"},
    "poe": {"ru": "підтримка poe", "ua": "підтримка poe"},
}

# Маппінг категорій на типи процесорів
CATEGORY_PROCESSORS = {
    "301105": ProcessorType.CAMERA,  # Камери відеоспостереження
    "301101": ProcessorType.DVR,     # Відеореєстратори
    "70704": ProcessorType.GENERIC,  # Жорсткі диски (HDD)
    "63705": ProcessorType.GENERIC,  # Карти пам'яті (SD/microSD)
    "70501": ProcessorType.GENERIC,  # USB-флешки
    "301112": ProcessorType.GENERIC, # Кронштейни та кожухи
    "5092913": ProcessorType.GENERIC, # Монтажні коробки
}

"""
Процесор для категорій відеореєстраторів (Viatec).
"""

import re
from typing import List, Dict, Optional, Set
import logging

from keywords.processors.viatec.base import ViatecBaseProcessor
from keywords.core.models import CategoryConfig, Spec, MAX_MODEL_KEYWORDS, MAX_SPEC_KEYWORDS
from keywords.core.helpers import SpecAccessor, KeywordBucket
from keywords.utils.name_helpers import extract_brand, extract_model, extract_technology
from keywords.utils.spec_helpers import is_spec_allowed


class DvrProcessor(ViatecBaseProcessor):
    """Процесор для DVR/NVR відеореєстраторів"""

    def generate(
        self,
        name: str,
        config: CategoryConfig,
        specs: List[Spec],
        lang: str,
        manufacturers: Dict[str, str],
        logger: logging.Logger
    ) -> List[str]:
        """Генерація ключових слів для DVR"""
        base = getattr(config, f"base_keyword_{lang}")
        if not base:
            return []

        accessor = SpecAccessor(specs)
        bucket = KeywordBucket(MAX_MODEL_KEYWORDS + MAX_SPEC_KEYWORDS)
        allowed = config.allowed_specs

        # Блок 1: Модель і бренд
        model_keywords = self._generate_model_keywords(name, manufacturers)
        bucket.extend(model_keywords)

        # Блок 2: Характеристики
        spec_keywords = self._generate_spec_keywords(
            name, base, accessor, lang, allowed, manufacturers
        )
        bucket.extend(spec_keywords)

        # Блок 3: Універсальні фрази
        universal_keywords = self._generate_universal_keywords(config, lang)
        bucket.extend(universal_keywords)

        return bucket.to_list()

    def _generate_model_keywords(
        self,
        name: str,
        manufacturers: Dict[str, str]
    ) -> List[str]:
        """Генерація ключових слів на основі моделі та бренду"""
        brand = extract_brand(name, manufacturers)
        model = extract_model(name)
        tech = extract_technology(name)

        keywords = []

        # Модель
        if model:
            keywords.append(model)
            if brand:
                keywords.append(f"{brand} {model}")

        # Технологія + бренд (з маленької букви)
        if brand and tech:
            keywords.append(f"{tech.lower()} {brand.lower()}")

        return keywords[:MAX_MODEL_KEYWORDS]

    def _generate_spec_keywords(
        self,
        name: str,
        base: str,
        accessor: SpecAccessor,
        lang: str,
        allowed: Set[str],
        manufacturers: Dict[str, str]
    ) -> List[str]:
        """Генерація ключових слів з характеристик"""
        bucket = KeywordBucket(MAX_SPEC_KEYWORDS)

        # Витягуємо характеристики
        brand = self._get_brand_from_accessor(accessor, allowed)
        channels = self._get_channels(accessor, allowed)
        dvr_type_keywords = self._get_dvr_type_keywords(accessor, lang, allowed)
        poe = self._get_poe_support(accessor, lang, allowed)
        ai_keywords = self._get_ai_technology_keywords(name, lang)

        # Бренд (з маленької букви)
        if brand:
            bucket.add(f"{base} {brand.lower()}")
            bucket.add(f"{brand.lower()} {base}")

        # Кількість каналів (формат: "N-канальний відеореєстратор")
        if channels:
            if lang == "ru":
                bucket.add(f"{channels}-канальный {base}")
            else:
                bucket.add(f"{channels}-канальний {base}")

        # Тип DVR (множинні варіанти)
        bucket.extend(dvr_type_keywords)

        # PoE підтримка (множинні варіанти)
        bucket.extend(poe)

        # AI технології (WizSense/AcuSense)
        bucket.extend(ai_keywords)

        return bucket.to_list()

    def _get_brand_from_accessor(
        self,
        accessor: SpecAccessor,
        allowed: Set[str]
    ) -> Optional[str]:
        """Витягування бренду з характеристик"""
        if not is_spec_allowed("Виробник", allowed):
            return None

        return accessor.value("Виробник")

    def _get_channels(
        self,
        accessor: SpecAccessor,
        allowed: Set[str]
    ) -> Optional[str]:
        """Витягування кількості каналів"""
        if not is_spec_allowed("Кількість каналів", allowed):
            return None

        value = accessor.value("Кількість каналів")
        if not value:
            return None

        # Витягуємо число зі значення
        match = re.search(r"\d+", value)
        if match:
            return match.group(0)

        return None

    def _get_dvr_type_keywords(
        self,
        accessor: SpecAccessor,
        lang: str,
        allowed: Set[str]
    ) -> List[str]:
        """Визначення типу відеореєстратора"""
        if not is_spec_allowed("Тип відеореєстратора", allowed):
            return []

        base = "видеорегистратор" if lang == "ru" else "відеореєстратор"
        keywords = []

        value = accessor.value("Тип відеореєстратора")
        if not value:
            return []

        value_lower = value.lower()

        # 1. IP відеореєстратор (NVR)
        if "ip" in value_lower or "nvr" in value_lower:
            if lang == "ru":
                keywords.extend([
                    f"ip {base}",
                    f"айпи {base}",
                    f"сетевой {base}"
                ])
            else:
                keywords.extend([
                    f"ip {base}",
                    f"айпі {base}",
                    f"мережевий {base}"
                ])

        # 2. HDVR (аналоговий/гібридний/мультиформатний)
        elif "hdvr" in value_lower or "xvr" in value_lower:
            if lang == "ru":
                keywords.extend([
                    f"аналоговый {base}",
                    f"гибридный {base}",
                    f"мультиформатный {base}"
                ])
            else:
                keywords.extend([
                    f"аналоговий {base}",
                    f"гібридний {base}",
                    f"мультиформатний {base}"
                ])

        return keywords

    def _get_ai_technology_keywords(
        self,
        name: str,
        lang: str
    ) -> List[str]:
        """Витягування ключових слів для AI технологій (WizSense/AcuSense)"""
        keywords = []
        name_lower = name.lower()
        base = "видеорегистратор" if lang == "ru" else "відеореєстратор"

        # Перевірка на WizSense (Dahua)
        if "wizsense" in name_lower:
            if lang == "ru":
                keywords.extend([
                    f"{base} с ai",
                    f"умный {base}",
                    f"{base} с искусственным интеллектом",
                    f"wizsense {base}"
                ])
            else:
                keywords.extend([
                    f"{base} з ai",
                    f"розумний {base}",
                    f"{base} зі штучним інтелектом",
                    f"wizsense {base}"
                ])

        # Перевірка на AcuSense (Hikvision)
        elif "acusense" in name_lower:
            if lang == "ru":
                keywords.extend([
                    f"{base} с ai",
                    f"умный {base}",
                    f"{base} с искусственным интеллектом",
                    f"acusense {base}"
                ])
            else:
                keywords.extend([
                    f"{base} з ai",
                    f"розумний {base}",
                    f"{base} зі штучним інтелектом",
                    f"acusense {base}"
                ])

        return keywords

    def _get_poe_support(
        self,
        accessor: SpecAccessor,
        lang: str,
        allowed: Set[str]
    ) -> List[str]:
        """Перевірка підтримки PoE"""
        if not is_spec_allowed("Підтримка PoE", allowed):
            return []

        base = "видеорегистратор" if lang == "ru" else "відеореєстратор"

        value = accessor.value("Підтримка PoE")
        if not value or value.strip().lower() != "так":
            return []

        # Повертаємо всі варіанти ключових фраз
        if lang == "ru":
            return [
                f"пое {base}",
                f"{base} с пое",
                "nvr poe",
                "регистратор poe"
            ]
        else:
            return [
                f"пое {base}",
                f"{base} з пое",
                "nvr poe",
                "реєстратор poe"
            ]

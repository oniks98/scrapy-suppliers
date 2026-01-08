"""
Процесор для загальних категорій (HDD, SD, USB та інші) - Viatec.
"""

from typing import List, Dict, Set
import logging

from keywords.processors.viatec.base import ViatecBaseProcessor
from keywords.core.models import CategoryConfig, Spec, MAX_MODEL_KEYWORDS, MAX_SPEC_KEYWORDS
from keywords.core.helpers import SpecAccessor, KeywordBucket
from keywords.utils.name_helpers import extract_brand, extract_model, extract_technology
from keywords.categories.viatec.router import get_category_handler


class GenericProcessor(ViatecBaseProcessor):
    """Процесор для загальних категорій"""

    def generate(
        self,
        name: str,
        config: CategoryConfig,
        specs: List[Spec],
        lang: str,
        manufacturers: Dict[str, str],
        logger: logging.Logger
    ) -> List[str]:
        """Генерація ключових слів для загальних категорій"""
        base = getattr(config, f"base_keyword_{lang}")
        if not base:
            return []

        accessor = SpecAccessor(specs)
        bucket = KeywordBucket(MAX_MODEL_KEYWORDS + MAX_SPEC_KEYWORDS)
        allowed = config.allowed_specs

        # Блок 1: Модель і бренд
        model_keywords = self._generate_model_keywords(name, manufacturers)
        bucket.extend(model_keywords)

        # Блок 2: Бренд + base
        brand = self._get_brand_from_accessor(accessor, allowed) or extract_brand(name, manufacturers)
        if brand:
            bucket.add(f"{base} {brand.lower()}")
            bucket.add(f"{brand.lower()} {base}")

        # Блок 3: Специфічні характеристики категорії (роутинг)
        category_handler = get_category_handler(config.category_id)
        if category_handler:
            category_keywords = category_handler(accessor, lang, base, allowed)
            bucket.extend(category_keywords)

        # Блок 4: Універсальні фрази
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

    def _get_brand_from_accessor(
        self,
        accessor: SpecAccessor,
        allowed: Set[str]
    ) -> str | None:
        """Витягування бренду з характеристик"""
        from keywords.utils.spec_helpers import is_spec_allowed

        if not is_spec_allowed("Виробник", allowed):
            return None

        return accessor.value("Виробник")

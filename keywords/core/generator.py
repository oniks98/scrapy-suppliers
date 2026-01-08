"""
Головний генератор ключових слів (тонкий оркестратор).
"""

from typing import List, Dict, Optional
import logging

from keywords.core.models import CategoryConfig, Spec, MAX_TOTAL_KEYWORDS
from keywords.core.helpers import KeywordBucket
from keywords.core.loaders import ConfigLoader
from keywords.processors.router import get_processor


class ProductKeywordsGenerator:
    """Генератор ключових слів для товарів (оркестратор)"""

    def __init__(
        self,
        keywords_csv_path: str,
        manufacturers_csv_path: str,
        logger: Optional[logging.Logger] = None
    ):
        """
        Args:
            keywords_csv_path: Шлях до CSV з налаштуваннями категорій
            manufacturers_csv_path: Шлях до CSV з виробниками
            logger: Опціональний логгер
        """
        self.logger = logger or logging.getLogger(__name__)
        self.categories: Dict[str, CategoryConfig] = {}
        self.manufacturers: Dict[str, str] = {}

        # Завантажуємо конфігурацію
        self.categories = ConfigLoader.load_keywords_mapping(keywords_csv_path, self.logger)
        self.manufacturers = ConfigLoader.load_manufacturers(manufacturers_csv_path, self.logger)

    def generate_keywords(
        self,
        product_name: str,
        category_id: str,
        specs_list: Optional[List[Spec]] = None,
        lang: str = "ru",
    ) -> str:
        """
        Генерація ключових слів для товару.

        Args:
            product_name: Назва товару
            category_id: ID категорії
            specs_list: Список характеристик
            lang: Мова (ru/ua)

        Returns:
            Рядок з ключовими словами через кому
        """
        # Валідація вхідних даних
        if not isinstance(specs_list, list):
            specs_list = []
        if lang not in {"ru", "ua"}:
            lang = "ru"

        # Отримуємо конфігурацію категорії
        config = self.categories.get(category_id)
        if not config:
            self.logger.warning(f"No config for category {category_id}")
            return ""

        # Отримуємо процесор для цієї категорії
        processor = get_processor(config.processor_type)
        if not processor:
            self.logger.warning(f"No processor for type {config.processor_type}")
            return ""

        # Генеруємо ключові слова через процесор
        keywords = processor.generate(
            name=product_name,
            config=config,
            specs=specs_list,
            lang=lang,
            manufacturers=self.manufacturers,
            logger=self.logger
        )

        # Дедуплікація і фінальне об'єднання
        return self._merge_keywords(keywords)

    @staticmethod
    def _merge_keywords(keywords: List[str]) -> str:
        """Об'єднання ключових слів з дедуплікацією"""
        bucket = KeywordBucket(MAX_TOTAL_KEYWORDS)
        bucket.extend(keywords)
        return ", ".join(bucket.to_list())

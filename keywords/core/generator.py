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
        self.categories: Dict[str, List[CategoryConfig]] = {}
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
        configs = self.categories.get(category_id)
        if not configs:
            self.logger.warning(f"No config for category {category_id}")
            return ""

        # Вибираємо правильну конфігурацію на основі характеристики "Тип устройства"
        config = self._select_config(configs, specs_list)

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

    @staticmethod
    def _select_config(configs: List[CategoryConfig], specs_list: List[Spec]) -> CategoryConfig:
        """
        Вибір правильної конфігурації на основі характеристики "Тип устройства".
        
        Args:
            configs: Список конфігурацій для категорії
            specs_list: Список характеристик товару
            
        Returns:
            Правильна конфігурація
        """
        # Якщо тільки одна конфігурація - повертаємо її
        if len(configs) == 1:
            return configs[0]
        
        # Шукаємо характеристику "Тип устройства"
        device_type_value = None
        for spec in specs_list:
            if spec.get("name", "").strip() == "Тип устройства":
                device_type_value = spec.get("value", "").strip().lower()
                break
        
        # Якщо характеристики немає - повертаємо першу конфігурацію
        if not device_type_value:
            return configs[0]
        
        # Вибираємо конфігурацію на основі значення "Тип устройства"
        for config in configs:
            base_keyword = config.base_keyword_ua.lower()  # Використовуємо український варіант
            
            # Відеодомофон
            if "відеодомофон" in base_keyword and "відеодомофон" in device_type_value:
                return config
            
            # Виклична панель
            if ("викличн" in base_keyword or "панел" in base_keyword) and ("панел" in device_type_value or "виклич" in device_type_value):
                return config
            
            # Аудіодомофон
            if "аудіодомофон" in base_keyword and "аудіодомофон" in device_type_value:
                return config
        
        # Якщо нічого не підійшло - повертаємо першу
        return configs[0]

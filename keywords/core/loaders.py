"""
Завантажувачі конфігурацій з CSV файлів.
"""

import csv
from pathlib import Path
from typing import Dict, List, Set
import logging

from keywords.core.models import CategoryConfig, ProcessorType, CATEGORY_PROCESSORS


class ConfigLoader:
    """Завантажувач конфігурацій категорій"""

    @staticmethod
    def load_keywords_mapping(csv_path: str, logger: logging.Logger) -> Dict[str, CategoryConfig]:
        """
        Завантаження налаштувань категорій з CSV.

        Args:
            csv_path: Шлях до CSV з налаштуваннями категорій
            logger: Логгер

        Returns:
            Словник категорій
        """
        categories: Dict[str, CategoryConfig] = {}

        try:
            path = Path(csv_path)
            if not path.exists():
                raise FileNotFoundError(f"Keywords CSV not found: {csv_path}")

            with open(csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    category_id = row.get("Ідентифікатор_підрозділу", "").strip()
                    if not category_id:
                        continue

                    # Визначаємо тип процесора
                    processor_type = CATEGORY_PROCESSORS.get(
                        category_id,
                        ProcessorType.GENERIC
                    )

                    categories[category_id] = CategoryConfig(
                        category_id=category_id,
                        base_keyword_ru=row.get("base_keyword_ru", "").strip(),
                        base_keyword_ua=row.get("base_keyword_ua", "").strip(),
                        universal_phrases_ru=ConfigLoader._split_phrases(
                            row.get("universal_phrases_ru", "")
                        ),
                        universal_phrases_ua=ConfigLoader._split_phrases(
                            row.get("universal_phrases_ua", "")
                        ),
                        allowed_specs=ConfigLoader._parse_allowed_specs(
                            row.get("allowed_specs", "")
                        ),
                        processor_type=processor_type
                    )

            logger.info(f"Loaded {len(categories)} categories")
            return categories

        except Exception as e:
            logger.error(f"Failed to load keywords CSV: {e}")
            raise

    @staticmethod
    def load_manufacturers(csv_path: str, logger: logging.Logger) -> Dict[str, str]:
        """
        Завантаження маппінгу виробників з CSV.

        Args:
            csv_path: Шлях до CSV з виробниками
            logger: Логгер

        Returns:
            Словник виробників
        """
        manufacturers: Dict[str, str] = {}

        try:
            path = Path(csv_path)
            if not path.exists():
                raise FileNotFoundError(f"Manufacturers CSV not found: {csv_path}")

            with open(csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    keyword = row.get("Слово в названии продукта", "").strip()
                    manufacturer = row.get("Производитель (виробник)", "").strip()
                    if keyword and manufacturer:
                        # Додаємо в нижньому регістрі для пошуку
                        manufacturers[keyword.lower()] = manufacturer

            logger.info(f"Loaded {len(manufacturers)} manufacturers")
            return manufacturers

        except Exception as e:
            logger.error(f"Failed to load manufacturers CSV: {e}")
            raise

    @staticmethod
    def _split_phrases(value: str) -> List[str]:
        """Розділення фраз з CSV"""
        cleaned = value.strip().strip('"')
        if not cleaned:
            return []
        return [phrase.strip() for phrase in cleaned.split(",") if phrase.strip()]

    @staticmethod
    def _parse_allowed_specs(value: str) -> Set[str]:
        """Парсинг дозволених характеристик"""
        cleaned = value.strip().strip('"')
        if not cleaned:
            return set()
        return {spec.strip().lower() for spec in cleaned.split(",") if spec.strip()}

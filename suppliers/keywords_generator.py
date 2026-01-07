"""
Генератор ключевых слов для товаров.

Поддерживает гибкую конфигурацию через CSV файлы:
- viatec_keywords.csv - настройки категорий
- viatec_manufacturers.csv - маппинг производителей
"""

import re
import csv
from typing import List, Dict, Optional, Set, TypedDict
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
import logging


# =====================================================
# ТИПЫ И КОНСТАНТЫ
# =====================================================

class ProcessorType(str, Enum):
    """Типы процессоров для обработки категорий"""
    CAMERA = "camera"
    DVR = "dvr"
    GENERIC = "generic"


class Spec(TypedDict):
    """Структура характеристики товара"""
    name: str
    value: str


@dataclass
class CategoryConfig:
    """Конфигурация категории из CSV"""
    category_id: str
    base_keyword_ru: str
    base_keyword_ua: str
    universal_phrases_ru: List[str]
    universal_phrases_ua: List[str]
    allowed_specs: Set[str]
    processor_type: ProcessorType


# Маппинг категорий на типы процессоров
CATEGORY_PROCESSORS = {
    "301105": ProcessorType.CAMERA,  # Камеры видеонаблюдения
    "301101": ProcessorType.DVR,     # Видеорегистраторы
    "70704": ProcessorType.GENERIC,  # Жесткие диски (HDD)
}

# Константы лимитов
MAX_MODEL_KEYWORDS = 10
MAX_SPEC_KEYWORDS = 15
MAX_UNIVERSAL_KEYWORDS = 10
MAX_TOTAL_KEYWORDS = 30

# Технологии камер
CAMERA_TECHNOLOGIES = {"ip", "ahd", "hdcvi", "tvi", "cvi"}

# Термины для разных языков
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


# =====================================================
# ОСНОВНОЙ КЛАСС
# =====================================================

class ProductKeywordsGenerator:
    """Генератор ключевых слов для товаров"""

    def __init__(
        self,
        keywords_csv_path: str,
        manufacturers_csv_path: str,
        logger: Optional[logging.Logger] = None
    ):
        """
        Args:
            keywords_csv_path: Путь к CSV с настройками категорий
            manufacturers_csv_path: Путь к CSV с производителями
            logger: Опциональный логгер
        """
        self.logger = logger or logging.getLogger(__name__)
        self.categories: Dict[str, CategoryConfig] = {}
        self.manufacturers: Dict[str, str] = {}
        
        self._load_keywords_mapping(keywords_csv_path)
        self._load_manufacturers(manufacturers_csv_path)

    # =====================================================
    # ЗАГРУЗКА ДАННЫХ
    # =====================================================

    def _load_keywords_mapping(self, csv_path: str) -> None:
        """Загрузка настроек категорий из CSV"""
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

                    # Определяем тип процессора
                    processor_type = CATEGORY_PROCESSORS.get(
                        category_id, 
                        ProcessorType.GENERIC
                    )

                    self.categories[category_id] = CategoryConfig(
                        category_id=category_id,
                        base_keyword_ru=row.get("base_keyword_ru", "").strip(),
                        base_keyword_ua=row.get("base_keyword_ua", "").strip(),
                        universal_phrases_ru=self._split_phrases(
                            row.get("universal_phrases_ru", "")
                        ),
                        universal_phrases_ua=self._split_phrases(
                            row.get("universal_phrases_ua", "")
                        ),
                        allowed_specs=self._parse_allowed_specs(
                            row.get("allowed_specs", "")
                        ),
                        processor_type=processor_type
                    )

            self.logger.info(f"Loaded {len(self.categories)} categories")

        except Exception as e:
            self.logger.error(f"Failed to load keywords CSV: {e}")
            raise

    def _load_manufacturers(self, csv_path: str) -> None:
        """Загрузка маппинга производителей из CSV"""
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
                        # Добавляем в нижнем регистре для поиска
                        self.manufacturers[keyword.lower()] = manufacturer

            self.logger.info(f"Loaded {len(self.manufacturers)} manufacturers")

        except Exception as e:
            self.logger.error(f"Failed to load manufacturers CSV: {e}")
            raise

    @staticmethod
    def _split_phrases(value: str) -> List[str]:
        """Разделение фраз из CSV"""
        cleaned = value.strip().strip('"')
        if not cleaned:
            return []
        return [phrase.strip() for phrase in cleaned.split(",") if phrase.strip()]

    @staticmethod
    def _parse_allowed_specs(value: str) -> Set[str]:
        """Парсинг разрешённых характеристик"""
        cleaned = value.strip().strip('"')
        if not cleaned:
            return set()
        return {spec.strip().lower() for spec in cleaned.split(",") if spec.strip()}

    # =====================================================
    # PUBLIC API
    # =====================================================

    def generate_keywords(
        self,
        product_name: str,
        category_id: str,
        specs_list: Optional[List[Spec]] = None,
        lang: str = "ru",
    ) -> str:
        """
        Генерация ключевых слов для товара.

        Args:
            product_name: Название товара
            category_id: ID категории
            specs_list: Список характеристик
            lang: Язык (ru/ua)

        Returns:
            Строка с ключевыми словами через запятую
        """
        # Валидация входных данных
        if not isinstance(specs_list, list):
            specs_list = []
        if lang not in {"ru", "ua"}:
            lang = "ru"

        # Получаем конфигурацию категории
        config = self.categories.get(category_id)
        if not config:
            self.logger.warning(f"No config for category {category_id}")
            return ""

        # Блок 1: Модель и бренд
        block1 = self._generate_model_keywords(product_name)

        # Блок 2: Спецификации (зависит от типа процессора)
        if config.processor_type == ProcessorType.CAMERA:
            block2 = self._generate_camera_keywords(
                product_name, config, specs_list, lang
            )
        elif config.processor_type == ProcessorType.DVR:
            block2 = self._generate_dvr_keywords(
                product_name, config, specs_list, lang
            )
        else:
            block2 = self._generate_generic_keywords(
                product_name, config, specs_list, lang
            )

        # Блок 3: Универсальные фразы
        block3 = self._generate_universal_keywords(config, lang)

        # Дедупликация и объединение
        return self._merge_keywords(block1, block2, block3)

    # =====================================================
    # БЛОК 1: МОДЕЛЬ И БРЕНД
    # =====================================================

    def _generate_model_keywords(self, name: str) -> List[str]:
        """Генерация ключевых слов на основе модели и бренда"""
        brand = self._extract_brand(name)
        model = self._extract_model(name)
        tech = self._extract_technology(name)

        keywords = []

        # Модель
        if model:
            keywords.append(model)
            if brand:
                keywords.append(f"{brand} {model}")

        # Технология + бренд (с маленькой буквы)
        if brand and tech:
            keywords.append(f"{tech.lower()} {brand.lower()}")

        return keywords[:MAX_MODEL_KEYWORDS]

    def _extract_brand(self, text: str) -> Optional[str]:
        """Извлечение бренда из названия"""
        text_lower = text.lower()
        
        # Поиск по маппингу производителей
        for keyword, manufacturer in self.manufacturers.items():
            if keyword in text_lower:
                return manufacturer

        return None

    @staticmethod
    def _extract_model(text: str) -> Optional[str]:
        """Извлечение модели из названия (паттерн: XX-XXXX)"""
        match = re.search(r"\b[A-Z]{2,5}-[A-Z0-9-]{3,}\b", text, re.I)
        return match.group(0).upper() if match else None

    @staticmethod
    def _extract_technology(text: str) -> Optional[str]:
        """Извлечение технологии из названия"""
        text_lower = text.lower()
        for tech in CAMERA_TECHNOLOGIES:
            if tech in text_lower:
                return tech.upper()
        return None

    # =====================================================
    # БЛОК 2: КАМЕРЫ
    # =====================================================

    def _generate_camera_keywords(
        self,
        name: str,
        config: CategoryConfig,
        specs: List[Spec],
        lang: str
    ) -> List[str]:
        """Генерация ключевых слов для камер"""
        base = getattr(config, f"base_keyword_{lang}")
        if not base:
            return []

        keywords = []

        # Извлекаем характеристики
        brand = self._get_brand_from_specs(specs, config.allowed_specs) or self._extract_brand(name)
        resolution = self._get_resolution(specs, config.allowed_specs)
        focal = self._get_focal_length(specs, config.allowed_specs)
        tech = self._get_camera_technology(specs, config.allowed_specs)
        camera_type = self._get_camera_type(specs, lang, config.allowed_specs)
        ip_rating = self._get_ip_rating(specs, lang, config.allowed_specs)
        has_wifi = self._check_wifi(name, specs)
        wide_angle = self._check_wide_angle(specs, config.allowed_specs)
        has_microphone = self._check_microphone(specs, config.allowed_specs)
        has_sd_card = self._check_sd_card(specs, config.allowed_specs)

        # Бренд (с маленькой буквы)
        if brand:
            keywords.extend([
                f"{base} {brand.lower()}",
                f"{brand.lower()} {base}"
            ])

        # Тип камеры
        if camera_type:
            keywords.append(f"{camera_type} {base}")

        # Разрешение
        if resolution:
            keywords.extend([
                f"{base} {resolution}",
                f"{resolution} {base}",
                f"{base} {resolution.replace('mp', 'мп')}",
                f"{resolution.replace('mp', 'мп')} {base}"
            ])

        # Технология (с вариантами)
        if tech:
            tech_lower = tech.lower()
            if tech_lower == "ip":
                keywords.extend([
                    f"ip {base}",
                    f"айпи {base}",
                    f"сетевая {base}" if lang == "ru" else f"мережева {base}"
                ])
            elif tech_lower in ["tvi", "cvi", "ahd"]:
                keywords.append(f"{tech_lower} {base}")

        # Фокусное расстояние
        if focal:
            keywords.append(f"{base} {focal}")

        # IP рейтинг
        if ip_rating:
            keywords.append(f"{ip_rating} {base}")

        # WiFi
        if has_wifi:
            keywords.append("wifi видеокамера" if lang == "ru" else "wifi відеокамера")

        # Широкоугольная
        if wide_angle:
            keywords.append("широкоугольная видеокамера" if lang == "ru" else "ширококутна відеокамера")

        # С микрофоном
        if has_microphone:
            keywords.append("видеокамера с микрофоном" if lang == "ru" else "відеокамера з мікрофоном")

        # С записью (SD-карта)
        if has_sd_card:
            keywords.append("видеокамера с записью" if lang == "ru" else "відеокамера з записом")

        return keywords[:MAX_SPEC_KEYWORDS]

    # =====================================================
    # БЛОК 2: ВИДЕОРЕГИСТРАТОРЫ
    # =====================================================

    def _generate_dvr_keywords(
        self,
        name: str,
        config: CategoryConfig,
        specs: List[Spec],
        lang: str
    ) -> List[str]:
        """Генерация ключевых слов для DVR/NVR"""
        base = getattr(config, f"base_keyword_{lang}")
        if not base:
            return []

        keywords = []

        # Извлекаем характеристики
        brand = self._get_brand_from_specs(specs, config.allowed_specs) or self._extract_brand(name)
        channels = self._get_channels(specs, lang, config.allowed_specs)
        dvr_type_keywords = self._get_dvr_type_keywords(specs, lang, config.allowed_specs)
        poe = self._get_poe_support(specs, lang, config.allowed_specs)
        ai_keywords = self._get_ai_technology_keywords(name, lang)

        # Бренд (с маленькой буквы)
        if brand:
            keywords.extend([
                f"{base} {brand.lower()}",
                f"{brand.lower()} {base}"
            ])

        # Количество каналов (формат: "N-канальный видеорегистратор")
        if channels:
            if lang == "ru":
                keywords.append(f"{channels}-канальный {base}")
            else:
                keywords.append(f"{channels}-канальний {base}")

        # Тип DVR (множественные варианты)
        if dvr_type_keywords:
            keywords.extend(dvr_type_keywords)

        # PoE поддержка (множественные варианты)
        if poe:
            keywords.extend(poe)

        # AI технологии (WizSense/AcuSense)
        if ai_keywords:
            keywords.extend(ai_keywords)

        return keywords[:MAX_SPEC_KEYWORDS]

    # =====================================================
    # БЛОК 2: GENERIC
    # =====================================================

    def _generate_generic_keywords(
        self,
        name: str,
        config: CategoryConfig,
        specs: List[Spec],
        lang: str
    ) -> List[str]:
        """
        Генерация ключевых слов для обычных категорий.
        Используется для HDD, кронштейнов и других простых категорий.
        """
        base = getattr(config, f"base_keyword_{lang}")
        if not base:
            return []

        keywords = []

        # Получаем бренд
        brand = self._get_brand_from_specs(specs, config.allowed_specs) or self._extract_brand(name)

        # Бренд (с маленькой буквы)
        if brand:
            keywords.extend([
                f"{base} {brand.lower()}",
                f"{brand.lower()} {base}"
            ])

        # Специфичные характеристики в зависимости от категории
        category_keywords = self._get_category_specific_keywords(
            config.category_id, specs, lang, base, config.allowed_specs
        )
        if category_keywords:
            keywords.extend(category_keywords)

        return keywords[:MAX_SPEC_KEYWORDS]

    def _get_category_specific_keywords(
        self,
        category_id: str,
        specs: List[Spec],
        lang: str,
        base: str,
        allowed: Set[str]
    ) -> List[str]:
        """
        Получение специфичных ключевых слов для категории.
        """
        # HDD (70704)
        if category_id == "70704":
            return self._get_hdd_keywords(specs, lang, base, allowed)
        
        # Можно добавить другие категории
        return []

    def _get_hdd_keywords(
        self,
        specs: List[Spec],
        lang: str,
        base: str,
        allowed: Set[str]
    ) -> List[str]:
        """
        Генерация ключевых слов для жестких дисков и SD-карт.
        """
        keywords = []

        # Объем накопителя
        capacity_info = self._get_hdd_capacity_with_size(specs, lang, allowed)
        if not capacity_info:
            return keywords

        capacity = capacity_info["formatted"]  # "форматированный объем (1tb, 500gb)"
        size_gb = capacity_info["size_gb"]    # размер в GB для проверки

        # Определяем тип: SD-карта (<=512GB) или HDD/SSD (>512GB)
        is_sd_card = size_gb <= 512

        if is_sd_card:
            # Это SD-карта
            keywords.extend(self._get_sd_card_keywords(capacity, lang))
        else:
            # Это HDD/SSD
            keywords.extend(self._get_hdd_disk_keywords(
                capacity, specs, lang, base, allowed
            ))

        return keywords

    def _get_sd_card_keywords(
        self,
        capacity: str,
        lang: str
    ) -> List[str]:
        """
        Генерация ключевых слов для SD-карт.
        """
        keywords = []

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

        return keywords

    def _get_hdd_disk_keywords(
        self,
        capacity: str,
        specs: List[Spec],
        lang: str,
        base: str,
        allowed: Set[str]
    ) -> List[str]:
        """
        Генерация ключевых слов для HDD/SSD.
        """
        keywords = []

        # Объем
        keywords.extend([
            f"{base} {capacity}",
            f"{capacity} {base}"
        ])

        # Интерфейс
        interface = self._get_hdd_interface(specs, lang, allowed)
        if interface:
            keywords.append(f"{base} {interface}")

        # Скорость вращения (если есть - HDD, если нет - возможно SSD)
        rpm = self._get_hdd_rpm(specs, lang, allowed)
        if rpm:
            # Для видеонаблюдения популярны диски 5400/7200 RPM
            if lang == "ru":
                keywords.append(f"{base} {rpm} об/мин")
            else:
                keywords.append(f"{base} {rpm} об/хв")
        else:
            # Если нет скорости вращения, возможно это SSD
            keywords.append(f"ssd {base}")

        # Для видеонаблюдения
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

    # =====================================================
    # БЛОК 3: УНИВЕРСАЛЬНЫЕ ФРАЗЫ
    # =====================================================

    def _generate_universal_keywords(
        self,
        config: CategoryConfig,
        lang: str
    ) -> List[str]:
        """Генерация универсальных ключевых слов"""
        phrases = getattr(config, f"universal_phrases_{lang}", [])
        return phrases[:MAX_UNIVERSAL_KEYWORDS]

    # =====================================================
    # HELPERS: ИЗВЛЕЧЕНИЕ ИЗ SPECS
    # =====================================================

    def _get_spec_value(
        self,
        specs: List[Spec],
        spec_name_key: str,
        allowed: Set[str],
        lang: str = "ru"
    ) -> Optional[str]:
        """Универсальный метод получения значения характеристики"""
        # Проверяем, разрешена ли эта характеристика
        if not self._is_spec_allowed(spec_name_key, allowed):
            return None

        # Получаем термин на нужном языке
        term = SPEC_TERMS.get(spec_name_key, {}).get(lang, "")
        if not term:
            return None

        # Ищем в характеристиках
        for spec in specs:
            if term in spec.get("name", "").lower():
                return spec.get("value", "")

        return None

    @staticmethod
    def _is_spec_allowed(spec_name: str, allowed: Set[str]) -> bool:
        """Проверка, разрешена ли характеристика"""
        if not allowed:
            return True
        
        spec_lower = spec_name.lower()
        return any(
            spec_lower in allowed_spec or allowed_spec in spec_lower 
            for allowed_spec in allowed
        )

    def _get_brand_from_specs(
        self,
        specs: List[Spec],
        allowed: Set[str]
    ) -> Optional[str]:
        """Извлечение бренда из характеристик"""
        value = self._get_spec_value(specs, "manufacturer", allowed)
        return value if value else None

    def _get_resolution(
        self,
        specs: List[Spec],
        allowed: Set[str]
    ) -> Optional[str]:
        """Извлечение разрешения"""
        if not self._is_spec_allowed("Роздільна здатність", allowed):
            return None

        for spec in specs:
            name_lower = spec.get("name", "").lower()
            # ТОЛЬКО точное совпадение с "Роздільна здатність"
            if "роздільна здатність" not in name_lower:
                continue
                
            value = str(spec.get("value", ""))
            
            # Вариант 1: Цифра + mp/мп ("2mp", "5 мп")
            match = re.search(r"(\d+)\s*[mм][pр]", value, re.I)
            if match:
                return f"{match.group(1)}mp"
            
            # Вариант 2: Просто цифра ("2", "5")
            match = re.search(r"^(\d+)$", value.strip())
            if match:
                return f"{match.group(1)}mp"

        return None

    def _get_focal_length(
        self,
        specs: List[Spec],
        allowed: Set[str]
    ) -> Optional[str]:
        """Извлечение фокусного расстояния"""
        if not self._is_spec_allowed("Фокусна відстань", allowed):
            return None

        for spec in specs:
            if "фокусна відстань" in spec.get("name", "").lower():
                value = str(spec.get("value", ""))
                match = re.search(r"(\d+(?:\.\d+)?)\s*(мм|mm)", value, re.I)
                if match:
                    return f"{match.group(1)} мм"

        return None

    def _get_camera_technology(
        self,
        specs: List[Spec],
        allowed: Set[str]
    ) -> Optional[str]:
        """Извлечение технологии камеры"""
        if not self._is_spec_allowed("Тип камери", allowed):
            return None

        for spec in specs:
            if "тип камери" in spec.get("name", "").lower():
                value = spec.get("value", "").lower()
                for tech in CAMERA_TECHNOLOGIES:
                    if tech in value:
                        return tech.upper()

        return None

    def _get_camera_type(
        self,
        specs: List[Spec],
        lang: str,
        allowed: Set[str]
    ) -> Optional[str]:
        """Извлечение типа камеры (купольная/поворотная)"""
        if not self._is_spec_allowed("Форм-фактор", allowed):
            return None

        mapping = {
            "ru": {"купол": "купольная", "ptz": "поворотная"},
            "ua": {"купол": "купольна", "ptz": "поворотна"},
        }

        for spec in specs:
            name_lower = spec.get("name", "").lower()
            if "форм-фактор" in name_lower or "форм" in name_lower:
                value = spec.get("value", "").lower()
                for keyword, result in mapping[lang].items():
                    if keyword in value:
                        return result

        return None

    def _get_ip_rating(
        self,
        specs: List[Spec],
        lang: str,
        allowed: Set[str]
    ) -> Optional[str]:
        """Проверка защиты IP65-68 (уличная камера)"""
        if not self._is_spec_allowed("Захист обладнання", allowed):
            return None

        for spec in specs:
            if "захист" in spec.get("name", "").lower():
                value = str(spec.get("value", ""))
                if re.search(r"ip6[5-8]", value, re.I):
                    return "уличная" if lang == "ru" else "вулична"

        return None

    def _get_channels(
        self,
        specs: List[Spec],
        lang: str,
        allowed: Set[str]
    ) -> Optional[str]:
        """Извлечение количества каналов (строгий поиск по названию характеристики)"""
        if not self._is_spec_allowed("Кількість каналів", allowed):
            return None

        # Строгий поиск: только точное совпадение названия характеристики
        for spec in specs:
            spec_name_lower = spec.get("name", "").lower().strip()
            if spec_name_lower == "кількість каналів":
                value = str(spec.get("value", "")).strip()
                # Извлекаем число из значения
                match = re.search(r"\d+", value)
                if match:
                    return match.group(0)

        return None

    def _get_dvr_type_keywords(
        self,
        specs: List[Spec],
        lang: str,
        allowed: Set[str]
    ) -> List[str]:
        """Определение типа видеорегистратора с множественными ключевыми фразами"""
        if not self._is_spec_allowed("Тип відеореєстратора", allowed):
            return []

        base = "видеорегистратор" if lang == "ru" else "відеореєстратор"
        keywords = []
        dvr_type_value = None

        # Ищем характеристику "Тип відеореєстратора"
        for spec in specs:
            spec_name_lower = spec.get("name", "").lower()
            if "тип відеореєстратора" == spec_name_lower.strip():
                dvr_type_value = spec.get("value", "").strip()
                break

        # Если характеристика не найдена, возвращаем пустой список
        if not dvr_type_value:
            return []

        value_lower = dvr_type_value.lower()

        # 1. IP видеорегистратор (NVR)
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

        # 2. HDVR (аналоговый/гибридный/мультиформатный)
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
        """Извлечение ключевых слов для AI технологий (WizSense/AcuSense)"""
        keywords = []
        name_lower = name.lower()
        base = "видеорегистратор" if lang == "ru" else "відеореєстратор"

        # Проверка на WizSense (Dahua)
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

        # Проверка на AcuSense (Hikvision)
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
        specs: List[Spec],
        lang: str,
        allowed: Set[str]
    ) -> List[str]:
        """Проверка поддержки PoE с множественными ключевыми фразами"""
        if not self._is_spec_allowed("Підтримка PoE", allowed):
            return []

        base = "видеорегистратор" if lang == "ru" else "відеореєстратор"
        poe_found = False

        # Строгий поиск характеристики "Підтримка PoE" = "Так"
        for spec in specs:
            spec_name_lower = spec.get("name", "").lower()
            # Точное совпадение названия характеристики
            if "підтримка poe" == spec_name_lower.strip():
                value = str(spec.get("value", "")).strip().lower()
                # Только если значение = "Так" (на украинском языке из Prom)
                if value == "так":
                    poe_found = True
                    break

        if not poe_found:
            return []

        # Возвращаем все варианты ключевых фраз
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

    @staticmethod
    def _check_wifi(name: str, specs: List[Spec]) -> bool:
        """Проверка наличия WiFi"""
        # Проверка в названии
        if re.search(r"wi[- ]?fi", name, re.I):
            return True

        # Проверка в характеристиках
        for spec in specs:
            spec_text = f"{spec.get('name', '')} {spec.get('value', '')}"
            if re.search(r"wi[- ]?fi", spec_text, re.I):
                return True

        return False

    def _check_wide_angle(self, specs: List[Spec], allowed: Set[str]) -> bool:
        """Проверка широкого угла обзора (>90 градусов)"""
        if not self._is_spec_allowed("Кут огляду по горизонталі", allowed):
            return False

        for spec in specs:
            name_lower = spec.get("name", "").lower()
            if "кут" in name_lower and "горизонт" in name_lower:
                value = str(spec.get("value", ""))
                # Ищем число
                match = re.search(r"(\d+)", value)
                if match:
                    angle = int(match.group(1))
                    return angle > 90

        return False

    def _check_microphone(self, specs: List[Spec], allowed: Set[str]) -> bool:
        """Проверка наличия встроенного микрофона"""
        if not self._is_spec_allowed("Вбудований мікрофон", allowed):
            return False

        for spec in specs:
            name_lower = spec.get("name", "").lower()
            if "мікрофон" in name_lower or "микрофон" in name_lower:
                value = str(spec.get("value", "")).lower()
                return value in {"так", "yes", "true", "є", "вбудований"}

        return False

    def _check_sd_card(self, specs: List[Spec], allowed: Set[str]) -> bool:
        """Проверка наличия порта для SD-карты"""
        if not self._is_spec_allowed("Порт для SD-карти", allowed):
            return False

        for spec in specs:
            name_lower = spec.get("name", "").lower()
            if "sd" in name_lower and "карт" in name_lower:
                value = str(spec.get("value", "")).lower()
                return value in {"так", "yes", "true", "є"}

        return False

    # =====================================================
    # HELPERS: HDD ХАРАКТЕРИСТИКИ
    # =====================================================

    def _get_hdd_capacity_with_size(
        self,
        specs: List[Spec],
        lang: str,
        allowed: Set[str]
    ) -> Optional[dict]:
        """
        Извлечение объема накопителя с информацией о размере в GB.
        Формат Prom: value="64", unit="GB" (раздельно)
        Возвращает: {"formatted": "64gb", "size_gb": 64} или None
        """
        if not self._is_spec_allowed("Об'єм накопичувача", allowed):
            return None

        for spec in specs:
            spec_name_lower = spec.get("name", "").lower().strip()
            if "об'єм накопичувача" == spec_name_lower:
                value_str = str(spec.get("value", "")).strip()
                unit = str(spec.get("unit", "")).strip().upper()
                
                # Извлекаем число из value
                match = re.search(r"(\d+)", value_str)
                if not match:
                    continue
                
                size = int(match.group(1))
                
                # Проверяем unit
                if unit not in ["GB", "TB", "MB"]:
                    continue
                
                # Конвертируем все в GB для сравнения
                if unit == "GB":
                    size_gb = size
                    formatted = f"{size // 1000}tb" if size >= 1000 else f"{size}gb"
                elif unit == "TB":
                    size_gb = size * 1000
                    formatted = f"{size}tb"
                elif unit == "MB":
                    size_gb = size / 1000
                    formatted = f"{size}mb"
                else:
                    return None
                
                return {
                    "formatted": formatted,
                    "size_gb": size_gb
                }

        return None

    def _get_hdd_interface(
        self,
        specs: List[Spec],
        lang: str,
        allowed: Set[str]
    ) -> Optional[str]:
        """
        Извлечение интерфейса HDD.
        Примеры: "SATA III (SATA/600)" -> "sata", "M.2" -> "m.2"
        """
        if not self._is_spec_allowed("Інтерфейс", allowed):
            return None

        for spec in specs:
            spec_name_lower = spec.get("name", "").lower().strip()
            if "інтерфейс" == spec_name_lower:
                value = str(spec.get("value", "")).strip().lower()
                
                # Проверяем популярные интерфейсы
                if "sata" in value:
                    return "sata"
                elif "m.2" in value or "m2" in value:
                    return "m.2"
                elif "nvme" in value:
                    return "nvme"
                elif "sas" in value:
                    return "sas"
                elif "ide" in value:
                    return "ide"

        return None

    def _get_hdd_rpm(
        self,
        specs: List[Spec],
        lang: str,
        allowed: Set[str]
    ) -> Optional[str]:
        """
        Извлечение скорости вращения HDD.
        Примеры: "7200 об/мин" -> "7200", "5400 RPM" -> "5400"
        """
        if not self._is_spec_allowed("Швидкість обертання", allowed):
            return None

        for spec in specs:
            spec_name_lower = spec.get("name", "").lower().strip()
            if "швидкість обертання" == spec_name_lower:
                value = str(spec.get("value", "")).strip()
                
                # Ищем число (5400, 7200, 10000)
                match = re.search(r"(\d{4,5})", value)
                if match:
                    return match.group(1)

        return None

    # =====================================================
    # ФИНАЛЬНАЯ ОБРАБОТКА
    # =====================================================

    def _merge_keywords(
        self,
        *blocks: List[str]
    ) -> str:
        """Объединение блоков ключевых слов с дедупликацией"""
        seen = set()
        result = []

        for block in blocks:
            for keyword in block:
                keyword_lower = keyword.lower().strip()
                if keyword_lower and keyword_lower not in seen:
                    seen.add(keyword_lower)
                    result.append(keyword)

        # Ограничиваем количество
        result = result[:MAX_TOTAL_KEYWORDS]

        return ", ".join(result)

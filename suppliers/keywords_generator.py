"""
Генератор ключевых слов для товаров.

Поддерживает гибкую конфигурацию через CSV файлы:
- viatec_keywords.csv - настройки категорий
- viatec_manufacturers.csv - маппинг производителей
"""

import re
import csv
from typing import List, Dict, Optional, Set, TypedDict, Callable
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
    "63705": ProcessorType.GENERIC,  # Карты памяти (SD/microSD)
    "70501": ProcessorType.GENERIC,  # USB-флешки
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
# HELPER КЛАССЫ
# =====================================================

class SpecAccessor:
    """Строгий доступ к характеристикам без ложных совпадений"""
    
    def __init__(self, specs: List[Spec]):
        """
        Args:
            specs: Список характеристик товара
        """
        self._map = {
            s["name"].strip().lower(): s
            for s in specs
            if s.get("name")
        }
    
    def get(self, name: str) -> Optional[Dict]:
        """Получить характеристику по имени"""
        return self._map.get(name.lower())
    
    def value(self, name: str) -> Optional[str]:
        """Получить значение характеристики"""
        s = self.get(name)
        return s["value"] if s else None
    
    def unit(self, name: str) -> Optional[str]:
        """Получить единицу измерения характеристики"""
        s = self.get(name)
        return s.get("unit") if s else None


class KeywordBucket:
    """Контейнер для ключевых слов с автоматической дедупликацией и лимитами"""
    
    def __init__(self, limit: int):
        """
        Args:
            limit: Максимальное количество ключевых слов
        """
        self.limit = limit
        self.items: List[str] = []
        self.seen: Set[str] = set()
    
    def add(self, value: Optional[str]) -> None:
        """Добавить одно ключевое слово"""
        if not value:
            return
        
        v = value.strip().lower()
        if v and v not in self.seen and len(self.items) < self.limit:
            self.seen.add(v)
            self.items.append(value.strip())
    
    def extend(self, values: List[str]) -> None:
        """Добавить несколько ключевых слов"""
        for v in values:
            self.add(v)
    
    def to_list(self) -> List[str]:
        """Получить список ключевых слов"""
        return self.items


# =====================================================
# HELPER ФУНКЦИИ
# =====================================================

def extract_capacity(
    specs: SpecAccessor,
    name: str
) -> Optional[Dict[str, any]]:
    """
    Универсальное извлечение объёма (для HDD/SD/USB).
    
    Args:
        specs: Accessor для характеристик
        name: Имя характеристики (например, "Об'єм накопичувача")
    
    Returns:
        {"formatted": "128gb", "size_gb": 128} или None
    """
    raw = specs.value(name)
    if not raw:
        return None
    
    # Извлекаем число
    match = re.search(r"(\d+)", raw)
    if not match:
        return None
    
    size = int(match.group(1))
    unit = (specs.unit(name) or "").upper()
    
    if unit not in ["GB", "TB", "MB"]:
        return None
    
    # Конвертируем в GB
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


def extract_speed(
    specs: SpecAccessor,
    name: str
) -> Optional[str]:
    """
    Извлечение скорости (для SD-карт).
    
    Args:
        specs: Accessor для характеристик
        name: Имя характеристики (например, "Швидкість зчитування")
    
    Returns:
        Скорость в виде числа (например, "90") или None
    """
    raw = specs.value(name)
    if not raw:
        return None
    
    # Ищем число
    match = re.search(r"(\d+)", raw)
    return match.group(1) if match else None


def extract_interface(
    specs: SpecAccessor,
    name: str
) -> Optional[str]:
    """
    Извлечение интерфейса (для HDD/USB).
    
    Args:
        specs: Accessor для характеристик
        name: Имя характеристики (например, "Інтерфейс")
    
    Returns:
        Интерфейс в нижнем регистре (например, "sata", "usb type-c") или None
    """
    raw = specs.value(name)
    if not raw:
        return None
    
    value_lower = raw.lower()
    
    # Проверяем популярные интерфейсы
    if "sata" in value_lower:
        return "sata"
    elif "m.2" in value_lower or "m2" in value_lower:
        return "m.2"
    elif "nvme" in value_lower:
        return "nvme"
    elif "sas" in value_lower:
        return "sas"
    elif "ide" in value_lower:
        return "ide"
    elif "type-c" in value_lower or "type c" in value_lower:
        return "usb type-c"
    elif "usb 3" in value_lower or "3." in value_lower:
        return "usb 3.0"
    elif "usb 2" in value_lower or "2." in value_lower:
        return "usb 2.0"
    
    return raw  # Возвращаем как есть, если не распознали


def extract_rpm(
    specs: SpecAccessor,
    name: str
) -> Optional[str]:
    """
    Извлечение скорости вращения (для HDD).
    
    Args:
        specs: Accessor для характеристик
        name: Имя характеристики (например, "Швидкість обертання")
    
    Returns:
        Скорость вращения (например, "7200") или None
    """
    raw = specs.value(name)
    if not raw:
        return None
    
    # Ищем число (5400, 7200, 10000)
    match = re.search(r"(\d{4,5})", raw)
    return match.group(1) if match else None


def is_spec_allowed(spec_name: str, allowed: Set[str]) -> bool:
    """
    Проверка, разрешена ли характеристика.
    
    Args:
        spec_name: Имя характеристики
        allowed: Множество разрешённых характеристик
    
    Returns:
        True, если характеристика разрешена
    """
    if not allowed:
        return True
    
    spec_lower = spec_name.lower()
    return any(
        spec_lower in allowed_spec or allowed_spec in spec_lower 
        for allowed_spec in allowed
    )


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

        # Блок 2: Спецификации (роутинг через handler)
        handler = PROCESSOR_HANDLERS.get(config.processor_type)
        if handler:
            block2 = handler(self, product_name, config, specs_list, lang)
        else:
            block2 = []

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

        accessor = SpecAccessor(specs)
        bucket = KeywordBucket(MAX_SPEC_KEYWORDS)
        allowed = config.allowed_specs

        # Извлекаем характеристики
        brand = self._get_brand_from_accessor(accessor, allowed) or self._extract_brand(name)
        resolution = self._get_resolution(accessor, allowed)
        focal = self._get_focal_length(accessor, allowed)
        tech = self._get_camera_technology(accessor, allowed)
        camera_type = self._get_camera_type(accessor, lang, allowed)
        ip_rating = self._get_ip_rating(accessor, lang, allowed)
        has_wifi = self._check_wifi(name, specs)
        wide_angle = self._check_wide_angle(accessor, allowed)
        has_microphone = self._check_microphone(accessor, allowed)
        has_sd_card = self._check_sd_card(accessor, allowed)

        # Бренд (с маленькой буквы)
        if brand:
            bucket.add(f"{base} {brand.lower()}")
            bucket.add(f"{brand.lower()} {base}")

        # Тип камеры
        if camera_type:
            bucket.add(f"{camera_type} {base}")

        # Разрешение
        if resolution:
            bucket.add(f"{base} {resolution}")
            bucket.add(f"{resolution} {base}")
            bucket.add(f"{base} {resolution.replace('mp', 'мп')}")
            bucket.add(f"{resolution.replace('mp', 'мп')} {base}")

        # Технология (с вариантами)
        if tech:
            tech_lower = tech.lower()
            if tech_lower == "ip":
                bucket.add(f"ip {base}")
                bucket.add(f"айпи {base}")
                bucket.add(f"сетевая {base}" if lang == "ru" else f"мережева {base}")
            elif tech_lower in ["tvi", "cvi", "ahd"]:
                bucket.add(f"{tech_lower} {base}")

        # Фокусное расстояние
        if focal:
            bucket.add(f"{base} {focal}")

        # IP рейтинг
        if ip_rating:
            bucket.add(f"{ip_rating} {base}")

        # WiFi
        if has_wifi:
            bucket.add("wifi видеокамера" if lang == "ru" else "wifi відеокамера")

        # Широкоугольная
        if wide_angle:
            bucket.add("широкоугольная видеокамера" if lang == "ru" else "ширококутна відеокамера")

        # С микрофоном
        if has_microphone:
            bucket.add("видеокамера с микрофоном" if lang == "ru" else "відеокамера з мікрофоном")

        # С записью (SD-карта)
        if has_sd_card:
            bucket.add("видеокамера с записью" if lang == "ru" else "відеокамера з записом")

        return bucket.to_list()

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

        accessor = SpecAccessor(specs)
        bucket = KeywordBucket(MAX_SPEC_KEYWORDS)
        allowed = config.allowed_specs

        # Извлекаем характеристики
        brand = self._get_brand_from_accessor(accessor, allowed) or self._extract_brand(name)
        channels = self._get_channels(accessor, allowed)
        dvr_type_keywords = self._get_dvr_type_keywords(accessor, lang, allowed)
        poe = self._get_poe_support(accessor, lang, allowed)
        ai_keywords = self._get_ai_technology_keywords(name, lang)

        # Бренд (с маленькой буквы)
        if brand:
            bucket.add(f"{base} {brand.lower()}")
            bucket.add(f"{brand.lower()} {base}")

        # Количество каналов (формат: "N-канальный видеорегистратор")
        if channels:
            if lang == "ru":
                bucket.add(f"{channels}-канальный {base}")
            else:
                bucket.add(f"{channels}-канальний {base}")

        # Тип DVR (множественные варианты)
        bucket.extend(dvr_type_keywords)

        # PoE поддержка (множественные варианты)
        bucket.extend(poe)

        # AI технологии (WizSense/AcuSense)
        bucket.extend(ai_keywords)

        return bucket.to_list()

    # =====================================================
    # БЛОК 2: GENERIC (HDD / SD / USB)
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
        Роутинг по категориям: HDD, SD-карты, USB-флешки.
        """
        base = getattr(config, f"base_keyword_{lang}")
        if not base:
            return []

        accessor = SpecAccessor(specs)
        bucket = KeywordBucket(MAX_SPEC_KEYWORDS)
        allowed = config.allowed_specs

        # Получаем бренд
        brand = self._get_brand_from_accessor(accessor, allowed) or self._extract_brand(name)

        # Бренд (с маленькой буквы)
        if brand:
            bucket.add(f"{base} {brand.lower()}")
            bucket.add(f"{brand.lower()} {base}")

        # Специфичные характеристики в зависимости от категории
        category_handler = CATEGORY_HANDLERS.get(config.category_id)
        if category_handler:
            category_keywords = category_handler(self, accessor, lang, base, allowed)
            bucket.extend(category_keywords)

        return bucket.to_list()

    # =====================================================
    # КАТЕГОРИЯ: ЖЕСТКИЕ ДИСКИ (70704)
    # =====================================================

    def _get_hdd_keywords(
        self,
        accessor: SpecAccessor,
        lang: str,
        base: str,
        allowed: Set[str]
    ) -> List[str]:
        """
        Генерация ключевых слов для жестких дисков (HDD/SSD).
        """
        keywords = []

        if not is_spec_allowed("Об'єм накопичувача", allowed):
            return keywords

        # 1. Объем накопителя
        capacity_info = extract_capacity(accessor, "Об'єм накопичувача")
        if not capacity_info:
            return keywords

        capacity = capacity_info["formatted"]

        keywords.extend([
            f"{base} {capacity}",
            f"{capacity} {base}"
        ])

        # 2. Интерфейс
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

        # 4. Скорость вращения (если есть - HDD, если нет - SSD)
        if is_spec_allowed("Швидкість обертання", allowed):
            rpm = extract_rpm(accessor, "Швидкість обертання")
            if rpm:
                if lang == "ru":
                    keywords.append(f"{base} {rpm} об/мин")
                else:
                    keywords.append(f"{base} {rpm} об/хв")
            else:
                # Если нет скорости вращения, возможно это SSD
                keywords.append(f"ssd {base}")

        # 5. Для видеонаблюдения
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
    # КАТЕГОРИЯ: КАРТЫ ПАМЯТИ (63705)
    # =====================================================

    def _get_sd_card_keywords(
        self,
        accessor: SpecAccessor,
        lang: str,
        base: str,
        allowed: Set[str]
    ) -> List[str]:
        """
        Генерация ключевых слов для SD-карт.
        """
        keywords = []

        if not is_spec_allowed("Об'єм пам'яті", allowed):
            return keywords

        # 1. Объем памяти
        capacity_info = extract_capacity(accessor, "Об'єм пам'яті")
        if not capacity_info:
            return keywords

        capacity = capacity_info["formatted"]

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

        # 2. Тип карты (microSD / SD)
        if is_spec_allowed("Тип карти пам'яті", allowed):
            card_type = accessor.value("Тип карти пам'яті")
            if card_type:
                card_type_lower = card_type.lower()
                if "microsd" in card_type_lower or "micro sd" in card_type_lower:
                    keywords.append(f"microsd {capacity}")
                elif "sd" in card_type_lower:
                    keywords.append(f"sd {capacity}")

        # 3. Скорость чтения (если высокая скорость)
        if is_spec_allowed("Швидкість зчитування", allowed):
            read_speed = extract_speed(accessor, "Швидкість зчитування")
            if read_speed and int(read_speed) >= 90:
                if lang == "ru":
                    keywords.append("быстрая sd карта")
                else:
                    keywords.append("швидка sd карта")

        return keywords

    # =====================================================
    # КАТЕГОРИЯ: USB-ФЛЕШКИ (70501)
    # =====================================================

    def _get_usb_flash_keywords(
        self,
        accessor: SpecAccessor,
        lang: str,
        base: str,
        allowed: Set[str]
    ) -> List[str]:
        """
        Генерация ключевых слов для USB-флешек.
        """
        keywords = []

        if not is_spec_allowed("Об'єм пам'яті", allowed):
            return keywords

        # 1. Объем памяти
        capacity_info = extract_capacity(accessor, "Об'єм пам'яті")
        if not capacity_info:
            return keywords

        capacity = capacity_info["formatted"]

        if lang == "ru":
            keywords.extend([
                f"флешка {capacity}",
                f"usb флешка {capacity}",
                f"{capacity} флешка",
                f"флеш накопитель {capacity}",
                "usb флешка для ноутбука",
                "флешка для компьютера"
            ])
        else:
            keywords.extend([
                f"флешка {capacity}",
                f"usb флешка {capacity}",
                f"{capacity} флешка",
                f"флеш накопичувач {capacity}",
                "usb флешка для ноутбука",
                "флешка для комп'ютера"
            ])

        # 2. Интерфейс (USB Type-C, USB 3.0, USB 2.0)
        if is_spec_allowed("Інтерфейс", allowed):
            interface = extract_interface(accessor, "Інтерфейс")
            if interface:
                interface_lower = interface.lower()
                if "type-c" in interface_lower or "type c" in interface_lower:
                    keywords.append("usb type-c флешка")
                elif "3." in interface_lower or "usb 3" in interface_lower:
                    keywords.append("usb 3.0 флешка")
                elif "2." in interface_lower or "usb 2" in interface_lower:
                    keywords.append("usb 2.0 флешка")

        # 3. Форм-фактор
        if is_spec_allowed("Форм-фактор", allowed):
            form_factor = accessor.value("Форм-фактор")
            if form_factor and "моноблок" in form_factor.lower():
                if lang == "ru":
                    keywords.append("компактная флешка")
                else:
                    keywords.append("компактна флешка")

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
    # HELPERS: ИЗВЛЕЧЕНИЕ ИЗ SPECS (КАМЕРЫ)
    # =====================================================

    def _get_brand_from_accessor(
        self,
        accessor: SpecAccessor,
        allowed: Set[str]
    ) -> Optional[str]:
        """Извлечение бренда из характеристик"""
        if not is_spec_allowed("Виробник", allowed):
            return None
        
        return accessor.value("Виробник")

    def _get_resolution(
        self,
        accessor: SpecAccessor,
        allowed: Set[str]
    ) -> Optional[str]:
        """Извлечение разрешения - ТОЛЬКО портальная характеристика 'Роздільна здатність (Мп)'"""
        if not is_spec_allowed("Роздільна здатність (Мп)", allowed):
            return None

        value = accessor.value("Роздільна здатність (Мп)")
        if not value:
            return None
        
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
        accessor: SpecAccessor,
        allowed: Set[str]
    ) -> Optional[str]:
        """Извлечение фокусного расстояния - ТОЛЬКО портальная характеристика 'Фокусна відстань'"""
        if not is_spec_allowed("Фокусна відстань", allowed):
            return None

        value = accessor.value("Фокусна відстань")
        if not value:
            return None
        
        # Если значение уже содержит мм, извлекаем число
        match = re.search(r"(\d+(?:\.\d+)?)\s*(мм|mm)", value, re.I)
        if match:
            return f"{match.group(1)} мм"
        
        # Если просто число - добавляем "мм"
        match = re.search(r"^(\d+(?:\.\d+)?)$", value.strip())
        if match:
            return f"{match.group(1)} мм"

        return None

    def _get_camera_technology(
        self,
        accessor: SpecAccessor,
        allowed: Set[str]
    ) -> Optional[str]:
        """Извлечение технологии камеры - ТОЛЬКО портальная характеристика 'Тип камери'"""
        if not is_spec_allowed("Тип камери", allowed):
            return None

        value = accessor.value("Тип камери")
        if not value:
            return None
        
        value_lower = value.lower()
        for tech in CAMERA_TECHNOLOGIES:
            if tech in value_lower:
                return tech.upper()

        return None

    def _get_camera_type(
        self,
        accessor: SpecAccessor,
        lang: str,
        allowed: Set[str]
    ) -> Optional[str]:
        """Извлечение типа камеры (купольная/поворотная) - СТРОГО портальная характеристика 'Форм-фактор'"""
        if not is_spec_allowed("Форм-фактор", allowed):
            return None

        mapping = {
            "ru": {
                "купол": "купольная", 
                "ptz": "поворотная",
                "поворот": "поворотная",  # Поворотний/Поворотная
                "циліндр": "цилиндрическая",  # Циліндричний/Циліндрична
                "куб": "кубическая",  # Кубічний/Кубічна
            },
            "ua": {
                "купол": "купольна", 
                "ptz": "поворотна",
                "поворот": "поворотна",  # Поворотний/Поворотна
                "циліндр": "циліндрична",  # Циліндричний/Циліндрична
                "куб": "кубічна",  # Кубічний/Кубічна
            },
        }

        value = accessor.value("Форм-фактор")
        if not value:
            return None
        
        value_lower = value.lower()
        for keyword, result in mapping[lang].items():
            if keyword in value_lower:
                return result

        return None

    def _get_ip_rating(
        self,
        accessor: SpecAccessor,
        lang: str,
        allowed: Set[str]
    ) -> Optional[str]:
        """Проверка защиты IP65-68 - ТОЛЬКО портальная характеристика 'Захист обладнання від води і пилу IP'"""
        if not is_spec_allowed("Захист обладнання від води і пилу IP", allowed):
            return None

        value = accessor.value("Захист обладнання від води і пилу IP")
        if not value:
            return None
        
        if re.search(r"ip6[5-8]", value, re.I):
            return "уличная" if lang == "ru" else "вулична"

        return None

    def _get_channels(
        self,
        accessor: SpecAccessor,
        allowed: Set[str]
    ) -> Optional[str]:
        """Извлечение количества каналов - ТОЛЬКО портальная характеристика 'Кількість каналів'"""
        if not is_spec_allowed("Кількість каналів", allowed):
            return None

        value = accessor.value("Кількість каналів")
        if not value:
            return None
        
        # Извлекаем число из значения
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
        """Определение типа видеорегистратора - ТОЛЬКО портальная характеристика 'Тип відеореєстратора'"""
        if not is_spec_allowed("Тип відеореєстратора", allowed):
            return []

        base = "видеорегистратор" if lang == "ru" else "відеореєстратор"
        keywords = []

        value = accessor.value("Тип відеореєстратора")
        if not value:
            return []

        value_lower = value.lower()

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
        accessor: SpecAccessor,
        lang: str,
        allowed: Set[str]
    ) -> List[str]:
        """Проверка поддержки PoE - ТОЛЬКО портальная характеристика 'Підтримка PoE'"""
        if not is_spec_allowed("Підтримка PoE", allowed):
            return []

        base = "видеорегистратор" if lang == "ru" else "відеореєстратор"

        value = accessor.value("Підтримка PoE")
        if not value or value.strip().lower() != "так":
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

    def _check_wide_angle(self, accessor: SpecAccessor, allowed: Set[str]) -> bool:
        """Проверка широкого угла обзора (>90 градусов) - ТОЛЬКО портальная характеристика 'Кут огляду по горизонталі'"""
        if not is_spec_allowed("Кут огляду по горизонталі", allowed):
            return False

        value = accessor.value("Кут огляду по горизонталі")
        if not value:
            return False
        
        # Ищем число
        match = re.search(r"(\d+)", value)
        if match:
            angle = int(match.group(1))
            return angle > 90

        return False

    def _check_microphone(self, accessor: SpecAccessor, allowed: Set[str]) -> bool:
        """Проверка наличия встроенного микрофона - ТОЛЬКО портальная характеристика 'Вбудований мікрофон'"""
        if not is_spec_allowed("Вбудований мікрофон", allowed):
            return False

        value = accessor.value("Вбудований мікрофон")
        if not value:
            return False
        
        return value.lower() in {"так", "yes", "true", "є", "вбудований"}

    def _check_sd_card(self, accessor: SpecAccessor, allowed: Set[str]) -> bool:
        """Проверка наличия порта для SD-карты - ТОЛЬКО портальная характеристика 'Порт для SD-карти'"""
        if not is_spec_allowed("Порт для SD-карти", allowed):
            return False

        value = accessor.value("Порт для SD-карти")
        if not value:
            return False
        
        return value.lower() in {"так", "yes", "true", "є"}

    # =====================================================
    # ФИНАЛЬНАЯ ОБРАБОТКА
    # =====================================================

    def _merge_keywords(
        self,
        *blocks: List[str]
    ) -> str:
        """Объединение блоков ключевых слов с дедупликацией"""
        bucket = KeywordBucket(MAX_TOTAL_KEYWORDS)
        
        for block in blocks:
            bucket.extend(block)
        
        return ", ".join(bucket.to_list())


# =====================================================
# РОУТИНГ ПРОЦЕССОРОВ И КАТЕГОРИЙ
# =====================================================

PROCESSOR_HANDLERS: Dict[ProcessorType, Callable] = {
    ProcessorType.CAMERA: ProductKeywordsGenerator._generate_camera_keywords,
    ProcessorType.DVR: ProductKeywordsGenerator._generate_dvr_keywords,
    ProcessorType.GENERIC: ProductKeywordsGenerator._generate_generic_keywords,
}

CATEGORY_HANDLERS: Dict[str, Callable] = {
    "70704": ProductKeywordsGenerator._get_hdd_keywords,      # HDD
    "63705": ProductKeywordsGenerator._get_sd_card_keywords,  # SD-карты
    "70501": ProductKeywordsGenerator._get_usb_flash_keywords, # USB-флешки
}

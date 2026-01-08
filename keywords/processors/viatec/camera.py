"""
Процесор для категорій камер відеоспостереження (Viatec).
"""

import re
from typing import List, Dict, Optional, Set
import logging

from keywords.processors.viatec.base import ViatecBaseProcessor
from keywords.core.models import CategoryConfig, Spec, MAX_MODEL_KEYWORDS, MAX_SPEC_KEYWORDS
from keywords.core.helpers import SpecAccessor, KeywordBucket
from keywords.utils.name_helpers import extract_brand, extract_model, extract_technology, check_wifi
from keywords.utils.spec_helpers import is_spec_allowed


class CameraProcessor(ViatecBaseProcessor):
    """Процесор для камер відеоспостереження"""

    def generate(
        self,
        name: str,
        config: CategoryConfig,
        specs: List[Spec],
        lang: str,
        manufacturers: Dict[str, str],
        logger: logging.Logger
    ) -> List[str]:
        """Генерація ключових слів для камер"""
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
            name, base, accessor, lang, allowed, specs, manufacturers
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
        specs: List[Spec],
        manufacturers: Dict[str, str]
    ) -> List[str]:
        """Генерація ключових слів з характеристик"""
        bucket = KeywordBucket(MAX_SPEC_KEYWORDS)

        # Витягуємо характеристики
        brand = self._get_brand_from_accessor(accessor, allowed, manufacturers)
        resolution = self._get_resolution(accessor, allowed)
        focal = self._get_focal_length(accessor, allowed)
        tech = self._get_camera_technology(accessor, allowed)
        camera_type = self._get_camera_type(accessor, lang, allowed)
        ip_rating = self._get_ip_rating(accessor, lang, allowed)
        has_wifi = check_wifi(name, specs)
        wide_angle = self._check_wide_angle(accessor, allowed)
        has_microphone = self._check_microphone(accessor, allowed)
        has_sd_card = self._check_sd_card(accessor, allowed)

        # Бренд (з маленької букви)
        if brand:
            bucket.add(f"{base} {brand.lower()}")
            bucket.add(f"{brand.lower()} {base}")

        # Тип камери
        if camera_type:
            bucket.add(f"{camera_type} {base}")

        # Роздільна здатність
        if resolution:
            bucket.add(f"{base} {resolution}")
            bucket.add(f"{resolution} {base}")
            bucket.add(f"{base} {resolution.replace('mp', 'мп')}")
            bucket.add(f"{resolution.replace('mp', 'мп')} {base}")

        # Технологія (з варіантами)
        if tech:
            tech_lower = tech.lower()
            if tech_lower == "ip":
                bucket.add(f"ip {base}")
                bucket.add(f"айпи {base}")
                bucket.add(f"сетевая {base}" if lang == "ru" else f"мережева {base}")
            elif tech_lower in ["tvi", "cvi", "ahd"]:
                bucket.add(f"{tech_lower} {base}")

        # Фокусна відстань
        if focal:
            bucket.add(f"{base} {focal}")

        # IP рейтинг
        if ip_rating:
            bucket.add(f"{ip_rating} {base}")

        # WiFi
        if has_wifi:
            bucket.add("wifi видеокамера" if lang == "ru" else "wifi відеокамера")

        # Ширококутна
        if wide_angle:
            bucket.add("широкоугольная видеокамера" if lang == "ru" else "ширококутна відеокамера")

        # З мікрофоном
        if has_microphone:
            bucket.add("видеокамера с микрофоном" if lang == "ru" else "відеокамера з мікрофоном")

        # З записом (SD-карта)
        if has_sd_card:
            bucket.add("видеокамера с записью" if lang == "ru" else "відеокамера з записом")

        return bucket.to_list()

    def _get_brand_from_accessor(
        self,
        accessor: SpecAccessor,
        allowed: Set[str],
        manufacturers: Dict[str, str]
    ) -> Optional[str]:
        """Витягування бренду з характеристик"""
        if not is_spec_allowed("Виробник", allowed):
            return None

        return accessor.value("Виробник")

    def _get_resolution(
        self,
        accessor: SpecAccessor,
        allowed: Set[str]
    ) -> Optional[str]:
        """Витягування роздільної здатності"""
        if not is_spec_allowed("Роздільна здатність (Мп)", allowed):
            return None

        value = accessor.value("Роздільна здатність (Мп)")
        if not value:
            return None

        # Варіант 1: Цифра + mp/мп ("2mp", "5 мп")
        match = re.search(r"(\d+)\s*[mм][pр]", value, re.I)
        if match:
            return f"{match.group(1)}mp"

        # Варіант 2: Просто цифра ("2", "5")
        match = re.search(r"^(\d+)$", value.strip())
        if match:
            return f"{match.group(1)}mp"

        return None

    def _get_focal_length(
        self,
        accessor: SpecAccessor,
        allowed: Set[str]
    ) -> Optional[str]:
        """Витягування фокусної відстані"""
        if not is_spec_allowed("Фокусна відстань", allowed):
            return None

        value = accessor.value("Фокусна відстань")
        if not value:
            return None

        # Якщо значення вже містить мм, витягуємо число
        match = re.search(r"(\d+(?:\.\d+)?)\s*(мм|mm)", value, re.I)
        if match:
            return f"{match.group(1)} мм"

        # Якщо просто число - додаємо "мм"
        match = re.search(r"^(\d+(?:\.\d+)?)$", value.strip())
        if match:
            return f"{match.group(1)} мм"

        return None

    def _get_camera_technology(
        self,
        accessor: SpecAccessor,
        allowed: Set[str]
    ) -> Optional[str]:
        """Витягування технології камери"""
        if not is_spec_allowed("Тип камери", allowed):
            return None

        value = accessor.value("Тип камери")
        if not value:
            return None

        value_lower = value.lower()
        from keywords.core.models import CAMERA_TECHNOLOGIES
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
        """Витягування типу камери (купольна/поворотна)"""
        if not is_spec_allowed("Форм-фактор", allowed):
            return None

        mapping = {
            "ru": {
                "купол": "купольная",
                "ptz": "поворотная",
                "поворот": "поворотная",
                "циліндр": "цилиндрическая",
                "куб": "кубическая",
            },
            "ua": {
                "купол": "купольна",
                "ptz": "поворотна",
                "поворот": "поворотна",
                "циліндр": "циліндрична",
                "куб": "кубічна",
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
        """Перевірка захисту IP65-68"""
        if not is_spec_allowed("Захист обладнання від води і пилу IP", allowed):
            return None

        value = accessor.value("Захист обладнання від води і пилу IP")
        if not value:
            return None

        if re.search(r"ip6[5-8]", value, re.I):
            return "уличная" if lang == "ru" else "вулична"

        return None

    def _check_wide_angle(self, accessor: SpecAccessor, allowed: Set[str]) -> bool:
        """Перевірка широкого кута огляду (>90 градусів)"""
        if not is_spec_allowed("Кут огляду по горизонталі", allowed):
            return False

        value = accessor.value("Кут огляду по горизонталі")
        if not value:
            return False

        # Шукаємо число
        match = re.search(r"(\d+)", value)
        if match:
            angle = int(match.group(1))
            return angle > 90

        return False

    def _check_microphone(self, accessor: SpecAccessor, allowed: Set[str]) -> bool:
        """Перевірка наявності вбудованого мікрофону"""
        if not is_spec_allowed("Вбудований мікрофон", allowed):
            return False

        value = accessor.value("Вбудований мікрофон")
        if not value:
            return False

        return value.lower() in {"так", "yes", "true", "є", "вбудований"}

    def _check_sd_card(self, accessor: SpecAccessor, allowed: Set[str]) -> bool:
        """Перевірка наявності порту для SD-карти"""
        if not is_spec_allowed("Порт для SD-карти", allowed):
            return False

        value = accessor.value("Порт для SD-карти")
        if not value:
            return False

        return value.lower() in {"так", "yes", "true", "є"}

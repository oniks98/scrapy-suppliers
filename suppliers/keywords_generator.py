"""
Генератор ключових слів для товарів.
Створює 15-22 фраз у 3 блоки:
- Блок 1: Ключі по моделі (5-6 шт.) - методом перестановки з назви
- Блок 2: Ключі по характеристикам (5-10 шт.) - базове слово + характеристики товару
- Блок 3: Універсальні фрази категорії (5-6 шт.) - з CSV
"""
import re
import csv
from pathlib import Path
from itertools import permutations, combinations


class ProductKeywordsGenerator:
    """Генератор ключових слів для товарів з 3 блоками"""
    
    def __init__(self, keywords_csv_path, logger=None):
        self.logger = logger
        self.keywords_mapping = {}
        self._load_keywords_mapping(keywords_csv_path)
    
    def _load_keywords_mapping(self, csv_path):
        """Завантаження мапінгу категорій → ключові слова
        
        ОНОВЛЕННЯ: allowed_specs тепер містить ПОРТАЛЬНІ НАЗВИ характеристик
        Приклад: "Роздільна здатність (Мп), Тип камери, Фокусна відстань"
        """
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                for row in reader:
                    category_id = row.get('Ідентифікатор_підрозділу', '').strip()
                    if category_id:
                        # Обробка universal_phrases - видаляємо лапки та парсимо
                        universal_ru_raw = row.get('universal_phrases_ru', '').strip()
                        universal_ua_raw = row.get('universal_phrases_ua', '').strip()
                        
                        # Видаляємо зовнішні лапки якщо є
                        if universal_ru_raw.startswith('"') and universal_ru_raw.endswith('"'):
                            universal_ru_raw = universal_ru_raw[1:-1]
                        if universal_ua_raw.startswith('"') and universal_ua_raw.endswith('"'):
                            universal_ua_raw = universal_ua_raw[1:-1]
                        
                        # Обробка allowed_specs - видаляємо лапки та створюємо set з портальних назв
                        allowed_specs_raw = row.get('allowed_specs', '').strip()
                        if allowed_specs_raw.startswith('"') and allowed_specs_raw.endswith('"'):
                            allowed_specs_raw = allowed_specs_raw[1:-1]
                        
                        # Нормалізуємо портальні назви (toLowerCase для порівняння)
                        allowed_specs_normalized = set([
                            s.strip().lower() for s in allowed_specs_raw.split(',') if s.strip()
                        ])
                        
                        self.keywords_mapping[category_id] = {
                            'universal_phrases_ru': [p.strip() for p in universal_ru_raw.split(',') if p.strip()],
                            'universal_phrases_ua': [p.strip() for p in universal_ua_raw.split(',') if p.strip()],
                            'base_keyword_ru': row.get('base_keyword_ru', '').strip(),
                            'base_keyword_ua': row.get('base_keyword_ua', '').strip(),
                            'allowed_specs': allowed_specs_normalized,  # Тепер це портальні назви
                        }
            if self.logger:
                self.logger.info(f"✅ Завантажено ключові слова для {len(self.keywords_mapping)} категорій")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Помилка завантаження ключових слів: {e}")
    
    def generate_keywords(self, product_name, category_id, specs_list, lang='ru'):
        """
        Генерація 15-22 ключових слів у 3 блоки
        
        Args:
            product_name: Назва товару
            category_id: ID категорії
            specs_list: Список характеристик товару
            lang: Мова ('ru' або 'ua')
        
        Returns:
            str: Ключові слова через кому
        """
        block1 = self._generate_block1_model_keys(product_name, lang)
        block2 = self._generate_block2_characteristics_keys(category_id, specs_list, lang, product_name)
        block3 = self._generate_block3_universal_phrases(category_id, lang)
        
        # Debug: показуємо блоки
        if self.logger:
            self.logger.debug(f"📦 Блок 1 ({len(block1)} шт): {block1}")
            self.logger.debug(f"📦 Блок 2 ({len(block2)} шт): {block2}")
            self.logger.debug(f"📦 Блок 3 ({len(block3)} шт): {block3}")
        
        # Об'єднання та дедуплікація
        all_keywords = block1 + block2 + block3
        unique_keywords = []
        seen = set()
        
        for kw in all_keywords:
            kw_lower = kw.lower().strip()
            if kw_lower not in seen:
                unique_keywords.append(kw)
                seen.add(kw_lower)
        
        # Обмеження до 15-22 фраз
        final_keywords = unique_keywords[:22]
        
        if self.logger:
            self.logger.debug(f"🔑 Згенеровано {len(final_keywords)} ключових слів для '{product_name[:50]}...'")
        
        return ', '.join(final_keywords)
    
    def _generate_block1_model_keys(self, product_name, lang):
        """
        Блок 1: Ключі по моделі (5-6 шт.)
        Метод перестановки з назви товару
        
        Приклад: 
        "Hikvision DS-2CE16H0T-ITF 5MP 2.8mm" →
        - DS-2CE16H0T-ITF
        - Hikvision DS-2CE16H0T-ITF
        - Turbo HD Hikvision 5MP
        - Hikvision 2.8mm
        """
        keywords = []
        
        # Витягуємо компоненти
        brand = self._extract_brand(product_name)
        model = self._extract_model(product_name)
        resolution = self._extract_resolution(product_name)
        focal_length = self._extract_focal_length(product_name)
        technology = self._extract_technology(product_name)
        
        # Генеруємо комбінації
        if model:
            keywords.append(model)
            
            if brand:
                keywords.append(f"{brand} {model}")
            
            if resolution:
                keywords.append(f"{model} {resolution}")
                if brand:
                    keywords.append(f"{brand} {model} {resolution}")
        
        if brand and resolution:
            keywords.append(f"{brand} {resolution}")
            if technology:
                keywords.append(f"{technology} {brand} {resolution}")
        
        if brand and focal_length:
            keywords.append(f"{brand} {focal_length}")
        
        # Обмеження до 5-15 фраз
        return keywords[:15]
    
    def _generate_block2_characteristics_keys(self, category_id, specs_list, lang, product_name=''):
        """
        Блок 2: Ключі по характеристикам (5-15 шт.)
        Базове слово + характеристики з товару
        
        ФІЛЬТРАЦІЯ: Використовує тільки дозволені ПОРТАЛЬНІ ХАРАКТЕРИСТИКИ з allowed_specs
        
        Приклад allowed_specs: "Роздільна здатність (Мп), Тип камери, Фокусна відстань"
        Результат:
        - камера 5mp  (якщо "Роздільна здатність (Мп)" у allowed_specs)
        - tvi камера  (якщо "Тип камери" у allowed_specs)
        - камера 2.8 мм  (якщо "Фокусна відстань" у allowed_specs)
        """
        keywords = []
        
        category_data = self.keywords_mapping.get(category_id, {})
        base_keyword = category_data.get(f'base_keyword_{lang}', '')
        allowed_specs = category_data.get('allowed_specs', set())  # Тепер це set портальних назв (lowercase)
        
        if not base_keyword:
            if self.logger:
                self.logger.warning(f"⚠️  Не знайдено base_keyword_{lang} для категорії {category_id}")
            return keywords
        
        # Якщо allowed_specs не вказано - використовуємо ВСІ характеристики (зворотна сумісність)
        if not allowed_specs:
            if self.logger:
                self.logger.debug(f"ℹ️  allowed_specs порожній для категорії {category_id}, використовуємо ВСІ характеристики")
        
        # Витягуємо характеристики (ТІЛЬКИ якщо вони дозволені)
        # Перевіряємо портальні назви в specs_list
        resolution = self._extract_resolution_from_specs(specs_list, allowed_specs)
        technology = self._extract_technology_from_specs(specs_list, allowed_specs)
        focal_length = self._extract_focal_length_from_specs(specs_list, allowed_specs)
        view_angle = self._extract_view_angle_from_specs(specs_list, lang, allowed_specs)
        ip_rating = self._extract_ip_rating_from_specs(specs_list, lang, allowed_specs)
        has_wifi = (self._extract_wifi_from_name(product_name) or self._extract_wifi_from_specs(specs_list)) if self._is_spec_allowed('Тип камери', allowed_specs) else False
        brand_from_specs = self._extract_brand_from_specs(specs_list, allowed_specs)  # Виробник з характеристик
        brand_from_name = self._extract_brand(product_name)  # Виробник з назви (запасний варіант)
        brand = brand_from_specs or brand_from_name  # Пріоритет характеристикам
        features = self._extract_features_from_specs(specs_list, lang, allowed_specs)
        
        # Debug логування
        if self.logger:
            self.logger.debug(f"🔍 Блок 2 - base_keyword: '{base_keyword}', resolution: '{resolution}', tech: '{technology}', focal: '{focal_length}', view_angle: '{view_angle}', ip_rating: '{ip_rating}', has_wifi: {has_wifi}, brand: '{brand}', features: {features}")
        
        # Генеруємо комбінації: базове слово + характеристика
        
        # ВИРОБНИК (дуже важливо для SEO!)
        if brand:
            keywords.append(f"{base_keyword} {brand}")
            keywords.append(f"{brand} {base_keyword}")
        
        if resolution:
            keywords.append(f"{base_keyword} {resolution}")
            # Додаємо варіант з кириличним "мп"
            resolution_cyrillic = resolution.replace('mp', 'мп')
            if resolution_cyrillic != resolution:
                keywords.append(f"{base_keyword} {resolution_cyrillic}")
        
        if technology:
            keywords.append(f"{technology} {base_keyword}")
            # Додаємо варіант з кирилицею для IP
            if technology.lower() == 'ip':
                cyrillic_ip = 'айпи' if lang == 'ru' else 'айпі'
                keywords.append(f"{cyrillic_ip} {base_keyword}")
        
        if focal_length:
            keywords.append(f"{base_keyword} {focal_length}")
        
        if view_angle:
            keywords.append(f"{base_keyword} {view_angle}")
        
        # Додаємо IP-захист (IP65+ = уличная/вулична)
        if ip_rating:
            keywords.append(f"{ip_rating} {base_keyword}")
        
        # Додаємо WiFi
        if has_wifi:
            wifi_mapping = {'ru': ['wifi видеокамера', 'видеокамера wi fi'], 'ua': ['wifi відеокамера', 'відеокамера wi fi']}
            for wifi_kw in wifi_mapping.get(lang, [])[:1]:  # Тільки 1 варіант
                keywords.append(wifi_kw)
        
        for feature in features[:2]:  # Максимум 2 фічі
            keywords.append(f"{base_keyword} {feature}")
        
        if self.logger:
            self.logger.debug(f"🔑 Блок 2 згенеровано: {keywords}")
        
        return keywords[:15]
    
    def _generate_block3_universal_phrases(self, category_id, lang):
        """
        Блок 3: Універсальні фрази категорії (5-6 шт.)
        Просто беремо з CSV
        """
        category_data = self.keywords_mapping.get(category_id, {})
        universal_phrases = category_data.get(f'universal_phrases_{lang}', [])
        return universal_phrases[:6]
    
    # === ДОПОМІЖНІ МЕТОДИ ===
    
    def _is_spec_allowed(self, portal_spec_name, allowed_specs):
        """
        Перевіряє чи дозволена портальна характеристика
        
        Args:
            portal_spec_name: Портальна назва (наприклад, "Роздільна здатність (Мп)")
            allowed_specs: Set дозволених портальних назв (lowercase)
        
        Returns:
            bool: True якщо дозволена або allowed_specs порожній
        """
        if not allowed_specs:
            return True  # Якщо allowed_specs порожній - дозволяємо все
        
        # Нормалізуємо назву для порівняння
        normalized_name = portal_spec_name.lower().strip()
        
        # Перевіряємо точне співпадіння або часткове
        for allowed in allowed_specs:
            if allowed in normalized_name or normalized_name in allowed:
                return True
        return False
    
    # === ДОПОМІЖНІ МЕТОДИ ВИТЯГУВАННЯ ===
    
    def _extract_brand_from_specs(self, specs_list, allowed_specs=None):
        """Витягує виробника з характеристик"""
        if not self._is_spec_allowed('Виробник', allowed_specs):
            return None
        
        brand_names = ['виробник', 'производитель', 'manufacturer', 'brand', 'бренд']
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower()
            if any(name in spec_name for name in brand_names):
                brand = spec.get('value', '').strip()
                if brand:
                    return brand
        return None
    
    def _extract_brand(self, text):
        brands = ['Hikvision', 'Dahua', 'EZVIZ', 'Arny', 'Ajax', 'Uniview', 'Tiandy']
        text_lower = text.lower()
        for brand in brands:
            if brand.lower() in text_lower:
                return brand
        return None
    
    def _extract_model(self, text):
        """Витягує модель (код товару)"""
        # Шаблони моделей: DS-2CE16H0T-ITF, DHI-IPC-HFW1230S, тощо
        patterns = [
            r'\b[A-Z]{2,4}-[A-Z0-9-]+\b',  # DS-2CE16H0T-ITF
            r'\b[A-Z]{3,5}-[A-Z]{3}-[A-Z0-9]+\b',  # DHI-IPC-HFW1230S
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0).upper()
        return None
    
    def _extract_resolution(self, text):
        """Витягує роздільну здатність з назви"""
        patterns = [
            r'\b(\d+)\s*[MМ][PР]\b',  # 5MP, 8MP
            r'\b(\d+)\s*[MМ][PР]x\b',  # 5MPx
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return f"{match.group(1)}MP"
        return None
    
    def _extract_focal_length(self, text):
        """Витягує фокусну відстань"""
        match = re.search(r'\b(\d+\.?\d*)\s*мм\b', text, re.IGNORECASE)
        if match:
            return f"{match.group(1)} мм"
        return None
    
    def _extract_technology(self, text):
        """Витягує технологію (TVI, AHD, IP)"""
        technologies = ['Turbo HD', 'HDTVI', 'AHD', 'HDCVI', 'IP']
        text_upper = text.upper()
        for tech in technologies:
            if tech.upper() in text_upper:
                return tech
        return None
    
    def _extract_resolution_from_specs(self, specs_list, allowed_specs=None):
        """
        Витягує роздільну здатність з характеристик
        
        Args:
            specs_list: Список портальних характеристик
            allowed_specs: Set дозволених портальних назв
        
        Returns:
            str: Роздільна здатність (напр. "5mp") або None
        """
        # Перевіряємо чи дозволена ця характеристика
        if not self._is_spec_allowed('Роздільна здатність (Мп)', allowed_specs):
            return None
        
        # Шукаємо портальну характеристику
        priority_names = ['роздільна здатність (мп)', 'разрешение (мп)', 'resolution (mp)']
        for spec in specs_list:
            spec_name = spec.get('name', '').lower()
            if any(name in spec_name for name in priority_names):
                value = spec.get('value', '').strip()
                if value.isdigit():
                    return f"{value}mp"
                match = re.search(r'(\d+)\s*[MМ][PР]', value, re.IGNORECASE)
                if match:
                    return f"{match.group(1)}mp"
        
        # Загальні назви
        resolution_names = ['роздільна здатність', 'разрешение', 'resolution']
        for spec in specs_list:
            spec_name = spec.get('name', '').lower()
            if any(name in spec_name for name in resolution_names):
                value = spec.get('value', '')
                match = re.search(r'(\d+)\s*[MМ][PР]', value, re.IGNORECASE)
                if match:
                    return f"{match.group(1)}mp"
        return None
    
    def _extract_technology_from_specs(self, specs_list, allowed_specs=None):
        """Витягує технологію з характеристик"""
        if not self._is_spec_allowed('Тип камери', allowed_specs):
            return None
        
        tech_names = ['тип камери', 'тип камеры', 'тип сигнала', 'тип сигналу', 'технология', 'технологія', 'signal type']
        technologies = ['tvi', 'ahd', 'cvi', 'ip']
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower()
            spec_value = spec.get('value', '').lower()
            
            if any(name in spec_name for name in tech_names):
                for tech in technologies:
                    if tech in spec_value:
                        return tech
            
            for tech in technologies:
                if tech in spec_name or tech in spec_value:
                    return tech
        return None
    
    def _extract_focal_length_from_specs(self, specs_list, allowed_specs=None):
        """Витягує фокусну відстань з характеристик"""
        if not self._is_spec_allowed('Фокусна відстань', allowed_specs):
            return None
        
        focal_names = ['фокусна відстань', 'фокусное расстояние', 'focal length', 'об\'єктив', 'фокус']
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower()
            spec_value = str(spec.get('value', '')).strip()
            spec_unit = str(spec.get('unit', '')).strip()
            
            if any(name in spec_name for name in focal_names):
                combined = f"{spec_value} {spec_unit}".strip()
                match = re.search(r'(\d+(?:\.\d+)?)\s*(?:мм|mm)', combined, re.IGNORECASE)
                if match:
                    return f"{match.group(1)} мм"
        return None
    
    def _extract_camera_type_from_specs(self, specs_list, lang):
        """Витягує тип камери (купольна, циліндрична, вулична/уличная)"""
        type_names = ['застосування', 'применение', 'тип корпусу', 'тип камеры', 'тип камери', 'camera type', 'форм-фактор', 'application']
        
        type_mapping = {
            'ru': {
                'купол': 'купольная',
                'цилиндр': 'цилиндрическая',
                'bullet': 'цилиндрическая',
                'dome': 'купольная',
                'ptz': 'ptz',
                'улиц': 'уличная',
                'внутр': 'внутренняя',
                # Додаємо українські варіанти для крос-мовного розпізнавання
                'вулич': 'уличная',
                'внутріш': 'внутренняя',
            },
            'ua': {
                'купол': 'купольна',
                'циліндр': 'циліндрична',
                'bullet': 'циліндрична',
                'dome': 'купольна',
                'ptz': 'ptz',
                'вулич': 'вулична',
                'внутр': 'внутрішня',
                # Додаємо російські варіанти для крос-мовного розпізнавання
                'улиц': 'вулична',
                'внутренн': 'внутрішня',
            }
        }
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower()
            spec_value = spec.get('value', '').lower()
            
            if any(name in spec_name for name in type_names):
                for key, camera_type in type_mapping[lang].items():
                    if key in spec_value:
                        return camera_type
        return None
    
    def _extract_features_from_specs(self, specs_list, lang, allowed_specs=None):
        """Витягує додаткові фічі (ІЧ-підсвітка, запис, тощо)
        
        ФІЛЬТРАЦІЯ: Тільки фічі з allowed_specs
        """
        if allowed_specs is None:
            allowed_specs = set()
        
        features = []
        
        feature_mapping = {
            'ru': {
                'ік-підсвічування': 'с ик-подсветкой',
                'ик-подсветка': 'с ик-подсветкой',
                'іч': 'с ик-подсветкой',
                'ик': 'с ик-подсветкой',
                'ir': 'с ик-подсветкой',
                'запись': 'с записью',
                'wifi': 'wifi',
                'wi-fi': 'wifi',
                # Видалено 'улиц' та 'вулич' - це покривається через ip_rating
                'h.265': 'h.265',
                'h.264': 'h.264',
            },
            'ua': {
                'ік-підсвічування': 'з іч-підсвіткою',
                'ик-подсветка': 'з іч-підсвіткою',
                'іч': 'з іч-підсвіткою',
                'ик': 'з іч-підсвіткою',
                'ir': 'з іч-підсвіткою',
                'запис': 'із записом',
                'wifi': 'wifi',
                'wi-fi': 'wifi',
                # Видалено 'вулиц' та 'вулич' - це покривається через ip_rating
                'h.265': 'h.265',
                'h.264': 'h.264',
            }
        }
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower()
            spec_value = spec.get('value', '').lower()
            combined_text = f"{spec_name} {spec_value}"
            
            for key, feature in feature_mapping[lang].items():
                if key in combined_text and feature not in features:
                    features.append(feature)
                    break  # Додаємо тільки одну фічу з кожної характеристики
        
        return features
    
    def _extract_view_angle_from_specs(self, specs_list, lang, allowed_specs=None):
        """Витягує кут огляду з характеристик"""
        if not self._is_spec_allowed('Кут огляду по горизонталі', allowed_specs):
            return None
        
        angle_names = [
            'кут огляду', 'угол обзора', 'view angle', 'viewing angle',
            'кут огляду по горизонталі', 'угол обзора по горизонтали',
            'horizontal angle', 'horizontal view'
        ]
        
        angle_mapping = {
            'ru': 'широкоугольная',
            'ua': 'ширококутна'
        }
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower()
            spec_value = spec.get('value', '').lower()
            
            if any(name in spec_name for name in angle_names):
                # Витягуємо число градусів
                match = re.search(r'(\d+(?:\.\d+)?)(?:\s*(?:град|°|degrees?))?', spec_value, re.IGNORECASE)
                if match:
                    angle = float(match.group(1))
                    # Якщо кут більше 90° - це широкий кут
                    if angle >= 90:
                        return angle_mapping.get(lang, '')
        return None
    
    def _extract_ip_rating_from_specs(self, specs_list, lang, allowed_specs=None):
        """Витягує IP-захист (якщо IP65+, то уличная)"""
        # Перевіряємо декілька можливих назв
        if not (self._is_spec_allowed('Ступінь захисту', allowed_specs) or 
                self._is_spec_allowed('Захист', allowed_specs)):
            return None
        
        ip_names = [
            'захист', 'защита', 'protection', 'ip rating',
            'захист обладнання', 'защита оборудования',
            'степень защиты', 'ступінь захисту'
        ]
        
        outdoor_mapping = {
            'ru': 'уличная',
            'ua': 'вулична'
        }
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower()
            spec_value = spec.get('value', '').upper()
            
            if any(name in spec_name for name in ip_names):
                # Шукаємо IP65, IP66, IP67, IP68
                match = re.search(r'IP\s*6[5-8]', spec_value, re.IGNORECASE)
                if match:
                    return outdoor_mapping.get(lang, '')
        return None
    
    def _extract_wifi_from_name(self, product_name):
        """Перевіряє наявність WiFi в назві товару"""
        wifi_patterns = ['wifi', 'wi-fi', 'wi fi']
        name_lower = product_name.lower()
        return any(pattern in name_lower for pattern in wifi_patterns)
    
    def _extract_wifi_from_specs(self, specs_list):
        """Перевіряє наявність WiFi в характеристиках"""
        wifi_names = ['wifi', 'wi-fi', 'беспроводной', 'бездротовий', 'wireless']
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower()
            spec_value = spec.get('value', '').lower()
            combined = f"{spec_name} {spec_value}"
            
            for wifi_name in wifi_names:
                if wifi_name in combined:
                    return True
        return False

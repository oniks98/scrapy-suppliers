"""
Тестовий скрипт для перевірки налаштувань проекту
"""
import sys
from pathlib import Path

print("=" * 80)
print("🔍 ПЕРЕВІРКА НАЛАШТУВАНЬ ПРОЕКТУ")
print("=" * 80)

# Додаємо кореневу директорію до sys.path
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

print(f"\n📁 Коренева директорія проекту: {PROJECT_ROOT}")
print(f"✅ Додано до sys.path: {str(PROJECT_ROOT) in sys.path}")

print("\n🔎 sys.path:")
for i, path in enumerate(sys.path, 1):
    print(f"   {i}. {path}")

print("\n" + "=" * 80)
print("🧪 ТЕСТ 1: Імпорт модуля suppliers")
print("=" * 80)

try:
    import suppliers
    print("✅ Модуль 'suppliers' успішно імпортовано")
    print(f"   Шлях: {suppliers.__file__}")
except ImportError as e:
    print(f"❌ Помилка імпорту: {e}")
    sys.exit(1)

print("\n" + "=" * 80)
print("🧪 ТЕСТ 2: Імпорт settings")
print("=" * 80)

try:
    from suppliers import settings
    print("✅ Модуль 'suppliers.settings' успішно імпортовано")
    print(f"   BOT_NAME: {settings.BOT_NAME}")
except ImportError as e:
    print(f"❌ Помилка імпорту: {e}")
    sys.exit(1)

print("\n" + "=" * 80)
print("🧪 ТЕСТ 3: Імпорт items")
print("=" * 80)

try:
    from suppliers import items
    print("✅ Модуль 'suppliers.items' успішно імпортовано")
except ImportError as e:
    print(f"❌ Помилка імпорту: {e}")
    sys.exit(1)

print("\n" + "=" * 80)
print("🧪 ТЕСТ 4: Імпорт pipelines")
print("=" * 80)

try:
    from suppliers import pipelines
    print("✅ Модуль 'suppliers.pipelines' успішно імпортовано")
except ImportError as e:
    print(f"❌ Помилка імпорту: {e}")
    sys.exit(1)

print("\n" + "=" * 80)
print("🧪 ТЕСТ 5: Перевірка структури проекту")
print("=" * 80)

required_dirs = [
    PROJECT_ROOT / "suppliers",
    PROJECT_ROOT / "suppliers" / "spiders",
    PROJECT_ROOT / "scripts",
    PROJECT_ROOT / "data",
    PROJECT_ROOT / "output",
]

required_files = [
    PROJECT_ROOT / "suppliers" / "__init__.py",
    PROJECT_ROOT / "suppliers" / "settings.py",
    PROJECT_ROOT / "suppliers" / "items.py",
    PROJECT_ROOT / "suppliers" / "pipelines.py",
    PROJECT_ROOT / "scrapy.cfg",
]

all_ok = True

print("\n📂 Перевірка директорій:")
for dir_path in required_dirs:
    exists = dir_path.exists()
    status = "✅" if exists else "❌"
    print(f"   {status} {dir_path.relative_to(PROJECT_ROOT)}")
    if not exists:
        all_ok = False

print("\n📄 Перевірка файлів:")
for file_path in required_files:
    exists = file_path.exists()
    status = "✅" if exists else "❌"
    print(f"   {status} {file_path.relative_to(PROJECT_ROOT)}")
    if not exists:
        all_ok = False

print("\n" + "=" * 80)
if all_ok:
    print("✅ ВСІ ТЕСТИ ПРОЙДЕНО УСПІШНО!")
    print("🚀 Проект готовий до роботи")
else:
    print("❌ ДЕЯКІ ТЕСТИ НЕ ПРОЙДЕНО")
    print("⚠️ Перевірте структуру проекту")
print("=" * 80 + "\n")

sys.exit(0 if all_ok else 1)

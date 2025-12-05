"""
УЛЬТРА-ЧИСТИЙ запуск Scrapy з автоматичною трансформацією в PROM версію
Використання: 
  python ultra_clean_run.py eserver_retail
  python ultra_clean_run.py eserver_retail --no-transform  (без трансформації)
"""
import sys
import os
import warnings
import subprocess
from pathlib import Path


# Встановлюємо environment variables ДО всього
os.environ['SCRAPY_SETTINGS_MODULE'] = 'suppliers.settings'

# Ігноруємо DeprecationWarning
warnings.filterwarnings('ignore', category=DeprecationWarning)


# Патчимо configure_logging ДО імпорту Scrapy
def silent_configure_logging(settings=None, install_root_handler=True):
    """Наша версія configure_logging яка приховує технічні логи"""
    import logging
    
    # Базова конфігурація
    logging.basicConfig(
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )
    
    # Відключаємо всі технічні Scrapy логери
    noisy = [
        'scrapy.utils.log',
        'scrapy.addons',
        'scrapy.middleware', 
        'scrapy.crawler',
        'scrapy.core.engine',
        'scrapy.core.scraper',
        'scrapy.extensions',
        'scrapy.statscollectors',
        'twisted',
        'filelock',
        'py.warnings',
    ]
    
    for name in noisy:
        logging.getLogger(name).setLevel(logging.ERROR)


# Патчимо Scrapy ДО імпорту
import scrapy.utils.log
scrapy.utils.log.configure_logging = silent_configure_logging

# Тепер імпортуємо решту
from scrapy.cmdline import execute


def run_transformation():
    """Запускає трансформацію retail → prom"""
    print("\n" + "="*80)
    print("🔄 ЗАПУСК АВТОМАТИЧНОЇ ТРАНСФОРМАЦІЇ: RETAIL → PROM")
    print("="*80 + "\n")
    
    # Шлях до скрипта трансформації
    base_dir = Path(__file__).parent
    transform_script = base_dir / "scripts" / "transform_retail_to_prom.py"
    
    if not transform_script.exists():
        print(f"❌ ПОМИЛКА: Скрипт трансформації не знайдено: {transform_script}")
        return False
    
    # Запускаємо скрипт трансформації
    try:
        result = subprocess.run(
            [sys.executable, str(transform_script)],
            capture_output=False,
            text=True,
            check=True
        )
        
        print("\n" + "="*80)
        print("✅ ТРАНСФОРМАЦІЯ ЗАВЕРШЕНА УСПІШНО")
        print("="*80 + "\n")
        return True
        
    except subprocess.CalledProcessError as e:
        print("\n" + "="*80)
        print(f"❌ ПОМИЛКА ПРИ ТРАНСФОРМАЦІЇ: {e}")
        print("="*80 + "\n")
        return False
    except Exception as e:
        print("\n" + "="*80)
        print(f"❌ НЕОЧІКУВАНА ПОМИЛКА: {e}")
        print("="*80 + "\n")
        return False


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("❌ Використання: python ultra_clean_run.py <spider_name> [--no-transform]")
        print("📝 Приклад: python ultra_clean_run.py eserver_retail")
        print("📝 Без трансформації: python ultra_clean_run.py eserver_retail --no-transform")
        sys.exit(1)
    
    spider_name = sys.argv[1]
    
    # Перевіряємо чи потрібна трансформація
    skip_transform = "--no-transform" in sys.argv
    
    # Автоматична трансформація тільки для eserver_retail
    should_transform = (spider_name == "eserver_retail" and not skip_transform)
    
    print("\n" + "="*80)
    print(f"🚀 ЗАПУСК SPIDER: {spider_name}")
    if should_transform:
        print("📦 Режим: З автоматичною трансформацією RETAIL → PROM")
    else:
        print("📦 Режим: Без трансформації")
    print("="*80 + "\n")
    
    # Запускаємо spider
    sys.argv = ['scrapy', 'crawl', spider_name]
    
    try:
        execute()
        spider_success = True
    except SystemExit as e:
        spider_success = (e.code == 0)
    except Exception as e:
        print(f"❌ ПОМИЛКА ПРИ ЗАПУСКУ SPIDER: {e}")
        spider_success = False
    
    # Якщо spider успішний і потрібна трансформація
    if spider_success and should_transform:
        transform_success = run_transformation()
        
        if transform_success:
            print("\n" + "🎉"*40)
            print("✅ ПОВНИЙ ЦИКЛ ЗАВЕРШЕНО УСПІШНО:")
            print("   1. ✅ Парсинг eserver_retail.csv")
            print("   2. ✅ Трансформація в eserver_prom.csv")
            print("🎉"*40 + "\n")
            sys.exit(0)
        else:
            print("\n⚠️ Spider виконано успішно, але трансформація не вдалася")
            sys.exit(1)
    elif spider_success:
        print("\n✅ Spider виконано успішно")
        sys.exit(0)
    else:
        print("\n❌ Spider завершився з помилками")
        sys.exit(1)

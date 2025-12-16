#!/bin/bash
# Запуск паука з фільтрацією виводу - показуємо тільки логи паука
# Використання: ./scripts/run_clean.sh viatec_retail

if [ -z "$1" ]; then
    echo "❌ Використання: ./scripts/run_clean.sh <spider_name>"
    echo "📝 Приклад: ./scripts/run_clean.sh viatec_retail"
    exit 1
fi

# Встановлюємо PYTHONPATH на кореневу директорію проекту
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export PYTHONPATH="$(dirname "$SCRIPT_DIR")"
export SCRAPY_SETTINGS_MODULE=suppliers.settings

# Запускаємо scrapy і фільтруємо вивід - показуємо тільки рядки з ім'ям паука
scrapy crawl "$1" 2>&1 | grep "\[$1\]"

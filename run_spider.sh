#!/bin/bash

# Встановлюємо PYTHONPATH на кореневу директорію проекту
export PYTHONPATH="$(cd "$(dirname "$0")" && pwd)"
export SCRAPY_SETTINGS_MODULE=suppliers.settings

# Запускаємо скрипт з переданими аргументами
python scripts/ultra_clean_run.py "$@"

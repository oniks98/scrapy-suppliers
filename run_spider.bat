@echo off
chcp 65001 >nul

REM Встановлюємо PYTHONPATH на кореневу директорію проекту
set PYTHONPATH=%~dp0
set SCRAPY_SETTINGS_MODULE=suppliers.settings

REM Запускаємо скрипт з переданими аргументами
python scripts\ultra_clean_run.py %*

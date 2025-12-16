@echo off
chcp 65001 >nul
color 0A
title Оновлення товарів

REM Встановлюємо PYTHONPATH на кореневу директорію проекту
set PYTHONPATH=%~dp0

echo.
echo ============================================================
echo          СКРИПТ ОНОВЛЕННЯ ТОВАРІВ
echo ============================================================
echo.

python scripts\update_products.py

echo.
echo Натисніть будь-яку клавішу для виходу...
pause >nul

@echo off
REM Чистий запуск Scrapy пауків
REM Використання: run.bat viatec_retail

if "%1"=="" (
    echo Використання: run.bat ^<spider_name^>
    echo Приклад: run.bat viatec_retail
    exit /b 1
)

python clean_run.py %1

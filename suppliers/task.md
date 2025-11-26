     
     в C:\FullStack\Scrapy\suppliers\suppliers\pipelines.py захорджено 200000
     
        # Ініціалізуємо лічильник і статистику
        self.product_counters[output_file] = 200000
        self.stats[output_file] = {
            "count": 0,
            "filtered_no_price": 0,
            "filtered_no_stock": 0,
        }

замени харкод и 
            сделай импорт старта счетчика  из файла C:\FullStack\Scrapy\data\viatec\viatec_counter_product_code.csv
           и для других пауков-поставщиков будут свои стартовіе счетчики например C:\FullStack\Scrapy\data\viatec\eserver_counter_product_code.csv
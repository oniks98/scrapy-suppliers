"""
Тестовый скрипт для проверки селекторов viatec.ua
Запуск: python test_selectors.py
"""
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


class TestSelectorsSpider(scrapy.Spider):
    name = "test_viatec"
    start_urls = [
        "https://viatec.ua/catalog/cameras/0:0;proizvoditel:hikvision",  # Пример категории
    ]
    
    custom_settings = {
        "LOG_LEVEL": "INFO",
    }
    
    def parse(self, response):
        """Тестируем различные селекторы"""
        self.logger.info("=" * 80)
        self.logger.info("🔍 ТЕСТ СЕЛЕКТОРОВ")
        self.logger.info("=" * 80)
        
        # Тест 1: Ссылки на товары
        self.logger.info("\n1️⃣ Тест: Ссылки на товары")
        
        # Вариант 1
        links_v1 = response.css("a.product-link::attr(href)").getall()
        self.logger.info(f"   Вариант 1 (a.product-link): {len(links_v1)} ссылок")
        if links_v1:
            self.logger.info(f"   Пример: {links_v1[0]}")
        
        # Вариант 2
        links_v2 = response.css("div.product a::attr(href)").getall()
        self.logger.info(f"   Вариант 2 (div.product a): {len(links_v2)} ссылок")
        if links_v2:
            self.logger.info(f"   Пример: {links_v2[0]}")
        
        # Вариант 3 - универсальный
        links_v3 = response.css("a[href*='/product/']::attr(href)").getall()
        self.logger.info(f"   Вариант 3 (a[href*='/product/']): {len(links_v3)} ссылок")
        if links_v3:
            self.logger.info(f"   Пример: {links_v3[0]}")
        
        # Тест 2: Названия товаров на странице категории
        self.logger.info("\n2️⃣ Тест: Названия товаров")
        
        names_v1 = response.css("h3.product-title::text").getall()
        self.logger.info(f"   Вариант 1 (h3.product-title): {len(names_v1)} названий")
        if names_v1:
            self.logger.info(f"   Пример: {names_v1[0]}")
        
        names_v2 = response.css("div.product-name::text").getall()
        self.logger.info(f"   Вариант 2 (div.product-name): {len(names_v2)} названий")
        if names_v2:
            self.logger.info(f"   Пример: {names_v2[0]}")
        
        # Тест 3: Цены
        self.logger.info("\n3️⃣ Тест: Цены")
        
        prices_v1 = response.css("span.price::text").getall()
        self.logger.info(f"   Вариант 1 (span.price): {len(prices_v1)} цен")
        if prices_v1:
            self.logger.info(f"   Пример: {prices_v1[0]}")
        
        prices_v2 = response.css("div.product-price::text").getall()
        self.logger.info(f"   Вариант 2 (div.product-price): {len(prices_v2)} цен")
        if prices_v2:
            self.logger.info(f"   Пример: {prices_v2[0]}")
        
        # Тест 4: Пагинация
        self.logger.info("\n4️⃣ Тест: Пагинация")
        
        next_v1 = response.css("a.next-page::attr(href)").get()
        self.logger.info(f"   Вариант 1 (a.next-page): {next_v1}")
        
        next_v2 = response.css("a[rel='next']::attr(href)").get()
        self.logger.info(f"   Вариант 2 (a[rel='next']): {next_v2}")
        
        next_v3 = response.css("li.pagination-next a::attr(href)").get()
        self.logger.info(f"   Вариант 3 (li.pagination-next a): {next_v3}")
        
        # Тест 5: Изображения
        self.logger.info("\n5️⃣ Тест: Изображения")
        
        images_v1 = response.css("img.product-image::attr(src)").getall()
        self.logger.info(f"   Вариант 1 (img.product-image): {len(images_v1)} изображений")
        if images_v1:
            self.logger.info(f"   Пример: {images_v1[0]}")
        
        images_v2 = response.css("div.product img::attr(src)").getall()
        self.logger.info(f"   Вариант 2 (div.product img): {len(images_v2)} изображений")
        if images_v2:
            self.logger.info(f"   Пример: {images_v2[0]}")
        
        self.logger.info("\n" + "=" * 80)
        self.logger.info("✅ Тест завершён")
        self.logger.info("=" * 80)
        
        # Переходим на первый товар для детального теста
        product_link = (links_v1 or links_v2 or links_v3)
        if product_link:
            yield response.follow(product_link[0], callback=self.parse_product)
    
    def parse_product(self, response):
        """Тестируем селекторы на странице товара"""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("🔍 ТЕСТ СЕЛЕКТОРОВ: СТРАНИЦА ТОВАРА")
        self.logger.info(f"URL: {response.url}")
        self.logger.info("=" * 80)
        
        # Название
        self.logger.info("\n1️⃣ Название товара")
        name_v1 = response.css("h1.product-name::text").get()
        self.logger.info(f"   Вариант 1 (h1.product-name): {name_v1}")
        
        name_v2 = response.css("h1::text").get()
        self.logger.info(f"   Вариант 2 (h1): {name_v2}")
        
        # Код товара
        self.logger.info("\n2️⃣ Код товара")
        code_v1 = response.css("span.product-code::text").get()
        self.logger.info(f"   Вариант 1 (span.product-code): {code_v1}")
        
        code_v2 = response.css("div.article::text").get()
        self.logger.info(f"   Вариант 2 (div.article): {code_v2}")
        
        # Цена
        self.logger.info("\n3️⃣ Цена")
        price_v1 = response.css("span.price::text").get()
        self.logger.info(f"   Вариант 1 (span.price): {price_v1}")
        
        price_v2 = response.css("div.product-price span::text").get()
        self.logger.info(f"   Вариант 2 (div.product-price span): {price_v2}")
        
        # Описание
        self.logger.info("\n4️⃣ Описание")
        desc_v1 = response.css("div.description::text").getall()
        self.logger.info(f"   Вариант 1 (div.description): {len(desc_v1)} блоков")
        
        desc_v2 = response.css("div.product-description p::text").getall()
        self.logger.info(f"   Вариант 2 (div.product-description p): {len(desc_v2)} блоков")
        
        # Изображения
        self.logger.info("\n5️⃣ Изображения")
        images_v1 = response.css("img.product-image::attr(src)").getall()
        self.logger.info(f"   Вариант 1 (img.product-image): {len(images_v1)} изображений")
        
        images_v2 = response.css("div.gallery img::attr(src)").getall()
        self.logger.info(f"   Вариант 2 (div.gallery img): {len(images_v2)} изображений")
        
        # Наличие
        self.logger.info("\n6️⃣ Наличие")
        avail_v1 = response.css("span.availability::text").get()
        self.logger.info(f"   Вариант 1 (span.availability): {avail_v1}")
        
        avail_v2 = response.css("div.stock-status::text").get()
        self.logger.info(f"   Вариант 2 (div.stock-status): {avail_v2}")
        
        # Производитель
        self.logger.info("\n7️⃣ Производитель")
        manuf_v1 = response.css("span.manufacturer::text").get()
        self.logger.info(f"   Вариант 1 (span.manufacturer): {manuf_v1}")
        
        manuf_v2 = response.css("div.brand a::text").get()
        self.logger.info(f"   Вариант 2 (div.brand a): {manuf_v2}")
        
        # Характеристики
        self.logger.info("\n8️⃣ Характеристики")
        specs_v1 = response.css("table.specifications tr")
        self.logger.info(f"   Вариант 1 (table.specifications tr): {len(specs_v1)} характеристик")
        
        specs_v2 = response.css("div.specs-table div.spec-row")
        self.logger.info(f"   Вариант 2 (div.specs-table div.spec-row): {len(specs_v2)} характеристик")
        
        if specs_v1:
            for i, row in enumerate(specs_v1[:3], 1):
                name = row.css("td:first-child::text").get()
                value = row.css("td:last-child::text").get()
                self.logger.info(f"      {i}. {name}: {value}")
        
        self.logger.info("\n" + "=" * 80)
        self.logger.info("✅ Тест страницы товара завершён")
        self.logger.info("=" * 80)


if __name__ == "__main__":
    print("\n🚀 Запуск тестирования селекторов viatec.ua\n")
    
    process = CrawlerProcess(get_project_settings())
    process.crawl(TestSelectorsSpider)
    process.start()

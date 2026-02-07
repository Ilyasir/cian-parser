CONCURRENT_TASKS: int = 3  # максимальное количество одновременных задач парсинга(вкладок браузера)
MAX_PAGES_TO_PARSE: int = 60 # максимальное количество страниц с объявлениями для парсинга в каждой категории
HEADLESS: bool = True # режим запуска браузера (безголовый или с интерфейсом)
CAPCHA_BLOCK_TEXT: str = "text=Кажется, у вас включён VPN" # текст, по которому определяется блокировка/капча

# ссылки на категории квартир ( 0 - студии, 1 - однушки, 2 - двушки, и т.д.)
URLS: list[str] = [
    # Студии
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=11000000&object_type%5B0%5D=1&offer_type=flat&region=1&room9=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&minprice=11000000&object_type%5B0%5D=1&offer_type=flat&region=1&room9=1&sort=creation_date_desc",
    # 1 комн.
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=11500000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=13500000&minprice=11500000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=16000000&minprice=13500000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=21500000&minprice=16000000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&minprice=21500000&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&sort=creation_date_desc",
    # 2 комн.
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=14000000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=16400000&minprice=14000000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=18800000&minprice=16400000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=22000000&minprice=18800000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=25800000&minprice=22000000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=32000000&minprice=25800000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=44500000&minprice=32000000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&minprice=44500000&object_type%5B0%5D=1&offer_type=flat&region=1&room2=1&sort=creation_date_desc",
    # 3 комн.
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=19000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=24000000&minprice=19000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=29000000&minprice=24000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=37000000&minprice=29000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=47000000&minprice=37000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=64000000&minprice=47000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=100000000&minprice=64000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&minprice=100000000&object_type%5B0%5D=1&offer_type=flat&region=1&room3=1&sort=creation_date_desc",
    # 4 комн.
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=64000000&object_type%5B0%5D=1&offer_type=flat&region=1&room4=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&maxprice=120000000&minprice=64000000&object_type%5B0%5D=1&offer_type=flat&region=1&room4=1&sort=creation_date_desc",
    "https://www.cian.ru/cat.php?currency=2&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&minprice=120000000&object_type%5B0%5D=1&offer_type=flat&region=1&room4=1&sort=creation_date_desc",
    # 5 комн.
    "https://www.cian.ru/cat.php?deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&object_type%5B0%5D=1&offer_type=flat&region=1&room5=1&sort=creation_date_desc",
    # 6 комн.
    "https://www.cian.ru/cat.php?deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&object_type%5B0%5D=1&offer_type=flat&region=1&room6=1&sort=creation_date_desc",
    # Своб. планировка
    "https://www.cian.ru/cat.php?deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&object_type%5B0%5D=1&offer_type=flat&region=1&room7=1&sort=creation_date_desc",
]

# Заголовки для маскировки под реальный браузер
DEFAULT_HEADERS: dict[str, str] = {
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}
# Список User-Agent для рандомизации запросов
USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Windows NT 10.0; rv:115.0) Gecko/20100101 Firefox/115.0",

    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 15.1; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:128.0) Gecko/20100101 Firefox/128.0",
    
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (X11; Linux i686; rv:130.0) Gecko/20100101 Firefox/130.0",
    
    "Mozilla/5.0 (X11; Debian; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
]

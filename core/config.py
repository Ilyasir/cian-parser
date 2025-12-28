import os
from pathlib import Path


CONCURRENT_TASKS: int = 7  # максимальное количество одновременных задач парсинга(вкладок браузера)
MAX_PAGES_TO_PARSE: int = 5 # максимальное количество страниц с объявлениями для парсинга в каждой категории
PAGE_TIMEOUT: int = 30000 # время ожидания загрузки страницы (в миллисекундах)
MIN_SLEEP: float = 2.0 # минимальная задержка между запросами к страницам (в секундах)
MAX_SLEEP: float = 5.0 # максимальная задержка между запросами к страницам (в секундах)

CAPCHA_BLOCK_TEXT: str = "text=Кажется, у вас включён VPN" # текст, по которому определяется блокировка/капча

# ссылки на категории квартир ( 0 - студии, 1 - однушки, 2 - двушки, и т.д.)
URLS: list[dict[str, str | int]] = [
    {"url": "https://www.cian.ru/kupit-kvartiru-studiu-vtorichka/", "rooms": 0},
    {"url": "https://www.cian.ru/kupit-1-komnatnuyu-kvartiru-vtorichka/", "rooms": 1},
    {"url": "https://www.cian.ru/kupit-2-komnatnuyu-kvartiru-vtorichka/", "rooms": 2},
    {"url": "https://www.cian.ru/kupit-3-komnatnuyu-kvartiru-vtorichka/", "rooms": 3},
    {"url": "https://www.cian.ru/kupit-4-komnatnuyu-kvartiru-vtorichka/", "rooms": 4},
]

HEADLESS: bool = True # режим запуска браузера (безголовый или с интерфейсом)
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


BASE_DIR = Path(__file__).resolve().parent.parent
# Папка для данных
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
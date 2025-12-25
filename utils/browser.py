import asyncio
import random
from playwright.async_api import Page
from core.logger import setup_logger
from core import config

logger = setup_logger()


async def block_heavy_resources(page: Page):
    """Отключает загрузку тяжелых ресурсов(картинок и стилей) для экономии трафика и скорости."""
    def route_handler(route):

        if route.request.resource_type in ["image", "font", "media", "stylesheet"]:
            return route.abort()
        return route.continue_()

    await page.route("**/*", route_handler)


async def collect_links_flats(urls_data: list[dict], page: Page, max_page: int) -> list[tuple[str, int]]:
    """
    Проходит по страницам категорий, собирает ссылки на квартиры 
    и возвращает список кортежей (ссылка, количество комнат).
    """
    links_and_rooms: list[tuple[str, int]] = []
    # Проход по категориям квартир
    for item in urls_data:
        url = item["url"]
        rooms_count = item["rooms"]

        logger.info(f"Открываю категорию: {url} (Количество комнат: {rooms_count})")

        await page.goto(url, wait_until="domcontentloaded", timeout=config.PAGE_TIMEOUT)
        # Проход по страницам категории
        for page_num in range(max_page):
            # Собрать карточки квартир на текущей странице
            cards = page.locator("article:has([data-mark='OfferTitle'])")
            total_cards = await cards.count()

            logger.info(f"Страница {page_num + 1}: ссылок найдено: {total_cards}")
            # Сбор ссылок с карточек
            for i in range(total_cards):
                card = cards.nth(i)
                link = await card.locator("a[href]").first.get_attribute("href")
            
                links_and_rooms.append((link, rooms_count))

            # Переход к следующей странице, если она есть
            if page_num + 1 < max_page:
                next_button = page.locator("nav[data-name='Pagination']").locator("a").filter(has_text="Дальше")

                if await next_button.count() > 0:
                    await next_button.click()

                    await page.wait_for_load_state("domcontentloaded")
                    await asyncio.sleep(random.uniform(1.0, 2.0))  # небольшая пауза после клика
                else:
                    logger.info("Кнопка 'Дальше' не найдена, перехожу к следующей категории.")
                    break

    unique_links = list(set(links_and_rooms)) # убрать дубликаты
    logger.info(f"Сбор ссылок завершен. Всего уникальных ссылок: {len(unique_links)}")
    return unique_links
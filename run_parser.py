import asyncio
import json
import time
import random
from playwright.async_api import async_playwright
from core import config
from parsers.cian import parse_flat_page
from core.logger import setup_logger
from utils.browser import block_heavy_resources, collect_links_flats

logger = setup_logger()
semaphore = asyncio.Semaphore(config.CONCURRENT_TASKS)


async def scrape_flat_page(browser, link: str, rooms: int) -> dict | None:
    """Парсинг одной страницы квартиры."""
    async with semaphore:
        context = await browser.new_context(
            user_agent=random.choice(config.USER_AGENTS),
            extra_http_headers=config.DEFAULT_HEADERS
        )
        page = await context.new_page()
        await block_heavy_resources(page)

        try:
            # небольшая случайная задержка перед загрузкой страницы
            await asyncio.sleep(random.uniform(config.MIN_SLEEP, config.MAX_SLEEP))
            await page.goto(link, wait_until="domcontentloaded", timeout=30000)

            if await page.locator(config.CAPCHA_BLOCK_TEXT).count() > 0: # проверить на капчу/блокировку
                logger.warning(f"Обнаружена блокировка/капча на: {link}")
                return None

            flat = await parse_flat_page(page, link, rooms)

            # проверить наличие обязательных полей (цена и заголовок)
            if not flat.get("price") or not flat.get("title"):
                logger.warning(f"Пропускаю объявление {link}: отсутствует цена или заголовок")
                return None
            
            return flat

        except Exception as e:
            logger.error(f"Ошибка при парсинге {link}: {e}")
            return None
        
        finally:
            await page.close()
            await context.close()


async def track_progress(tasks: list) -> list[dict]:
    """Выполняет задачи и логирует прогресс в реальном времени."""
    parsed_count = 0
    failed_count = 0
    total = len(tasks)
    results = []

    for coro in asyncio.as_completed(tasks):
        result = await coro
        results.append(result)
        
        if result:
            parsed_count += 1
        else:
            failed_count += 1

        logger.info(
            f"Прогресс: {parsed_count + failed_count}/{total} | OK: {parsed_count} | FAIL: {failed_count}")

    return [r for r in results if r] # убрать пропуски квартир


async def main() -> list[dict]:
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=config.HEADLESS)

        context_for_links = await browser.new_context(
                user_agent=random.choice(config.USER_AGENTS))
        page_for_links = await context_for_links.new_page()

        logger.info("Начинаю сбор ссылок...")
        links = await collect_links_flats(config.URLS, page_for_links, config.MAX_PAGES_TO_PARSE)  # собрать ссылки на квартиры
        
        await page_for_links.close()
        await context_for_links.close()

        if not links:
            logger.error("Ссылки не найдены!")
            await browser.close()
            return []

        tasks = [
            scrape_flat_page(browser, link, rooms)
            for link, rooms in links
        ]

        flats = await track_progress(tasks)

        await browser.close()
        return flats


if __name__ == "__main__":
    start_time = time.time()

    try:
        flats = asyncio.run(main())

        with open(config.OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(flats, f, ensure_ascii=False, indent=4)

        duration = time.time() - start_time

        logger.info(f"Весь процесс завершен. Спарсено: {len(flats)} за {duration:.2f} сек")
        logger.info(f"Результаты: {config.OUTPUT_FILE}")

    except KeyboardInterrupt:
        logger.info("Парсер остановлен вручную.")
        
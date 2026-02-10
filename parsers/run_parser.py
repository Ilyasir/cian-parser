import asyncio
import os
import random
import time
from datetime import datetime

from bs4 import BeautifulSoup
from parser.core import config_parser
from parser.core.logger import setup_logger
from parser.utils.browser import block_heavy_resources, click_next_page, extract_cian_id
from parser.utils.files import save_to_jsonl
from parser.utils.s3_client import upload_file_to_s3
from playwright.async_api import async_playwright

logger = setup_logger()


async def collect_flats_from_url(browser, flat_ids: set, url: str, filename: str) -> None:
    """Собирает данные о квартирах с одного URL и сохраняет их в jsonl файл
    Параметры:
    - browser: экземпляр браузера Playwright
    - flat_ids: множество уже собранных id квартир для избежания дубликатов
    - url: URL страницы для парсинга
    - filename: имя файла для сохранения результатов"""
    context = await browser.new_context(
        user_agent=random.choice(config_parser.USER_AGENTS), extra_http_headers=config_parser.DEFAULT_HEADERS
    )
    page = await context.new_page()
    await block_heavy_resources(page)

    await page.goto(url, wait_until="domcontentloaded", timeout=20000)
    # чекаем капчу или блокировку
    if await page.locator(config_parser.CAPCHA_BLOCK_TEXT).count() > 0:
        logger.error(f"❌ БЛОКИРОВКА ИЛИ КАПЧА: {url}")
        raise Exception(f"Капча на {url}")
    # парсим все страницы, пока есть кнопка пагинации
    for page_num in range(config_parser.MAX_PAGES_TO_PARSE):
        # получаем html страницы и парсим его через bs4
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")

        # находим все карточки с квартирами на странице
        cards = soup.find_all("article", {"data-name": "CardComponent"})

        logger.info(f"🔎 Квартир спарсено - {len(flat_ids)}. Обрабатываю страницу {page_num + 1}. URL: {url}")
        # проходим по каждой карточке и извлекаем данные
        for card in cards:
            try:
                link_el = card.find("a", href=True)
                if not link_el:  # без ссылки нет смысла парсить карточку, скип
                    continue

                link = link_el["href"]
                cian_id = extract_cian_id(link)
                # скип, если уже парсили квартиру с таким id
                if cian_id in flat_ids:
                    continue
                # цена
                price_el = card.find("span", {"data-mark": "MainPrice"})
                price_text = price_el.get_text() if price_el else None
                # заголовок
                title_el = card.find("span", {"data-mark": "OfferSubtitle"}) or card.find(
                    "span", {"data-mark": "OfferTitle"}
                )
                title = title_el.get_text() if title_el else None
                # фулл адрес
                geo_labels = card.find_all("a", {"data-name": "GeoLabel"})
                all_geo_texts = [g.get_text() for g in geo_labels]
                address = ", ".join(all_geo_texts)
                # инфа о метро, если есть
                metro_container = card.find("div", {"data-name": "SpecialGeo"})
                metro = None
                if metro_container:
                    metro = metro_container.get_text()
                # описание обьявления, если есть
                desc_el = card.find("div", {"data-name": "Description"})
                description = desc_el.get_text(strip=True) if desc_el else None

                flat_data = {
                    "id": cian_id,
                    "link": link,
                    "title": title,
                    "price": price_text,
                    "address": address,
                    "metro": metro,
                    "parsed_at": datetime.now().isoformat(),
                    "description": description,
                }

                # сохраняем в файл и добавляем id в множество
                await save_to_jsonl(flat_data, filename)
                flat_ids.add(cian_id)

            except Exception as e:
                logger.error(f"❌ Ошибка при парсинге URL: {url}. На странице: {page_num + 1}. {e}")
                continue
        # кликаем по кнопке "Дальше" для перехода на следующую страницу
        if page_num + 1 < config_parser.MAX_PAGES_TO_PARSE:
            success = await click_next_page(page, "nav[data-name='Pagination'] a")
            if not success:
                logger.warning(f"⚠️ Не нашел кнопку 'Дальше' на странице {page_num + 1}")
                break

    logger.info(f"✅ Завершен сбор с URL: {url}")
    await context.close()  # когда спарчили URL, закрываем вкладку браузера


async def main():
    os.makedirs("data", exist_ok=True)

    env_date = os.getenv("EXECUTION_DATE")
    if env_date:
        start_time_dt = datetime.strptime(env_date, "%Y-%m-%d")
        logger.info(f"Использую дату из Airflow: {env_date}")
    else:
        start_time_dt = datetime.now()
        logger.info(f"Дата из Airflow не передана, использую текущую: {start_time_dt.strftime('%Y-%m-%d')}")

    async with async_playwright() as p:
        start_time = time.time()  # для измерения времени парсинга
        browser = await p.chromium.launch(
            headless=config_parser.HEADLESS,
            # эти аргументы помогают работать в докере(без них много памяти жрет и может не работать)
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu", "--disable-http-cache"],
        )
        # ограничиваем колво одновременно открытых вкладок, чтобы не забанили
        semaphore = asyncio.Semaphore(config_parser.CONCURRENT_TASKS)
        flat_ids = set()

        date_str = start_time_dt.strftime("%Y-%m-%d")
        final_local = f"data/flats_{date_str}.jsonl"
        temp_local = f"data/flats_{date_str}_temp.jsonl"

        if os.path.exists(temp_local):
            os.remove(temp_local)

        async def sem_task(url):
            async with semaphore:
                # рандом задержка перед началом парсинга каждого URL
                await asyncio.sleep(random.uniform(2, 5))
                return await collect_flats_from_url(browser, flat_ids, url, temp_local)

        # запускаем парсинг по всем URL параллельно, но с ограничением по семафору
        tasks = [sem_task(u) for u in config_parser.URLS]
        await asyncio.gather(*tasks, return_exceptions=True)

        await browser.close()
        logger.info(f"✅ Парсинг завершен. Спарсено {len(flat_ids)} квартир за {round(time.time() - start_time)} сек.")

        if os.path.exists(temp_local):
            os.replace(temp_local, final_local)
            # после сохранения локально, загружаем в S3 и удаляем локальный файл
            year = start_time_dt.strftime("%Y")
            month = start_time_dt.strftime("%m")
            day = start_time_dt.strftime("%d")

            s3_object_name = f"cian/year={year}/month={month}/day={day}/flats.jsonl"

            if upload_file_to_s3(final_local, s3_object_name):
                logger.info(f"✅ Данные успешно сохранены в S3: {s3_object_name}")
                if os.path.exists(final_local):
                    os.remove(final_local)
            else:
                logger.error("❌ Ошибка при загрузке в S3. Файл оставлен локально: " + final_local)


if __name__ == "__main__":
    asyncio.run(main())

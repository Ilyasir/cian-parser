import os
import asyncio
import random
import time
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from parser.core import config_parser
from parser.utils.files import save_to_jsonl
from parser.utils.normalize import extract_cian_id
from parser.utils.browser import block_heavy_resources
from parser.core.logger import setup_logger
from parser.utils.s3_client import upload_file_to_s3

logger = setup_logger()


async def collect_flats_from_url(browser, flat_ids: set, url: str, filename):
    context = await browser.new_context(
        user_agent=random.choice(config_parser.USER_AGENTS),
        extra_http_headers=config_parser.DEFAULT_HEADERS
    )
    page = await context.new_page()
    await block_heavy_resources(page)

    
    await page.goto(url, wait_until="domcontentloaded", timeout=20000)
    
    if await page.locator(config_parser.CAPCHA_BLOCK_TEXT).count() > 0:
        logger.error(f"❌ БЛОКИРОВКА ИЛИ КАПЧА: {url}")
        raise Exception(f"Капча на {url}")
        
    for page_num in range(60):
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        cards = soup.find_all("article", {"data-name": "CardComponent"}) 

        logger.info(f"🔎 Квартир спарсено - {len(flat_ids)}. Обрабатываю страницу {page_num + 1}. URL: {url}")
        for card in cards:
            try:
                link_el = card.find("a", href=True)
                if not link_el: continue
                link = link_el['href']
                cian_id = extract_cian_id(link)

                if cian_id in flat_ids: continue
                
                price_el = card.find("span", {"data-mark": "MainPrice"})
                price_text = price_el.get_text() if price_el else None

                title_el = card.find("span", {"data-mark": "OfferSubtitle"}) or \
                            card.find("span", {"data-mark": "OfferTitle"})
                title = title_el.get_text() if title_el else None

                geo_labels = card.find_all("a", {"data-name": "GeoLabel"})
                all_geo_texts = [g.get_text() for g in geo_labels]
                address = ", ".join(all_geo_texts)

                metro_container = card.find("div", {"data-name": "SpecialGeo"})
                metro = None
                if metro_container:
                    metro = metro_container.get_text()

                desc_el = card.find('div', {'data-name': 'Description'})
                description = desc_el.get_text(strip=True) if desc_el else None

                flat_data = {
                    "id": cian_id,
                    "link": link,
                    "title": title,
                    "price": price_text,
                    "address": address,
                    "metro": metro,
                    "parsed_at": datetime.now().isoformat(),
                    "description": description
                }

                await save_to_jsonl(flat_data, filename)
                flat_ids.add(cian_id)

            except Exception as e:
                logger.error(f"❌ Ошибка при парсинге URL: {url}. На странице: {page_num + 1}. {e}")
                continue

        if page_num + 1 < 60:
            try:
                next_button = page.locator("nav[data-name='Pagination'] a").filter(has_text="Дальше")
                if await next_button.count() > 0:
                    await next_button.click()
                    await page.wait_for_load_state("domcontentloaded")
                    await asyncio.sleep(random.uniform(1.5, 2.5))
                else:
                    break
            except Exception:
                logger.warning(f"⚠️ Не нашел кнопку 'Дальше' на странице {page_num + 1}")
                break

    logger.info(f"✅ Завершен сбор с URL: {url}")
    await context.close()


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
        start_time = time.time()
        browser = await p.chromium.launch(
            headless=config_parser.HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage=true", 
            ]
        )

        semaphore = asyncio.Semaphore(3)
        flat_ids = set()

        date_str = start_time_dt.strftime("%Y-%m-%d")

        final_local = f"data/flats_{date_str}.jsonl"
        temp_local = f"data/flats_{date_str}_temp.jsonl"

        # Очищаем временный файл перед стартом
        if os.path.exists(temp_local):
            os.remove(temp_local)

        async def sem_task(url):
            async with semaphore:
                # задержка между стартами потоков
                await asyncio.sleep(random.uniform(1, 4)) 
                return await collect_flats_from_url(
                    browser,
                    flat_ids, 
                    url,
                    temp_local
                )

        tasks = [sem_task(u) for u in config_parser.URLS]
        await asyncio.gather(*tasks, return_exceptions=True)

        await browser.close()
        logger.info(f"✅ Парсинг завершен. Спарсено {len(flat_ids)} квартир за {round(time.time() - start_time)} сек.")

        if os.path.exists(temp_local):
            os.replace(temp_local, final_local)
            
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
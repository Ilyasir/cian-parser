import asyncio
import random
import re

from playwright.async_api import Page


async def click_next_page(page, selector: str, wait_range=(1.5, 2.5)) -> bool:
    """Пытается найти кнопку пагинации и кликнуть по ней"""
    try:
        next_button = page.locator(selector).filter(has_text="Дальше")
        if await next_button.count() > 0:
            await next_button.click()
            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(random.uniform(*wait_range))
            return True
        return False
    except Exception:
        return False


async def block_heavy_resources(page: Page):
    """Отключает загрузку тяжелых ресурсов(картинок, рекламы, аналитики)"""
    blacklisted_domains = [
        "google-analytics.com",
        "googletagmanager.com",
        "yandex.ru/metrika",
        "mc.yandex.ru",
        "facebook.net",
        "ads",
        "pda.cian.ru/dev/metrics",
    ]

    def route_handler(route):
        url = route.request.url.lower()
        resource_type = route.request.resource_type

        if resource_type in ["image", "font", "media", "manifest", "object"]:
            return route.abort()

        if any(domain in url for domain in blacklisted_domains):
            return route.abort()
        return route.continue_()

    await page.route("**/*", route_handler)


def extract_cian_id(url: str) -> int | None:
    """Извлекает ID из ссылки и возвращает его как целое число (int)."""
    if not url:
        return None
    match = re.search(r"/(\d{7,15})", url)

    if match:
        return int(match.group(1))
    return None

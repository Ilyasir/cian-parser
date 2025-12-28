from bs4 import BeautifulSoup
from playwright.async_api import Page
from utils.normalize import ( 
    price_to_int,
    area_to_float,
    underground_minutes_to_int,
    year_built_to_int,
    floors_str_to_tuple
)

def get_title(soup: BeautifulSoup) -> str | None:
    item = soup.find("div", {"data-name": "OfferTitleNew"})
    if item:
        title_h1 = item.find("h1")
        return title_h1.get_text(strip=True) if title_h1 else None
    return None


def get_price(soup: BeautifulSoup) -> int | None:
    item = soup.find("div", {"data-testid": "price-amount"})
    return price_to_int(item.get_text()) if item else None


def get_total_area(soup: BeautifulSoup) -> float | None:
    items = soup.find_all("div", {"data-name": "OfferSummaryInfoItem"})
    for item in items:
        if "Общая площадь" in item.get_text():
            val = item.find_all("p")
            if len(val) > 1:
                return area_to_float(val[1].get_text())
    return None


def get_living_area(soup: BeautifulSoup) -> float | None:
    items = soup.find_all("div", {"data-name": "OfferSummaryInfoItem"})
    for item in items:
        if "Жилая площадь" in item.get_text():
            val = item.find_all("p")
            if len(val) > 1:
                return area_to_float(val[1].get_text())
    return None


def get_floors(soup: BeautifulSoup) -> tuple[int | None, int | None]:
    items = soup.find_all("div", {"data-name": "ObjectFactoidsItem"})
    
    for item in items:
        if "Этаж" in item.get_text():
            floors_str = item.get_text(separator=" ", strip=True)
            return floors_str_to_tuple(floors_str)
            
    return (None, None)


def get_address(soup: BeautifulSoup) -> dict | None:
    container = soup.find("div", {"data-name": "AddressContainer"})
    if not container:
        return None
    
    address_links = container.find_all("a")

    parts = [link.get_text(strip=True).replace(",", "") for link in address_links]
    parts = [p for p in parts if p.lower() != "на карте"]

    if not parts:
        return None

    okrug_mapping = {
        "новомосковский": "НАО",
        "троицкий": "ТАО",
        "центральный": "ЦАО",
        "северный": "САО",
        "южный": "ЮАО",
        "западный": "ЗАО",
        "восточный": "ВАО",
        "юго-западный": "ЮЗАО",
        "юго-восточный": "ЮВАО",
        "северо-западный": "СЗАО",
        "северо-восточный": "СВАО",
        "зеленоградский": "ЗелАО"
    }

    raw_okrug = parts[1] if len(parts) > 1 else None
    cleaned_okrug = raw_okrug

    if raw_okrug:
        low_okrug = raw_okrug.lower()
        for long_name, short_name in okrug_mapping.items():
            if long_name in low_okrug:
                cleaned_okrug = short_name
                break

    address_data = {
        "full_address": ", ".join(parts),
        "city": parts[0] if len(parts) > 0 else None,
        "okrug": cleaned_okrug,
        "district": parts[2] if len(parts) > 2 else None,
        "street": parts[-2] if len(parts) > 1 else None,
        "house": parts[-1] if len(parts) > 0 else None,
    }
    
    return address_data


def get_repair(soup: BeautifulSoup) -> str | None:
    items = soup.find_all("div", {"data-name": "OfferSummaryInfoItem"})
    
    for item in items:
        if "Ремонт" in item.get_text():
            val = item.find_all("p")
            if len(val) > 1:
                return val[1].get_text(strip=True)
                
    return None


def get_description(soup: BeautifulSoup) -> str:
    container = soup.find("div", {"data-id": "content"})
    
    if not container:
        return ""
    
    desc_span = container.find("span")
    
    if desc_span:
        return desc_span.get_text(separator="\n", strip=True)
        
    return ""


def get_year_built(soup: BeautifulSoup) -> int | None:
    items = soup.find_all("div", {"data-name": ["ObjectFactoidsItem", "OfferSummaryInfoItem"]})

    for item in items:
        if "Год постройки" in item.get_text():
            values = item.find_all(["span", "p"])
            if len(values) > 1:
                year_text = values[1].get_text(strip=True)
                return year_built_to_int(year_text)
    
    return None


def get_ceiling_height(soup: BeautifulSoup) -> float | None:
    items = soup.find_all("div", {"data-name": "OfferSummaryInfoItem"})

    for item in items:
        if "Высота потолков" in item.get_text():
            val = item.find_all("p")
            if len(val) > 1:
                return area_to_float(val[1].get_text(strip=True))

    return None


def get_building_type(soup: BeautifulSoup) -> str | None:
    items = soup.find_all("div", {"data-name": "OfferSummaryInfoItem"})

    for item in items:
        if "Тип дома" in item.get_text():
            values = item.find_all("p")
            if len(values) > 1:
                return values[1].get_text(strip=True)
                
    return None


def get_undergrounds(soup: BeautifulSoup) -> list[dict] | None:
    underground_list = []
    ul = soup.find("ul", {"data-name": "UndergroundList"})
    if not ul: return None

    items = ul.find_all("li", {"data-name": "UndergroundItem"})
    
    for item in items:
        link_tag = item.find("a")
        if not link_tag: continue
        name = link_tag.get_text(strip=True)

        all_spans = item.find_all("span")
        
        time_span = None
        for s in all_spans:
            if "мин." in s.get_text():
                time_span = s
                break

        if not time_span:
            underground_list.append({"name": name, "minutes": None, "type": "unknown"})
            continue

        minutes_raw = time_span.get_text(strip=True)
        minutes = underground_minutes_to_int(minutes_raw)

        transport_type = "пешком"
        svg_icon = time_span.find("svg")
        if svg_icon:
            path_data = svg_icon.find("path")

            if path_data and path_data.has_attr("clip-rule"):
                transport_type = "транспорт"
            elif svg_icon.find("g"):
                transport_type = "пешком"

        underground_list.append({
            "name": name,
            "minutes": minutes,
            "type": transport_type
        })
    
    return underground_list if underground_list else None


async def parse_flat_page(page: Page, link: str, rooms: int) -> dict:
    """Парсит страницу квартиры и возвращает словарь с данными."""
    content = await page.content()
    soup = BeautifulSoup(content, 'html.parser')

    floors_tuple = get_floors(soup)
    floor, floors_total = floors_tuple

    # parsed_at = 

    result = {
        "link": link,
        "title": get_title(soup),
        "price": get_price(soup),
        "rooms": rooms,
        "total_area": get_total_area(soup),
        "living_area": get_living_area(soup),
        "address": get_address(soup),
        "floor": floor,
        "floors_total": floors_total,
        "description": get_description(soup),
        "repair": get_repair(soup),
        "year_built": get_year_built(soup),
        "ceiling_height": get_ceiling_height(soup),
        "building_type": get_building_type(soup),
        "underground": get_undergrounds(soup)
    }

    return result
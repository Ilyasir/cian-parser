import re

def _extract_digits(text: str | None) -> str:
    """Извлекает и возвращает все цифры из строки"""
    if not text:
        return ""
    return re.sub(r"\D", "", text)

def price_to_int(text: str | None) -> int | None:
    digits = _extract_digits(text)
    return int(digits) if digits else None

def area_to_float(area_str: str | None) -> float | None:
    if not area_str:
        return None

    cleaned = area_str.replace("\xa0", "").replace("м²", "").replace("м", "").replace(",", ".").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None

def underground_minutes_to_int(minutes_str: str | None) -> int | None:
    digits = _extract_digits(minutes_str)
    return int(digits) if digits else None

def year_built_to_int(year_str: str | None) -> int | None:
    if not year_str:
        return None
    # год. 4 цифры подряд, начинающиеся на 18, 19 или 20
    match = re.search(r"\b(18|19|20)\d{2}\b", year_str)
    return int(match.group()) if match else None

def floors_str_to_tuple(floors_str: str | None) -> tuple[int | None, int | None]:
    if not floors_str:
        return None, None

    text = floors_str.replace("\xa0", " ").lower()

    # если "5 из 10" или "5/10"
    match = re.search(r"(\d+)\s*(?:из|/)\s*(\d+)", text)
    if match:
        return int(match.group(1)), int(match.group(2))

    # и если просто "5" без всех этажей
    digits = _extract_digits(text)
    return (int(digits), None) if digits else (None, None)
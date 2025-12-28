import json

async def save_to_jsonl(item: dict, filename: str):
    """Записывает объект в файл мгновенно."""
    with open(filename, "a", encoding="utf-8") as f:
        line = json.dumps(item, ensure_ascii=False)
        f.write(line + "\n")
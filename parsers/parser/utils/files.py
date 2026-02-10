import json

import aiofiles


async def save_to_jsonl(item: dict, filename: str) -> None:
    line = json.dumps(item, ensure_ascii=False) + "\n"
    async with aiofiles.open(filename, mode="a", encoding="utf-8") as f:
        await f.write(line)

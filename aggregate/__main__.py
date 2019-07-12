import asyncio

from .scraper import Scraper
from . import worker


async def main():
    await worker("redis://localhost", "http://localhost:8100/scrape/all", "http://localhost:8101/add")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
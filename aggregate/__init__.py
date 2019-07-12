import aioredis
import ujson
from dataclasses import dataclass
from datetime import datetime
from typing import List
from .helper import dumps
from .pusher import Document, PusherRequest, Pusher
from .scraper import Scraper, ScrapeResponse


@dataclass
class QueueElement:
    url: str
    indexes: List[str]
    title: str = None
    feed_url: str = None
    starred: bool = False
    read_later: bool = False

    @property
    def from_feed(self) -> bool:
        return True if self.feed_url else False


async def worker(redis_uri, scraper_url: str, pusher_url: str):
    redis = await aioredis.create_connection(redis_uri)
    scraper = Scraper(scraper_url)
    pusher = Pusher(pusher_url)

    while True:
        try:
            _, queue_element = await redis.execute("BLPOP", "queue:items", "0")
            to_process = QueueElement(**ujson.loads(queue_element))
            print(f"[+] Processing {to_process.url}")

            scraped = await scraper.scrape(to_process.url)

            to_add = Document(**{
                "created": datetime.utcnow(),
                "author": scraped.author,
                "title": to_process.title or scraped.title,
                "raw_content": scraped.raw_content,
                "markdown_content": scraped.markdown_content,
                "pdf": scraped.pdf,
                "screenshot": scraped.screenshot,
                "thumbnail": scraped.thumbnail,
                "url": to_process.url,
                "from_feed": to_process.from_feed,
                "feed_url": to_process.feed_url,
                "starred": to_process.starred,
                "read_later": to_process.read_later
            })

            print(f"[+] Done scraping {to_process.url}")

            pushed = dumps(
                PusherRequest(
                    indexes=to_process.indexes,
                    docs=[to_add]
                )
            )

            print(to_process.indexes)

            if pushed:
                print(f"[+] Done processing {to_process.url}")
            else:
                print(f"[+] Error pushing {to_process.url}")

        except Exception as e:
            print(e)
            pass

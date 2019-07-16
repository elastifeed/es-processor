from dataclasses import dataclass
from datetime import datetime
from typing import List
import aioredis
import ujson
from sanic.log import logger
from .pusher import Document, PusherRequest, Pusher
from .scraper import Scraper, ScrapeResponse

@dataclass
class QueueElement:
    """ Queued job element stored in redis """
    url: str
    indexes: List[str]
    categories: List[str]
    title: str = None
    feed_url: str = None
    starred: bool = False
    read_later: bool = False

    @property
    def from_feed(self) -> bool:
        """ Checks if the queued job is originated from an RSS scrape job """
        return True if self.feed_url else False


async def worker(redis_uri: str, scraper_url: str, pusher_url: str):
    """ Async worker which processes URLs (calls API endpoints)

    :param redis_uri: Redis connection URI
    :param scraper_url: Scraper Endpoint for everything
    :param pusher_url: Push gateway for Elasticsearch
    """
    print("[+] Created worker")
    redis = await aioredis.create_connection(redis_uri)
    scraper = Scraper(scraper_url)
    pusher = Pusher(pusher_url)

    while True:
        _, queue_element = await redis.execute("BLPOP", "queue:items", "0")
        try:
            to_process = QueueElement(**ujson.loads(queue_element))
            logger.info(f"[+] Processing {to_process.url}")

            scraped = await scraper.scrape(to_process.url)

            to_add = Document(**{
                "created": datetime.now().astimezone(),
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
                "categories": to_process.categories,
                "starred": to_process.starred,
                "read_later": to_process.read_later
            })

            logger.info(f"[+] Done scraping {to_process.url}")

            pushed = await pusher.push(
                PusherRequest(
                    indexes=to_process.indexes,
                    docs=[to_add]
                )
            )

            if pushed:
                logger.info(f"[+] Done processing {to_process.url}")
            else:
                logger.error(f"[+] Error pushing {to_process.url}")

        except Exception as e:
            logger.exception(e)
            # Add element to queue so we can try it again
            # Background: Request Rate limiting
            await redis.execute("RPUSH", "queue:items", queue_element)
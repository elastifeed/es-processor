import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List
import aioredis
import ujson
import http3
from sanic.log import logger
from .helper import dumps
from .job import QueueElement


@dataclass
class RssResponse:
    """ es-rss response """
    title: str
    description: str
    url: str


class Rss:
    """ Interface to es-rss """

    def __init__(self, rss_url: str):
        self.rss_url = rss_url

    async def get(self, feed_url: str, from_time: datetime) -> List[RssResponse]:
        """ Retrieves posts from an rss feed """
        async with http3.AsyncClient(timeout=None) as client:
            resp = await client.request("POST", self.rss_url, json={
                "url": feed_url,
                "from_time": from_time.isoformat()
            })

            if 200 != resp.status_code:
                return []

            # Parse json array into the correct container
            return [RssResponse(**r) for r in ujson.loads(resp.text) or []]


async def worker(
        redis_uri: str,
        rss_url: str,
        rss_scrape_endpoint: str,
        loop: asyncio.AbstractEventLoop = None):
    """ pulls rss feeds from es-collector and creates a task for each of them

    :param redis_uri: Redis connection endpoint
    :param rss_url: es-rss endpoint
    :param rss_scrape_endpoint: es-collector endpoint
    :param loop: Eventloop on which this task should run
    """

    loop = loop or asyncio.get_event_loop()

    rss = Rss(rss_url)

    redis = await aioredis.create_connection(redis_uri)

    # Get all feeds and subscribed users from endpoint
    async with http3.AsyncClient() as client:
        resp = await client.request("GET", rss_scrape_endpoint)

        if 200 != resp.status_code:
            logger.warning(
                f"Could not retrieve feeds from {rss_scrape_endpoint}")

        parsed = ujson.loads(resp.text)

        for feed in parsed:
            # Get last parsed timestamp out of redis or assume it was scraped on 10.05.2019
            last_time_raw = await redis.execute("get", f"feed:{feed['link']}") or b"2019-05-10T00:00:00.000000+00:00"
            last_time = datetime.fromisoformat(last_time_raw.decode("UTF-8"))
            for job in await rss.get(feed["link"], last_time):

                # Add to queue
                await redis.execute("RPUSH", "queue:items", dumps(QueueElement(
                    url=job.url,
                    title=job.title,
                    feed_url=feed["link"],
                    categories=[],
                    indexes=[f"user-{u['id']}" for u in feed["users"]]  # @TODO
                )))

            # Update timestamp
            await redis.execute("set", f"feed:{feed['link']}", datetime.now(timezone.utc).astimezone().isoformat())
            
            logger.info(f"Parsed feed {feed['link']}")

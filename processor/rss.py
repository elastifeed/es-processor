import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List
import aioredis
from ujson import loads
from sanic import Sanic
import http3
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
            return [RssResponse(**r) for r in loads(resp.text) or []]


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
    # Get last parsed timestamp out of redis or assume it was scraped on 10.05.2019
    #  @TODO
    last_time_raw = await redis.execute("get", "https://rss.golem.de/rss.php?feed=RSS2.0") or b"2019-05-10T00:00:00.000000+00:00"
    last_time = datetime.fromisoformat(last_time_raw.decode("UTF-8"))

    #  @TODO
    for job in await rss.get("https://rss.golem.de/rss.php?feed=RSS2.0", last_time):
        # Add task to queue
        await redis.execute("RPUSH", "queue:items", dumps(QueueElement(
            url=job.url,
            title=job.title,
            feed_url="https://rss.golem.de/rss.php?feed=RSS2.0",
            indexes=["dummy_new"]  # @TODO
        )))

    # Update timestamp
    await redis.execute("set", "https://rss.golem.de/rss.php?feed=RSS2.0", datetime.now(timezone.utc).astimezone().isoformat())

import asyncio
import aioredis
from datetime import datetime, timezone
from typing import Coroutine
from .scraper import Scraper
from .helper import dumps
from . import worker, QueueElement
from .rss import Rss


async def every(seconds: int, to_schedule, loop: asyncio.AbstractEventLoop = None):
    """ Schedules a task every n seconds """
    loop = loop or asyncio.get_event_loop()
    while True:
        loop.create_task(to_schedule())
        await asyncio.sleep(seconds)


async def rss_task(loop: asyncio.AbstractEventLoop = None):
    """ pulls rss feeds from es-collector and creates a task for each of them """
    loop = loop or asyncio.get_event_loop()

    rss = Rss("http://localhost:8080/parse")

    # Get last parsed timestamp out of redis or assume it was scraped on 10.05.2019
    redis = await aioredis.create_connection("redis://localhost")
    last_time_raw = await redis.execute("get", "https://rss.golem.de/rss.php?feed=RSS2.0") or b"2019-05-10T00:00:00.000000+00:00"
    last_time = datetime.fromisoformat(last_time_raw.decode("UTF-8"))

    for task in await rss.get("https://rss.golem.de/rss.php?feed=RSS2.0", last_time):
        # Add task to queue
        await redis.execute("RPUSH", "queue:items", dumps(QueueElement(
            url = task.url,
            title = task.title,
            feed_url = "https://rss.golem.de/rss.php?feed=RSS2.0",
            indexes = ["dummy_new"]
        )))
    
    await redis.execute("set", "https://rss.golem.de/rss.php?feed=RSS2.0", datetime.now(timezone.utc).astimezone().isoformat()) # Update timestamp


async def main(loop: asyncio.AbstractEventLoop):

    # Create workers
    for _ in range(4):
        loop.create_task(worker(
            "redis://localhost",
            "http://localhost:8100/scrape/all",
            "http://localhost:9090/add"
        ))
    
    loop.create_task(every(10, rss_task, loop=loop))



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.run_forever()

""" Just a dummy middleware to fill elasticsearch with testing data """
import asyncio
import json
import os
from datetime import datetime, timezone
import logging
import aioredis
import aiohttp


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Get configuration out of environment, if not present, set development paths
ES_RSS_URL = os.environ.get("ES_RSS_URL") or "http://localhost:8001/api/v1/namespaces/elastifeed/services/es-rss-service:80/proxy/parse"
ES_SCRAPER_URL = os.environ.get("ES_SCRAPER_URL") or "http://localhost:8001/api/v1/namespaces/elastifeed/services/es-scraper-service:80/scrape/all"
ES_PUSHER_URL = os.environ.get("ES_PUSHER_URL") or "http://localhost:8001/api/v1/namespaces/elastifeed/services/es-pusher-service:80/proxy/add"
REDIS_URL = os.environ.get("ES_REDIS_URL") or "redis://localhost"

USER = "dummy2"

# A couple of active feeds
FEEDS = [
    "https://rss.golem.de/rss.php?feed=RSS2.0",
    "https://www.heise.de/rss/heise-atom.xml",
    "http://rss.focus.de/fol/XML/rss_folnews.xml"

]


async def get_content(url):
    # Get content
    async with aiohttp.ClientSession() as sess:
        async with sess.post(ES_SCRAPER_URL, json={"url": url}) as resp:
            return await resp.json()


async def job(rss, redis):
    tasks = []
    documents = []

    async def add_content(post):

        scraped = await get_content(post["url"])

        documents.append({
            "created": datetime.now(timezone.utc).astimezone().isoformat(),
            "content": await get_content(post["url"]),
            "url": post["url"],
            "isFromFeed": True,
            "feedUrl": rss,
            "starred": False,
            "read_later": False,
            **{
                "raw_content": scraped["raw_content"],
                "markdown_content": scraped["markdown_content"],
                "pdf": scraped["pdf"],
                "thumbnail": scraped["thumbnail"],
                "screenshot": scraped["screenshot"],
                "author": scraped["author"],
                "title": scraped["title"] or post["title"]
            }
        })

    # Get last parsed timestamp out of redis or assume it was scraped on 10.05.2019
    last_time = await redis.execute("get", rss) or b"2019-05-10T00:00:00.000000+00:00"

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.post(ES_RSS_URL, json={"url": rss, "from_time": last_time.decode("utf-8")}) as resp:
                posts = None

                try:
                    posts = json.loads(await resp.text())
                except:
                    pass

                if posts:
                    for post in posts:
                        tasks.append(asyncio.create_task(add_content(post)))

        for t in tasks:
            await t

        async with aiohttp.ClientSession() as sess:
            async with sess.post(ES_PUSHER_URL, json={"indexes": [ USER, USER + "-DOUBLE_INDEX_TEST" ], "docs": documents}) as resp:
                logger.info(f"Added {len(documents)} to elasticsearch index {USER}")
                logger.debug(await resp.text())

        await redis.execute("set", rss, datetime.now(timezone.utc).astimezone().isoformat()) # Update timestamp

    except Exception as e:
        logger.exception(e)


async def amain():
    redis = await aioredis.create_connection(REDIS_URL)
    tasks = []

    for feed in FEEDS:
        tasks.append(asyncio.create_task(job(feed, redis)))

    for t in tasks:
        await t


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(amain())

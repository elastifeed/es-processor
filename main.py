""" Just a dummy middleware to fill elasticsearch with testing data """
import asyncio
import json
import os
from datetime import datetime, timezone
from logging import getLogger
import aioredis
import aiohttp

logger = getLogger(__name__)

# Get configuration out of environment, if not present, set development paths
ES_RSS_URL = os.environ.get("ES_RSS_URL") or "http://localhost:8001/api/v1/namespaces/elastifeed/services/es-rss-service:80/proxy/parse"
ES_EXTRACTOR_URL = os.environ.get("ES_EXTRACTOR_URL") or "http://localhost:8001/api/v1/namespaces/elastifeed/services/es-extractor-service:80/proxy/mercury/url"
ES_PUSHER_URL = os.environ.get("ES_PUSHER_URL") or "http://localhost:8001/api/v1/namespaces/elastifeed/services/es-pusher-service:80/proxy/add"
REDIS_URL = os.environ.get("ES_REDIS_URL") or "redis://localhost"

USER = "dummy"

# A couple of active feeds
FEEDS = [
    "https://rss.golem.de/rss.php?feed=RSS2.0",
    "https://www.heise.de/rss/heise-atom.xml",
    "http://rss.focus.de/fol/XML/rss_folnews.xml"
]


async def get_content(url):
    # Get content
    async with aiohttp.ClientSession() as sess:
        async with sess.post(ES_EXTRACTOR_URL, json={"url": url}) as resp:
            return (await resp.json())["raw_content"]


async def job(rss, redis):
    tasks = []
    documents = []

    async def add_content(post):
        documents.append({
            "created": datetime.now(timezone.utc).astimezone().isoformat(),
            "caption": post["title"],
            "content": await get_content(post["url"]),
            "url": post["url"],
            "isFromFeed": True,
            "feedUrl": rss
        })

    # Get last parsed timestamp out of redis or assume it was scraped on 10.05.2019
    last_time = await redis.execute("get", rss) or b"2019-05-10T00:00:00.000000+00:00"

    async with aiohttp.ClientSession() as sess:
        async with sess.post(ES_RSS_URL, json={"url": rss, "from_time": last_time.decode("utf-8")}) as resp:
            posts = json.loads(await resp.text())

            if posts:
                for post in posts:
                    tasks.append(asyncio.create_task(add_content(post)))

    for t in tasks:
        await t

    async with aiohttp.ClientSession() as sess:
        async with sess.post(ES_PUSHER_URL, json={"index": USER, "docs": documents}) as resp:
            logger.info(f"Added {len(documents)} to elasticsearch index {USER}")
            logger.debug(await resp.text())

    await redis.execute("set", rss, datetime.now(timezone.utc).astimezone().isoformat()) # Update timestamp


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
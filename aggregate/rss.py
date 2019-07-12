from dataclasses import dataclass
from datetime import datetime
from typing import List
from ujson import loads
import http3
from .helper import dumps


class RssResponse:
    """ es-rss response """
    title: str
    description:str
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
            print(resp.text)
            print(loads(resp.text))
            return [RssResponse(**r) for r in loads(resp.text)]
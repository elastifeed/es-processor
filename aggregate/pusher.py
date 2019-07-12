from dataclasses import dataclass
from datetime import datetime
from typing import List
import http3
from .helper import dumps


@dataclass
class Document:
    """ Elastifeed document definition """
    created: datetime
    author: str
    title: str
    raw_content: str
    markdown_content: str
    pdf: str
    screenshot: str
    thumbnail: str
    url: str
    from_feed: bool
    feed_url: str
    starred: bool
    read_later: bool


@dataclass
class PusherRequest:
    indexes: List[str]
    docs: List[Document]

    def add_index(self, *args):
        """  """

class Pusher:

    def __init__(self, pusher_url: str):
        self.pusher_url = pusher_url

    async def push(self, to_push: PusherRequest) -> bool:
        """ Pushes to the elasticsearch gateway """

        print(to_push)

        async with http3.AsyncClient(timeout=None) as client:
            resp = await client.request(
                "POST",
                self.pusher_url,
                json=dumps(to_push)
            )

            print(resp.text)

            return 200 == resp.status_code

        return False
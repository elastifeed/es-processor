import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import List
from ujson import loads
from sanic import Sanic
import http3


@dataclass
class ScrapeResponse:
    """ Response from the scraper """

    author: str
    title: str
    date_published: str
    dek: str
    direction: str
    url: str
    domain: str
    excerpt: str
    raw_content: str
    pdf: str
    screenshot: str
    thumbnail: str
    markdown_content: str
    total_pages: int
    next_page_url: str
    rendered_pages: int
    word_count: int


class Scraper:
    """ Provides an interface to es-scraper """

    def __init__(self, scraper_url: str):
        self.scrape_url = scraper_url

    async def scrape(self, url: str) -> ScrapeResponse:
        """ Queries es-scraper for a specific url """

        async with http3.AsyncClient(timeout=None) as client:
            resp = await client.request("POST", self.scrape_url, json={
                "url": url
            })

            try:
                if 200 != resp.status_code:
                    return None

                # Try to load resposne into the container
                return ScrapeResponse(**loads(resp.text))

            except Exception as e:  # @TODO
                return None

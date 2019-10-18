import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import List
from ujson import loads
from sanic import Sanic
from sanic.log import logger
import http3


@dataclass
class ScrapeResponse:
    """ Response from the scraper """

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
    word_count: int
    # @TODO. Somehow it gets one of these two xD
    rendered_pages: int = 0
    pages_rendered: int = 0



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

                # Try to load response into the container
                resp = loads(resp.text):
                if resp:
                    return ScrapeResponse(**loads(resp.text))
                else:
                    logger.error("scraper did not return the correct result")
                    retur None

            except Exception as e:  # @TODO
                logger.exception(e)
                return None

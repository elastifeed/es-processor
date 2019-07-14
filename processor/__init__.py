import functools
import aioredis
from sanic import Sanic
from sanic.response import json

from . import scheduler
from . import scraper
from . import job
from . import rss
from . import helper


def create_app() -> Sanic:
    """ Creates the sanic application which provides api endpoints and runs
    the job processor
    """

    app = Sanic(__name__)

    # Load env variables for configuration
    app.config.load_environment_vars(prefix="ES_")

    @app.listener("before_server_start")
    async def startup(app, loop):  # pylint: disable-msg=unused-variable
        """ Creates tasks needed for processing jobs """
        # Workers
        for _ in range(app.config.WORKER_COUNT):
            # Create workers for processing queued jobs
            app.add_task(job.worker(
                redis_uri=app.config.REDIS,
                scraper_url=app.config.SCRAPER,
                pusher_url=app.config.PUSHER,
            ))

        # If requested, pull RSS feeds from es-collector and add jobs to the
        # queue
        if app.config.RSS_SCRAPE_INTERVAL > 0:
            app.add_task(scheduler.every(
                app, app.config.RSS_SCRAPE_INTERVAL, functools.partial(
                    rss.worker,
                    redis_uri=app.config.REDIS,
                    rss_url=app.config.RSS,
                    rss_scrape_endpoint=app.config.RSS_SCRAPE,
                )))

        # Redis for the API endpoint
        app.redis = await aioredis.create_connection(app.config.REDIS)

    @app.route("/add", methods=["POST"])
    async def add_job(request):  # pylint: disable-msg=unused-variable
        """ Adds a job to the task queue """
        try:
            await request.app.redis.execute("LPUSH", "queue:items", dumps(
                job.QueueElement(
                    url=request.json["url"],
                    title=request.json.get("title", None),
                    indexes=request.json["indexes"]
                )
            ))
        except:
            pass

    return app

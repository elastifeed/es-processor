from sanic import Sanic
from sanic.response import json

from . import scheduler
from . import scraper
from . import job
from . import rss


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
        if app.config.ES_SCRAPE_INTERVAL > 0:
            app.add_task(scheduler.every(app, app.config.ES_SCRAPE_INTERVAL, rss.worker))

    @app.route("/add")
    async def add_job(request):  # pylint: disable-msg=unused-variable
        """ Adds a job to the task queue """
        return json({"todo": "not implemented"})

    return app

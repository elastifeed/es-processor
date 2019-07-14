import asyncio
from sanic import Sanic

async def every(app: Sanic, seconds: int, to_schedule):
    """ Schedules a task every n seconds """
    while True:
        app.create_task(to_schedule())
        await asyncio.sleep(seconds)
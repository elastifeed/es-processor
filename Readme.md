# Elastifeed job handler

## Environment options:
- `ES_REDIS` Redis uri used by the job processor
- `ES_PUSHER` REST Endpoint for es-pusher
- `ES_SCRAPER` REST Endpoint for es-scraper
- `ES_RSS` REST Endpoint for es-rss
- `ES_RSS_SCRAPE` REST Endpoint (es-collector) for the retrieving RSS feeds and corresponding users
- `ES_RSS_SCRAPE_INTERVAL` Interval on which RSS feeds are retrieved from es-collector (set to 0 if you want to use this instance for processing only)
- `ES_WORKER_COUNT` How many workers to start (for processing queued jobs)

## REST Endpoints

### Adding a URL to the job queue. This is handled with highest priority
```
POST /add
{
    "url": "url to add",
    "title": "if you want to, set a title here",
    "indexes": ["user1", "user2", ...]
}
```
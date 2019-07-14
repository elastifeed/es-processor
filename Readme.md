# Elastifeed job handler

## Environment options:
- `ES_REDIS` Redis uri used by the job processor
- `ES_PUSHER` REST Endpoint for es-pusher
- `ES_SCRAPER` REST Endpoint for es-scraper
- `ES_RSS` REST Endpoint for es-rss
- `ES_RSS_SCRAPE` REST Endpoint (es-collector) for the retrieving RSS feeds and corresponding users
- `ES_RSS_SCRAPE_INTERVAL` Interval on which RSS feeds are retrieved from es-collector
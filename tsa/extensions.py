# -*- coding: utf-8 -*-
"""Extensions module. Each extension is initialized in the app factory located in app.py."""
import redis
from atenvironment import environment
from flask_caching import Cache
from flask_cors import CORS
from raven.contrib.flask import Sentry

cache = Cache()
sentry = Sentry()
cors = CORS()


@environment('REDIS')
def get_redis(redis_cfg):
    """Create a redis connectiion pool."""
    return redis.ConnectionPool().from_url(redis_cfg, charset='utf-8', decode_responses=True)


redis_pool = get_redis()

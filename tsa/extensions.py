# -*- coding: utf-8 -*-
"""Extensions module. Each extension is initialized in the app factory located in app.py."""
from flask_caching import Cache
from flask_cors import CORS
from raven.contrib.flask import Sentry
import redis
from atenvironment import environment

cache = Cache()
sentry = Sentry()
cors = CORS()

@environment('REDIS')
def get_redis(redis_cfg):
    return redis.ConnectionPool().from_url(redis_cfg, charset='utf-8', decode_responses=True)

redis_pool = get_redis()

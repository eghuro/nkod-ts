"""User agent and robots cache."""
import resource
from functools import lru_cache

import redis
import requests
import requests_toolbelt
import reppy.robots

import tsa
from tsa.extensions import redis_pool

soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)

user_agent = requests_toolbelt.user_agent('tsa', tsa.__version__, extras=[('requests', requests.__version__)])
session = requests.Session()
session.headers.update({'User-Agent': user_agent})
a = requests.adapters.HTTPAdapter(pool_connections=1000, pool_maxsize=(soft - 10), max_retries=3, pool_block=True)
session.mount('http://', a)
session.mount('https://', a)


def allowed(iri):
    robots_iri = reppy.robots.Robots.robots_url(iri)
    text = fetch_robots(robots_iri)
    if text is None:
        return True, None, robots_iri
    else:
        robots = reppy.robots.Robots.parse('', text)
        return robots.allowed(iri, user_agent), robots.agent(user_agent).delay, robots_iri


@lru_cache()
def fetch_robots(robots_iri):
    red = redis.Redis(connection_pool=redis_pool)
    key = f'robots_{robots_iri}'
    with red.pipeline() as pipe:
        if red.exists(key):
            return red.get(key)
        else:
            red.sadd('purgeable', key, f'delay_{robots_iri}')
            r = session.get(robots_iri)
            if r.status_code != 200:
                red.set(key, None)
                red.expire(key, 30 * 24 * 60 * 60)
                return None
            else:
                t = r.text
                red.set(key, t)
                red.expire(key, 30 * 24 * 60 * 60)
                return t
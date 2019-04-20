"""Celery tasks for querying."""

import json
from collections import defaultdict

import redis
from atenvironment import environment

from tsa.celery import celery


@celery.task
@environment('REDIS')
def index_distribution_query(iri, redis_url):
    """Query the index and construct related datasets for the iri of a distribution.

    Final result is stored in redis.
    """
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)

    related = defaultdict(set)
    for rel_type in r.smembers(f'reltype:{iri}'):
        for key in r.smembers(f'key:{iri}'):
            related[rel_type].update(r.smembers(f'related:{rel_type!s}:{key!s}'))
        related[rel_type].discard(iri)
        related[rel_type] = list(related[rel_type])

    exp = 30 * 24 * 60 * 60  # 30D
    key = f'distrquery:{iri}'
    with r.pipeline() as pipe:
        pipe.set(key, json.dumps(related))
        pipe.sadd('purgeable', key)
        pipe.expire(key, exp)
        pipe.execute()

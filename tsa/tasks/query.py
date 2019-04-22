"""Celery tasks for querying."""

import json
from collections import defaultdict

import redis

from tsa.celery import celery
from tsa.extensions import redis_pool


@celery.task
def index_distribution_query(iri):
    """Query the index and construct related datasets for the iri of a distribution.

    Final result is stored in redis.
    """
    related = defaultdict(set)
    red = redis.Redis(connection_pool=redis_pool)
    for rel_type in red.smembers(f'reltype:{iri}'):
        for key in red.smembers(f'key:{iri}'):
            related[rel_type].update(red.smembers(f'related:{rel_type!s}:{key!s}'))
        related[rel_type].discard(iri)
        related[rel_type] = list(related[rel_type])

    exp = 30 * 24 * 60 * 60  # 30D
    key = f'distrquery:{iri}'
    with red.pipeline() as pipe:
        pipe.set(key, json.dumps(related))
        pipe.sadd('purgeable', key)
        pipe.expire(key, exp)
        pipe.execute()

"""Celery tasks for querying."""

import json
import logging

import redis
from collections import defaultdict
from atenvironment import environment

from tsa.celery import celery


@celery.task
@environment('REDIS')
def index_query(iri, redis_url): #TODO needs rewriting, probably just distrquery
    """Query the index and construct related datasets for the iri.

    Final result is stored in redis.
    """
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)

    log = logging.getLogger(__name__)

    all_ds = set()
    d = dict()
    for key in r.smembers(f'key:{iri}'):
        related = set(r.smembers(f'related:{key}'))
        log.info(f'Related datasets: {related!s}')
        all_ds.update(related)
        log.info(f'All DS: {all_ds!s}')
        related.discard(iri)
        if len(related) > 0:
            d[key] = list(related)
    e = dict()
    for ds in all_ds:
        e[ds] = list(r.smembers(f'distr:{ds}'))

    exp = 24 * 60 * 60  # 24H
    key = f'query:{iri}'
    with r.pipeline() as pipe:
        pipe.set(key, json.dumps({'related': d, 'distribution': e}))
        pipe.expire(key, exp)
        pipe.execute()
    log.info(f'Calculated result stored under {key}')


@celery.task
@environment('REDIS')
def index_distribution_query(iri, redis_url):
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
        pipe.expire(key, exp)
        pipe.execute()

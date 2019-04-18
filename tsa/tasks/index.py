"""Celery tasks for indexing."""
import logging

import rdflib
import redis
from atenvironment import environment

from tsa.analyzer import AbstractAnalyzer
from tsa.celery import celery
from tsa.endpoint import SparqlGraph


@celery.task
@environment('REDIS')
def index_endpoint(iri, redis_cfg):
    """Index related resources in an endpoint by initializing a SparqlGraph."""
    g = SparqlGraph(iri)
    r = redis.StrictRedis.from_url(redis_cfg)
    return run_indexer(None, g, r)


@celery.task
@environment('REDIS')
def index(iri, format_guess, redis_cfg):
    """Index related resources."""
    log = logging.getLogger(__name__)
    r = redis.StrictRedis.from_url(redis_cfg)
    key = f'data:{iri!s}'

    log.debug('Parsing graph')
    try:
        g = rdflib.ConjunctiveGraph()
        g.parse(data=r.get(key), format=format_guess)
    except rdflib.plugin.PluginException:
        log.debug('Failed to parse graph')
        return 0
    except ValueError:
        log.debug('Failed to parse graph')
        return 0

    return run_indexer(iri, g, r)


def run_indexer(iri, g, r):
    """Get all available analyzers and let them find relationships."""
    log = logging.getLogger(__name__)
    pipe = r.pipeline()
    exp = 30 * 24 * 60 * 60  # 30D

    log.info(f'Indexing {iri}')
    cnt = 0
    for analyzer in [it() for it in AbstractAnalyzer.__subclasses__()]:
        for key, rel_type in analyzer.find_relation(g):
            log.debug(f'Distribution: {iri!s}, relationship type: {rel_type!s}, shared key: {key!s}')
            # pipe.sadd(f'related:{key!s}', iri)
            pipe.sadd(f'related:{rel_type!s}:{key!s}', iri)
            pipe.sadd(f'relationship', rel_type)
            pipe.sadd(f'key:{iri!s}', key)
            pipe.sadd(f'reltype:{iri!s}', rel_type)

            # pipe.expire(f'related:{key!s}', exp)
            pipe.expire(f'related:{rel_type!s}:{key!s}', exp)
            pipe.expire(f'relationship', exp)
            pipe.expire(f'key:{iri!s}', exp)
            pipe.expire(f'reltype:{iri!s}', exp)

            cnt = cnt + 4
            pipe.execute()

    log.info(f'Indexed {cnt!s} records')
    return cnt

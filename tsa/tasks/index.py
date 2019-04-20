"""Celery tasks for indexing."""
import logging

import rdflib
import redis
from atenvironment import environment
from celery import group

from tsa.analyzer import AbstractAnalyzer
from tsa.celery import celery
from tsa.endpoint import SparqlGraph


@celery.task
def index_named(iri, named):
    """Index related resources in an endpoint by initializing a SparqlGraph."""
    tokens = [it.token for it in AbstractAnalyzer.__subclasses__()]
    return group(run_one_named_indexer.si(token, iri, named) for token in tokens).apply_async()


@celery.task
@environment('REDIS')
def run_one_named_indexer(token, iri, named, redis_cfg):
    """Run indexer on the named graph of the endpoint."""
    g = SparqlGraph(iri, named)
    r = redis.StrictRedis.from_url(redis_cfg)
    return run_indexer(token, f'{iri}/{named}', g, r)


@celery.task
def index(iri, format_guess):
    """Index related resources."""
    tokens = [it.token for it in AbstractAnalyzer.__subclasses__()]
    return group(run_one_indexer.si(token, iri, format_guess) for token in tokens).apply_async()


def run_indexer(token, iri, g, r):
    """Get all available analyzers and let them find relationships."""
    log = logging.getLogger(__name__)
    exp = 30 * 24 * 60 * 60  # 30D

    log.info(f'Indexing {iri}')
    cnt = 0
    analyzer = get_analyzer(token)
    for key, rel_type in analyzer.find_relation(g):
        pipe = r.pipeline()
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


def get_analyzer(analyzer_token):
    """Retrieve an analyzer identified by its token."""
    for a in AbstractAnalyzer.__subclasses__():
        if a.token == analyzer_token:
            return a()
    raise ValueError(analyzer_token)


@celery.task
@environment('REDIS')
def run_one_indexer(token, iri, format_guess, redis_cfg):
    """Extract graph from redis and run indexer identified by token on it."""
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

    return run_indexer(token, iri, g, r)

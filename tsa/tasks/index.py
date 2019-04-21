"""Celery tasks for indexing."""
import logging

import rdflib
import redis
from celery import group

from tsa.analyzer import AbstractAnalyzer
from tsa.celery import celery
from tsa.extensions import redis_pool


@celery.task
def index_named(iri, named):
    """Index related resources in an endpoint by initializing a SparqlGraph."""
    tokens = [it.token for it in AbstractAnalyzer.__subclasses__()]
    return group(run_one_named_indexer.si(token, iri, named) for token in tokens).apply_async()


@celery.task
def run_one_named_indexer(token, iri, named):
    """Run indexer on the named graph of the endpoint."""
    g = Graph(store='SPARQLStore', identifier=named)
    g.open(iri)
    red = redis.Redis(connection_pool=redis_pool)
    return run_indexer(token, f'{iri}/{named}', g, red)


@celery.task
def index(iri, format_guess):
    """Index related resources."""
    tokens = [it.token for it in AbstractAnalyzer.__subclasses__()]
    return group(run_one_indexer.si(token, iri, format_guess) for token in tokens).apply_async()


def run_indexer(token, iri, g, red):
    """Get all available analyzers and let them find relationships."""
    log = logging.getLogger(__name__)
    exp = 30 * 24 * 60 * 60  # 30D

    log.info(f'Indexing {iri}')
    cnt = 0
    analyzer = get_analyzer(token)
    for key, rel_type in analyzer.find_relation(g):
        with red.pipeline() as pipe:
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

            pipe.sadd('purgeable', f'related:{rel_type!s}:{key!s}', f'relationship', f'key:{iri!s}', f'reltype:{iri!s}')

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
def run_one_indexer(token, iri, format_guess):
    """Extract graph from redis and run indexer identified by token on it."""
    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)
    key = f'data:{iri!s}'

    log.debug('Parsing graph')
    try:
        g = rdflib.ConjunctiveGraph()
        g.parse(data=red.get(key), format=format_guess)
    except rdflib.plugin.PluginException:
        log.debug('Failed to parse graph')
        return 0
    except ValueError:
        log.debug('Failed to parse graph')
        return 0

    return run_indexer(token, iri, g, red)

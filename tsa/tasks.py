"""Celery tasks invoked from the API endpoints."""
import json
import logging

import rdflib
import redis
import requests
from atenvironment import environment
from celery import group

from tsa.analyzer import Analyzer
from tsa.celery import celery


@celery.task
@environment('ETL', 'VIRTUOSO', 'REDIS')
def system_check(etl, virtuoso, redis_url):
    """Runs an availability test of additional systems.

    Tested are: virtuoso, ETL, redis.
    """
    log = logging.getLogger(__name__)
    log.info('System check started')
    log.info(f'Testing LP-ETL, URL: {etl!s}')
    requests.get(etl).raise_for_status()

    virtuoso_url = f'{virtuoso!s}/sparql'
    log.info(f'Testing virtuoso, URL: {virtuoso_url}')
    requests.get(virtuoso_url).raise_for_status()

    log.info(f'Testing redis, URL: {redis_url}')
    r = redis.StrictRedis().from_url(redis_url)
    r.ping()
    log.info('System check successful')


@celery.task
def hello():
    """Dummy task returning hello world used for testing of Celery."""
    return 'Hello world!'


@celery.task
def analyze(iri, etl=True):
    """Analyze an RDF distribution under given IRI."""
    log = logging.getLogger(__name__)
    log.info(f'Analyzing {iri!s}')
    guess = rdflib.util.guess_format(iri)
    if guess is None:
        r = requests.head(iri)
        r.raise_for_status()
        guess = r.headers.get('content-type')
    log.info(f'Guessing format to be {guess!s}')

    return group(index.si(iri, guess), run_analyzer.si(iri, guess))()


@celery.task(serializer='json')
def run_analyzer(iri, format_guess):
    """Actually run the analyzer."""
    log = logging.getLogger(__name__)

    log.debug('Parsing graph')
    g = rdflib.ConjunctiveGraph()
    g.parse(iri, format=format_guess)

    a = Analyzer(iri)
    return a.analyze(g)


@celery.task
@environment('REDIS')
def index(iri, format_guess, redis_cfg):
    """Index related resources."""
    log = logging.getLogger(__name__)

    log.debug('Parsing graph')
    g = rdflib.ConjunctiveGraph()
    g.parse(iri, format=format_guess)

    r = redis.StrictRedis.from_url(redis_cfg)
    pipe = r.pipeline()
    exp = 60 * 60  # 1H

    analyzer = Analyzer(iri)
    log.info('Indexing ...')
    cnt = 0
    for ds, key in analyzer.find_relation(g):
        log.info(f'Related: {ds} - {key}')
        pipe.sadd(f'related:{key}', ds)
        pipe.sadd(f'key:{ds}', key)
        pipe.sadd(f'ds:{analyzer.distribution}', ds)
        pipe.sadd(f'distr:{ds}', analyzer.distribution)

        pipe.expire(f'related:{key}', exp)
        pipe.expire(f'key:{ds}', exp)
        pipe.expire(f'ds:{analyzer.distribution}', exp)
        pipe.expire(f'distr:{ds}', exp)

        cnt = cnt + 6
    pipe.execute()
    log.info(f'Indexed {cnt!s} records')
    return cnt


@celery.task
@environment('REDIS')
def index_query(iri, redis_url):
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

    exp = 60 * 60  # 1H
    key = f'query:{iri}'
    with r.pipeline() as pipe:
        pipe.set(key, json.dumps({'related': d, 'distribution': e}))
        pipe.expire(key, exp)
        pipe.execute()
    log.info(f'Calculated result stored under {key}')

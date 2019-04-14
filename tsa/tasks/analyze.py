"""Celery tasks for running analyses."""
import json
import logging

import rdflib
import redis
import requests
from atenvironment import environment
from celery import group, chord

from tsa.analyzer import AbstractAnalyzer
from tsa.celery import celery
from tsa.monitor import monitor
from tsa.endpoint import SparqlGraph
from tsa.tasks.index import index, index_endpoint


@celery.task
@environment('REDIS')
def analyze(iri, redis_url):
    """Analyze an RDF distribution under given IRI."""
    log = logging.getLogger(__name__)
    red = redis.StrictRedis().from_url(redis_url)
    key = f'distributions'
    if red.sadd(key, iri) == 0:
        log.debug(f'Skipping distribution as it was recently analyzed: {iri!s}')
        return
    red.expire(key, 30*24*60*60)

    log.info(f'Analyzing {iri!s}')

    if iri.endswith('sparql'):
        log.info(f'Guessing it is a SPARQL endpoint')
        process_endpoint.delay(iri)

    guess = rdflib.util.guess_format(iri)
    try:
        r = requests.get(iri, verify=False, stream=True)
        r.raise_for_status()

        conlen = int(r.headers.get('Content-Length'))
        if conlen > 1024 * 1024 * 1024:
            log.error(f'Skipping as distribution file is too large: {conlen!s}')
            return {}

        if guess is None:
            guess = r.headers.get('content-type')
        monitor.log_format(str(guess))
        log.info(f'Guessing format to be {guess!s}')

        if guess not in ['html', 'hturtle', 'mdata', 'microdata', 'n3',
                          'nquads', 'nt', 'rdfa', 'rdfa1.0', 'rdfa1.1',
                          'trix', 'trig', 'turtle', 'xml', 'json-ld']:
            log.info(f'Skipping this distribution')
            return

        key = f'data:{iri!s}'
        if not red.exists(key):
            chsize = 1024
            for chunk in r.iter_content(chunk_size=chsize):
                if chunk:
                    red.append(key, chunk)
            red.expire(key, 60*60)

        pipeline = group(index.si(iri, guess), run_analyzer.si(iri, guess))
        return pipeline.delay()
    except:
        log.warn(f'Failed to get {iri!s}')
        red.sadd('stat:failed', str(iri))
        return {}


@celery.task
@environment('REDIS')
def run_analyzer(iri, format_guess, redis_cfg):
    """Actually run the analyzer."""
    key = f'data:{iri!s}'
    tokens = [it.token for it in AbstractAnalyzer.__subclasses__()]
    chord(run_one_analyzer.si(token, key, format_guess) for token in tokens)(store_analysis.s(iri, redis_cfg))


@celery.task
def store_analysis(results, iri, redis_cfg):
    red = redis.StrictRedis().from_url(redis_cfg)
    key_result = f'analyze:{iri!s}'
    red.set(key_result, results)
    red.expire(key_result, 30*24*60*60) #30D


@celery.task
@environment('REDIS')
def run_one_analyzer(analyzer_token, key, format_guess, redis_cfg):
    log = logging.getLogger(__name__)
    analyzer = getAnalyzer(analyzer_token)

    red = redis.StrictRedis().from_url(redis_cfg)
    try:
        g = rdflib.ConjunctiveGraph()
        log.debug('Parsing graph')
        g.parse(data=red.get(key), format=format_guess)
        return json.dumps(analyzer.analyze(g))
    except rdflib.plugin.PluginException:
        log.debug('Failed to parse graph')
        return None


def getAnalyzer(analyzer_token):
    for a in AbstractAnalyzer.__subclasses__():
        if a.token == analyzer_token:
            return a()
    raise ValueError(analyzer_token)


@celery.task
@environment('REDIS')
def process_endpoint(iri, redis_cfg):
    red = redis.StrictRedis().from_url(redis_cfg)
    key = f'endpoints'
    if red.sadd(key, iri) > 0:
        # run analyzer and indexer on the endpoint instead of parsing the graph
        group(index_endpoint.si(iri), analyze_endpoint.si(iri)).delay()
        red.expire(key, 30*24*60*60) #30D
    else:
        log = logging.getLogger(__name__)
        log.debug(f'Skipping endpoint as it was recently analyzed: {iri!s}')


@celery.task
@environment('REDIS')
def analyze_endpoint(iri, redis_cfg):
    red = redis.StrictRedis().from_url(redis_cfg)
    sg = SparqlGraph(iri)
    g = sg.extract_graph() #TODO: refactor the analyze method to use SPARQL queries as well
    key = f'analyze:{iri!s}'
    analyzers = [it() for it in AbstractAnalyzer.__subclasses__()]
    red.set(key, json.dumps([a.analyze(g) for a in analyzers]))
    red.expire(key, 30*24*60*60) #30D

"""Celery tasks invoked from the API endpoints."""
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
from tsa.endpoint import SparqlEndpointAnalyzer, SparqlGraph


@celery.task
@environment('REDIS')
def system_check(redis_url):
    """Runs an availability test of additional systems.

    Tested are: redis.
    """
    log = logging.getLogger(__name__)
    log.info('System check started')

    log.info(f'Testing redis, URL: {redis_url}')
    r = redis.StrictRedis().from_url(redis_url)
    r.ping()
    log.info('System check successful')


@celery.task
def hello():
    """Dummy task returning hello world used for testing of Celery."""
    return 'Hello world!'


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


@celery.task
@environment('REDIS')
def index_endpoint(iri, redis_cfg):
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

    return run_indexer(iri, g, r)


def run_indexer(iri, g, r):
    log = logging.getLogger(__name__)
    pipe = r.pipeline()
    exp = 30 * 24 * 60 * 60  # 30D

    log.info('Indexing ...')
    cnt = 0
    for analyzer in [it() for it in AbstractAnalyzer.__subclasses__()]:
        for key, rel_type in analyzer.find_relation(g):
            log.info(f'Distribution: {iri!s}, relationship type: {rel_type!s}, shared key: {key!s}')
            #pipe.sadd(f'related:{key!s}', iri)
            pipe.sadd(f'related:{rel_type!s}:{key!s}', iri)
            pipe.sadd(f'relationship', rel_type)
            pipe.sadd(f'key:{iri!s}', key)
            pipe.sadd(f'reltype:{iri!s}', rel_type)

            #pipe.expire(f'related:{key!s}', exp)
            pipe.expire(f'related:{rel_type!s}:{key!s}', exp)
            pipe.expire(f'relationship', exp)
            pipe.expire(f'key:{iri!s}', exp)
            pipe.expire(f'reltype:{iri!s}', exp)

            cnt = cnt + 4

    pipe.execute()
    log.info(f'Indexed {cnt!s} records')
    return cnt


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

    related = set()
    for ds in r.smembers(f'ds:{iri}'):
        for key in r.smembers(f'key:{ds}'):
            related.update(r.smembers(f'related:{key}'))
    for ds in r.smembers(f'ds:{iri}'):
        related.discard(ds)

    exp = 24 * 60 * 60  # 24H
    key = f'distrquery:{iri}'
    with r.pipeline() as pipe:
        pipe.set(key, json.dumps(list(related)))
        pipe.expire(key, exp)
        pipe.execute()


@celery.task
@environment('REDIS')
def inspect_catalog(iri, redis_cfg):
    """Analyze DCAT datasets listed in the catalog."""
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

    for distribution in g.subjects(RDF.type, rdflib.URIRef('http://www.w3.org/ns/dcat#Distribution')):
        for access in g.objects(d, rdflib.URIRef('http://www.w3.org/ns/dcat#accessURL')):
            log.debug(f'Scheduling analysis of {access!s}')
            analyze.si(str(access)).delay()
    for dataset in g.subjects(RDF.type, rdflib.URIRef('http://rdfs.org/ns/void#Dataset')):
        for dump in g.objects(dataset, rdflib.URIRef('http://rdfs.org/ns/void#dataDump')):
            log.debug(f'Scheduling analysis of {access!s}')
            analyze.si(str(dump)).delay()
        for endpoint in g.objects(dataset, rdflib.URIRef('http://rdfs.org/ns/void#sparqlEndpoint')):
            log.debug(f'Scheduling analysis of {endpoint!s}')
            process_endpoint.si(str(endpoint)).delay()

@celery.task
def inspect_endpoint(iri):
    inspector = SparqlEndpointAnalyzer()
    inspector.peek_endpoint(iri)
    inspect_catalog.delay(iri)

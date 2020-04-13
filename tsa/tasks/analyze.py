"""Celery tasks for running analyses."""
import json
import logging

import rdflib
import redis
import requests
from celery import chord, group
from reppy.robots import Robots
from gevent.timeout import Timeout as GeventTimeout

from tsa.analyzer import AbstractAnalyzer
from tsa.celery import celery
from tsa.compression import SizeException, decompress_7z
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.extensions import redis_pool
from tsa.monitor import monitor
from tsa.robots import robots_cache, session, user_agent
from tsa.tasks.index import index, index_named


class RobotsRetry(BaseException):
    """Exception indicating retry is neeeded because of crawl delay."""

    def __init__(self, delay):
        """Note the delay."""
        self.delay = delay


class Skip(BaseException):
    """Exception indicating we have to skip this distribution."""


# Following 2 tasks are doing the same thing but with different priorities
# This is to speed up known RDF distributions
@celery.task(bind=True)
def analyze_priority(self, iri):
    do_analyze(iri, self, True)

@celery.task(bind=True, time_limit=60)
def analyze(self, iri):
    do_analyze(iri, self)

def do_analyze(iri, task, is_prio=False):
    """Analyze an RDF distribution under given IRI."""
    log = logging.getLogger(__name__)

    key = f'distributions'
    red = redis.Redis(connection_pool=redis_pool)
    if red.sadd(key, iri) == 0:
        log.warn(f'Skipping distribution as it was recently analyzed: {iri!s}')
        return
    red.expire(key, 30 * 24 * 60 * 60)
    red.sadd('purgeable', key)

    log.info(f'Analyzing {iri!s}')

    if iri.endswith('sparql'):
        log.info(f'Guessing it is a SPARQL endpoint')
        return process_endpoint.si(iri).apply_async(queue='low_priority')

    try:
        try:
            r = fetch(iri, log, red)
        except RobotsRetry as e:
            red.srem(key, iri)
            task.retry(countdown=e.delay)
        except requests.exceptions.RequestException as e:
            red.srem(key, iri)
            task.retry(exc=e)
        except GeventTimeout as e:  # this is gevent.timeout.Timeout
            red.srem(key, iri)
            task.retry(exc=e, countdown=e.seconds)

        try:
            test_content_length(iri, r, log)
            guess = guess_format(iri, r, log, red)
        except Skip:
            return {}

        decompress_task = decompress
        if is_prio:
            decompress_task = decompress_prio
        if guess in ['application/x-7z-compressed', 'application/x-zip-compressed', 'application/zip']:
            #delegate this into low_priority task
            return decompress_task.si(iri, 'zip').apply_async(queue='low_priority')
        elif guess in ['application/gzip', 'application/x-gzip']:
            return decompress_task.si(iri, 'gzip').apply_async(queue='low_priority')
        else:
            try:
                store_content(iri, r, red)
            except SizeException:
                log.error(f'File is too large: {iri}')
            else:
                pipeline = group(index.si(iri, guess), run_analyzer.si(iri, guess))
        if is_prio:
            return pipeline.apply_async(queue='high_priority')
        return pipeline.apply_async()
    except:
        log.exception(f'Failed to get {iri!s}')
        red.sadd('stat:failed', str(iri))
        red.sadd('purgeable', 'stat:failed')
        return {}


@celery.task(bind=True, time_limit=60)
def decompress(self, iri, type):
    do_decompress(self, iri, type)

@celery.task(bind=True)
def decompress_prio(self, iri, type):
    do_decompress(self, iri, type, True)

def do_decompress(task, iri, type, is_prio=False):
    #maybe we cannot pass request here, so we have to make a new one
    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)

    try:
        r = fetch(iri, log, red)
    except RobotsRetry as e:
        task.retry(countdown=e.delay)
    except requests.exceptions.RequestException as e:
        task.retry(exc=e)

    def gen_iri_guess(iri, r):
        deco = {
            'zip': decompress_7z,
            'gzip': decompress_gzip
        }
        for sub_iri in deco[type](iri, r, red):
            # TODO: asi zbytecne, archiv neoteviram vicekrat
            if red.sadd(key, sub_iri) == 0:
                log.debug(f'Skipping distribution as it was recently analyzed: {sub_iri!s}')
                continue
            red.expire(key, 30 * 24 * 60 * 60)
            red.sadd('purgeable', key)
            # end_todo
            try:
                guess = guess_format(sub_iri, r, log, red)
            except Skip:
                continue
            if guess is None:
                log.warn(f'Unknown format after decompression: {sub_iri}')
                red.expire(f'data:{sub_iri}', 1)
            else:
                yield sub_iri, guess

    def gen_tasks(iri, r):
        lst = []
        try:
            for x in gen_iri_guess(iri, r): #this does the decompression
                lst.append(x)
        except SizeException as e:
            log.warn(f'One of the files in archive {iri} is too large ({e.name})')
            for sub_iri, _ in lst:
                log.debug(f'Expire {sub_iri}')
                red.expire(f'data:{sub_iri}', 0)
        else:
            for sub_iri, guess in lst:
                yield index.si(sub_iri, guess)
                yield run_analyzer.si(sub_iri, guess)

        g = group([t for t in gen_tasks(iri, r)])
        if is_prio:
            return g.apply_async(queue='high_priority')
        else:
            return g.apply_async()


def test_content_length(iri, r, log):
    """Test content length header if the distribution is not too large."""
    if 'Content-Length' in r.headers.keys():
        conlen = int(r.headers.get('Content-Length'))
        if conlen > 512 * 1024 * 1024:
            # Due to redis limitation
            log.error(f'Skipping {iri} as it is too large: {conlen!s}')
            raise Skip()


def guess_format(iri, r, log, red):
    """
    Guess format of the distribution.

    Skip if not known 5* distribution format.
    """
    guess = rdflib.util.guess_format(iri)
    if guess is None:
        guess = r.headers.get('content-type').split(';')[0]
    monitor.log_format(str(guess))
    log.info(f'Guessing format to be {guess!s}')

    if guess not in ['hturtle', 'mdata', 'microdata', 'n3',
                     'nquads', 'nt', 'rdfa', 'rdfa1.0', 'rdfa1.1',
                     'trix', 'trig', 'turtle', 'xml', 'json-ld',
                     'application/x-7z-compressed', 'application/rdf+xml',
                     'text/xml', 'application/ld+json', 'application/json',
                     'application/gzip', 'application/x-zip-compressed',
                     'application/zip', 'application/rss+xml', 'text/plain',
                     'application/x-gzip']:
        log.info(f'Skipping this distribution')
        red.sadd('stat:skipped', str(iri))
        red.sadd('purgeable', 'stat:skipped')
        raise Skip()

    return guess


def store_content(iri, r, red):
    """Store contents into redis."""
    key = f'data:{iri!s}'
    if not red.exists(key):
        chsize = 1024
        conlen = 0
        with red.pipeline() as pipe:
            for chunk in r.iter_content(chunk_size=chsize):
                if chunk:
                    if len(chunk) + conlen > 512 * 1024 * 1024:
                        pipe.expire(key, 0)
                        pipe.execute()
                        raise SizeException(iri)
                    pipe.append(key, chunk)
                    conlen = conlen + len(chunk)
            pipe.expire(key, 30 * 24 * 60 * 60)  # 30D
            pipe.sadd('purgeable', key)
            pipe.execute()
        monitor.log_size(conlen)


def fetch(iri, log, red):
    """Fetch the distribution. Mind robots.txt."""
    key = f'delay_{Robots.robots_url(iri)!s}'
    red.sadd('purgeable', key)
    if not robots_cache.allowed(iri):
        log.warn(f'Not allowed to fetch {iri!s} as {user_agent!s}')
    else:
        wait = red.ttl(key)
        if wait > 0:
            log.info(f'Analyze {iri} in {wait} because of crawl-delay')
            raise RobotsRetry(wait)

    timeout = 512 * 1024 / 500
    log.debug(f'Timeout {timeout!s} for {iri}')
    r = session.get(iri, stream=True, headers={'User-Agent': user_agent}, timeout=timeout)
    r.raise_for_status()

    delay = robots_cache.get(iri).delay
    if delay is not None:
        log.info(f'Recording crawl-delay of {delay} for {iri}')
        try:
            delay = int(delay)
        except ValueError:
            log.error('Invalid delay value - could not convert to int')
        else:
            try:
                red.set(key, 1)
                red.expire(key, delay)
            except redis.exceptions.ResponseError:
                log.error(f'Failed to set crawl-delay for {iri}: {delay}')
    return r


@celery.task
def run_analyzer(iri, format_guess):
    """Actually run the analyzer."""
    key = f'data:{iri!s}'
    tokens = [it.token for it in AbstractAnalyzer.__subclasses__()]
    chord(run_one_analyzer.si(token, key, format_guess) for token in tokens)(store_analysis.s(iri))


@celery.task
def store_analysis(results, iri):
    """Store results of the analysis in redis."""
    red = redis.Redis(connection_pool=redis_pool)
    key_result = f'analyze:{iri!s}'
    with red.pipeline() as pipe:
        pipe.set(key_result, json.dumps(results))
        pipe.sadd('purgeable', key_result)
        pipe.expire(key_result, 30 * 24 * 60 * 60)  # 30D
        pipe.expire(f'data:{iri!s}', 0)  # trash original content
        pipe.execute()


@celery.task
def run_one_analyzer(analyzer_token, key, format_guess):
    """Run one analyzer identified by its token."""
    log = logging.getLogger(__name__)
    analyzer = get_analyzer(analyzer_token)

    try:
        g = rdflib.ConjunctiveGraph()
        log.debug('Parsing graph')
        red = redis.Redis(connection_pool=redis_pool)
        g.parse(data=red.get(key), format=format_guess)
        return json.dumps(analyzer.analyze(g))
    except rdflib.plugin.PluginException:
        log.debug('Failed to parse graph')
        return None


def get_analyzer(analyzer_token):
    """Retrieve an analyzer identified by its token."""
    for a in AbstractAnalyzer.__subclasses__():
        if a.token == analyzer_token:
            return a()
    raise ValueError(analyzer_token)


@celery.task
def process_endpoint(iri): #Low priority as we are doing scan of all graphs in the endpoint
    log = logging.getLogger(__name__)

    """Index and analyze triples in the endpoint."""
    key = f'endpoints'
    red = redis.Redis(connection_pool=redis_pool)
    red.sadd('purgeable', key)
    if red.sadd(key, iri) > 0:
        a = SparqlEndpointAnalyzer()
        tasks = []
        for g in a.get_graphs_from_endpoint(iri):
            key = f'graphs:{iri}'
            red.sadd('purgeable', key)
            if red.sadd(key, g) > 0:
                tasks.append(index_named.si(iri, g))
                tasks.append(analyze_named.si(iri, g))
        red.expire(key, 30 * 24 * 60 * 60)  # 30D
        return group(tasks).apply_async(queue='low_priority')
    log.debug(f'Skipping endpoint as it was recently analyzed: {iri!s}')


@celery.task
def analyze_named(endpoint_iri, named_graph):
    """Analyze triples in a named graph of an endpoint."""
    key = f'analyze:{endpoint_iri!s}:{named_graph!s}'
    tokens = [it.token for it in AbstractAnalyzer.__subclasses__()]
    tasks = [run_one_named_analyzer.si(token, endpoint_iri, named_graph) for token in tokens]
    return chord(tasks)(store_named_analysis.si(key))


@celery.task
def run_one_named_analyzer(token, endpoint_iri, named_graph):
    """Run an analyzer identified by its token on a triples in a named graph of an endpoint."""
    g = rdflib.Graph(store='SPARQLStore', identifier=named_graph)
    g.open(endpoint_iri)
    a = get_analyzer(token)
    return json.dumps(a.analyze(g))


@celery.task
def store_named_analysis(results, key):
    """Store results of the analysis in redis."""
    red = redis.Redis(connection_pool=redis_pool)
    with red.pipeline() as pipe:
        pipe.sadd('purgeable', key)
        pipe.set(key, json.dumps(results))
        pipe.expire(key, 30 * 24 * 60 * 60)  # 30D
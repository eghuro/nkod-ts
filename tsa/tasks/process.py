"""Celery tasks for running analyses."""
import json
import logging

import rdflib
import redis
import requests
from celery import group
from gevent.timeout import Timeout as GeventTimeout

from tsa.celery import celery
from tsa.compression import SizeException, decompress_7z, decompress_gzip
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.monitor import monitor
from tsa.robots import session, user_agent, allowed as robots_allowed
from tsa.tasks.common import TrackableTask
from tsa.tasks.index import index, index_named
from tsa.tasks.analyze import analyze


class RobotsRetry(BaseException):
    """Exception indicating retry is neeeded because of crawl delay."""

    def __init__(self, delay):
        """Note the delay."""
        self.delay = delay


class Skip(BaseException):
    """Exception indicating we have to skip this distribution."""


# Following 2 tasks are doing the same thing but with different priorities
# This is to speed up known RDF distributions
@celery.task(bind=True, base=TrackableTask)
def process_priority(self, iri):
    do_process(iri, self, True)

@celery.task(bind=True, time_limit=60, base=TrackableTask)
def process(self, iri):
    do_process(iri, self)

def do_process(iri, task, is_prio=False):
    """Analyze an RDF distribution under given IRI."""
    log = logging.getLogger(__name__)

    key = f'distributions'
    red = task.redis
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
        except requests.exceptions.HTTPError:
            log.exception('HTTP Error') # this is a 404 or similar, not worth retrying
            raise
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
                log.warn(f'File is too large: {iri}')
            else:
                pipeline = group(index.si(iri, guess), analyze.si(iri, guess))
        if is_prio:
            return pipeline.apply_async(queue='high_priority')
        return pipeline.apply_async()
    except:
        log.exception(f'Failed to get {iri!s}')
        red.sadd('stat:failed', str(iri))
        red.sadd('purgeable', 'stat:failed')
        return {}


@celery.task(bind=True, time_limit=60, base=TrackableTask)
def decompress(self, iri, type):
    do_decompress(self, iri, type)

@celery.task(bind=True, base=TrackableTask)
def decompress_prio(self, iri, type):
    do_decompress(self, iri, type, True)

def do_decompress(task, iri, type, is_prio=False):
    #we cannot pass request here, so we have to make a new one
    log = logging.getLogger(__name__)
    red = task.redis

    key = f'distributions'
    try:
        r = fetch(iri, log, red)
    except RobotsRetry as e:
        red.srem(key, iri)
        task.retry(countdown=e.delay)
    except requests.exceptions.RequestException as e:
        red.srem(key, iri)
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
        except TypeError:
            log.exception(f'iri: {iri!s}, type: {type!s}')
        else:
            for sub_iri, guess in lst:
                yield index.si(sub_iri, guess)
                yield analyze.si(sub_iri, guess)

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
            log.warn(f'Skipping {iri} as it is too large: {conlen!s}')
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
                        pipe.delete(key)
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
    is_allowed, delay, robots_url = robots_allowed(iri)
    key = f'delay_{robots_url!s}'
    if not is_allowed:
        log.warn(f'Not allowed to fetch {iri!s} as {user_agent!s}')
    else:
        wait = red.ttl(key)
        if wait > 0:
            log.info(f'Analyze {iri} in {wait} because of crawl-delay')
            raise RobotsRetry(wait)

    timeout = 512 * 1024 / 500
    log.debug(f'Timeout {timeout!s} for {iri}')
    r = session.get(iri, stream=True, timeout=timeout)
    r.raise_for_status()

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


@celery.task(base=TrackableTask)
def process_endpoint(iri): #Low priority as we are doing scan of all graphs in the endpoint
    log = logging.getLogger(__name__)

    """Index and analyze triples in the endpoint."""
    key = f'endpoints'
    red = process_endpoint.redis
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
        red.expire(key, 24 * 60 * 60)  # 30D
        return group(tasks).apply_async(queue='low_priority')
    log.debug(f'Skipping endpoint as it was recently analyzed: {iri!s}')
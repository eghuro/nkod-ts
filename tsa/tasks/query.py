"""Celery tasks for querying."""
import math
import statistics
import json
import logging
import itertools
from collections import defaultdict, OrderedDict
from json import JSONEncoder, JSONDecoder

import redis
import rfc3987
from celery import group, chain, chord

from tsa.celery import celery
from tsa.extensions import redis_pool
from tsa.analyzer import SkosAnalyzer

#TODO: add an endpoint: here's SPARQL endpoint and many graphs in a JSON list, run the whole process inc. analysis_query


# See:
# - https://www.ovh.com/blog/doing-big-automation-with-celery/
# - https://github.com/celery/celery/issues/5000
# - https://github.com/celery/celery/issues/5286
# - https://github.com/celery/celery/issues/5327
# - https://github.com/ovh/celery-dyrygent
#
# TL;DR: certain bugs in celery prevent us from using desired canvas and celery-dyrygent project also has bugs

# @celery.task
# def analysis_query(iris, small, trans, cross, stats, id):
#     red = redis.Redis(connection_pool=redis_pool)
#     red.sadd('relationship', 'skosCross', 'skosTransitive')
#
#     log = logging.getLogger(__name__)
#     log.info(f'IRIs: {len(iris)}')
#
#     chain_lst = [
#         group([index_distribution_query.si(iri) for iri in all_missing(iris, red)]),
#         group([gather_initial.si(iri) for iri in iris])
#     ]
#     if trans:
#         chain_lst.append(group([transitive.si(iri) for iri in iris]))
#     if cross:
#         chain_lst.append(group([log_common.si(iri_in, iri_out) for iri_in, iri_out in itertools.product(iris, iris)]))
#
#     #using Workflow -> cannot pass arguments -> using redis key 'anal'
#     chain_lst.extend([
#         compile_analyses.si(iris),
#         cut_small.si(small),
#         extend_queries.si(iris),
#         add_stats.si(stats),
#         store_analysis.si(id)
#     ])
#
#     canvas = chain(chain_lst)
#     wf = Workflow()
#     wf.add_celery_canvas(canvas)
#     return wf


@celery.task(ignore_result=True)
def store_analysis(analyses, id):
    red = redis.Redis(connection_pool=redis_pool)
    log = logging.getLogger(__name__)

    key = f'analysis:{id}'
    if analyses is not None:
        red.set(key, json.dumps([x for x in analyses if x is not None]))
    else:
        red.set(key, json.dumps([]))
    red.expire(key, 30*24*60*60)
    log.info("Done")


@celery.task
def cut_small(analyses, small):
    if small:
        return analyses[-2:]
    return analyses


@celery.task
def extend_queries(analyses, lst):
    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)
    log.info('Gather queries')
    analyses.extend(x for x in gather_queries(lst, red, log)) #FIXME: gather_queries has for iri in iris
    return analyses


@celery.task
def add_stats(analyses, stats):
    if stats:
        logging.getLogger(__name__).info('Stats')
        red = redis.Redis(connection_pool=redis_pool)
        analyses.append({
            'format': list(red.hgetall('stat:format')),
            'size': retrieve_size_stats(red) #check
        })
    return analyses


@celery.task
def compile_analyses(iris):
    red = redis.Redis(connection_pool=redis_pool)
    predicates = red.hgetall('predicates')
    classes = red.hgetall('classes')
    analyzes = [json.loads(dump) for dump in red.lrange('analyses', 0, -1)]
    analyzes.append({'predicates': dict(OrderedDict(sorted(predicates.items(), key=lambda kv: kv[1], reverse=True)))})
    analyzes.append({'classes': dict(OrderedDict(sorted(classes.items(), key=lambda kv: kv[1], reverse=True)))})
    red.delete('predicates', 'classes', *[f'external:{iri}' for iri in iris], *[f'internal:{iri}' for iri in iris])
    return analyzes


@celery.task(ignore_result=True)
def gather_initial(iri):
    red = redis.Redis(connection_pool=redis_pool)
    log = logging.getLogger(__name__)
    with red.pipeline() as pipe:
        if skip(iri, red):
            return
        key = f'analyze:{iri!s}'
        x = red.get(key)
        if x is None:
            return
        for analysis in json.loads(x):  # from several analyzers
            if analysis is None:
                continue
            analysis = json.loads(analysis)
            if analysis is None:
                continue

            if 'predicates' in analysis:
                log.info(f'Predicates: {analysis["predicates"]!s}')
                for p in analysis['predicates']:
                    pipe.hincrby('predicates', p, int(analysis['predicates'][p]))
            if 'classes' in analysis:
                log.info(f'Classes: {analysis["classes"]!s}')
                for c in analysis['classes']:
                    pipe.hincrby('classes', c, int(analysis['classes'][c]))
            if 'external' in analysis:
                lst = analysis['external']['not_subject'] + analysis['external']['no_type']
                if len(lst) > 0:
                    pipe.sadd(f'external:{iri}', *lst)
            if 'concept' in analysis:
                if len(analysis['concept'].keys()) > 0:
                    pipe.sadd(f'internal:{iri}', *analysis['concept'].keys())
                if len(analysis['schema'].keys()) > 0:
                    pipe.sadd(f'internal:{iri}', *analysis['schema'].keys())
                if len(analysis['collection']) > 0:
                    pipe.sadd(f'internal:{iri}', *analysis['collection'])
                if len(analysis['orderedCollection']) > 0:
                    pipe.sadd(f'internal:{iri}', *analysis['orderedCollection'])

            pipe.rpush('analyses', json.dumps({'iri': iri, 'analysis': analysis}))
        pipe.sadd('gathered', iri)

        expiration = 60*60 #1H
        pipe.expire('predicates', expiration)
        pipe.expire('classes', expiration)
        pipe.expire('analyses', expiration)
        pipe.expire(f'external:{iri}', expiration)
        pipe.expire(f'internal:{iri}', expiration)
        pipe.execute()
    #log.info(f'Gathered initial, {len(predicates.keys())}')


@celery.task(ignore_result=True)
def transitive(iri):
    red = redis.Redis(connection_pool=redis_pool)
    key_internal = f'internal:{iri}'
    key_external = f'external:{iri}'
    for common in red.sunion(key_internal, key_external):
        for reltype in SkosAnalyzer.relations + 'sameAs':
            key = f'related:{reltype}:{common}'
            for ds in red.smembers(key):
                log_related('skosTransitive', common, iri, ds, red)
        key = f'related:sameAs:{common}'
        for ds in red.smembers(key):
            log_related('sameAsTransitive', common, iri, ds, red)


@celery.task(ignore_result=True)
def log_common(iri_in, iri_out):
    red = redis.Redis(connection_pool=redis_pool)
    for common in red.sinter(f'external:{iri_out}', f'internal:{iri_in}'):
        log_related('cross', common, iri_in, iri_out, red)


@celery.task(ignore_result=True)
def index_distribution_query(iri):
    """Query the index and construct related datasets for the iri of a distribution.

    Final result is stored in redis.
    """
    red = redis.Redis(connection_pool=redis_pool)
    if not missing(iri, red):
        return

    related = defaultdict(set)
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


def log_related(rel_type, common, iri_in, iri_out, red):
    """Log related distributions."""
    exp = 30 * 24 * 60 * 60  # 30D
    pipe = red.pipeline()
    pipe.sadd(f'related:{rel_type}:{common!s}', iri_in)
    pipe.sadd(f'related:{rel_type}:{common!s}', iri_out)
    pipe.sadd(f'key:{iri_in!s}', common)
    pipe.sadd(f'key:{iri_out!s}', common)
    pipe.sadd(f'reltype:{iri_in!s}', rel_type)
    pipe.sadd(f'reltype:{iri_out!s}', rel_type)
    pipe.expire(f'related:{rel_type}:{common!s}', exp)
    pipe.expire(f'key:{iri_in!s}', exp)
    pipe.expire(f'key:{iri_out!s}', exp)
    pipe.expire(f'reltype:{iri_in!s}', exp)
    pipe.expire(f'reltype:{iri_out!s}', exp)
    keys = [f'related:{rel_type}:{common!s}',
            f'key:{iri_in!s}',
            f'key:{iri_out!s}',
            f'reltype:{iri_in!s}',
            f'reltype:{iri_out!s}']
    pipe.sadd('purgeable', *keys)

    # distribution queries no longer valid
    pipe.expire(f'distrquery:{iri_in!s}', 0)
    pipe.expire(f'distrquery:{iri_out!s}', 0)

    pipe.execute()


def gather_queries(iris, red, log):
    """Compile queries for all iris."""
    for iri in iris:
        key = f'distrquery:{iri!s}'
        if missing(iri, red):
            log.warn(f'Missing index query result for {iri!s}')
        else:
            related = red.get(key)

            if related is not None:
                rel_json = json.loads(related)
            else:
                rel_json = {}

            if len(rel_json.keys()) > 0:
                yield {
                    'iri': iri,
                    'related': rel_json
                }


def skip(iri, red):
    """Condition if iri should be skipped from gathering analyses."""
    key = f'analyze:{iri!s}'
    return red.sismember('stat:failed', iri) or \
           red.sismember('stat:skipped', iri) or \
           not rfc3987.match(iri) or \
           not red.exists(key)


def all_gather(iris, red):
    prefix_len = len('analyze:')
    return (set(x[prefix_len:] for x in red.keys('analyze:*')) - red.sunion('stat:failed', 'stat:skipped', 'gathered')).intersection(set(iris))


def all_process(iris, red):
    prefix_len = len('analyze:')
    return (set(x[prefix_len:] for x in red.keys('analyze:*')) - red.sunion('stat:failed', 'stat:skipped')).intersection(set(iris))


def missing(iri, red):
    """Condition if index query result is missing."""
    key = f'distrquery:{iri!s}'
    return not red.exists(key) and not red.sismember('stat:failed', iri) and not red.sismember('stat:skipped', iri)


def all_missing(iris, red):
    l = len('distrquery:')
    return set(iris) - set(x[l:] for x in red.keys('distrquery:*')) - red.sunion('stat:failed', 'stat:skipped')


def convert_size(size_bytes):
    """Convert size in bytes into a human readable string."""
    if size_bytes == 0:
        return '0B'
    size_name = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return '%s %s' % (s, size_name[i])


def retrieve_size_stats(red):
    """Load sizes from redis and calculate some stats about it."""
    lst = sorted([int(x) for x in red.lrange('stat:size', 0, -1)])
    try:
        mode = statistics.mode(lst)
    except statistics.StatisticsError:
        mode = None
    try:
        mean = statistics.mean(lst)
    except statistics.StatisticsError:
        mean = None
    try:
        stdev = statistics.stdev(lst, mean)
    except statistics.StatisticsError:
        stdev = None
    try:
        var = statistics.variance(lst, mean)
    except statistics.StatisticsError:
        var = None

    try:
        minimum = min(lst)
    except ValueError:
        minimum = None

    try:
        maximum = max(lst)
    except ValueError:
        maximum = None

    return {
        'min': convert_size(minimum),
        'max': convert_size(maximum),
        'mean': convert_size(mean),
        'mode': convert_size(mode),
        'stdev': convert_size(stdev),
        'var': var
    }


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return super(JSONEncoder, self).default(obj)

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
from tsa.analyzer import AbstractAnalyzer, SkosAnalyzer
from tsa.redis import EXPIRATION_CACHED, EXPIRATION_TEMPORARY, related as related_key


### ANALYSIS ###

@celery.task
def compile_analyses(iris):
    red = redis.Redis(connection_pool=redis_pool)
    analyzes = [json.loads(x) for x in [red.get(f'analyze:{iri}') for iri in iris] if x is not None]
    #analyzes = [json.loads(dump) for dump in red.lrange('analyze', 0, -1)]
    #red.delete('analyses', 'predicates', 'classes', *[f'external:{iri}' for iri in iris], *[f'internal:{iri}' for iri in iris])
    return analyzes  # the BIG report


@celery.task
def split_analyses_by_iri(analyses, id):
    red = redis.Redis(connection_pool=redis_pool)
    iris = set()
    log = logging.getLogger(__name__)
    #with red.pipeline() as pipe:  # MULTI ... EXEC block
    for analysis in analyses:  # long loop
        log.debug(str(analysis))
        if 'analysis' in analysis.keys():
            content = analysis['analysis']
            if 'iri' in analysis.keys():
                iri = analysis['iri']
                iris.add(iri)
            elif 'endpoint' in analysis.keys() and 'graph' in analysis.keys():
                iri = f'{analysis["endpoint"]}/{analysis["graph"]}'
                iris.add(analysis['endpoint'])  # this is because named graph is not extracted from DCAT
            else:
                log.error('Missing iri and endpoint/graph')

            log.debug(iri)
            key = f'analysis:{id}:{iri!s}'
            log.debug(key)
            red.rpush(key, json.dumps(content))
            #red.expire(key, EXPIRATION_TEMPORARY)

        else:
            log.error('Missing content')
    return list(iris)


@celery.task(ignore_result=True)
def merge_analyses_by_distribution_iri_and_store(iris, id):
    red = redis.Redis(connection_pool=redis_pool)
    log = logging.getLogger(__name__)

    # Get list of dcat:Dataset iris for distribution iris
    # and create a mapping of (used) distribution iris per dataset iri

    # in key = f'analysis:{id}:{iri!s}' there's a list of analyses (list of lists)

    ds = []
    if len(iris) > 0:
        ds_iris = red.hmget('distrds', *iris)
        for distr_iri, ds_iri in zip(iris, ds_iris):
            if ds_iri is None:
                log.error(f'Missing DS IRI for {distr_iri}')
                continue

            key = f'dsdistr:{ds_iri}'
            red.sadd(key, distr_iri)
            red.expire(key, EXPIRATION_TEMPORARY)
            ds.append(ds_iri)

    ds = set(ds)

    # Merge individual distribution analyses into DS analysis (for query endpoint) and into batch report
    batch = {}
    for ds_iri in ds:
        batch[ds_iri] = {}
        key_out = f'dsanalyses:{ds_iri}'
        for distr_iri in red.smembers(f'dsdistr:{ds_iri}'):
            key_in = f'analysis:{id}:{distr_iri!s}'
            for a in [json.loads(a) for a in red.lrange(key_in, 0, -1)]:
                for b in a:  # flatten
                    for key in b.keys():  # 1 element
                        batch[ds_iri][key] = b[key]  # merge dicts
            #red.expire(key_in, 0)
        red.set(key_out, json.dumps(batch[ds_iri]))
        red.expire(key_out, EXPIRATION_CACHED)
        #red.expire(f'dsdistr:{ds_iri}', 0)

        # now we have ds_analyses for every ds_iri known
        # those are NOT labeled by the batch query id as it will be used in querying from dcat-ap-viewer

    # dump the batch report
    key = f'analysis:{id}'
    red.set(key, json.dumps(batch))
    red.expire(key, EXPIRATION_CACHED)
    log.info("Done")


### INDEX ###
reltypes = sum((analyzer.relations for analyzer in AbstractAnalyzer.__subclasses__() if 'relations' in analyzer.__dict__), [])

@celery.task
def gen_related_ds():
    red = redis.Redis(connection_pool=redis_pool)
    related_ds = {}

    for rel_type in reltypes:
        related_ds[rel_type] = dict()
        root = f'related:{rel_type!s}:'
        for key in red.scan_iter(match=f'related:{rel_type!s}:*'):
            token = key[len(root):]
            related_dist = red.smembers(related_key(rel_type, token))  # these are related by token
            if len(related_dist) > 0:
                related_ds[rel_type][token] = list(set(red.hmget('distrds', *related_dist)))

    with red.pipeline() as pipe:
        pipe.set('relatedds', json.dumps(related_ds))
        pipe.sadd('purgeable', 'relatedds')
        pipe.expire(key, EXPIRATION_CACHED)
        pipe.execute()
    return related_ds


@celery.task(ignore_result=True)
def index_distribution_query(iri):
    """Query the index and construct related datasets for the iri of a distribution.

    Final result is stored in redis.
    """
    red = redis.Redis(connection_pool=redis_pool)
    #if not missing(iri, red):
    #    return

    related_ds = json.loads(red.get('relatedds'))
    current_dataset = red.hget('distrds', iri)

    for rel_type in reltypes:
        to_delete = []
        for token in related_ds[rel_type].keys():
            if current_dataset in related_ds[rel_type][token]:
                related_ds[rel_type][token].remove(current_dataset)
            else:
                to_delete.append(token)
        for token in to_delete:
            del related_ds[rel_type][token]

    exp = EXPIRATION_CACHED  # 30D
    key = f'distrquery:{current_dataset}'
    with red.pipeline() as pipe:
        pipe.set(key, json.dumps(related_ds))
        pipe.sadd('purgeable', key)
        pipe.expire(key, exp)
        pipe.execute()


### MISC ###
@celery.task  # (ignore_result=True)
def store_analysis(analyses, id):
    red = redis.Redis(connection_pool=redis_pool)
    log = logging.getLogger(__name__)

    key = f'distr_analysis:{id}'
    if analyses is not None:
        red.set(key, json.dumps([x for x in analyses if x is not None]))
    else:
        red.set(key, json.dumps([]))
    red.expire(key, EXPIRATION_CACHED)
    # log.info("Done")
    return analyses





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
    analyses.extend(x for x in gather_queries(lst, red, log))  # FIXME: gather_queries has a for loop iri in iris
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
            #analysis = json.loads(analysis)
            #if analysis is None:
            #    continue

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

        expiration = EXPIRATION_TEMPORARY #1H
        #pipe.expire('predicates', expiration)
        #pipe.expire('classes', expiration)
        #pipe.expire('analyses', expiration)
        #pipe.expire(f'external:{iri}', expiration)
        #pipe.expire(f'internal:{iri}', expiration)
        pipe.execute()
    #log.info(f'Gathered initial, {len(predicates.keys())}')


@celery.task(ignore_result=True)
def transitive(iri):
    red = redis.Redis(connection_pool=redis_pool)
    key_internal = f'internal:{iri}'
    key_external = f'external:{iri}'
    # relations =  sum((analyzer.relations for analyzer in AbstractAnalyzer.__subclasses__() if 'relations' in analyzer.__dict__), [])
    for common in red.sunion(key_internal, key_external):
        for reltype in SkosAnalyzer.relations:
            key = f'related:{reltype}:{common}'
            for ds in red.smembers(key):
                log_related('skosTransitive', common, iri, ds, red)
        key = f'related:sameAs:{common}'
        for ds in red.smembers(key):
            log_related('sameAsTransitive', common, iri, ds, red)
        key = f'related:containedInPlace:{common}'
        for ds in red.smembers(key):
            log_related('containedInPlaceTransitive', common, iri, ds, red)


@celery.task(ignore_result=True)
def log_common(iri_in, iri_out):
    red = redis.Redis(connection_pool=redis_pool)
    for common in red.sinter(f'external:{iri_out}', f'internal:{iri_in}'):
        log_related('cross', common, iri_in, iri_out, red)


def log_related(rel_type, common, iri_in, iri_out, red):
    """Log related distributions."""
    exp = EXPIRATION_CACHED  # 30D
    pipe = red.pipeline()
    pipe.sadd(f'related:{rel_type}:{common!s}', iri_in)
    pipe.sadd(f'related:{rel_type}:{common!s}', iri_out)

    keys = [f'related:{rel_type}:{common!s}']
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

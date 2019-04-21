# -*- coding: utf-8 -*-
"""Query endpoints."""
import json
from collections import OrderedDict, defaultdict

import redis
import rfc3987
from atenvironment import environment
from celery import group
from flask import Blueprint, abort, current_app, jsonify, request

from tsa.analyzer import SkosAnalyzer
from tsa.cache import cached
from tsa.monitor import Monitor
from tsa.tasks.query import index_distribution_query

from .stat import retrieve_size_stats

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/api/v1/analyze', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
@environment('REDIS')
def api_analyze_get(redis_url):
    """Read the analysis."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    iri = request.args.get('iri', None)
    if rfc3987.match(iri):
        key = f'analyze:{iri!s}'
        if not r.exists(key):
            abort(404)
        else:
            return jsonify(json.loads(r.get(key)))
    else:
        abort(400)


@blueprint.route('/api/v1/query/dataset', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
@environment('REDIS')
def ds_index(redis_url):
    """Query a DCAT dataset."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    iri = request.args.get('iri', None)
    current_app.logger.info(f'Querying distributions from DCAT dataset: {iri}')
    if rfc3987.match(iri):
        if not r.sismember('dcatds', iri):
            abort(404)
        else:
            return batch(r.smembers(f'dsdistr:{iri}'))
    else:
        abort(400)


@blueprint.route('/api/v1/query/distribution', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
@environment('REDIS')
def distr_index(redis_url):
    """Query an RDF distribution sumbitted for analysis."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    iri = request.args.get('iri', None)
    current_app.logger.info(f'Querying distribution for: {iri}')
    if rfc3987.match(iri):
        if not r.exists(f'ds:{iri}'):
            abort(404)
        else:
            index_distribution_query.s(iri).apply_async().get()
            return jsonify(json.loads(r.get(f'distrquery:{iri}')))
    abort(400)


def _graph_iris(r):
    for e in r.smembers('endpoints'):
        for g in r.smembers(f'graphs:{e}'):
            yield f'{e}/{g}'


@environment('REDIS')
def _get_known_distributions(redis_url):
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    distr_endpoints = r.smembers('distributions').union(frozenset(_graph_iris(r)))
    failed_skipped = r.smembers('stat:failed').union(r.smembers('stat:skipped'))
    return distr_endpoints.difference(failed_skipped)


@blueprint.route('/api/v1/query/analysis', methods=['GET'])
def known_distributions():
    """List known distributions and endpoints without failed or skipped ones."""
    return jsonify(list(_get_known_distributions()))


def skip(iri, r):
    """Condition if iri should be skipped from gathering analyses."""
    key = f'analyze:{iri!s}'
    return r.sismember('stat:failed', iri) or\
        r.sismember('stat:skipped', iri) or\
        not rfc3987.match(iri) or\
        not r.exists(key)


def missing(iri, r):
    """Condition if index query result is missing."""
    key = f'distrquery:{iri!s}'
    return not r.exists(key) and not r.sismember('stat:failed', iri) and not r.sismember('stat:skipped', iri)


def gather_analyses(iris, transitive, cross, r):
    """Compile analyses for all iris from all analyzers."""
    current_app.logger.info(f'Gather analyses')
    analyses = []
    predicates = defaultdict(int)
    classes = defaultdict(int)

    external = defaultdict(set)
    internal = defaultdict(set)

    for iri in iris:
        if skip(iri, r):
            continue
        key = f'analyze:{iri!s}'
        x = json.loads(r.get(key))
        analyses_red = []
        for y in x:
            analyses_red.append(y)
        for analysis in analyses_red:  # from several analyzers
            analysis = json.loads(analysis)
            if analysis is None:
                continue
            current_app.logger.info(f'{analysis!s}')
            if 'predicates' in analysis:
                for p in analysis['predicates']:
                    predicates[p] += int(analysis['predicates'][p])
            if 'classes' in analysis:
                for c in analysis['classes']:
                    classes[c] += int(analysis['classes'][c])
            if 'external' in analysis:
                external[iri].update(analysis['external']['not_subject'] + analysis['external']['no_type'])
            if 'concept' in analysis:
                internal[iri].update(analysis['concept'].keys())
                internal[iri].update(analysis['schema'].keys())
                internal[iri].update(analysis['collection'])
                internal[iri].update(analysis['orderedCollection'])

            analyses.append({'iri': iri, 'analysis': analysis})
    current_app.logger.info(f'Gathered initial, {len(predicates.keys())}')
    r.sadd('relationship', 'skosCross', 'skosTransitive')
    cnt = 0
    if transitive:
        for iri in iris:
            for common in internal[iri].union(external['iri']):
                for reltype in SkosAnalyzer.relations:
                    key = f'related:{reltype}:{common}'
                    for ds in r.smembers(key):
                        log_related('skosTransitive', common, iri, ds, r)
                        cnt = cnt + 6
                key = f'related:sameAs:{common}'
                for ds in r.smembers(key):
                    log_related('sameAsTransitive', common, iri, ds, r)
                    cnt = cnt + 6
        current_app.logger.info(f'Calculated transitive')

    if cross:
        for iri_in in iris:
            for iri_out in iris:
                for common in external[iri_out].intersection(internal[iri_in]):
                    log_related('cross', common, iri_in, iri_out, r)
                    cnt = cnt + 6
    current_app.logger.info(f'Indexed {cnt!s} records')

    analyses.append({'predicates': dict(OrderedDict(sorted(predicates.items(), key=lambda kv: kv[1], reverse=True)))})
    analyses.append({'classes': dict(OrderedDict(sorted(classes.items(), key=lambda kv: kv[1], reverse=True)))})

    return analyses


def log_related(rel_type, common, iri_in, iri_out, r):
    exp = 30 * 24 * 60 * 60  # 30D
    pipe = r.pipeline()
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
    pipe.sadd('purgeable', f'related:{rel_type}:{common!s}', f'key:{iri_in!s}', f'key:{iri_out!s}', f'reltype:{iri_in!s}', f'reltype:{iri_out!s}')

    # distribution queries no longer valid
    pipe.expire(f'distrquery:{iri_in!s}', 0)
    pipe.expire(f'distrquery:{iri_out!s}', 0)

    pipe.execute()


def fetch_missing(iris, r):
    """Trigger index distribution query where needed."""
    missing_query = []
    for iri in iris:
        if missing(iri, r):
            current_app.logger.debug(f'Missing index query result for {iri!s}')
            missing_query.append(iri)

    current_app.logger.info('Fetching missing query results')
    t = group(index_distribution_query.si(iri) for iri in missing_query).apply_async()
    current_app.logger.info('Call get')
    t.get()
    current_app.logger.info('Leaving')


def gather_queries(iris, r):
    """Compile queries for all iris."""
    current_app.logger.info('Appending results')
    for iri in iris:
        key = f'distrquery:{iri!s}'
        if missing(iri, r):
            current_app.logger.warn(f'Missing index query result for {iri!s}')
        else:
            related = r.get(key)

            if related is not None:
                rel_json = json.loads(related)
            else:
                rel_json = {}

            yield {
                'iri': iri,
                'related': rel_json
            }


@blueprint.route('/api/v1/query/analysis', methods=['POST'])
@environment('REDIS')
def batch_analysis(redis_url):
    """
    Get a big report for all required distributions.

    Get a list of distributions in request body as JSON, compile analyses,
    query the index return the compiled report.
    """
    lst = request.get_json()
    if lst is None:
        lst = _get_known_distributions()

    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    small = 'small' in request.args
    pretty = 'pretty' in request.args
    transitive = 'noTransitive' not in request.args
    cross = 'noCross' not in request.args
    return batch(lst, r, transitive, cross, small, pretty, True)


def batch(lst, r, transitive=False, cross=False, small=False, pretty=False, stats=False):
    analyses = gather_analyses(lst, transitive, cross, r)
    if small:
        current_app.logger.info('Small')
        analyses = analyses[-2:]
    fetch_missing(lst, r)
    analyses.extend(x for x in gather_queries(lst, r))
    if stats:
        analyses.append({
            'format': list(r.hgetall('stat:format')),
            'size': retrieve_size_stats(r)
        })
    if pretty:
        return json.dumps(analyses, indent=4, sort_keys=True)
    return jsonify(analyses)


@blueprint.route('/api/v1/cleanup', methods=['POST', 'DELETE'])
@environment('REDIS')
def cleanup(redis_url):
    extra = ['purgeable']
    stats = 'stats' in request.args
    if stats:
        extra.extend(Monitor.KEYS)

    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    cache_items = [key for key in r.keys() if key.startswith(current_app.config['CACHE_KEY_PREFIX'])]
    current_app.logger.debug('Flask cache items: ' + str(cache_items))
    extra.extend(cache_items)
    r.delete([key for key in r.smembers('purgeable')] + extra)

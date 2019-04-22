# -*- coding: utf-8 -*-
"""Query endpoints."""
import json
from collections import OrderedDict, defaultdict

import redis
import rfc3987
from celery import group
from flask import Blueprint, abort, current_app, jsonify, request

from tsa.analyzer import SkosAnalyzer
from tsa.cache import cached
from tsa.extensions import redis_pool
from tsa.monitor import Monitor
from tsa.tasks.query import index_distribution_query

from .stat import retrieve_size_stats

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/api/v1/analyze', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def api_analyze_get():
    """Read the analysis."""
    red = redis.Redis(connection_pool=redis_pool)
    iri = request.args.get('iri', None)
    if rfc3987.match(iri):
        key = f'analyze:{iri!s}'
        if not red.exists(key):
            abort(404)
        else:
            return jsonify(json.loads(red.get(key)))
    else:
        abort(400)


@blueprint.route('/api/v1/query/dataset', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def ds_index():
    """Query a DCAT dataset."""
    iri = request.args.get('iri', None)
    red = redis.Redis(connection_pool=redis_pool)
    if iri is not None:
        current_app.logger.info(f'Querying distributions from DCAT dataset: {iri}')
        if rfc3987.match(iri):
            red = redis.Redis(connection_pool=redis_pool)
            if not red.sismember('dcatds', iri):
                abort(404)
            else:
                small = 'small' in request.args
                pretty = 'pretty' in request.args
                transitive = 'noTransitive' not in request.args
                cross = 'noCross' not in request.args
                iris = red.smembers(f'dsdistr:{iri}')
                analyses = batch_prepare(iris, red, transitive, cross, small, False)

                # Remove distributions from the same dataset
                for record in analyses:
                    if 'iri' in record.keys():
                        related_keys = record['related'].keys()
                        for key in related_keys:
                            new_related = set(record['related'][key]).difference(iris)
                            record['related'][key] = list(new_related)

                distr_to_ds = defaultdict(None)
                for ds in red.smembers('dcatds'):
                    for distr in red.smembers(f'dsdistr:{ds}'):
                        distr_to_ds[distr] = ds
                current_app.logger.info(json.dumps(distr_to_ds, indent=4, sort_keys=True))

                # Convert distributions to datasets
                new_analyses = []
                for record in analyses:
                    if 'iri' in record.keys():
                        related_keys = record['related'].keys()
                        for key in related_keys:
                            new_related = set()
                            for x in record['related'][key]:
                                if x in distr_to_ds.keys():
                                    new_related.insert(distr_to_ds[x])
                            record['related'][key] = list(new_related.difference(set(iri)))
                    else:
                        new_analyses.append(record)

                # Merge dicts into one
                related = defaultdict(set)
                for record in analyses:
                    if 'iri' in record.keys():
                        for key in record['related'].keys():
                            related[key].update(record['related'][key])
                keys = related.keys()
                for key in keys:
                    related[key] = list(related[key])
                new_analyses.append(related)

                if pretty:
                    current_app.logger.info('Pretty')
                    return json.dumps(new_analyses, indent=4, sort_keys=True)
                return jsonify(new_analyses)
        else:
            abort(400)
    else:
        return jsonify(list(red.smembers('dcatds')))


@blueprint.route('/api/v1/query/distribution', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def distr_index():
    """Query an RDF distribution sumbitted for analysis."""
    iri = request.args.get('iri', None)
    current_app.logger.info(f'Querying distribution for: {iri}')
    if rfc3987.match(iri):
        red = redis.Redis(connection_pool=redis_pool)
        if not red.exists(f'ds:{iri}'):
            abort(404)
        else:
            index_distribution_query.s(iri).apply_async().get()
            return jsonify(json.loads(red.get(f'distrquery:{iri}')))
    abort(400)


def _graph_iris(red):
    for e in red.smembers('endpoints'):
        for g in red.smembers(f'graphs:{e}'):
            yield f'{e}/{g}'


def _get_known_distributions(red):
    distr_endpoints = red.smembers('distributions').union(frozenset(_graph_iris(red)))
    failed_skipped = red.smembers('stat:failed').union(red.smembers('stat:skipped'))
    return distr_endpoints.difference(failed_skipped)


@blueprint.route('/api/v1/query/analysis', methods=['GET'])
def known_distributions():
    """List known distributions and endpoints without failed or skipped ones."""
    red = redis.Redis(connection_pool=redis_pool)
    return jsonify(list(_get_known_distributions(red)))


def skip(iri, red):
    """Condition if iri should be skipped from gathering analyses."""
    key = f'analyze:{iri!s}'
    return red.sismember('stat:failed', iri) or\
        red.sismember('stat:skipped', iri) or\
        not rfc3987.match(iri) or\
        not red.exists(key)


def missing(iri, red):
    """Condition if index query result is missing."""
    key = f'distrquery:{iri!s}'
    return not red.exists(key) and not red.sismember('stat:failed', iri) and not red.sismember('stat:skipped', iri)


def gather_analyses(iris, transitive, cross, red):
    """Compile analyses for all iris from all analyzers."""
    current_app.logger.info(f'Gather analyses')
    analyses = []
    predicates = defaultdict(int)
    classes = defaultdict(int)

    external = defaultdict(set)
    internal = defaultdict(set)

    for iri in iris:
        if skip(iri, red):
            continue
        key = f'analyze:{iri!s}'
        x = json.loads(red.get(key))
        analyses_red = []
        for y in x:
            analyses_red.append(y)
        for analysis in analyses_red:  # from several analyzers
            analysis = json.loads(analysis)
            if analysis is None:
                continue
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
    red.sadd('relationship', 'skosCross', 'skosTransitive')
    cnt = 0
    if transitive:
        for iri in iris:
            for common in internal[iri].union(external['iri']):
                for reltype in SkosAnalyzer.relations:
                    key = f'related:{reltype}:{common}'
                    for ds in red.smembers(key):
                        log_related('skosTransitive', common, iri, ds, red)
                        cnt = cnt + 6
                key = f'related:sameAs:{common}'
                for ds in red.smembers(key):
                    log_related('sameAsTransitive', common, iri, ds, red)
                    cnt = cnt + 6
        current_app.logger.info(f'Calculated transitive')

    if cross:
        for iri_in in iris:
            for iri_out in iris:
                for common in external[iri_out].intersection(internal[iri_in]):
                    log_related('cross', common, iri_in, iri_out)
                    cnt = cnt + 6
    current_app.logger.info(f'Indexed {cnt!s} records')

    analyses.append({'predicates': dict(OrderedDict(sorted(predicates.items(), key=lambda kv: kv[1], reverse=True)))})
    analyses.append({'classes': dict(OrderedDict(sorted(classes.items(), key=lambda kv: kv[1], reverse=True)))})

    return analyses


def log_related(rel_type, common, iri_in, iri_out, red):
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
    pipe.sadd('purgeable', f'related:{rel_type}:{common!s}', f'key:{iri_in!s}', f'key:{iri_out!s}', f'reltype:{iri_in!s}', f'reltype:{iri_out!s}')

    # distribution queries no longer valid
    pipe.expire(f'distrquery:{iri_in!s}', 0)
    pipe.expire(f'distrquery:{iri_out!s}', 0)

    pipe.execute()


def fetch_missing(iris, red):
    """Trigger index distribution query where needed."""
    missing_query = []
    for iri in iris:
        if missing(iri, red):
            current_app.logger.debug(f'Missing index query result for {iri!s}')
            missing_query.append(iri)

    t = group(index_distribution_query.si(iri) for iri in missing_query).apply_async()
    t.get()


def gather_queries(iris, red):
    """Compile queries for all iris."""
    for iri in iris:
        key = f'distrquery:{iri!s}'
        if missing(iri, red):
            current_app.logger.warn(f'Missing index query result for {iri!s}')
        else:
            related = red.get(key)

            if related is not None:
                rel_json = json.loads(related)
            else:
                rel_json = {}

            yield {
                'iri': iri,
                'related': rel_json
            }


@blueprint.route('/api/v1/query/analysis', methods=['POST'])
def batch_analysis():
    """
    Get a big report for all required distributions.

    Get a list of distributions in request body as JSON, compile analyses,
    query the index return the compiled report.
    """
    red = redis.Redis(connection_pool=redis_pool)
    lst = request.get_json()
    if lst is None:
        lst = _get_known_distributions(red)

    small = 'small' in request.args
    pretty = 'pretty' in request.args
    transitive = 'noTransitive' not in request.args
    cross = 'noCross' not in request.args
    return batch(lst, red, transitive, cross, small, pretty, True)


def batch(lst, red, transitive=False, cross=False, small=False, pretty=False, stats=False):
    analyses = batch_prepare(lst, red, transitive, cross, small, stats)
    if pretty:
        current_app.logger.info('Pretty')
        return json.dumps(analyses, indent=4, sort_keys=True)
    return jsonify(analyses)


def batch_prepare(lst, red, transitive, cross, small, stats):
    analyses = gather_analyses(lst, transitive, cross, red)
    if small:
        current_app.logger.info('Small')
        analyses = analyses[-2:]
    fetch_missing(lst, red)
    analyses.extend(x for x in gather_queries(lst, red))
    if stats:
        current_app.logger.info('Stats')
        analyses.append({
            'format': list(red.hgetall('stat:format')),
            'size': retrieve_size_stats(red)
        })
    return analyses


@blueprint.route('/api/v1/cleanup', methods=['POST', 'DELETE'])
def cleanup():
    extra = ['purgeable']
    stats = 'stats' in request.args
    if stats:
        extra.extend(Monitor.KEYS)

    red = redis.Redis(connection_pool=redis_pool)
    with red.pipeline() as pipe:
        cache_items = [key for key in red.keys() if key.startswith(current_app.config['CACHE_KEY_PREFIX'])]
        current_app.logger.debug('Flask cache items: ' + str(cache_items))
        extra.extend(cache_items)

        for key in [key for key in pipe.smembers('purgeable')] + extra:
            pipe.delete(key)
        pipe.execute()
    return 'OK'

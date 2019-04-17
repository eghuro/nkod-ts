# -*- coding: utf-8 -*-
"""Public section, including homepage and signup."""
import json
import logging
from collections import OrderedDict, defaultdict

import redis
import rfc3987
from atenvironment import environment
from celery import group
from flask import Blueprint, abort, current_app, jsonify, request

from tsa.cache import cached
from tsa.tasks.analyze import analyze, process_endpoint
from tsa.tasks.batch import inspect_catalog, inspect_endpoint
from tsa.tasks.query import index_distribution_query
from tsa.tasks.system import hello, system_check

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/api/v1/test/base')
def test_basic():
    """Basic test returning hello world."""
    return 'Hello world!'


@blueprint.route('/api/v1/test/job')
def test_celery():
    """Hello world test using Celery task."""
    r = hello.delay()
    return r.get()


@blueprint.route('/api/v1/test/system')
def test_system():
    """Test systems and provide a hello world."""
    x = (system_check.s() | hello.si()).delay().get()
    log = logging.getLogger(__name__)
    log.info(f'System check result: {x!s}')
    return str(x)


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


@blueprint.route('/api/v1/analyze/distribution', methods=['POST'])
def api_analyze_iri():
    """Analyze a distribution."""
    iri = request.args.get('iri', None)

    if iri is not None:
        current_app.logger.info(f'Analyzing distribution for: {iri}')
        if rfc3987.match(iri):
            analyze.delay(iri)
            return 'OK'
        else:
            abort(400)
    else:
        iris = []
        for iri in request.get_json():
            if rfc3987.match(iri):
                iris.append(iri)
        for iri in iris:
            current_app.logger.info(f'Analyzing distribution for: {iri}')
            analyze.delay(iri)
        return 'OK'


@blueprint.route('/api/v1/analyze/endpoint', methods=['POST'])
def api_analyze_endpoint():
    """Analyze an Endpoint."""
    iri = request.args.get('sparql', None)

    current_app.logger.info(f'Analyzing SPARQL endpoint: {iri}')

    if rfc3987.match(iri):
        (process_endpoint.si(iri) | index_distribution_query.si(iri)).apply_async()
        return 'OK'
    else:
        abort(400)


@blueprint.route('/api/v1/analyze/catalog', methods=['POST'])
def api_analyze_catalog():
    """Analyze a catalog."""
    if 'iri' in request.args:
        iri = request.args.get('iri', None)
        current_app.logger.info(f'Analyzing a DCAT catalog from a distribution under {iri}')
        if rfc3987.match(iri):
            (inspect_catalog.si(iri) | index_distribution_query.si(iri)).apply_async()
            return 'OK'
        else:
            abort(400)
    elif 'sparql' in request.args:
        iri = request.args.get('sparql', None)
        current_app.logger.info(f'Analyzing datasets from an endpoint under {iri}')
        if rfc3987.match(iri):
            (inspect_endpoint.si(iri) | index_distribution_query.si(iri)).apply_async()
            return 'OK'
        else:
            abort(400)
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
    else:
        abort(400)


@blueprint.route('/api/v1/stat/format', methods=['GET'])
@environment('REDIS')
def stat_format(redis_url):
    """List distribution formats logged."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    return jsonify(r.hgetall('stat:format'))


@blueprint.route('/api/v1/stat/failed', methods=['GET'])
@environment('REDIS')
def stat_failed(redis_url):
    """List failed distributions."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    return jsonify(list(r.smembers('stat:failed')))


@blueprint.route('/api/v1/query/analysis', methods=['GET'])
@environment('REDIS')
def known_distributions(redis_url):
    """List known distributions and endpoints without failed or skipped ones."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    distr_endpoints = r.smembers('distributions').union(r.smembers('endpoints'))
    failed_skipped = r.smembers('stat:failed').union(r.smembers('stat:skipped'))
    return jsonify(list(distr_endpoints.difference(failed_skipped)))


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


def gather_analyses(iris, r):
    """Compile analyses for all iris from all analyzers."""
    analyses = []
    predicates = defaultdict(int)
    classes = defaultdict(int)

    for iri in request.get_json():
        if skip(iri, r):
            continue
        key = f'analyze:{iri!s}'
        x = json.loads(r.get(key))
        analyses_red = []
        for y in x:
            analyses_red.append(json.loads(y))
        for analysis in analyses_red:  # from several analyzers
            if analysis is None:
                continue
            if 'predicates' in analysis:
                for p in analysis['predicates']:
                    predicates[p] += int(analysis['predicates'][p])
            if 'classes' in analysis:
                for c in analysis['classes']:
                    classes[c] += int(analysis['classes'][c])
            analyses.append({'iri': iri, 'analysis': analysis})

    analyses.append({'predicates': dict(OrderedDict(sorted(predicates.items(), key=lambda kv: kv[1], reverse=True)))})
    analyses.append({'classes': dict(OrderedDict(sorted(classes.items(), key=lambda kv: kv[1], reverse=True)))})

    return analyses, predicates, classes


def fetch_missing(iris, r):
    """Trigger index distribution query where needed."""
    missing_query = []
    for iri in iris:
        if missing(iri, r):
            current_app.logger.debug(f'Missing index query result for {iri!s}')
            missing_query.append(iri)

    current_app.logger.info('Fetching missing query results')
    group(index_distribution_query.si(iri) for iri in missing_query).apply_async().get()


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
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    analyses, predicates, classes = gather_analyses(request.get_json(), r)
    fetch_missing(request.get_json(), r)
    analyses.extend(x for x in gather_queries(request.get_json(), r))
    return jsonify(analyses)

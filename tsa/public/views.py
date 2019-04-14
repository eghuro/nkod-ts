# -*- coding: utf-8 -*-
"""Public section, including homepage and signup."""
import json
import logging
from collections import defaultdict, OrderedDict

import redis
import rfc3987
from atenvironment import environment
from flask import Blueprint, abort, current_app, jsonify, request

from tsa.cache import cached
from tsa.tasks.analyze import analyze, process_endpoint
from tsa.tasks.batch import inspect_catalog, inspect_endpoint
from tsa.tasks.query import index_query, index_distribution_query
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
    """Read analysis"""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    iri = request.args.get('iri', None)
    if rfc3987.match(iri):
        key = f'analyze:{iri!s}'
        if not r.exists(key):
            abort(404)
        else:
            return jsonify(json.loads(r.get(key)))


@blueprint.route('/api/v1/analyze/distribution', methods=['POST'])
def api_analyze_iri():
    """Analyze a distribution."""
    iri = request.args.get('iri', None)

    current_app.logger.info(f'Analyzing distribution for: {iri}')

    if rfc3987.match(iri):
        analyze.delay(iri)
        return "OK"
    else:
        abort(400)

@blueprint.route('/api/v1/analyze/endpoint', methods=['POST'])
def api_analyze_endpoint():
    """Analyze an Endpoint."""
    iri = request.args.get('sparql', None)

    current_app.logger.info(f'Analyzing SPARQL endpoint: {iri}')

    if rfc3987.match(iri):
        process_endpoint.delay(iri)
        return "OK"
    else:
        abort(400)


@blueprint.route('/api/v1/analyze/catalog', methods=['POST'])
def api_analyze_catalog():
    """Analyze a catalog."""
    if 'iri' in request.args:
        iri = request.args.get('iri', None)
        current_app.logger.info(f'Analyzing a DCAT catalog from a distribution under {iri}')
        if rfc3987.match(iri):
            inspect_catalog.delay(iri)
            return "OK"
        else:
            abort(400)
    elif 'sparql' in request.args:
        iri = request.args.get('sparql', None)
        current_app.logger.info(f'Analyzing datasets from an endpoint under {iri}')
        if rfc3987.match(iri):
            inspect_endpoint.delay(iri)
            return "OK"
        else:
            abort(400)
    else:
        abort(400)

@blueprint.route('/api/v1/query/dataset', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
@environment('REDIS')
def ds_index(redis_url):
    """Query a datacube dataset."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    iri = request.args.get('iri', None)
    current_app.logger.info(f'Querying dataset for: {iri}')

    result_key = f'query:{iri}'
    current_app.logger.info(f'Result key: {result_key}')

    if rfc3987.match(iri):
        if not r.exists(f'key:{iri}'):
            abort(404)
        elif not r.exists(result_key):
            current_app.logger.info(f'Constructing result')
            index_query.s(iri).apply_async().get()
        current_app.logger.info(f'Return result from redis')
        return jsonify(json.loads(r.get(result_key)))
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
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    return jsonify(r.hgetall("stat:format"))

@blueprint.route('/api/v1/stat/failed', methods=['GET'])
@environment('REDIS')
def stat_failed(redis_url):
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    return jsonify(list(r.smembers("stat:failed")))

@blueprint.route('/api/v1/query/analysis', methods=['POST'])
@environment('REDIS')
def batch_analysis(redis_url):
    analyses = []
    predicates = defaultdict(int)
    classes = defaultdict(int)

    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    for iri in request.get_json():
        if rfc3987.match(iri):
            key = f'analyze:{iri!s}'
            if r.exists(key):
                x = json.loads(r.get(key))
                analyses_red = []
                for y in x:
                    analyses_red.append(json.loads(y))
                for analysis in analyses_red: #from several analyzers
                    if analysis is None:
                        continue
                    if 'predicates' in analysis:
                        for p in analysis['predicates']:
                            predicates[p] += analysis['predicates'][p]
                    if 'classes' in analysis:
                        for c in analysis['classes']:
                            classes[c] += analysis['classes'][c]
                    analyses.append({'iri':iri, 'analysis': analysis})
    analyses.append(OrderedDict(sorted(predicates.items(), key=lambda kv: kv[1], reverse=True)))
    analyses.append(OrderedDict(sorted(classes.items(), key=lambda kv: kv[1], reverse=True)))
    return jsonify(analyses)

# -*- coding: utf-8 -*-
"""Public section, including homepage and signup."""
import json
import logging

import redis
import rfc3987
from atenvironment import environment
from flask import Blueprint, abort, current_app, jsonify, request

from tsa.cache import cached
from tsa.tasks import analyze, hello, index_distribution_query, index_query, system_check

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
def api_analyze_iri():
    """Analyze a distribution."""
    iri = request.args.get('iri', None)
    etl = bool(int(request.args.get('etl', 0)))

    current_app.logger.info(f'Analyzing distribution for: {iri}')
    current_app.logger.info(f'ETL:{etl!s}')
    if etl:  # FIXME: ETL not used at the moment
        current_app.logger.warn('Request to use ETL is currently ignored!')

    if rfc3987.match(iri):
        t = analyze.delay(iri, etl)
        return jsonify([v for v in t.collect()][1][1][1])
        # TODO: switch to trigger - status - fetch result model
    else:
        abort(400)


@blueprint.route('/api/v1/query/dataset')
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


@blueprint.route('/api/v1/query/distribution')
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

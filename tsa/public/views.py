# -*- coding: utf-8 -*-
"""Public section, including homepage and signup."""
import logging

import redis
import rfc3987
from atenvironment import environment
from flask import Blueprint, abort, current_app, jsonify, render_template, request

from tsa.tasks import analyze, hello, system_check

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/', methods=['GET'])
def home():
    """Landing page."""
    return render_template('public/landing.html')


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
def api_analyze_iri():
    """Analyze a distribution."""
    iri = request.args.get('iri', None)
    etl = bool(int(request.args.get('etl', 0)))

    current_app.logger.info(f'ETL:{etl!s}')

    if rfc3987.match(iri):
        return jsonify(analyze.delay(iri, etl).get())
    else:
        abort(400)


@blueprint.route('/api/v1/query/dataset')
@environment('REDIS')
def ds_index(redis_url):
    """Query a datacube dataset."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    iri = request.args.get('iri', None)
    current_app.logger.info(f'Querying dataset for: {iri}')
    if rfc3987.match(iri):
        if not r.exists(f'key:{iri}'):
            abort(404)
        else:
            all_ds = set()
            d = dict()
            for key in r.smembers(f'key:{iri}'):
                related = set(r.smembers(f'related:{key}'))
                current_app.logger.info(f'Related datasets: {related!s}')
                all_ds.update(related)
                current_app.logger.info(f'All DS: {all_ds!s}')
                related.discard(iri)
                if len(related) > 0:
                    d[key] = list(related)
            e = dict()
            for ds in all_ds:
                e[ds] = list(r.smembers(f'distr:{ds}'))
            return jsonify({'related': d, 'distribution': e})
    else:
        abort(400)


@blueprint.route('/api/v1/query/distribution')
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
            return jsonify(list(r.smembers('ds:{iri}')))
    else:
        abort(400)

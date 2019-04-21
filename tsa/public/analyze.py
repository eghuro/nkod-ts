"""Endpoints to start the analysis."""
import redis
import rfc3987
from flask import Blueprint, abort, current_app, request
from celery import group

from tsa.tasks.analyze import analyze, process_endpoint
from tsa.tasks.batch import inspect_catalog, inspect_endpoint
from tsa.tasks.query import index_distribution_query
from tsa.extensions import redis_pool
from tsa.robots import session

blueprint = Blueprint('analyze', __name__, static_folder='../static')


@blueprint.route('/api/v1/analyze/distribution', methods=['POST'])
def api_analyze_iri():
    """Analyze a distribution."""
    iri = request.args.get('iri', None)

    if iri is not None:
        current_app.logger.info(f'Analyzing distribution for: {iri}')
        if rfc3987.match(iri):
            analyze.delay(iri)
            return 'OK'
        abort(400)
    else:
        iris = []
        for iri in request.get_json():
            if rfc3987.match(iri):
                iris.append(iri)
        tasks = []
        for iri in iris:
            current_app.logger.info(f'Analyzing distribution for: {iri}')
            tasks.append(analyze.si(iri))
        group(tasks).apply_async()
        return 'OK'


@blueprint.route('/api/v1/analyze/endpoint', methods=['POST'])
def api_analyze_endpoint():
    """Analyze an Endpoint."""
    iri = request.args.get('sparql', None)

    current_app.logger.info(f'Analyzing SPARQL endpoint: {iri}')

    if rfc3987.match(iri):
        (process_endpoint.si(iri) | index_distribution_query.si(iri)).apply_async()
        return 'OK'
    abort(400)


@blueprint.route('/api/v1/analyze/catalog', methods=['POST'])
def api_analyze_catalog():
    """Analyze a catalog."""
    if 'iri' in request.args:
        iri = request.args.get('iri', None)
        current_app.logger.info(f'Analyzing a DCAT catalog from a distribution under {iri}')
        if rfc3987.match(iri):
            key = f'catalog:{iri}'
            req = session.get(iri)
            red = redis.Redis(connection_pool=redis_pool)
            with red.pipeline() as pipe:
                pipe.set(key, req.text)
                exp = 30 * 24 * 60 * 60  # 30D
                pipe.expire(key, exp)
                pipe.sadd('purgeable', key)
                pipe.execute()
            inspect_catalog.si(key).apply_async()
            return 'OK'
        abort(400)
    elif 'sparql' in request.args:
        iri = request.args.get('sparql', None)
        current_app.logger.info(f'Analyzing datasets from an endpoint under {iri}')
        if rfc3987.match(iri):
            (inspect_endpoint.si(iri) | index_distribution_query.si(iri)).apply_async()
            return 'OK'
        abort(400)
    else:
        abort(400)

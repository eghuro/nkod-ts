"""Endpoints to start the analysis."""
import redis
import rfc3987
import uuid
from celery import group
from flask import Blueprint, abort, current_app, request, session

from tsa.extensions import redis_pool
from tsa.tasks.process import process
from tsa.tasks.batch import inspect_endpoint, inspect_graph
from tsa.tasks.query import index_distribution_query

blueprint = Blueprint('analyze', __name__, static_folder='../static')


@blueprint.route('/api/v1/analyze/distribution', methods=['POST'])
def api_analyze_iri():
    """Analyze a distribution."""
    iri = request.args.get('iri', None)

    if iri is not None:
        current_app.logger.info(f'Analyzing distribution for: {iri}')
        if rfc3987.match(iri):
            #batch id not set
            process.delay(iri)
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
            #batch id not set
            tasks.append(process.si(iri))
        group(tasks).apply_async()
        return 'OK'


@blueprint.route('/api/v1/analyze/catalog', methods=['POST'])
def api_analyze_catalog():
    """Analyze a catalog."""
    if 'token' in session:
        token = session['token']
    else:
        token = str(uuid.uuid4())
        session['token'] = token

    if 'sparql' in request.args:
        iri = request.args.get('sparql', None)
        graph = request.args.get('graph', None)
        current_app.logger.info(f'Analyzing datasets from an endpoint under {iri}')
        if rfc3987.match(iri):
            if graph is not None:
                if rfc3987.match(graph):
                    current_app.logger.info(f'Analyzing named graph {graph} only')
                    red = redis.Redis(connection_pool=redis_pool)
                    try:
                        with red.lock(f'batchLock', blocking_timeout=60) as lock:
                            t = inspect_graph.si(iri, graph).apply_async()
                            current_app.logger.info(f'Batch id: {token}, task id: {t.id}')
                            red.hset('taskBatchId', t.id, token)
                    except redis.exceptions.LockError:
                        abort(500)
                    return 'OK'
                else:
                    abort(400)
            else:
                current_app.logger.warn(f'Requested full endpoint scan')
                ch = (inspect_endpoint.si(iri) | index_distribution_query.si(iri))
                ch.batch_id = token
                ch.apply_async()
                return 'OK'
        else:
            abort(400)
    else:
        abort(400)

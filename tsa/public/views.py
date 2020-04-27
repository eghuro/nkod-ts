# -*- coding: utf-8 -*-
"""Query endpoints."""
import itertools
import json

import redis
import rfc3987
import uuid
from flask import Blueprint, abort, current_app, jsonify, request

from tsa.extensions import redis_pool
from tsa.monitor import Monitor
from tsa.tasks.system import cleanup
from tsa.report import query_dataset
from tsa.cache import cached
from tsa.query import query

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/api/v1/query/dataset', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def dcat_viewer_index_query():
    iri = request.args.get('iri', None)
    lang = request.args.get('language', 'cs')
    if iri is not None:
        if rfc3987.match(iri):
            current_app.logger.info(f'Valid dataset request ({lang}) for {iri}')
            #LABELS: key = f'dstitle:{ds!s}:{t.language}' if t.language is not None else f'dstitle:{ds!s}'

            translation = {
                'https://data.gov.cz/zdroj/datové-sady/https---data.cssz.cz-api-3-action-package_show-id-prehled-o-celkovem-poctu-osvc-podle-okresu': 'https://data.gov.cz/zdroj/datové-sady/CSShZbzpcn/695492977/f24c8df0bce40fd04c3c4bfc81d7e68e',
                'https://data.gov.cz/zdroj/datové-sady/https---data.cssz.cz-api-3-action-package_show-id-pomocne-ciselniky': 'https://data.gov.cz/zdroj/datové-sady/CSShZbzpcn/695492977/38534df5f78ce360ba0cf32665bb2729'
            }

            if iri in translation.keys():
                iri = translation[iri]
                current_app.logger.warn(f'Translating iri to {iri}')

            try:
                return jsonify({
                    "jsonld": query_dataset(iri)
                })
            except TypeError:
                current_app.logger.exception(f'Failed to query {iri}')
                abort(404)
    abort(400)


def _graph_iris(red):
    for e in red.smembers('endpoints'):
        for g in red.smembers(f'graphs:{e}'):
            yield f'{e}:{g}'





@blueprint.route('/api/v1/query/analysis', methods=['POST'])
def batch_analysis():
    """
    Get a big report for all required distributions.
    """
    red = redis.Redis(connection_pool=redis_pool)
    result_id = str(uuid.uuid4())
    query(result_id, red)
    return result_id


@blueprint.route('/api/v1/query/analysis/result', methods=['GET'])
@cached(True, must_revalidate=True, client_only=False, client_timeout=900, server_timeout=1800)
def fetch_analysis():
    red = redis.Redis(connection_pool=redis_pool)
    id = request.args.get('id', None)
    if id is not None:
        key = f'analysis:{id}'
        if red.exists(key):
            analyses = json.loads(red.get(key))
            if red.exists('relatedds'):
                relatedds = json.loads(red.get('relatedds'))
                return jsonify({'analyses': analyses, 'related': relatedds})
            return jsonify({'analyses': analyses})
        else:
            abort(404)
    else:
        abort(400)


@blueprint.route('/api/v1/cleanup', methods=['POST', 'DELETE'])
def cleanup_endpoint():
    """Clean any purgeable records, Flask cache and possibly also stats."""
    extra = ['purgeable']
    stats = 'stats' in request.args
    if stats:
        extra.extend(Monitor.KEYS)

    cleanup.si(current_app.config['CACHE_KEY_PREFIX'], extra).apply_async(queue='low_priority').get()
    return 'OK'

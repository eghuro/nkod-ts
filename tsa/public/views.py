# -*- coding: utf-8 -*-
"""Query endpoints."""
import itertools
import json

import redis
import rfc3987
import uuid
from celery import group
from flask import Blueprint, abort, current_app, jsonify, request

from tsa.extensions import redis_pool
from tsa.monitor import Monitor
from tsa.tasks.query import *
from tsa.tasks.system import cleanup
from tsa.report import query_dataset

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/api/v1/query/dataset', methods=['GET'])
def mock_index():
    iri = request.args.get('iri', None)
    lang = request.args.get('language', 'cs')
    if iri is not None:
        if rfc3987.match(iri):
            current_app.logger.info(f'Valid mock request ({lang}) for {iri}')
            #LABELS: key = f'dstitle:{ds!s}:{t.language}' if t.language is not None else f'dstitle:{ds!s}'
            return jsonify({
                "jsonld": query_dataset(iri)
            })


def _graph_iris(red):
    for e in red.smembers('endpoints'):
        for g in red.smembers(f'graphs:{e}'):
            yield f'{e}:{g}'


def _get_known_distributions(red):
    distr_endpoints = red.smembers('distributions').union(frozenset(_graph_iris(red)))
    failed_skipped = red.smembers('stat:failed').union(red.smembers('stat:skipped'))
    return distr_endpoints.difference(failed_skipped)


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
    trans = 'noTransitive' not in request.args
    cross = 'noCross' not in request.args
    stats = 'stats' in request.args
    result_id = str(uuid.uuid4())
    ###
    ###red.sadd('relationship', 'skosCross', 'skosTransitive')
    iris = list(lst)

    current_app.logger.info("Profile")
    iris = list(lst)
    chain([
        compile_analyses.si(iris),
        ###cut_small.s(small),
        ###extend_queries.s(iris),
        ###add_stats.s(stats),
        #store_analysis.s(result_id),
        split_analyses_by_iri.s(result_id),
        merge_analyses_by_distribution_iri_and_store.s(result_id),
        gen_related_ds.si(),
        index_distribution_query.chunks(zip(lst), 8)
    ]).apply_async(queue='query')

    #current_app.logger.info("Stage 2")
    ###gather_initial.chunks(zip(all_gather(lst, red)), 8).apply_async(queue='query').get()
    ###iris = all_process(lst, red)
    ###if trans:
    ###    current_app.logger.info("Stage 2a")
    ###    transitive.chunks(zip(iris), 8).apply_async(queue='query').get()
    ###if cross:
    ###    current_app.logger.info("Stage 2b")
    ###    log_common.chunks(itertools.product(iris, repeat=2), 8).apply_async(queue='query').get()
    #### index


    ###
    #analysis_query(list(lst), small, transitive, cross, stats, result_id).apply_async()
    return result_id


@blueprint.route('/api/v1/query/analysis/result', methods=['GET'])
def fetch_analysis():
    red = redis.Redis(connection_pool=redis_pool)
    id = request.args.get('id', None)
    if id is not None:
        key = f'analysis:{id}'
        if red.exists(key):
            return jsonify(json.loads(red.get(key)))
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

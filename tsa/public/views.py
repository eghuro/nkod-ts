# -*- coding: utf-8 -*-
"""Public section, including homepage and signup."""
import logging
import uuid

import redis
import rfc3987
from atenvironment import environment
from celery import group
from flask import Blueprint, abort, current_app, jsonify, render_template, request

from tsa.tasks import analyze, analyze_upload, hello, system_check

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/', methods=['GET'])
def home():
    """Landing page."""
    return render_template('public/landing.html')


@blueprint.route('/api/v1/test/base')
def test_basic():
    return 'Hello world!'


@blueprint.route('/api/v1/test/job')
def test_celery():
    r = hello.delay()
    return r.get()


@blueprint.route('/api/v1/test/system')
def test_system():
    x = (system_check.s() | hello.si()).delay().get()
    log = logging.getLogger(__name__)
    log.info(f'System check result: {x!s}')
    return str(x)


@blueprint.route('/api/v1/analyze', methods=['GET'])
def api_analyze_iri():
    iri = request.args.get('iri', None)
    etl = bool(int(request.args.get('etl', 1)))

    current_app.logger.info(f'ETL:{etl!s}')

    if rfc3987.match(iri):
        return jsonify(analyze.delay(iri, etl).get())
    else:
        abort(400)


@blueprint.route('/api/v1/analyze', methods=['POST'])
@environment('REDIS')
def api_analyze_upload(redis_url):
    etl = bool(int(request.args.get('etl', 1)))

    def read_in_chunks(file_object, chunk_size=1024):
        """Lazy function (generator) to read a file piece by piece.

        Default chunk size: 1k.
        """
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data

    keys = []
    mimes = []
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    for file in request.files:
        key = str(uuid.uuid4())
        keys.append(key)
        mimes.append(file.mimetype)
        for piece in read_in_chunks(file):
            r.append(key, piece)
        r.expire(key, 60)

    g = group(analyze_upload.s(k, m, etl) for k, m in zip(keys, mimes))
    return jsonify(g.apply_async().get())


@blueprint.route('/api/v1/query')
@environment('REDIS')
def index(redis_url):
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    iri = request.args.get('iri', None)
    current_app.logger.info(f'Querying for: {iri}')
    if rfc3987.match(iri):
        if not r.exists(iri):
            abort(404)
        else:
            return jsonify([str(x) for x in r.smembers(iri)])
    else:
        abort(400)

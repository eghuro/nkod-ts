# -*- coding: utf-8 -*-
"""Public section, including homepage and signup."""
from collections import defaultdict
from flask import abort, Blueprint, jsonify, render_template, request, url_for
import rfc3987
from tsa.tasks import hello, analyze

blueprint = Blueprint('public', __name__, static_folder='../static')


@blueprint.route('/', methods=['GET'])
def home():
    """Landing page."""
    return render_template('public/landing.html')

@blueprint.route('/api/1/test/base')
def test_basic():
    return ""

@blueprint.route('/api/1/test/job')
def test_celery():
    r = hello.delay()
    return r.get()

@blueprint.route('/api/1/analyze')
def api_analyze():
    iri = request.args.get('iri', None)
    if rfc3987.match(iri):
        task = analyze.delay(iri)
        return "", 202, {'Location': url_for('public.check_status', task_id=task.id)}
    else:
        abort(400)

@blueprint.route('/api/1/analyze/status/<task_id>')
def check_status(task_id):
    task = analyze.AsyncResult(task_id)
    def default(value):
        return { 'state': value.state, 'status': str(value.info) }

    return jsonify(defaultdict(default,
        PENDING={ 'state': task.state, 'status': 'Pending' },
        SUCCESS={ 'state': task.state, 'status': 'Completed' },
        FAILURE={ 'state': task.state, 'status': 'Failed' }
    )[task.state])

@blueprint.route('/api/1/query')
def query():
    iri = request.args.get('iri', None)
    if rfc3987.match(iri):
        return ""
    else:
        abort(400)

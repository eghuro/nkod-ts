import logging
from flask import Blueprint
from tsa.tasks.system import hello, system_check


blueprint = Blueprint('test', __name__, static_folder='../static')


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

"""Celery tasks invoked from the API endpoints."""
import logging

import redis
from atenvironment import environment

from tsa.celery import celery


@celery.task
@environment('REDIS')
def system_check(redis_url):
    """Runs an availability test of additional systems.

    Tested are: redis.
    """
    log = logging.getLogger(__name__)
    log.info('System check started')

    log.info(f'Testing redis, URL: {redis_url}')
    r = redis.StrictRedis().from_url(redis_url)
    r.ping()
    log.info('System check successful')


@celery.task
def hello():
    """Dummy task returning hello world used for testing of Celery."""
    return 'Hello world!'

# -*- coding: utf-8 -*-
"""Application configuration."""

import os
import uuid

import raven


class Config(object):
    """Base configuration."""

    SECRET_KEY = os.environ.get('NKOD_TSA_SECRET', str(uuid.uuid4()))
    APP_DIR = os.path.abspath(os.path.dirname(__file__))  # This directory
    PROJECT_ROOT = os.path.abspath(os.path.join(APP_DIR, os.pardir))
    RELEASE = raven.fetch_git_sha(PROJECT_ROOT)
    DEBUG_TB_ENABLED = False  # Disable Debug toolbar
    DEBUG_TB_INTERCEPT_REDIRECTS = False
    CACHE_TYPE = 'redis'  # Can be "memcached", "redis", etc.
    CELERY_BROKER_URL = os.environ['REDIS']
    CELERY_RESULT_BACKEND = os.environ['REDIS']
    REDIS_HOST = 'redis://redis'
    REDIS_PORT = 6379
    REDIS_DB = 0
    CACHE_KEY_PREFIX = 'fcache'
    CACHE_REDIS_HOST = 'redis://redis'
    CACHE_REDIS_PORT = '6379'
    CACHE_REDIS_URL = os.environ['REDIS']
    SENTRY_CONFIG = {
        'dsn': 'https://9df1f926d1854fa4884d1f0ce9489a0b@sentry.io/1304923',
        'release': RELEASE,
    }

    MAX_CONTENT_LENGTH = 1024 * 1024 * 1024
    DEFAULT_EXPIRE = 30 * 24 * 60 * 60  # 30D


class ProdConfig(Config):
    """Production configuration."""

    ENV = 'prod'
    DEBUG = False
    DEBUG_TB_ENABLED = False  # Disable Debug toolbar


class DevConfig(Config):
    """Development configuration."""

    ENV = 'dev'
    DEBUG = True
    DEBUG_TB_ENABLED = True
    CACHE_TYPE = 'simple'  # Can be "memcached", "redis", etc.


class TestConfig(Config):
    """Test configuration."""

    TESTING = True
    DEBUG = True

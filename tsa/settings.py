# -*- coding: utf-8 -*-
"""Application configuration."""

import os

import raven


class Config(object):
    """Base configuration."""

    SECRET_KEY = os.environ.get('NKOD_TSA_SECRET', '017bc90c-82fe-4452-b7a7-853d100ad35a')  # TODO: Change me
    APP_DIR = os.path.abspath(os.path.dirname(__file__))  # This directory
    PROJECT_ROOT = os.path.abspath(os.path.join(APP_DIR, os.pardir))
    RELEASE = raven.fetch_git_sha(PROJECT_ROOT)
    BCRYPT_LOG_ROUNDS = 13
    DEBUG_TB_ENABLED = False  # Disable Debug toolbar
    DEBUG_TB_INTERCEPT_REDIRECTS = False
    CACHE_TYPE = 'redis'  # Can be "memcached", "redis", etc.
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    WEBPACK_MANIFEST_PATH = 'webpack/manifest.json'
    CELERY_BROKER_URL = os.environ['REDIS']
    CELERY_RESULT_BACKEND = os.environ['REDIS']
    REDIS_HOST = 'redis'
    REDIS_PORT = 6379
    REDIS_DB = 0
    CACHE_KEY_PREFIX = 'fcache'
    CACHE_REDIS_HOST = 'redis'
    CACHE_REDIS_PORT = '6379'
    CACHE_REDIS_URL = os.environ['REDIS']
    SENTRY_CONFIG = {
        'dsn': 'https://9df1f926d1854fa4884d1f0ce9489a0b@sentry.io/1304923',
        'release': RELEASE,
    }


class ProdConfig(Config):
    """Production configuration."""

    ENV = 'prod'
    DEBUG = False
    SQLALCHEMY_DATABASE_URI = 'postgresql://alex@localhost/nkod'
    DEBUG_TB_ENABLED = False  # Disable Debug toolbar


class DevConfig(Config):
    """Development configuration."""

    ENV = 'dev'
    DEBUG = True
    DB_NAME = 'dev.db'
    # Put the db file in project root
    DB_PATH = os.path.join(Config.PROJECT_ROOT, DB_NAME)
    SQLALCHEMY_DATABASE_URI = 'sqlite:///{0}'.format(DB_PATH)
    DEBUG_TB_ENABLED = True
    CACHE_TYPE = 'simple'  # Can be "memcached", "redis", etc.


class TestConfig(Config):
    """Test configuration."""

    TESTING = True
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'sqlite://'
    BCRYPT_LOG_ROUNDS = 4  # For faster tests; needs at least 4 to avoid "ValueError: Invalid rounds"
    WTF_CSRF_ENABLED = False  # Allows form testing

"""Celery setup - raven hook and configuration."""

import celery
import sentry_sdk
from atenvironment import environment
from sentry_sdk.integrations.celery import CeleryIntegration


@environment('DSN')
def init_sentry(dsn_str):
    sentry_sdk.init(dsn_str, integrations=[CeleryIntegration()])

init_sentry()
celery = celery.Celery()
celery.config_from_object('tsa.celeryconfig')

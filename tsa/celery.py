"""Celery setup - raven hook and configuration."""

import celery
import raven
from raven.contrib.celery import register_logger_signal, register_signal


class Celery(celery.Celery):
    """Wrapper over Celery class providing Raven hook for logging."""

    def on_configure(self):
        """Hook Raven for logging."""
        client = raven.Client('https://9df1f926d1854fa4884d1f0ce9489a0b@sentry.io/1304923')

        # register a custom filter to filter out duplicate logs
        register_logger_signal(client)

        # hook into the Celery error handler
        register_signal(client)


celery = Celery()
celery.config_from_object('tsa.celeryconfig')

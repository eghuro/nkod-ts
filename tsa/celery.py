"""Celery setup - raven hook and configuration."""

import celery
import raven
from raven.contrib.celery import register_signal, register_logger_signal


class Celery(celery.Celery):
    def on_configure(self):
        client = raven.Client('https://d1b87aca769e489fb67debf20df91c0b:032bb57bcfc94024a43434e5a266ab40@sentry.io/1260183')

        # register a custom filter to filter out duplicate logs
        register_logger_signal(client)

        # hook into the Celery error handler
        register_signal(client)


celery = Celery()
celery.config_from_object('tsa.celeryconfig')

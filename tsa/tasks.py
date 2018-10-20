"""Celery tasks invoked from the API endpoints."""
from tsa.celery import celery

@celery.task
def hello():
    return "Hello world!"

@celery.task
def analyze(iri):
    return ""

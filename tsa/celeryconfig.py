"""Celery configuration."""
import os

broker_url = os.environ['REDIS']
broker_pool_limit = 100
result_backend = os.environ['REDIS']
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Europe/Prague'
enable_utc = True
include = ['tsa.tasks']

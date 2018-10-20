"""Celery configuration."""

broker_url = 'redis://redis:6379/0'
broker_pool_limit = 100
result_backend = 'redis://redis:6379/0'
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Europe/Prague'
enable_utc = True
include = ['tsa.tasks']

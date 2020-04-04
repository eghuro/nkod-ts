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
include = ['tsa.tasks.analyze', 'tsa.tasks.batch', 'tsa.tasks.index', 'tsa.tasks.query', 'tsa.tasks.system']
broker_transport_options = {
    'fanout_prefix': True,
    'fanout_patterns': True
}
task_create_missing_queues = True
task_default_queue = 'default'
task_routes = {
    'tsa.tasks.analyze.analyze_priority': {
        'queue': 'high_priority'
    },

    'tsa.tasks.analyze.process_endpoint': {
        'queue': 'high_priority'
    },

    'tsa.tasks.analyze.analyze_named': {
        'queue': 'high_priority'
    },

    'tsa.tasks.analyze.run_one_named_analyzer': {
        'queue': 'high_priority'
    },

    'tsa.tasks.analyze.run_one_analyzer': {
        'queue': 'high_priority'
    },

    'tsa.tasks.analyze.run_analyzer': {
        'queue': 'high_priority'
    },

    'tsa.tasks.analyze.store_named_analysis': {
        'queue': 'high_priority'
    },

    'tsa.tasks.index.index_named': {
        'queue': 'high_priority'
    },

    'tsa.tasks.index.run_one_named_indexer': {
        'queue': 'high_priority'
    },

    'tsa.tasks.index.index': {
        'queue': 'high_priority'
    },

    'tsa.tasks.index.run_one_indexer': {
        'queue': 'high_priority'
    },
}

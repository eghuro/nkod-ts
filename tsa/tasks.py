"""Celery tasks invoked from the API endpoints."""
import json
import logging
from urllib.parse import urlparse

import rdflib
import redis
import requests
from atenvironment import environment
from rdflib import URIRef

from tsa.analyzer import Analyzer
from tsa.celery import celery
from tsa.transformation import PipelineFactory


@celery.task
@environment('ETL', 'VIRTUOSO', 'REDIS')
def system_check(etl, virtuoso, redis_url):
    log = logging.getLogger(__name__)
    log.info('System check started')
    log.info(f'Testing LP-ETL, URL: {etl!s}')
    requests.get(etl).raise_for_status()

    virtuoso_url = f'{virtuoso!s}/sparql'
    log.info(f'Testing virtuoso, URL: {virtuoso_url}')
    requests.get(virtuoso_url).raise_for_status()

    r = redis.StrictRedis().from_url(redis_url)
    r.ping()
    log.info('System check successful')


@celery.task
def hello():
    return 'Hello world!'


@celery.task
def analyze(iri, etl=True):
    log = logging.getLogger(__name__)
    log.info(f'Analyzing {iri!s}')
    if etl:
        (transform.s(iri) | poll.s() | inspect.s()).apply_async()
    else:
        #TODO: split into sub-tasks: parse -> index | analyze
        guess = rdflib.util.guess_format(iri)
        if guess is None:
            r = requests.head(iri)
            r.raise_for_status()
            guess = r.headers.get('content-type')
        g = rdflib.ConjunctiveGraph()
        log.info(f'Guessing format to be {guess!s}')
        g.parse(iri, format=guess)

        a = Analyzer(iri)
        index(g, analyzer)
        return a.analyze(g)


@environment('REDIS')
def index(g, analyzer, redis_cfg):
    r = redis.StrictRedis.from_url(redis_cfg)
    pipe = r.pipeline()
    exp = 60 * 60  # 1H

    for iri_a, iri_b in analyzer.find_related(g):
        pipe.sadd(iri_a, iri_b)
        pipe.sadd(iri_b, iri_a)

        pipe.expire(iri_a, exp)
        pipe.expire(iri_b, exp)
    pipe.execute()


@celery.task
@environment('REDIS')
def analyze_upload(key, mime, etl, redis_cfg):
    log = logging.getLogger(__name__)
    r = redis.StrictRedis.from_url(redis_cfg)
    if r.strlen(key) < 1024 * 1024:  # approx 1MB
        g = rdflib.ConjunctiveGraph()
        g.parse(data=r.get(key), format=mime)
        a = Analyzer()
        return a.analyze(g)
    else:
        log.warn(f"Not analyzing an upload as it's too big: {key!s}")
        r.delete(key)


@celery.task
def inspect(iri):
    g = rdflib.ConjunctiveGraph()
    g.parse(iri)
    a = Analyzer()
    return a.analyze(g)


@celery.task
@environment('ETL', 'VIRTUOSO', 'DBA_PASSWORD')
def transform(iri, etl, virtuoso, dba_pass):
    log = logging.getLogger(__name__)
    # create pipeline and call to start executions
    # prepare JSON-LD pipeline

    log.info(f'Prepare pipeline for {iri!s}')
    pf = PipelineFactory()
    p = urlparse(virtuoso)
    pipeline = json.dumps(pf.create_pipeline(iri, {'server': p.hostname,
                                                   'port': 1111,
                                                   'user': 'dba',
                                                   'password': dba_pass,
                                                   'iri': iri}))

    log.info(f'Pipeline:\n{pipeline!s}')

    # create the pipeline
    r = requests.post(f'{etl!s}/resources/pipelines', files={'pipeline': pipeline})
    r.raise_for_status()

    g = rdflib.ConjunctiveGraph()
    g.parse(data=r.text, format='trig')

    pipeline = g.value(object=URIRef('http://linkedpipes.com/ontology/Pipeline'), predicate=rdflib.namespace.RDF.type)
    log.info(f'Pipeline IRI: {pipeline!s}')

    # POST /resources/executions
    r = requests.post(f'{etl!s}/resources/executions?pipeline={pipeline}')
    r.raise_for_status()
    log.info(f'Execution trigger result:\n{r.json()!s}')
    return f"{etl!s}/resources/executions/{r.json()['iri'].split('/')[-1]}"


@celery.task(bind=True, retry_backoff=True, max_retries=None, default_retry_delay=30, time_limit=60 * 60)
def poll(self, iri):
    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        cleanup.apply_async()
    self.after_return = after_return

    log = logging.getLogger(__name__)
    log.info(f'Polling {iri!s}')

    r = requests.get(iri + '/overview')
    content = r.text
    log.info(content)
    r.raise_for_status()

    j = json.loads(content)
    if j['status']['@id'] == 'http://etl.linkedpipes.com/resources/status/failed':
        log.error('Execution failed')

        try:
            r = requests.get(iri + '/logs')
            r.raise_for_status()
            log.error('ETL log:\n' + r.text)
        except requests.HTTPError as e:
            raise EtlJobFailed(r) from e

        raise EtlJobFailed(r)
    elif not (j['status']['@id'] == 'http://etl.linkedpipes.com/resources/status/finished'):
        log.info('Execution is not finished yet')
        self.retry()
    else:
        # get result uri
        result = ''
        return result


@celery.task
@environment('ETL')
def cleanup(iri, etl):
    log = logging.getLogger(__name__)
    log.info(f'Deleting {iri!s}')

    r = requests.delete(f'{etl!s}/pipelines?iri={iri!s}')
    r.raise_for_status()

    log.info(f'Pipeline {iri!s} deleted')


class EtlError(Exception):
    pass


class EtlJobFailed(EtlError):
    pass

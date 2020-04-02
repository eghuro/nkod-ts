"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging

import rdflib
import redis
import rfc3987
from celery import group
from rdflib import Namespace
from rdflib.namespace import RDF

from tsa.celery import celery
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.extensions import redis_pool
from tsa.tasks.analyze import analyze, process_endpoint


def _log_dataset_distribution(g, log, access, endpoint, red):
    dcat = Namespace('http://www.w3.org/ns/dcat#')
    accesses = frozenset(access + endpoint)
    pipe = red.pipeline()
    for ds in g.subjects(RDF.type, dcat.Dataset):
        pipe.sadd('dcatds', str(ds))
        key = f'dsdistr:{ds!s}'
        pipe.sadd('purgeable', 'dcatds', key)
        for dist in g.objects(ds, dcat.distribution):
            for accessURL in g.objects(dist, dcat.accessURL):
                if str(accessURL) in accesses:
                    log.debug('Distribution {accessURL!s} from DCAT dataset {ds!s}')
                    pipe.sadd(key, str(accessURL))
    pipe.execute()


@celery.task
def inspect_catalog(key):
    """Analyze DCAT datasets listed in the catalog."""
    log = logging.getLogger(__name__)
    red = redis.Redis(connection_pool=redis_pool)

    log.debug('Parsing graph')
    try:
        g = rdflib.ConjunctiveGraph()
        g.parse(data=red.get(key), format='N3')
    except rdflib.plugin.PluginException:
        log.debug('Failed to parse graph')
        return 0

    distributions = []
    endpoints = []
    dcat = Namespace('http://www.w3.org/ns/dcat#')
    for d in g.subjects(RDF.type, dcat.Distribution):
        for access in g.objects(d, dcat.accessURL):
            if rfc3987.match(str(access)):
                distributions.append(str(access))
            else:
                log.warn(f'{access!s} is not a valid access URL')
    for dataset in g.subjects(RDF.type, rdflib.URIRef('http://rdfs.org/ns/void#Dataset')):
        for dump in g.objects(dataset, rdflib.URIRef('http://rdfs.org/ns/void#dataDump')):
            if rfc3987.match(str(dump)):
                distributions.append(str(dump))
            else:
                log.warn(f'{dump!s} is not a valid dump URL')
        for endpoint in g.objects(dataset, rdflib.URIRef('http://rdfs.org/ns/void#sparqlEndpoint')):
            if rfc3987.match(str(endpoint)):
                endpoints.append(str(endpoint))
            else:
                log.warn(f'{endpoint!s} is not a valid endpoint URL')

    _log_dataset_distribution(g, log, distributions, endpoints, red)

    tasks = [analyze.si(a) for a in distributions]
    tasks.extend(process_endpoint.si(e) for e in endpoints)
    return group(tasks).apply_async()


@celery.task
def inspect_endpoint(iri):
    """Extract DCAT datasets from the given endpoint and schedule their analysis."""
    inspector = SparqlEndpointAnalyzer()
    return group(inspect_catalog.si(key) for key in inspector.peek_endpoint(iri)).apply_async()


@celery.task
def inspect_graph(endpoint_iri, graph_iri):
    inspector = SparqlEndpointAnalyzer()
    return inspect_catalog.si(inspector.process_graph(endpoint_iri, graph_iri)).apply_async()
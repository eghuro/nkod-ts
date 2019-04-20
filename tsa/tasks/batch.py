"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging

import rdflib
import redis
from atenvironment import environment
from rdflib.namespace import RDF
from rdflib import Namespace

from tsa.celery import celery
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.tasks.analyze import analyze, process_endpoint


def _log_dataset_distribution(g, r, log, access, endpoint):
    dcat = Namespace('http://www.w3.org/ns/dcat#')
    accesses = frozenset(access + dump + endpoint)
    pipe = r.pipeline()
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
@environment('REDIS')
def inspect_catalog(key, redis_cfg):
    """Analyze DCAT datasets listed in the catalog."""
    log = logging.getLogger(__name__)
    r = redis.StrictRedis.from_url(redis_cfg)
    # key = f'data:{iri!s}'

    log.debug('Parsing graph')
    try:
        g = rdflib.ConjunctiveGraph()
        g.parse(data=r.get(key), format='turtle')
    except rdflib.plugin.PluginException:
        log.debug('Failed to parse graph')
        return 0

    distributions = []
    endpoints = []
    dcat = Namespace('http://www.w3.org/ns/dcat#')
    for d in g.subjects(RDF.type, dcat.Distribution):
        for access in g.objects(d, dcat.accessURL):
            distributions.append(str(access))
    for dataset in g.subjects(RDF.type, rdflib.URIRef('http://rdfs.org/ns/void#Dataset')):
        for dump in g.objects(dataset, rdflib.URIRef('http://rdfs.org/ns/void#dataDump')):
            distributions.append(str(dump))
        for endpoint in g.objects(dataset, rdflib.URIRef('http://rdfs.org/ns/void#sparqlEndpoint')):
            endpoints.append(str(endpoint))

    _log_dataset_distribution(g, r, log, distributions, endpoints)

    tasks = [analyze.si(a) for a in distributions]
    tasks.extend(process_endpoint.si(e) for e in endpoints)
    return group(tasks).apply_async()


@celery.task
def inspect_endpoint(iri):
    """Extract DCAT datasets from the given endpoint and schedule their analysis."""
    inspector = SparqlEndpointAnalyzer()
    return group(inspect_catalog.si(key) for key in inspector.peek_endpoint(iri)).apply_async()

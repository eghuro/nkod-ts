"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging
import redis
import rdflib
from atenvironment import environment
from tsa.celery import celery
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.tasks.analyze import analyze, process_endpoint


@celery.task
@environment('REDIS')
def inspect_catalog(iri, redis_cfg):
    """Analyze DCAT datasets listed in the catalog."""
    log = logging.getLogger(__name__)
    r = redis.StrictRedis.from_url(redis_cfg)
    key = f'data:{iri!s}'

    log.debug('Parsing graph')
    try:
        g = rdflib.ConjunctiveGraph()
        g.parse(data=r.get(key), format=format_guess)
    except rdflib.plugin.PluginException:
        log.debug('Failed to parse graph')
        return 0

    for distribution in g.subjects(RDF.type, rdflib.URIRef('http://www.w3.org/ns/dcat#Distribution')):
        for access in g.objects(d, rdflib.URIRef('http://www.w3.org/ns/dcat#accessURL')):
            log.debug(f'Scheduling analysis of {access!s}')
            analyze.si(str(access)).delay()
    for dataset in g.subjects(RDF.type, rdflib.URIRef('http://rdfs.org/ns/void#Dataset')):
        for dump in g.objects(dataset, rdflib.URIRef('http://rdfs.org/ns/void#dataDump')):
            log.debug(f'Scheduling analysis of {access!s}')
            analyze.si(str(dump)).delay()
        for endpoint in g.objects(dataset, rdflib.URIRef('http://rdfs.org/ns/void#sparqlEndpoint')):
            log.debug(f'Scheduling analysis of {endpoint!s}')
            process_endpoint.si(str(endpoint)).delay()

@celery.task
def inspect_endpoint(iri):
    inspector = SparqlEndpointAnalyzer()
    inspector.peek_endpoint(iri)
    inspect_catalog.delay(iri)

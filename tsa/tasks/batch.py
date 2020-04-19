"""Celery tasks for batch processing of endpoiint or DCAT catalog."""
import logging

import rdflib
import rfc3987
from celery import group
from rdflib import Namespace
from rdflib.namespace import RDF

from tsa.celery import celery
from tsa.endpoint import SparqlEndpointAnalyzer
from tsa.tasks.process import process, process_endpoint
from tsa.tasks.common import TrackableTask


def _log_dataset_distribution(g, log, access, endpoint, red):
    dcat = Namespace('http://www.w3.org/ns/dcat#')
    accesses = frozenset(access + endpoint)
    with red.pipeline() as pipe:
        for ds in g.subjects(RDF.type, dcat.Dataset):
            for dist in g.objects(ds, dcat.distribution):
                for downloadURL in g.objects(dist, dcat.downloadURL):
                    if str(downloadURL) in accesses:
                        log.debug('Distribution {downloadURL!s} from DCAT dataset {ds!s}')
                        # pipe.hmset('dsdistr', str(ds), str(downloadURL))
                        # as for now we use batch approach, so this might not be needed
                        pipe.hset('distrds', str(downloadURL), str(ds))  # reverse mapping in hashset
                        # this doesn't allow one downloadURL to be in multiple DS
            # TODO: log here DCAT2 services as well
        pipe.sadd('purgeable', 'dsdistr', 'distrds')
        # TODO: expire


def _dcat_extractor(g, red, log):
    distributions, distributions_priority = [], []
    endpoints = []
    dcat = Namespace('http://www.w3.org/ns/dcat#')
    dcterms = Namespace('http://purl.org/dc/terms/')
    media_priority = set([
        'https://www.iana.org/assignments/media-types/application/rdf+xml',
        'https://www.iana.org/assignments/media-types/application/trig',
        'https://www.iana.org/assignments/media-types/text/n3',
        'https://www.iana.org/assignments/media-types/application/ld+json',
        'https://www.iana.org/assignments/media-types/application/n-triples',
        'https://www.iana.org/assignments/media-types/application/n-quads',
        'https://www.iana.org/assignments/media-types/text/turtle'
    ]) #IANA
    format_priority = set([
        'http://publications.europa.eu/resource/authority/file-type/RDF',
        'http://publications.europa.eu/resource/authority/file-type/RDFA',
        'http://publications.europa.eu/resource/authority/file-type/RDF_N_QUADS',
        'http://publications.europa.eu/resource/authority/file-type/RDF_N_TRIPLES',
        'http://publications.europa.eu/resource/authority/file-type/RDF_TRIG',
        'http://publications.europa.eu/resource/authority/file-type/RDF_TURTLE',
        'http://publications.europa.eu/resource/authority/file-type/RDF_XML',
        'http://publications.europa.eu/resource/authority/file-type/JSON_LD',
        'http://publications.europa.eu/resource/authority/file-type/N3'
    ]) #EU
    queue = distributions

    log.info("Extracting distributions")
    #DCAT Distribution
    for d in g.subjects(RDF.type, dcat.Distribution):
        # put RDF distributions into a priority queue
        for media in g.objects(d, dcat.mediaType):
            if str(media) in media_priority:
                queue = distributions_priority

        for format in g.objects(d, dcterms.format):
            if str(format) in format_priority:
                queue = distributions_priority

        # access URL to files
        for access in g.objects(d, dcat.downloadURL):
            if rfc3987.match(str(access)):
                queue.append(str(access))
            else:
                log.warn(f'{access!s} is not a valid access URL')

        # TODO: scan DCAT2 data services here as well

    # TODO: scan for service description as well

    # VOID Dataset implies RDF
    for dataset in g.subjects(RDF.type, rdflib.URIRef('http://rdfs.org/ns/void#Dataset')):
        for dump in g.objects(dataset, rdflib.URIRef('http://rdfs.org/ns/void#dataDump')):
            if rfc3987.match(str(dump)):
                distributions_priority.append(str(dump))
            else:
                log.warn(f'{dump!s} is not a valid dump URL')
        for endpoint in g.objects(dataset, rdflib.URIRef('http://rdfs.org/ns/void#sparqlEndpoint')):
            if rfc3987.match(str(endpoint)):
                endpoints.append(str(endpoint))
            else:
                log.warn(f'{endpoint!s} is not a valid endpoint URL')

    _log_dataset_distribution(g, log, distributions + distributions_priority, endpoints, red)

    tasks = [process_priority.si(a) for a in distributions_priority]
    tasks.extend(process_endpoint.si(e) for e in endpoints)
    tasks.extend(process.si(a) for a in distributions)
    return group([tasks[0]]).apply_async()


@celery.task(base=TrackableTask)
def inspect_catalog(key):
    """Analyze DCAT datasets listed in the catalog."""
    log = logging.getLogger(__name__)
    red = inspect_catalog.redis

    log.debug('Parsing graph')
    try:
        g = rdflib.ConjunctiveGraph()
        g.parse(data=red.get(key), format='n3')
    except rdflib.plugin.PluginException:
        log.debug('Failed to parse graph')
        return None

    return _dcat_extractor(g, red, log)


@celery.task(base=TrackableTask)
def inspect_endpoint(iri):
    """Extract DCAT datasets from the given endpoint and schedule their analysis."""
    inspector = SparqlEndpointAnalyzer()
    return group(inspect_catalog.si(key) for key in inspector.peek_endpoint(iri)).apply_async()


@celery.task(base=TrackableTask)
def inspect_graph(endpoint_iri, graph_iri):
    inspector = SparqlEndpointAnalyzer()
    log = logging.getLogger(__name__)
    red = inspect_graph.redis
    return _dcat_extractor(inspector.process_graph(endpoint_iri, graph_iri, False), red, log)


@celery.task(base=TrackableTask)
def cleanup_batches():
    redis = cleanup_batches.redis
    root = 'batch:'
    log = logging.getLogger(__name__)
    for key in redis.keys(f'{root}*'):
        batch_id = key[len(root):]
        for task_id in red.smembers(key):
            task = TrackableTask.AsyncResult(task_id)
            if task.state in ['SUCCESS', 'FAILURE']:
                log.warning(f'Removing {task_id} as it is {task.state}')
                redis.srem(key, task_id)
                redis.hdel('taskBatchId', task_id)
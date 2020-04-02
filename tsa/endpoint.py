"""SPARQL endpoint utilities."""
import logging

import redis
import rfc3987
from rdflib import Graph

from tsa.extensions import redis_pool


class SparqlEndpointAnalyzer(object):
    """Extract DCAT datasets from a SPARQL endpoint."""

    def __query(self, endpoint, named=None):
        str1 = """
        construct {
          ?ds a <http://www.w3.org/ns/dcat#Dataset>;
          <http://www.w3.org/ns/dcat#keyword> ?keyword;
          <http://purl.org/dc/terms/accrualPeriodicity> ?accrualPeriodicity;
          <http://purl.org/dc/terms/contactPoint> ?contactPoint;
          <http://purl.org/dc/terms/description> ?description;
          <http://purl.org/dc/terms/language> ?language;
          <http://purl.org/dc/terms/modified> ?modified;
          <http://purl.org/dc/terms/title> ?title;
          <http://purl.org/dc/terms/publisher> ?publisher;
          <http://purl.org/dc/terms/rightsHolder> ?holder;
          <http://purl.org/dc/terms/spatial> ?spatial;
          <http://purl.org/dc/terms/language> ?language;
          <http://www.w3.org/ns/dcat#distribution> ?d.

          ?d a <http://www.w3.org/ns/dcat#Distribution>;
          <http://purl.org/dc/terms/title> ?dist_title;
          <http://www.w3.org/ns/dcat#accessURL> ?accessURL;
          <http://purl.org/dc/terms/format> ?format.

          ?d a <http://www.w3.org/ns/dcat#Distribution>;
          <http://purl.org/dc/terms/title> "SPARQL Endpoint";
          <http://purl.org/dc/terms/description> "SPARQL Endpoint";
          <http://www.w3.org/ns/dcat#accessURL>
          """

        str2 = """
         ?void a <http://rdfs.org/ns/void#Dataset>;
         <http://rdfs.org/ns/void#dataDump> ?dump;
         <http://rdfs.org/ns/void#exampleResource> ?exampleResource;
         <http://rdfs.org/ns/void#sparqlEndpoint> ?sparqlEndpoint;
         <http://rdfs.org/ns/void#triples> ?triples.
       }
       """

        str3 = """
       where {
         ?ds a <http://www.w3.org/ns/dcat#Dataset>;
         <http://purl.org/dc/terms/title> ?title.
         OPTIONAL { ?ds <http://purl.org/dc/terms/publisher> ?publisher. }
         OPTIONAL { ?ds <http://purl.org/dc/terms/language> ?language. }

         OPTIONAL { ?ds <http://purl.org/dc/terms/accrualPeriodicity> ?accrualPeriodicity. }
         OPTIONAL { ?ds <http://purl.org/dc/terms/contactPoint> ?contactPoint. }
         OPTIONAL { ?ds <http://purl.org/dc/terms/description> ?description. }
         OPTIONAL { ?ds <http://purl.org/dc/terms/language> ?language. }
         OPTIONAL { ?ds <http://purl.org/dc/terms/modified> ?modified. }
         OPTIONAL { ?ds <http://purl.org/dc/terms/title> ?title. }
         OPTIONAL { ?ds <http://purl.org/dc/terms/publisher> ?publisher. }
         OPTIONAL { ?ds <http://purl.org/dc/terms/rightsHolder> ?holder. }
         OPTIONAL { ?ds <http://purl.org/dc/terms/spatial> ?spatial. }
         OPTIONAL { ?ds <http://purl.org/dc/terms/language> ?language. }
         OPTIONAL { ?ds <http://www.w3.org/ns/dcat#keyword> ?keyword. }
         OPTIONAL { ?ds <http://www.w3.org/ns/dcat#distribution> ?d.
           ?d a <http://www.w3.org/ns/dcat#Distribution>.
           OPTIONAL { ?d <http://purl.org/dc/terms/title> ?dist_title. }
           OPTIONAL { ?d <http://www.w3.org/ns/dcat#accessURL> ?accessURL. }
           OPTIONAL { ?d <http://purl.org/dc/terms/format> ?format. }
         }

         OPTIONAL {
             ?void a <http://rdfs.org/ns/void#Dataset>.
             OPTIONAL { ?void <http://rdfs.org/ns/void#dataDump> ?dump. }
             OPTIONAL { ?void <http://rdfs.org/ns/void#exampleResource> ?exampleResource. }
             OPTIONAL { ?void <http://rdfs.org/ns/void#sparqlEndpoint> ?sparqlEndpoint. }
             OPTIONAL { ?void <http://rdfs.org/ns/void#triples> ?triples. }
         }
       }
       """

        if named is not None:
            return f'{str1} <{endpoint}>. {str2} from <{named}> {str3}'
        else:
            logging.getLogger(__name__).warn('No named graph when constructing catalog from {endpoint!s}')
            return f'{str1} <{endpoint}>. {str2} {str3}'

    def peek_endpoint(self, endpoint):
        """Extract DCAT datasets from the given endpoint and store them in redis."""
        log = logging.getLogger(__name__)
        if not rfc3987.match(endpoint):
            log.warn(f'{endpoint!s} is not a valid endpoint URL')
            return
        for graph_iri in self.get_graphs_from_endpoint(endpoint):
            ret = self.process_graph(endpoint, graph_iri)
            if ret is not None:
                yield ret

    def process_graph(self, endpoint, graph_iri):
        """Extract DCAT datasets from the given named graph of an endpoint and store them in redis."""
        log = logging.getLogger(__name__)
        if not rfc3987.match(endpoint):
            log.warn(f'{endpoint!s} is not a valid endpoint URL')
            return None
        if not rfc3987.match(graph_iri):
            log.warn(f'{graph_iri!s} is not a valid graph URL')
            return None

        g = Graph(store='SPARQLStore', identifier=graph_iri)
        g.open(endpoint)

        r = redis.Redis(connection_pool=redis_pool)
        key = f'data:{endpoint!s}:{graph_iri!s}'
        with r.pipeline() as pipe:
            pipe.set(key, g.serialize(format='N3'))
            pipe.sadd('purgeable', key)
            pipe.expire(key, 30 * 24 * 60 * 60)  # 30D
            pipe.execute()
        log.info(key)
        return key

    def get_graphs_from_endpoint(self, endpoint):
        """Extract named graphs from the given endpoint."""
        g = Graph(store='SPARQLStore')
        g.open(endpoint)
        for row in g.query('select distinct ?g where { GRAPH ?g {?s ?p ?o} }'):
            yield row['g']

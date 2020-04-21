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
          <http://purl.org/dc/terms/title> ?title;
          <http://www.w3.org/ns/dcat#distribution> ?d.

          ?d a <http://www.w3.org/ns/dcat#Distribution>;
          <http://www.w3.org/ns/dcat#downloadURL> ?downloadURL;
          <http://purl.org/dc/terms/format> ?format;
          <http://www.w3.org/ns/dcat#mediaType> ?media;
          <https://data.gov.cz/slovník/nkod/mediaType> ?mediaNkod.

          ?d <http://www.w3.org/ns/dcat#accessURL> ?accessPoint.
          ?accessPoint <http://www.w3.org/ns/dcat#endpointURL> ?endpointUrl;
          <http://www.w3.org/ns/dcat#endpointDescription> ?sd.
       }
       """

        str3 = """
       where {
         ?ds a <http://www.w3.org/ns/dcat#Dataset>.
         ?ds <http://purl.org/dc/terms/title> ?title.
         ?ds <http://www.w3.org/ns/dcat#distribution> ?d.
         OPTIONAL { ?d <http://www.w3.org/ns/dcat#downloadURL> ?downloadURL. }
         OPTIONAL { ?d <http://purl.org/dc/terms/format> ?format. }
         OPTIONAL { ?d <http://www.w3.org/ns/dcat#mediaType> ?media. }
         OPTIONAL { ?d <http://www.w3.org/ns/dcat#accessURL> ?accessPoint.
            ?accessPoint <http://www.w3.org/ns/dcat#endpointURL> ?endpointUrl.
            OPTIONAL { ?accessPoint <http://www.w3.org/ns/dcat#endpointDescription> ?sd. }
         }
         OPTIONAL { ?d <https://data.gov.cz/slovník/nkod/mediaType> ?mediaNkod. }
       }
       """

        if named is not None:
            return f'{str1} from <{named}> {str3}'
        else:
            logging.getLogger(__name__).warn('No named graph when constructing catalog from {endpoint!s}')
            return f'{str1} {str3}'

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

    def process_graph(self, endpoint, graph_iri, dump_result_to_redis=True):
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

        result = Graph()
        for s, p, o in g.query(self.__query(endpoint, graph_iri)):
            result.add( (s, p, o) )

        if dump_result_to_redis:
            r = redis.Redis(connection_pool=redis_pool)
            key = f'data:{endpoint!s}:{graph_iri!s}'
            with r.pipeline() as pipe:
                pipe.set(key, result.serialize(format='n3'))
                pipe.sadd('purgeable', key)
                pipe.expire(key, 30 * 24 * 60 * 60)  # 30D
                pipe.execute()
            log.info(key)
            return key
        else:
            return result

    def get_graphs_from_endpoint(self, endpoint):
        """Extract named graphs from the given endpoint."""
        g = Graph(store='SPARQLStore')
        g.open(endpoint)
        for row in g.query('select distinct ?g where { GRAPH ?g {?s ?p ?o} }'):
            yield row['g']

    # TODO
    # all above is extracting DCAT for use in batch
    # however we might have some real datasets there
    # -> service description
    # and VOID


    #if we have SD of ?endpoint then use query '?x sd:endpoint ?endpoint; sd:namedGraph/sd:name ?name.' on SD to
    #get named graphs (taken from LPA)
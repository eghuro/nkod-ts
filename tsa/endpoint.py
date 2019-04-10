from SPARQLWrapper import SPARQLWrapper, N3
from rdflib import Graph, ConjunctiveGraph
from rdflib.namespace import RDF
import rdflib
from atenvironment import environment
import redis


class SparqlGraph(object):

    def __init__(self, endpoint):
        self.__sparql = SPARQLWrapper(endpoint, returnFormat=N3)

    def query(self, query_str):
        self.__sparql.setQuery(query_str)
        results = self.__sparql.query().convert()
        g = Graph()
        g.parse(data=results, format="n3")
        return g.query(query_str)

    def extract_graph(self):
        extractor = "construct {?s ?p ?o} where {?s ?p ?o}"
        self.__sparql.setQuery(extractor)
        ret = self.__sparql.query().convert()
        g = Graph()
        g.parse(data=ret, format="n3")
        return g


class SparqlEndpointAnalyzer(object):

    @environment('REDIS')
    def __init__(self, redis_url):
        r = redis.StrictRedis().from_url(redis_url)

    def __query(self, endpoint):
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
       } where {
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
        return  str1 + "<" + endpoint + ">." + str2

    def peek_endpoint(self, endpoint):
        sparql = SPARQLWrapper(endpoint, returnFormat=N3)
        sparql.setQuery(self.__query(endpoint))

        ret = sparql.query().convert()
        g = Graph()
        g.parse(data=ret, format="n3")

        key = f'data:{endpoint!s}'
        r.set(key, g.serialize(format='turtle'))
        r.expire(key, 60*60)
        return key

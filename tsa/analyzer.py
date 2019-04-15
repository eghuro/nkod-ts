"""Dataset analyzer."""

from abc import ABC
import logging
from collections import defaultdict

from rdflib import Namespace
from rdflib.namespace import RDF


class AbstractAnalyzer(ABC):
    pass


class CubeAnalyzer(AbstractAnalyzer):
    """RDF dataset analyzer focusing on DataCube."""

    token = 'cube'

    def find_relation(self, graph):
        """We consider DSs to be related if they share a resource on dimension."""
        log = logging.getLogger(__name__)
        log.info('Looking up resources used on a dimension')
        for ds, resource in self.__resource_on_dimension(graph):
            log.info(f'Dataset: {ds} - Resource on dimension: {resource}')
            yield resource, 'qb'

    def __dimensions(self, graph):
        d = defaultdict(set)
        qb_query = """
        SELECT ?dsd ?dimension
        WHERE {
            ?dsd a <http://purl.org/linked-data/cube#DataStructureDefinition>;
            <http://purl.org/linked-data/cube#component>/<http://purl.org/linked-data/cube#dimension> ?dimension.
        }
        """
        qres = graph.query(qb_query)
        for row in qres:
            d[row.dsd].add(row.dimension)

        return d

    def __dataset_dimensions(self, graph, dimensions):
        d = defaultdict(set)
        qb_query = """
        SELECT ?ds ?structure
        WHERE {
            ?ds a <http://purl.org/linked-data/cube#DataSet>;
            <http://purl.org/linked-data/cube#structure> ?structure.
        }
        """
        qres = graph.query(qb_query)
        for row in qres:
            if row.structure in dimensions.keys():
                d[row.ds].update(dimensions[row.structure])

        return d

    def __resource_on_dimension(self, graph):
        log = logging.getLogger(__name__)
        log.info('Looking up resources on dimensions')
        ds_dimensions = self.__dataset_dimensions(graph, self.__dimensions(graph))
        log.info(f'Dimensions: {ds_dimensions!s}')

        ds_query = """
            SELECT ?observation ?dataset
            WHERE {
                ?observation a <http://purl.org/linked-data/cube#Observation>;
                <http://purl.org/linked-data/cube#dataSet> ?dataset.
            }
        """
        qres0 = graph.query(ds_query)
        for row in qres0:
            for dimension in ds_dimensions[row.dataset]:
                qb_query = "SELECT ?resource WHERE { <" + str(row.observation) + "> <" + str(dimension) + "> ?resource. }"
                qres1 = graph.query(qb_query)
                for row1 in qres1:
                    yield row.dataset, row1.resource

    def analyze(self, graph):
        """Analysis of a datacube."""

        datasets = defaultdict(QbDataset)

        for row in graph.query("SELECT DISTINCT ?ds WHERE {?ds a <http://purl.org/linked-data/cube#DataSet>}"):
            dataset = row['ds']
            for row in graph.query(f'SELECT DISTINCT ?structure WHERE {{ <{dataset}> <http://purl.org/linked-data/cube#structure> ?structure }}'):
                structure = row['structure']
                for row in graph.query(f'SELECT DISTINCT ?component WHERE {{ <{structure}> <http://purl.org/linked-data/cube#component> ?component }}'):
                    component = row['component']

                    for row in graph.query(f'SELECT DISTINCT ?dimension WHERE {{ <{component}> <http://purl.org/linked-data/cube#dimension> ?dimension }}'):
                        dimension = row['dimension']
                        datasets[str(dataset)].dimensions.add(str(dimension))
                    for row in graph.query(f'SELECT DISTINCT ?measure WHERE {{ <{component}> <http://purl.org/linked-data/cube#measure> ?measure }}'):
                        measure = row['measure']
                        datasets[str(dataset)].measures.add(str(measure))

        d = {}
        for k in datasets.keys():
            d[k] = {}
            d[k]['dimensions'] = list(datasets[k].dimensions)
            d[k]['measures'] = list(datasets[k].measures)

        summary = {
            'datasets': d
        }

        return summary


class SkosAnalyzer(AbstractAnalyzer):

    token = 'skos'

    def analyze(self, graph):
        concept_count = dict()
        schemes_count = dict()
        top_concept = dict()

        concepts = [row['concept'] for row in graph.query("""
        SELECT DISTINCT ?concept WHERE {
            ?concept a <http://www.w3.org/2004/02/skos/core#Concept>.
        }
        """)]

        def count_query(concept):
            return f'SELECT ?a (count(?a) as ?count) WHERE {{ ?a ?b <{concept}>. }}'

        for c in concepts:
            for row in graph.query(count_query(c)):
                concept_count[c] = row['count']

        schemes = [row['scheme'] for row in graph.query("""
        SELECT DISTINCT ?scheme WHERE {
            OPTIONAL {?scheme a <http://www.w3.org/2004/02/skos/core#Scheme>.}
            OPTIONAL {?_ <http://www.w3.org/2004/02/skos/core#inScheme> ?scheme.}
        }
        """)]

        def scheme_count_query(scheme):
            return f'SELECT (count(*) as ?count) WHERE {{ ?_ <http://www.w3.org/2004/02/skos/core#inScheme> <{scheme}> }}'

        for schema in schemes:
            for row in graph.query(scheme_count_query(schema)):
                schemes_count[schema] = row['count']

        def scheme_top_concept(scheme):
            return f'SELECT ?concept WHERE {{ OPTIONAL {{ ?concept <http://www.w3.org/2004/02/skos/core#topConceptOf> <{scheme}>. }} OPTIONAL {{ <{scheme}> <http://www.w3.org/2004/02/skos/core#hasTopConcept> ?concept }} }}'

        for schema in schemes:
            top_concept[schema] = [row['concept'] for row in graph.query(scheme_top_concept(schema))]

        collections = [row['coll'] for row in graph.query("""
        SELECT DISTINCT ?coll WHERE {
            OPTIONAL { ?coll a <http://www.w3.org/2004/02/skos/core#Collection>. }
            OPTIONAL { ?coll a <http://www.w3.org/2004/02/skos/core#OrderedCollection>. }
            OPTIONAL { ?_ <http://www.w3.org/2004/02/skos/core#member> ?coll. }
            OPTIONAL { ?coll <http://www.w3.org/2004/02/skos/core#memberList> ?_. }
        }
        """)]

        ord_collections = [row['coll'] for row in graph.query("""
        SELECT DISTINCT ?coll WHERE {
            ?coll a <http://www.w3.org/2004/02/skos/core#OrderedCollection>.
        }
        """)]

        return {
            'concept':  concept_count,
            'schema': schemes_count,
            'topConcepts': top_concept,
            'collection': collections,
            'orderedCollection': ord_collections
        }


    def find_relation(self, graph):
        for row in graph.query("SELECT DISTINCT ?scheme WHERE {?a <http://www.w3.org/2004/02/skos/core#inScheme> ?scheme}"):
            yield row['scheme'], 'inScheme'

        for row in graph.query("SELECT DISTINCT ?collection WHERE {?collection <http://www.w3.org/2004/02/skos/core#member> ?a}"):
            yield row['collection'], 'collection'

        for row in graph.query("""
        SELECT ?a ?b WHERE {
            ?a <http://www.w3.org/2004/02/skos/core#exactMatch> ?b.
        }
        """):
            yield row['a'], 'exactMatch'
            yield row['b'], 'exactMatch'

        for row in graph.query("""
        SELECT ?a ?b WHERE {
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#related> ?b}
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#semanticRelation> ?b}
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#broader> ?b}
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#broaderTransitive> ?b}
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#narrower> ?b}
            OPTIONAL {?a <http://www.w3.org/2004/02/skos/core#narrowerTransitive> ?b}
        }
        """):
            yield row['a'], 'broadNarrow'
            yield row['b'], 'broadNarrow'


class GenericAnalyzer(AbstractAnalyzer):

    token = 'generic'

    def analyze(self, graph):
        """Basic graph analysis."""
        predicates_count = dict()
        classes_count = dict()

        triples = None
        for row in graph.query("select (COUNT(*) as ?c) where { ?s ?p ?o}"):
            triples = row['c']

        for row in graph.query("SELECT ?p (COUNT(?p) AS ?count) WHERE { ?s ?p ?o . } GROUP BY ?p ORDER BY DESC(?count)"):
            predicates_count[row['p']] = row['count']

        for row in graph.query("SELECT ?c (COUNT(?c) AS ?count) WHERE { ?s a ?c . } GROUP BY ?c ORDER BY DESC(?count)"):
            classes_count[row['c']] = row['count']

        summary = {
            'triples': triples,
            'predicates': predicates_count,
            'classes': classes_count
        }
        return summary

    def find_relation(self, graph):
        for row in graph.query("SELECT ?a ?b WHERE { ?a <http://www.w3.org/2002/07/owl#sameAs> ?b }"):
            yield row['a'], 'sameAs'
            yield row['b'], 'sameAs'


class QbDataset(object):
    """Model for reporting DataCube dataset.

    The model contains sets of dimensions and measures used.
    """

    def __init__(self):
        """Init model by initializing sets."""
        self.dimensions = set()
        self.measures = set()

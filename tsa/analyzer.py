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

    qb = Namespace('http://purl.org/linked-data/cube#')
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

        for dataset in graph.subjects(RDF.type, CubeAnalyzer.qb.DataSet):
            for structure in graph.objects(dataset, CubeAnalyzer.qb.structure):
                for component in graph.objects(structure, CubeAnalyzer.qb.component):
                    for dimension in graph.objects(component, CubeAnalyzer.qb.dimension):
                        datasets[str(dataset)].dimensions.add(str(dimension))
                    for measure in graph.objects(component, CubeAnalyzer.qb.measure):
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
        # pocet konceptu
        # pocet schemat, jejich mohutnost
        # top koncept dle schematu
        # pocet kolekci, jejich mohutnost
        pass

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
        triples = len(graph)
        predicates_count = defaultdict(int)
        classes_count = defaultdict(int)

        for s, p, o in graph:
            predicates_count[p] = predicates_count[p] + 1
            if p == RDF.type:
                classes_count[o] = classes_count[o] + 1

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

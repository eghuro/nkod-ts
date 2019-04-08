"""Dataset analyzer."""

import logging
from collections import defaultdict

from rdflib import Namespace
from rdflib.namespace import RDF


class Analyzer(object):
    """RDF dataset analyzer focusing on DataCube."""

    qb = Namespace('http://purl.org/linked-data/cube#')

    def __init__(self, iri):
        """Initialize a class by recording a distribution IRI we inspect."""
        self.distribution = iri

    def find_relation(self, graph):
        """We consider DSs to be related if they share a resource on dimension."""
        log = logging.getLogger(__name__)
        log.info('Looking up resources used on a dimension')
        for ds, resource in self.__resource_on_dimension(graph):
            log.info(f'Dataset: {ds} - Resource on dimension: {resource}')
            yield ds, resource

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
        """Basic graph analysis and basic analysis of a datacube."""
        triples = len(graph)
        predicates_count = defaultdict(int)
        classes = set()

        datasets = defaultdict(QbDataset)

        for s, p, o in graph:
            predicates_count[p] = predicates_count[p] + 1
            if p == RDF.type:
                classes.add(o)

        for dataset in graph.subjects(RDF.type, Analyzer.qb.DataSet):
            for structure in graph.objects(dataset, Analyzer.qb.structure):
                for component in graph.objects(structure, Analyzer.qb.component):
                    for dimension in graph.objects(component, Analyzer.qb.dimension):
                        datasets[str(dataset)].dimensions.add(str(dimension))
                    for measure in graph.objects(component, Analyzer.qb.measure):
                        datasets[str(dataset)].measures.add(str(measure))

        d = {}
        for k in datasets.keys():
            d[k] = {}
            d[k]['dimensions'] = list(datasets[k].dimensions)
            d[k]['measures'] = list(datasets[k].measures)

        summary = {
            'triples': triples,
            'predicates': predicates_count,
            'classes': list(classes),
            'datasets': d
        }

        return summary


class SparqlEndpointAnalyzer(object):
    pass

class SkosAnalyzer(object):
    def __init__(self, iri):
        pass

    def analyze(self, graph):
        # pocet konceptu
        # pocet schemat, jejich mohutnost
        # top koncept dle schematu
        # pocet kolekci, jejich mohutnost
        pass

    def find_relation(self, graph):
        # ?a skos:semanticRelation ?b
        # ?a skos:related ?b
        # ?a skos:broaderTransitive ?b
        # ?a skos:broader ?b
        # ?a skos:narrowerTransitive ?b
        # ?a skos:narrower ?b
        # ?collection skos:member ?a, ?b
        pass


class QbDataset(object):
    """Model for reporting DataCube dataset.

    The model contains sets of dimensions and measures used.
    """

    def __init__(self):
        """Init model by initializing sets."""
        self.dimensions = set()
        self.measures = set()

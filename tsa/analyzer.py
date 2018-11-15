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
        for dsd in graph.subjects(RDF.type, Analyzer.qb.DataStructureDefinition):
            for component in graph.objects(dsd, Analyzer.qb.component):
                for dimension in graph.objects(component, Analyzer.qb.dimension):
                    d[dsd].add(dimension)
        return d

    def __dataset_dimensions(self, graph, dimensions):
        d = defaultdict(set)
        for ds in graph.subjects(RDF.type, Analyzer.qb.DataSet):
            for structure in graph.objects(ds, Analyzer.qb.structure):
                if structure in dimensions.keys():
                    d[ds].update(dimensions[structure])
        return d

    def __resource_on_dimension(self, graph):
        log = logging.getLogger(__name__)
        log.info('Looking up resources on dimensions')
        ds_dimensions = self.__dataset_dimensions(graph, self.__dimensions(graph))
        log.info(f'Dimensions: {ds_dimensions!s}')
        for observation in graph.subjects(RDF.type, Analyzer.qb.Observation):
            log.info(f'Observation: {observation!s}')
            for dataset in graph.objects(observation, Analyzer.qb.dataSet):
                log.info(f'Dataset: {dataset!s}')
                for dimension in ds_dimensions[dataset]:
                    log.info(f'Dimension: {dimension!s}')
                    for resource in graph.objects(observation, dimension):
                        log.info(f'Resource: {resource!s}')
                        yield dataset, resource

    def analyze(self, graph):
        """Basic graph analysis and basic analysis of a datacube."""
        triples = len(graph)
        predicates_count = defaultdict(int)
        classes = set()

        datasets = defaultdict(QbDataset)

        qb = Namespace('http://purl.org/linked-data/cube#')

        for s, p, o in graph:
            predicates_count[p] = predicates_count[p] + 1
            if p == RDF.type:
                classes.add(o)

        for dataset in graph.subjects(RDF.type, qb.DataSet):
            for structure in graph.objects(dataset, qb.structure):
                for component in graph.objects(structure, qb.component):
                    for dimension in graph.objects(component, qb.dimension):
                        datasets[str(dataset)].dimensions.add(str(dimension))
                    for measure in graph.objects(component, qb.measure):
                        datasets[str(dataset)].measures.add(str(measure))

        summary = {
            'triples': triples,
            'predicates': predicates_count,
            'classes': list(classes),
            'datasets': str(datasets)
        }

        return summary


class QbDataset(object):
    """Model for reporting DataCube dataset.

    The model contains sets of dimensions and measures used.
    """

    def __init__(self):
        """Init model by initializing sets."""
        self.dimensions = set()
        self.measures = set()

    def __repr__(self):
        """Return string representation of a model."""
        return str({
            'dimensions': list(self.dimensions),
            'measures': list(self.measures)
        })

    def __str__(self):
        """Return string representation of a model."""
        return self.__repr__()

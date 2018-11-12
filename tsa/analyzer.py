from collections import defaultdict
from rdflib import Namespace
from rdflib.namespace import RDF


class Analyzer(object):

    def __init__(self, iri):
        self.__iri_analyzed = iri


    def find_related(self, graph):
        for s, p, o in g:
            yield (s, p)
            yield (p, o)
            yield (s, o)
            yield (s, self.__iri_analyzed)
            yield (p, self.__iri_analyzed)
            yield (o, self.__iri_analyzed)


    def analyze(self, graph):
        triples = len(graph)
        predicates_count = defaultdict(int)
        classes = set()

        datasets = defaultdict(QbDataset)

        qb = Namespace("http://purl.org/linked-data/cube#")

        for s, p, o in graph:
            predicates_count[p] = predicates_count[p] + 1
            if p == RDF.type:
                classes.add(o)

        for dataset in graph.subjects(RDF.type, qb.DataSet):
            for structure in graph.objects(dataset, qb.structure):
                for component in graph.objects(structure, qb.component):
                    for dimension in graph.objects(component, qb.dimension):
                        datasets[dataset].dimensions.add(dimension)
                    for measure in graph.objects(component, qb.measure):
                        datasets[dataset].measures.add(measure)

        summary = {
            'triples': triples,
            'predicates': predicates_count,
            'classes': list(classes),
            'datasets': datasets
        }

        return summary


class QbDataset(object):

    def __init__(self):
        self.dimensions = set()
        self.measures = set()

    def __repr__(self):
        return str({
            'dimensions': list(self.dimensions),
            'measures': list(self.measures)
        })

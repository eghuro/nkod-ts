from collections import defaultdict

from rdflib.namespace import RDF


class Analyzer(object):

    def analyze(self, graph):
        triples = len(graph)
        predicates_count = defaultdict(int)
        classes = set()
        for s, p, o in graph:
            predicates_count[p] = predicates_count[p] + 1
            if p == RDF.type:
                classes.add(o)

        summary = {
            'triples': triples,
            'predicates': predicates_count,
            'classes': list(classes)
        }

        return summary

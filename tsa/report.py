import json
import logging
import redis

from tsa.extensions import redis_pool


def query_dataset(iri):
    return {
        "related": query_related(iri),
        "profile": query_profile(iri)
    }


def query_related(ds_iri):
    key = f'distrquery:{ds_iri}'
    red = redis.Redis(connection_pool=redis_pool)
    return json.loads(red.get(key))


def query_profile(ds_iri):
    key = f'dsanalyses:{ds_iri}'
    red = redis.Redis(connection_pool=redis_pool)
    log = logging.getLogger(__name__)

    analysis = json.loads(red.get(key))
    output = {}
    output["triples"] = analysis["generic"]["triples"]

    output["classes"] = []
    for cls in analysis["generic"]["classes"].keys():
        iri = cls
        count = analysis["generic"]["classes"][cls]
        label = create_labels(ds_iri, ["cs", "en"])
        output["classes"].append({'iri': iri, 'count': count, 'label': label})

    output["predicates"] = []
    for pred in analysis["generic"]["predicates"].keys():
        output["predicates"].append({
            'iri': pred,
            'count': analysis["generic"]["predicates"][pred]
        })

    output["concepts"] = []
    if "concepts" in analysis["skos"]:
        for concept in analysis["skos"]["concepts"].keys():
            output["concepts"].append({
                'iri': concept,
                'label': create_labels(concept, ["cs", "en"])
            })

    output["schemata"] = []
    for schema in analysis["skos"]["schema"].keys():
        output["schemata"].append({
            'iri': concept,
            'label': create_labels(schema, ["cs", "en"])
        })

    return output



def create_labels(ds_iri, tags):
    labels = query_label(ds_iri)

    label = {}
    for tag in tags:
        label[tag] = ""

    available = set()

    if "default" in labels.keys():
        for tag in tags:
            label[tag] = labels["default"]
            available.add(tag)

    for tag in tags:
        if tag in labels.keys():
            label[tag] = labels[tag]
            available.add(tag)

    available = list(available)
    if len(available) > 0:
        for tag in tags:
            if len(label[tag]) == 0:
                label[tag] = label[available[0]]  # put anything there
    else:
        log = logging.getLogger(__name__)
        log.error(f'Missing labels for {ds_iri}')

    return label


def query_label(ds_iri):
    #LABELS: key = f'dstitle:{ds!s}:{t.language}' if t.language is not None else f'dstitle:{ds!s}'
    #red.set(key, title)
    red = redis.Redis(connection_pool=redis_pool)
    log = logging.getLogger(__name__)
    result = {}
    for x in red.keys(f'dstitle:{ds_iri!s}*'):
        prefix_lang = f'dstitle:{ds_iri!s}:'
        if x.startswith(prefix_lang):
            language_code = x[len(prefix_lang):]
            title = red.get(x)
            result[language_code] = title
        else:
            result['default'] = red.get(x)
    return result
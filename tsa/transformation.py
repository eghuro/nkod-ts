"""Prepare ETL pipeline."""
import json
import os
from urllib.parse import urlparse

from jinja2 import Template


class PipelineFactory(object):
    """ETL pipeline factory."""

    _TEMPLATE_NAME = 'Conversion.jsonld.jinja2'
    _USER_AGENT = 'NKOD-TSA/0.0'

    def __init__(self):
        """Load template file and initialize jinja2 Template object."""
        path = os.path.join(os.path.dirname(__file__), PipelineFactory._TEMPLATE_NAME)
        with open(path, 'r') as f:
            self.__pipelineTemplate = Template(f.read())

    def create_pipeline(self, source_uri, virtuoso_config):
        """Create a pipeline using the jinja2 template."""
        for token in ['server', 'port', 'user', 'password', 'iri']:
            assert token in virtuoso_config

        return json.loads(self.__pipelineTemplate.render({
            'filename': os.path.basename(urlparse(source_uri).path),
            'sourceUri': source_uri,
            'userAgent': PipelineFactory._USER_AGENT,
            'virtuosoIri': virtuoso_config['iri'],
            'virtuosoPassword': virtuoso_config['password'],
            'virtuosoServer': virtuoso_config['server'],
            'virtuosoPort': virtuoso_config['port'],
            'virtuosoUser': virtuoso_config['user']
        }))

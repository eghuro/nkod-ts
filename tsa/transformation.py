import os
import json
from urllib.parse import urlparse
from jinja2 import Template

class PipelineFactory(object):

   _TEMPLATE_NAME = "Conversion.jsonld.jinja2"
   _USER_AGENT = "NKOD-TSA/0.0"

   def __init__(self):
       #load template file and initialize jinja2 Template object
       path = os.path.join(os.path.dirname(__file__), PipelineFactory._TEMPLATE_NAME)
       with open(path, 'r') as f:
           self.__pipelineTemplate = Template(f.read())

   def createPipeline(self, sourceUri, virtuosoConfig):
       for token in ['server', 'port', 'user', 'password', 'iri']:
           assert token in virtuosoConfig

       return json.loads(self.__pipelineTemplate.render({
           'filename': os.path.basename(urlparse(sourceUri).path),
           'sourceUri': sourceUri,
           'userAgent': PipelineFactory._USER_AGENT,
           'virtuosoIri': virtuosoConfig['iri'],
           'virtuosoPassword': virtuosoConfig['password'],
           'virtuosoServer': virtuosoConfig['server'],
           'virtuosoPort': virtuosoConfig['port'],
           'virtuosoUser': virtuosoConfig['user']
       }))

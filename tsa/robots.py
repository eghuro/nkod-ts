"""User agent and robots cache."""
import requests
import requests_toolbelt
from reppy.cache import AgentCache

import tsa

user_agent = requests_toolbelt.user_agent('tsa', tsa.__version__, extras=[('requests', requests.__version__)])
robots_cache = AgentCache(agent=user_agent, capacity=1000)

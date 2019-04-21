"""User agent and robots cache."""
import requests
import requests_toolbelt
from reppy.cache import AgentCache
import resource

import tsa

soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)

user_agent = requests_toolbelt.user_agent('tsa', tsa.__version__, extras=[('requests', requests.__version__)])
session = requests.Session()
session.headers.update({'User-Agent': user_agent})
a = requests.adapters.HTTPAdapter(pool_connections = 500, pool_maxsize = soft - 10, max_retries = 3, pool_block = True)
session.mount('http://', a)
session.mount('https://', a)
robots_cache = AgentCache(agent=user_agent, capacity=1000, timeout=10)

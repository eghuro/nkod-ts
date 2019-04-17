# -*- coding: utf-8 -*-
"""Extensions module. Each extension is initialized in the app factory located in app.py."""
from flask_caching import Cache
from flask_cors import CORS
from raven.contrib.flask import Sentry

cache = Cache()
sentry = Sentry()
cors = CORS()

"""Helper tools for caching."""

import os
import binascii
import datetime
from functools import wraps
from flask import request, make_response
from tsa.extensions import cache


def cached(cacheable=False, must_revalidate=True, client_only=True, client_timeout=0, server_timeout=5 * 60, key='view/%s'):
    """Flask cache decorator.

    See https://codereview.stackexchange.com/q/147038,
        https://jakearchibald.com/2016/caching-best-practices/ and
        https://developers.google.com/web/fundamentals/performance/optimizing-content-efficiency/http-caching
        for more details.
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            cache_key = key % request.full_path  # include querystring
            cache_policy = ''
            if not cacheable:
                cache_policy += ', no-store'  # tells the browser not to cache at all
            else:
                if must_revalidate:  # this looks contradicting if you haven't read the article.
                    # no-cache doesn't mean "don't cache", it means it must check
                    # (or "revalidate" as it calls it) with the server before
                    # using the cached resource
                    cache_policy += ', no-cache'
                else:
                    # Also must-revalidate doesn't mean "must revalidate", it
                    # means the local resource can be used if it's younger than
                    # the provided max-age, otherwise it must revalidate
                    cache_policy += ', must-revalidate'

                if client_only:
                    cache_policy += ', private'
                else:
                    cache_policy += ', public'

                cache_policy += ', max-age=%d' % (client_timeout)

            headers = {}
            cache_policy = cache_policy.strip(',')
            headers['Cache-Control'] = cache_policy
            now = datetime.datetime.utcnow()

            client_etag = request.headers.get('If-None-Match')

            response = cache.get(cache_key)
            # respect the hard-refresh
            if response is not None and request.headers.get('Cache-Control', '') != 'no-cache':
                headers['X-Cache'] = 'HIT from Server'
                cached_etag = response.headers.get('ETag')
                if client_etag and cached_etag and client_etag == cached_etag:
                    headers['X-Cache'] = 'HIT from Client'
                    headers['X-Last-Modified'] = response.headers.get('X-LastModified')
                    response = make_response('', 304)
            else:
                response = make_response(f(*args, **kwargs))
                if response.status_code == 200 and request.method in ['GET', 'HEAD']:
                    headers['X-Cache'] = 'MISS'
                    # - Added the headers to the response object instead of the
                    # headers dict so they get cached too
                    # - If you can find any faster random algorithm go for it.
                    response.headers.add('ETag', binascii.hexlify(os.urandom(4)))
                    response.headers.add('X-Last-Modified', str(now))
                    cache.set(cache_key, response, timeout=server_timeout)

            response.headers.extend(headers)
            return response
        return decorated_function
    return decorator

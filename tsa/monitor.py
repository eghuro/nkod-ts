"""Recording various runtime metrics into redis."""
import redis

from tsa.extensions import redis_pool

class Monitor(object):
    """Monitor is recording various runtime metrics into redis."""

    KEYS = ['stat:size', 'stat:format']

    def __init__(self):
        """Try to connect to redis and reset counters."""
        try:
            self.__client = redis.Redis(connection_pool=redis_pool)
        except redis.exceptions.ConnectionError:
            self.__client = None

    def log_format(self, guess):
        """Record distribution format."""
        key = 'stat:format'
        self.__client.hincrby(key, guess, 1)

    def log_size(self, size):
        """Record distribution size."""
        key = 'stat:size'
        self.__client.lpush(key, size)


monitor = Monitor()

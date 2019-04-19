"""Recording various runtime metrics into redis."""
import redis
from atenvironment import environment


class Monitor(object):
    """Monitor is recording various runtime metrics into redis."""

    @environment('REDIS')
    def __init__(self, redis_url):
        """Try to connect to redis and reset counters."""
        try:
            self.__client = redis.StrictRedis().from_url(redis_url)
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

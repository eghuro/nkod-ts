from atenvironment import environment
import redis

class Monitor(object):
    @environment("REDIS")
    def __init__(self, redis_url):
        self.__client = redis.StrictRedis().from_url(redis_url)

    def log_format(self, guess):
        key = 'stat:format'
        self.__client.hincrby(key, guess, 1)

monitor = Monitor()

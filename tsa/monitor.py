from atenvironment import environment
import redis

class Monitor(object):
    @environment("REDIS")
    def __init__(self, redis_url):
        self.__client = redis.StrictRedis().from_url(redis_url)
        for key in self.__client.hkeys('stat:format'):
            self.__client.hdel('stat:format', key)
        self.__client.set('stat:sum', 0)
        self.__client.set('stat:count', 0)


    def log_format(self, guess):
        key = 'stat:format'
        self.__client.hincrby(key, guess, 1)


    def log_size(self, size):
        key = 'stat:sum'
        self.__client.incrby(key, size)
        key = 'stat:count'
        self.__client.incr(key)


monitor = Monitor()

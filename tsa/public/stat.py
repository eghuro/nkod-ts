"""Stat endpoints."""
import math
import redis
import statistics
from atenvironment import environment
from flask import Blueprint, current_app, jsonify

blueprint = Blueprint('stat', __name__, static_folder='../static')


@blueprint.route('/api/v1/stat/format', methods=['GET'])
@environment('REDIS')
def stat_format(redis_url):
    """List distribution formats logged."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    return jsonify(r.hgetall('stat:format'))


@blueprint.route('/api/v1/stat/failed', methods=['GET'])
@environment('REDIS')
def stat_failed(redis_url):
    """List failed distributions."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    return jsonify(list(r.smembers('stat:failed')))


@blueprint.route('/api/v1/stat/size', methods=['GET'])
@environment('REDIS')
def stat_size(redis_url):
    """List min, max and average distribution size."""
    r = redis.StrictRedis.from_url(redis_url, charset='utf-8', decode_responses=True)
    return jsonify(retrieve_size_stats(r))


def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])


def retrieve_size_stats(r):
    """Load sizes from redis and calculate some stats about it."""
    lst = sorted([int(x) for x in r.lrange('stat:size', 0, -1)])
    try:
        mode = statistics.mode(lst)
    except statistics.StatisticsError:
        mode = None
    try:
        mean = statistics.mean(lst)
    except statistics.StatisticsError:
        mean = None
    try:
        stdev = statistics.stdev(lst, mean)
    except statistics.StatisticsError:
        stdev = None
    try:
        var = statistics.variance(lst, mean)
    except statistics.StatisticsError:
        var = None

    try:
        minimum = min(lst)
    except ValueError:
        minimum = None

    try:
        maximum = max(lst)
    except ValueError:
        maximum = None

    return {
        'min': convert_size(minimum),
        'max': convert_size(maximum),
        'mean': convert_size(mean),
        'mode': convert_size(mode),
        'stdev': convert_size(stdev),
        'var': var
    }

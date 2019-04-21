"""Stat endpoints."""
import math
import statistics
import redis
from flask import Blueprint, current_app, jsonify
from tsa.extensions import redis_pool

blueprint = Blueprint('stat', __name__, static_folder='../static')


@blueprint.route('/api/v1/stat/format', methods=['GET'])
def stat_format():
    """List distribution formats logged."""
    red = redis.Redis(connection_pool=redis_pool)
    return jsonify(red.hgetall('stat:format'))


@blueprint.route('/api/v1/stat/failed', methods=['GET'])
def stat_failed():
    """List failed distributions."""
    red = redis.Redis(connection_pool=redis_pool)
    return jsonify(list(red.smembers('stat:failed')))


@blueprint.route('/api/v1/stat/size', methods=['GET'])
def stat_size():
    """List min, max and average distribution size."""
    red = redis.Redis(connection_pool=redis_pool)
    return jsonify(retrieve_size_stats(red))


def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])


def retrieve_size_stats(red):
    """Load sizes from redis and calculate some stats about it."""
    lst = sorted([int(x) for x in red.lrange('stat:size', 0, -1)])
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

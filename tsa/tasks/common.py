import logging
import redis

from celery import Task

from tsa.extensions import redis_pool

class TrackableException(BaseException):
    pass


class TrackableTask(Task):
    _red = None

    @property
    def redis(self):
        if self._red is None:
            self._red = redis.Redis(connection_pool=redis_pool)
        return self._red


    def __call__(self, *args, **kwargs):
        my_id = self.request.id
        parent_id = self.request.parent_id
        root_id = self.request.root_id
        batch_id = None
        if parent_id is not None:
            batch_id = self.redis.hget('taskBatchId', parent_id)
        if batch_id is None and root_id is not None:
            batch_id = self.redis.hget('taskBatchId', root_id)
        if batch_id is None:
            batch_id = self.redis.hget('taskBatchId', my_id)

        if batch_id is None:
            logging.getLogger(__name__).error('Missing batch id')
            raise TrackableException()

        self.redis.hset('taskBatchId', my_id, batch_id)
        self.redis.sadd(f'batch:{batch_id}', my_id)
        return super(TrackableTask, self).__call__(*args, **kwargs)


    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        batch_id = self.redis.hget('taskBatchId', task_id)
        self.redis.srem(f'batch:{batch_id}', task_id)
        self.redis.hdel('taskBatchId', task_id)
        if self.redis.scard == 0:
            logging.getLogger(__name__).info(f'Completed batch {batch_id}')
        if batch_id is None:
            raise TrackableException()
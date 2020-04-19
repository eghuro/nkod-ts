import logging
import redis as redis_lib

from celery import Task

from tsa.extensions import redis_pool

class TrackableException(BaseException):
    pass


class TrackableTask(Task):
    _red = None

    @property
    def redis(self):
        if self._red is None:
            self._red = redis_lib.Redis(connection_pool=redis_pool)
        return self._red


    def __call__(self, *args, **kwargs):
        my_id = self.request.id
        parent_id = self.request.parent_id
        root_id = self.request.root_id
        batch_id = None
        try:
            with self.redis.lock(f'batchLock', blocking_timeout=60) as lock:
                if parent_id is not None:
                    batch_id = self.redis.hget('taskBatchId', parent_id)
                if batch_id is None and root_id is not None:
                    batch_id = self.redis.hget('taskBatchId', root_id)
                if batch_id is None:
                    batch_id = self.redis.hget('taskBatchId', my_id)
        except redis_lib.exceptions.LockError as e:
            logging.getLogger(__name__).exception('Lock error')
            raise TrackableException() from e

        if batch_id is None:
            logging.getLogger(__name__).error(f'({self.name}) Missing batch id, my id: {my_id}, parent id: {parent_id}, root id: {root_id}')
            raise TrackableException()

        self.redis.hset('taskBatchId', my_id, batch_id)
        self.redis.sadd(f'batch:{batch_id}', my_id)
        return super(TrackableTask, self).__call__(*args, **kwargs)


    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        batch_id = self.redis.hget('taskBatchId', task_id)
        self.redis.srem(f'batch:{batch_id}', task_id)
        #keep taskBatchId as we need it for children tasks
        if self.redis.scard == 0:
            logging.getLogger(__name__).info(f'Completed batch {batch_id}')
            # here we can cleanup taskBatchId
        if batch_id is None:
            raise TrackableException()
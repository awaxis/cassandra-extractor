# External imports
from prometheus_client import Counter
import time

from worker import WorkerType
from s3worker import S3Worker
from kafkaworker import KafkaWorker
from rdsworker import RDSWorker
from jsonworker import JSONWorker

class WorkerManager:
    def __init__(self, config, logger=None, metric_registry=None):
        self.config = config
        self.logger = logger
        self.counter = Counter('assigned_tasks', 'Assigned tasks counter', registry=metric_registry)
        self.workers = {
            WorkerType.S3 : {
                'workers' : list(),
                'next' : 0
            },
            WorkerType.KAFKA : {
                'workers' : list(),
                'next' : 0
            },
            WorkerType.RDS : {
                'workers' : list(),
                'next' : 0
            },
            WorkerType.JSON : {
                'workers' : list(),
                'next' : 0
            }
        }
        self.cache_session = None

    def assignTask(self, worker_type, data):
        worker = None
        recycle = False
        idle_worker = None
        # lookup for idle worker to recycle instead of creating new worker
        for i, w in enumerate(self.workers[worker_type]['workers']):
            if w.is_idle():
                recycle = True
                idle_worker = i
                break
    
        if recycle is False and len(self.workers[worker_type]['workers']) < self.config.max_worker_threads:
            if worker_type == WorkerType.S3:
                worker = S3Worker(data, self.config, self.logger, self.cache_session)
            elif worker_type == WorkerType.KAFKA:
                worker = KafkaWorker(data, self.config, self.logger, self.cache_session)
            elif worker_type == WorkerType.RDS:
                worker = RDSWorker(data, self.config, self.logger, self.cache_session)
            elif worker_type == WorkerType.JSON:
                worker = JSONWorker(data, self.config, self.logger, self.cache_session)
            else:
                return None
            self.workers[worker_type]['workers'].append(worker)
            worker.start()
            self.logger.info("New worker created '%s:%s'" % (worker.type, worker.id))
        else:
            recycle = True

        if recycle is True:
            lookup_active_worker = True
            assigned = False
            next_worker = idle_worker if idle_worker is not None else self.workers[worker_type]['next']
            while lookup_active_worker:
                worker = self.workers[worker_type]['workers'][next_worker]
                if not worker.is_paused():
                    worker.queueTask(data)
                    assigned = True
                    next_worker = next_worker + 1
                    if next_worker >= self.config.max_worker_threads:
                        next_worker = 0   
                    self.workers[worker_type]['next'] = next_worker
                    self.logger.info("Task assigned to worker '%s:%s'" % (worker.type, worker.id))
                    self.logger.info("Next worker '%d'" % next_worker)
                    break
                else:
                    # Lookup next active worker, loop ones to try to find a previous active worker
                    next_worker = next_worker + 1
                    if next_worker >= self.config.max_worker_threads:
                        next_worker = 0
                        if lookup_active_worker == False:
                            # second loop reached, stop looking for an active worker
                            break
                        lookup_active_worker = False

            # No active worker found, signal data crawler to pause crawling
            if assigned == False:
                return None

        # Increment metrics
        self.counter.inc()

        return worker

    def abort(self):
        for key in self.workers:
            for worker in self.workers[key]['workers']:
                worker.abort()

    def pause(self):
        for key in self.workers:
            for worker in self.workers[key]['workers']:
                worker.pause()

    def resume(self):
        for key in self.workers:
            for worker in self.workers[key]['workers']:
                worker.resume()

    def end(self, wait_idle=False):
        for key in self.workers:
            for worker in self.workers[key]['workers']:
                if wait_idle is True and not worker.is_paused():
                    while not worker.is_idle():
                        time.sleep(0.100)
                worker.stop()

    def reload_config(self, config):
        self.config = config.copy()
        for key in self.workers:
            for worker in self.workers[key]['workers']:
                worker.reload_config(config)

    def scale_up(self):
        self.config.max_worker_threads += 1
        return self.config.max_worker_threads

    def scale_down(self):
        if self.config.max_worker_threads <= 0:
            return 0
        self.config.max_worker_threads -= 1
        for key in self.workers:
            count = len(self.workers[key]['workers'])
            while count > self.config.max_worker_threads:
                count -= 1
                worker = self.workers[key]['workers'][count]
                worker.stop()
                while not worker.is_idle():
                    time.sleep(1)
                self.workers[key]['workers'].remove(worker)
                del worker

        return self.config.max_worker_threads

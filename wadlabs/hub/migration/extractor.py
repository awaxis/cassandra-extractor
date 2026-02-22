# External imports
import uuid
import time
from datetime import datetime
from threading import Thread as Process, Event
from prometheus_client import CollectorRegistry

from workermanager import WorkerManager
from datacrawler import DataCrawler

class Extractor(Process):
    def __init__(self, table, config, logger):
        Process.__init__(self)
        self.id=uuid.uuid1()
        self.config=config.copy()
        self.logger=logger
        self.table = table
        self.stop_event = Event()
        self.registry = CollectorRegistry()
        self.worker_manager = WorkerManager(self.config, self.logger, self.registry)
        self.data_crawler = DataCrawler(self.config, self.worker_manager, self.logger, self.registry)

    def stop(self):
        self.logger.info("Shutdown Extractor instance (%s)..." % self.table)
        self.stop_event.set()
        self.join()

    def run(self):
        self.logger.info("Starting Extractor instance (%s)..." % self.table)
        self.data_crawler.start()
        try:
            while True:
                time.sleep(1)
                if self.stop_event.is_set():
                    break
        except Exception as e:
            self.logger.info(str(e))
            self.stop_event.set()

        self.data_crawler.stop()
        self.worker_manager.end(wait_idle=True)

        del self.data_crawler
        del self.worker_manager

    def scale_up(self):
        self.config.max_worker_threads = self.worker_manager.scale_up()
        return self.config.max_worker_threads

    def scale_down(self):
        self.config.max_worker_threads = self.worker_manager.scale_down()
        return self.config.max_worker_threads

    def set_config(self, config):
        self.config = config
        self.data_crawler.reload_config(config)
        self.worker_manager.reload_config(config)

    def queueTask(self, task):
        self.data_crawler.queueTask(task)
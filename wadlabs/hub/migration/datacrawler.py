# External imports
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from threading import Event, Timer
import redis
import json
from datetime import datetime
import uuid
import time
from prometheus_client import Histogram
import psutil
import os
import gc

from worker import Worker
from worker import WorkerType
from config import Serializable

class PagingMode:
    DRIVER = "driver"
    CURSOR = "cursor"

class PageState:
    DONE = True
    PENDING = False

class PagedResultHandler(object):

    def __init__(self, future):
        self.error = None
        self.finished_event = Event()
        self.fetch_result_event = Event()
        self.future = future
        self.result = None
        self.future.add_callbacks(
            callback = self.handle_page,
            errback = self.handle_error)

    def handle_page(self, result):
        self.result = result
        self.fetch_result_event.set()

    def handle_error(self, exc):
        self.error = exc
        self.fetch_result_event.set()
        self.finished_event.set()

    def first(self):
        self.fetch_result_event.wait()
        return self.result

    def next(self):
        if self.future.has_more_pages:
            self.fetch_result_event.clear()
            self.future.start_fetching_next_page()
            self.fetch_result_event.wait()
        else:
            self.finished_event.set()
            return None
        return self.result

class DataCrawler(Worker):
    def __init__(self, config, worker_manager, logger, metric_registry):
        Worker.__init__(self, WorkerType.DATACRAWLER, config, logger)
        self.worker_manager = worker_manager
        self.state = Serializable()
        self.state.session = None
        self.state.cache = None
        self.state.data_fetch_execute_timestamp = None
        self.state.enabled_workers = 0
        self.metrics.fetch_latency = Histogram('fetch_latency', 'Data fetch latency (per fetch_size)', ['instance'], registry=metric_registry)
        self.hold_on_memory_usage_limits = False
        self.timer_memory_usage_check = None

    def stop(self):
        self.print_message("Shutdown Data Crawler service...")
        self.stop_event.set()
        self.resume()
        while not self.is_idle():
            time.sleep(1)
        if not self.state.session is None:
            self.state.session.shutdown()
        self.join()

    def abort(self):
        self.abort_event.set()

    def setup_cache_session(self):
        """
        Configure cache session
        :param config:
        :return: cache session
        """
        try:
            if self.config.redis.password is None:
                cache_provider = redis.Redis(host=self.config.redis.host, port=self.config.redis.port, db=self.config.redis.db)
            else:
                cache_provider = redis.Redis(host=self.config.redis.host, port=self.config.redis.port, db=self.config.redis.db, password=self.config.redis.password)
            self.logger.debug("Cache session connected to redis %s:%d/%s" % (self.config.redis.host, self.config.redis.port, self.config.redis.db))
        
            return cache_provider
        except Exception as e:
            self.last_error = "Redis connection error: " + str(e)
            self.logger.error(self.last_error)

        return None

    def setup_database(self):
        """
        Configure source data base
        :param config:
        :return: source session
        """
        try:
            self.last_error = None
            source_cluster = Cluster(
                contact_points=self.config.cassandra.hosts,
                port=self.config.cassandra.port,
                auth_provider=PlainTextAuthProvider(username=self.config.cassandra.username, password=self.config.cassandra.password),
                connect_timeout=self.config.cassandra.connect_timeout)
            session = source_cluster.connect(self.config.cassandra.keyspace)
            self.logger.debug("Source session connected to cluster " + self.config.cassandra.keyspace)
        
            return session
        except Exception as e:
            self.last_error = "Cassandra connection error: " + str(e)
            self.logger.error(self.last_error)

        return None

    def fetch_data(self):
        query = "SELECT JSON "
        for col in self.config.cassandra.columns:
            query += col + ','
        query = query[:-1] + " FROM " + self.config.cassandra.table
        if not self.config.cassandra.filter is None and self.config.cassandra.filter.strip() != "":
            query = query + " WHERE solr_query = '{\"q\":\"" + self.config.cassandra.filter + "\", "
            if self.config.cassandra.paging_mode == PagingMode.DRIVER:
                query = query + "\"paging\":\"" + PagingMode.DRIVER + "\""
            else:
                query = query + "\"start\":" + str(self.state.page*self.config.cassandra.fetch_size)

            if not self.config.cassandra.sort_by is None:
                query = query + ", \"sort\":\"" + self.config.cassandra.sort_by + "\""

            query = query + ", \"useFieldCache\":true}'"

            if not self.config.cassandra.paging_mode == PagingMode.DRIVER:
                query = query + " LIMIT " + str(self.config.cassandra.fetch_size)

        if self.state.page == 0:
            self.logger.info(query)

        self.last_error = None
        results = None
        with self.metrics.fetch_latency.labels(self.instance_id).time():
            try:
                consistency_level = ConsistencyLevel.name_to_value.get(self.config.cassandra.consistency_level, None)
                if self.config.cassandra.paging_mode == PagingMode.DRIVER:
                    if self.state.page == 0:
                        statement = SimpleStatement(query, fetch_size=self.config.cassandra.fetch_size, consistency_level=consistency_level)    
                        self.state.paged_result_handlfer = PagedResultHandler(self.state.session.execute_async(statement, timeout=self.config.cassandra.connect_timeout))
                        self.state.data_fetch_execute_timestamp = datetime.utcnow()
                        results = self.state.paged_result_handlfer.first()
                    else:
                        results = self.state.paged_result_handlfer.next()
                else:
                    self.state.statement = SimpleStatement(query, fetch_size=self.config.cassandra.fetch_size, consistency_level=consistency_level)
                    if self.state.page > 0:
                        # resume the pagination if any...
                        results = self.state.session.execute(self.state.statement, paging_state=self.state.paging_state, timeout=self.config.cassandra.connect_timeout)
                    else:
                        results = self.state.session.execute(self.state.statement, timeout=self.config.cassandra.connect_timeout)
                        self.state.data_fetch_execute_timestamp = datetime.utcnow()

                    # save the paging_state somewhere and return current results
                    self.state.paging_state = results.paging_state
            except Exception as e:
                self.last_error = str(e)
                self.logger.error(self.last_error)

        return results

    def count_data(self):
        query = "SELECT count(*) FROM " + self.config.cassandra.table
        if not self.config.cassandra.filter is None and self.config.cassandra.filter.strip() != "":
            query = query + " WHERE solr_query = '{\"q\":\"" + self.config.cassandra.filter + "\", \"useFieldCache\":true}'"

        self.last_error = None
        try:
            statement = SimpleStatement(query, fetch_size=self.config.cassandra.fetch_size, consistency_level=ConsistencyLevel.ALL)
            results = self.state.session.execute(statement)
            self.config.cassandra.estimated_total_rows = results[0][0]
            return self.config.cassandra.estimated_total_rows
        except Exception as e:
            self.last_error = str(e)
            self.logger.error(self.last_error)
        return self.config.cassandra.estimated_total_rows or 0

    def page_state(self, worker_type, page):
        if self.state.cache is None:
            return PageState.PENDING
        page_state = self.state.cache.get('%s-%s-page-%d-state'%(self.config.cassandra.table, worker_type, page))
        if page_state is not None:
            if page_state.decode().lower() in ('done', 'yes', 'true', 'on', 't', 'y', '1'):
                return PageState.DONE
        return PageState.PENDING
    
    def get_process_memory(self):
        process = psutil.Process(os.getpid())
        return process.memory_full_info()
    
    def check_memory_usage(self):
        # Free unused resources first
        gc.collect()

        self.memory_info = self.get_process_memory()
        memory_usage = self.memory_info.uss / 1e6
        if memory_usage >= self.config.max_memory_usage:
            if not self.is_paused() and self.hold_on_memory_usage_limits is False:
                self.pause()
                self.hold_on_memory_usage_limits = True
                #self.setup_memory_usage_check_timer()
                self.print_warning("Memory usage limit exceeded, data crawling paused")
        elif self.hold_on_memory_usage_limits is True:
            if memory_usage <= self.config.max_memory_usage_resume_threshold:
                self.hold_on_memory_usage_limits = False
                if self.is_paused():
                    self.resume()
                    self.print_warning("Memory usage back to normal, data crawling resumed")
            #else:
            #    self.setup_memory_usage_check_timer()

    def setup_memory_usage_check_timer(self):
        # duration is in seconds
        self.timer_memory_usage_check = Timer(self.config.memory_usage_check_delay, self.check_memory_usage)
        self.timer_memory_usage_check.start()

        # wait for time completion
        self.timer_memory_usage_check.join()

    def dispatch_to_s3(self, result, count):
        # S3 dispatch
        worker = None
        pause = False
        escape = False
        if not self.state.s3_enabled is True:
            return (worker, pause, escape)

        if (self.state.cached_s3_page is None or
            self.state.page > self.state.cached_s3_page or
            self.page_state(WorkerType.S3, self.state.page) == PageState.PENDING):
            worker = self.worker_manager.assignTask(WorkerType.S3, (result, self.state.page))
            # if no available worker found, hold crawling
            if worker is None:
                self.print_warning("No S3 worker available found")
                # Hold data crawling if only one worker type is enabled
                if self.state.enabled_workers <= 1:
                    pause = True
        else:
            escape = True
            self.print_message("S3 dispatcher: Escaped page %d (%d records)" % (self.state.page, count))

        
        return (worker, pause, escape)

    def dispatch_to_kafka(self, result, count):
        # S3 dispatch
        worker = None
        pause = False
        escape = False
        if not self.state.kafka_enabled is True:
            return (worker, pause, escape)

        if (self.state.cached_kafka_page is None or
            self.state.page > self.state.cached_kafka_page or
            self.page_state(WorkerType.KAFKA, self.state.page) == PageState.PENDING):
            worker = self.worker_manager.assignTask(WorkerType.KAFKA, (result, self.state.page))
            # if no available worker found, hold crawling
            if worker is None:
                self.print_warning("No KAFKA worker available found")
                # Hold data crawling if only one worker type is enabled
                if self.state.enabled_workers <= 1:
                    pause = True
        else:
            escape = True
            self.print_message("KAFKA dispatcher: Escaped page %d (%d records)" % (self.state.page, count))
        
        return (worker, pause, escape)

    def dispatch_to_rds(self, result, count):
        # S3 dispatch
        worker = None
        pause = False
        escape = False
        if not self.state.rds_enabled is True:
            return (worker, pause, escape)

        if (self.state.cached_rds_page is None or
            self.state.page > self.state.cached_rds_page or
            self.page_state(WorkerType.RDS, self.state.page) == PageState.PENDING):
            worker = self.worker_manager.assignTask(WorkerType.RDS, (result, self.state.page))
            # if no available worker found, hold crawling
            if worker is None:
                self.print_warning("No RDS worker available found")
                # Hold data crawling if only one worker type is enabled
                if self.state.enabled_workers <= 1:
                    pause = True
        else:
            escape = True
            self.print_message("RDS dispatcher: Escaped page %d (%d records)" % (self.state.page, count))
        
        return (worker, pause, escape)

    def dispatch_to_json(self, result, count):
        # S3 dispatch
        worker = None
        pause = False
        escape = False
        if not self.state.json_enabled is True:
            return (worker, pause, escape)

        if (self.state.cached_json_page is None or
            self.state.page > self.state.cached_json_page or
            self.page_state(WorkerType.JSON, self.state.page) == PageState.PENDING):
            worker = self.worker_manager.assignTask(WorkerType.JSON, (result, self.state.page))
            # if no available worker found, hold crawling
            if worker is None:
                self.print_warning("No JSON worker available found")
                # Hold data crawling if only one worker type is enabled
                if self.state.enabled_workers <= 1:
                    pause = True
        else:
            escape = True
            self.print_message("JSON dispatcher: Escaped page %d (%d records)" % (self.state.page, count))
        
        return (worker, pause, escape)

    def wait_resume(self):
        while self.is_paused():
            time.sleep(1)
            if self.is_stopped() or self.is_aborted():
                self.print_message("Data crawling aborted")
                return False
        return True

    def get_cache(self, pattern=None):
        cache = {}
        if self.config.stateless is True:
            return cache

        if self.state.cache is None:
            self.state.cache = self.setup_cache_session()
        keys = self.state.cache.keys(pattern or ('%s-*' % self.config.cassandra.table))
        
        for key in keys:
            cache[key.decode()] = self.state.cache.get(key).decode()
        return cache

    def clear_cache(self, pattern=None):
        if self.config.stateless is True:
            return

        if self.state.cache is None:
            self.state.cache = self.setup_cache_session()
        keys = self.state.cache.keys(pattern or ('%s-*' % self.config.cassandra.table))
        
        for key in keys:
            self.state.cache.delete(key.decode())

    def run(self):
        self.print_message("Starting Data Crawler service")
        while True:
            try:
                if not self.queue.empty():
                    self.idle_event.clear()
                    task = self.queue.get()
                    if task[0]['action'] != 'start':
                        continue

                    if self.is_stopped():
                        self.idle()
                        break

                    if self.is_aborted():
                        self.abort_event.clear()

                    if self.state.session is None:
                        self.state.session = self.setup_database()
                    
                    if self.state.session is None:
                        continue

                    # Prepare Cache Session
                    if self.config.stateless is False and self.state.cache is None:
                        self.state.cache = self.setup_cache_session()

                    # Check Timestamp Filter set in a previous sesion
                    if not self.state.cache is None:
                        self.worker_manager.cache_session = self.state.cache
                        if self.config.cassandra.modified_timestamp_filter is None:
                            ts = self.state.cache.get('%s-modified-timestamp-filter'%(self.config.cassandra.table))
                            ts = None if ts is None else int(ts)
                            self.config.cassandra.modified_timestamp_filter = ts
                            self.worker_manager.config.cassandra.modified_timestamp_filter = ts
                    self.print_message("Timestamp filter: %s" % ("Not set" if self.config.cassandra.modified_timestamp_filter is None else self.config.cassandra.modified_timestamp_filter))

                    # Prepare metrics
                    self.metrics.update_start_time()
                    self.metrics.tasks.labels(self.instance_id).inc()

                    # Prepare active Workers
                    self.state.cached_fetch_size = None
                    self.state.cached_s3_page = None
                    self.state.cached_kafka_page = None
                    self.state.cached_rds_page = None
                    self.state.cached_json_page = None
                    self.state.page = 0
                    self.state.more = True
                    self.state.enabled_workers = 0

                    self.state.kafka_enabled = self.config.enable_kafka is True and len(self.config.kafka.topics) > 0 and self.config.kafka.topics[0] != ''
                    self.state.s3_enabled = self.config.enable_s3 is True and not self.config.s3.bucket is None
                    self.state.rds_enabled = self.config.enable_rds is True and not (self.config.rds.table is None and self.config.rds.schema is None)
                    self.state.json_enabled = self.config.enable_json is True and not (self.config.json.filename is None)
                    
                    if self.state.kafka_enabled: self.state.enabled_workers += 1
                    if self.state.s3_enabled: self.state.enabled_workers += 1
                    if self.state.rds_enabled: self.state.enabled_workers += 1
                    if self.state.json_enabled: self.state.enabled_workers += 1

                    # Calculate data count, if possible
                    estimated_total_rows = self.count_data()
                    self.metrics.total.labels(self.instance_id).set(estimated_total_rows)

                    # Retreive cached metrics from a previous session
                    if not self.state.cache is None:
                        self.state.cache.set('%s-start-time'%(self.config.cassandra.table), int(self.metrics.start_time.labels(self.instance_id)._value.get()))

                        self.state.cached_fetch_size = self.state.cache.get('%s-fetch-size'%(self.config.cassandra.table))
                        if not self.state.cached_fetch_size is None: self.state.cached_fetch_size = int(self.state.cached_fetch_size)
                        
                        self.state.cached_s3_page = self.state.cache.get('%s-s3-page'%(self.config.cassandra.table))
                        if not self.state.cached_s3_page is None: self.state.cached_s3_page = int(self.state.cached_s3_page)
                        
                        self.state.cached_kafka_page = self.state.cache.get('%s-kafka-page'%(self.config.cassandra.table))
                        if not self.state.cached_kafka_page is None: self.state.cached_kafka_page = int(self.state.cached_kafka_page)
                        
                        self.state.cached_rds_page = self.state.cache.get('%s-rds-page'%(self.config.cassandra.table))
                        if not self.state.cached_rds_page is None: self.state.cached_rds_page = int(self.state.cached_rds_page)
                        
                        self.state.cached_json_page = self.state.cache.get('%s-json-page'%(self.config.cassandra.table))
                        if not self.state.cached_json_page is None: self.state.cached_json_page = int(self.state.cached_json_page)
                        
                        if self.config.cassandra.fetch_size != self.state.cached_fetch_size:
                            self.state.cached_s3_page = None
                            self.state.cached_kafka_page = None
                            self.state.cached_rds_page = None
                            self.state.cached_json_page = None

                    # Results will be an empty list if no more results are found
                    while self.state.more == True:
                        self.check_memory_usage()

                        if self.is_stopped():
                            self.state.more = False
                            self.print_message("Data crawling stopped")
                            continue

                        if self.is_aborted():
                            self.state.more = False
                            self.print_message("Data crawling aborted")
                            continue
 
                        if self.is_paused():
                            time.sleep(1)
                            continue

                        self.print_message("Fetch page %d" % (self.state.page + 1))
                        try:
                            result = self.fetch_data()

                            # store first select timestamp
                            if self.config.cassandra.modified_timestamp_filter is None:
                                ts = int(self.state.data_fetch_execute_timestamp.timestamp()*1000)
                                self.config.cassandra.modified_timestamp_filter = ts
                                self.worker_manager.config.cassandra.modified_timestamp_filter = ts
                                if not self.state.cache is None:
                                    self.state.cache.set('%s-modified-timestamp-filter'%(self.config.cassandra.table), ts)
                        except Exception as e:
                            self.last_error = str(e)
                            self.print_error(self.last_error)
                            # Hold data crawling
                            self.pause()
                            result = None
                        if result is not None:
                            count = len(result)
                            s3_worker = None
                            kafka_worker = None
                            rds_worker = None
                            json_worker = None
                            if count > 0:
                                 # in case of pause triggered while fetching data
                                if not self.wait_resume():
                                    # aborting
                                    self.state.more = False
                                    continue

                                self.state.page += 1                                
                                self.print_message("Processing %d records" % count)

                                s3_escape = False
                                kafka_escape = False
                                rds_escape = False
                                json_escape = False

                                # try to assign task until abort or stop, and handle 'pause/resume'
                                while True:
                                    # S3 dispatch
                                    if s3_worker is None:
                                        s3_worker, s3_pause, s3_escape = self.dispatch_to_s3(result, count)
                                    # Kafka dispatch
                                    if kafka_worker is None:
                                        kafka_worker, kafka_pause, kafka_escape = self.dispatch_to_kafka(result, count)
                                    # RDS dispatch
                                    if rds_worker is None:
                                        rds_worker, rds_pause, rds_escape = self.dispatch_to_rds(result, count)
                                    # JSON dispatch
                                    if json_worker is None:
                                        json_worker, json_pause, json_escape = self.dispatch_to_json(result, count)

                                    # No worker available found
                                    if s3_pause is True or kafka_pause is True or rds_pause is True or json_pause is True:
                                        if self.is_paused():
                                            # paused while assigning task
                                            pass
                                        self.pause()
                                        self.print_warning("Data crawling paused")

                                        if not self.wait_resume():
                                            # aborting, stop retry dispatch
                                            self.state.more = False
                                        else:
                                            # retry dispatch
                                            continue
                                    break
                                
                                # aborting on dispatch
                                if self.state.more is False:
                                    continue

                                if s3_escape is True or kafka_escape is True or rds_escape is True or json_escape is True:
                                    self.metrics.ignored.labels(self.instance_id).inc(count)

                                self.metrics.processed.labels(self.instance_id).inc(count)

                            if count < self.config.cassandra.fetch_size:
                                self.print_message("No more results, exiting pagination loop")
                                self.state.more = False

                            if self.config.cassandra.pages is not None and self.state.page >= self.config.cassandra.pages:
                                self.state.more = False

                            # Cache important metrics
                            if not self.state.cache is None:
                                if self.state.page == 1:
                                    self.state.cache.set('%s-fetch-size'%(self.config.cassandra.table), self.config.cassandra.fetch_size)
                                if self.state.s3_enabled is True and s3_worker is not None:
                                    self.state.cache.set('%s-s3-page'%(self.config.cassandra.table), self.state.page)
                                if self.state.kafka_enabled is True and kafka_worker is not None:
                                    self.state.cache.set('%s-kafka-page'%(self.config.cassandra.table), self.state.page)
                                if self.state.rds_enabled is True and rds_worker is not None:
                                    self.state.cache.set('%s-rds-page'%(self.config.cassandra.table), self.state.page)
                                if self.state.json_enabled is True and json_worker is not None:
                                    self.state.cache.set('%s-json-page'%(self.config.cassandra.table), self.state.page)
                        else:
                            self.print_message("No results")
                            self.state.more = False

                        # Updat Cache
                        if self.state.more is False:
                            self.metrics.update_end_time()
                            if not self.state.cache is None:
                                self.state.cache.set('%s-end-time'%(self.config.cassandra.table), int(self.metrics.end_time.labels(self.instance_id)._value.get()))

                        time.sleep(0.001)
                else:
                    if not self.is_idle():
                        self.idle()
                    if self.reload_config_event.is_set():
                        if not self.state.session is None:
                            self.state.session.shutdown()
                        self.state.session = self.setup_database()
                        if not self.state.cache is None:
                            self.state.cache = self.setup_cache_session()
                        self.reload_config_event.clear()
                        self.print_message('Configuration reloaded')

                if self.is_stopped():
                    self.idle()
                    break

            except (Exception, UnicodeDecodeError) as e:
                self.last_error = str(e)
                self.logger.error(self.last_error)
                self.idle()

            time.sleep(0.001)
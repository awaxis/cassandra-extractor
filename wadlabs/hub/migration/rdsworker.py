# External imports
from rdsdatabase import RDSDatabase
import json
from datetime import datetime
import uuid
import time
import gc

from worker import Worker
from worker import WorkerType

class RDSWorker(Worker):
    def __init__(self, data, config, logger, cache=None):
        Worker.__init__(self, WorkerType.RDS, config, logger, cache)
        self.queueTask(data)
        self.ds=self.create_rds_session()
        self.table_partitions=None

    def create_rds_session(self):
        return RDSDatabase(
            host=self.config.rds.host,
            port=self.config.rds.port,
            db=self.config.rds.schema,
            user=self.config.rds.username,
            password=self.config.rds.password,
            logger=self.logger
        )

    def insert(self, data):
        if not self.config.rds.partition_column is None:
            if self.ds.partition_exists(self.config.rds.table, data[self.config.rds.partition_column], self.table_partitions) is False:
                partition_name = self.ds.create_partition(self.config.rds.table, data[self.config.rds.partition_column])
                self.table_partitions = self.ds.get_table_partitions(self.config.rds.table)

        sql = self.config.rds.template % (data)
        self.ds.execute_query(sql, True if self.config.rds.use_transaction is False else False)

    def run(self):
        while True:
            try:
                if self.is_stopped():
                    self.idle()
                    break

                # Hold processing tasks
                if self.is_paused():
                    time.sleep(1)
                    continue

                if not self.queue.empty():
                    if self.is_aborted():
                        self.abort_event.clear()

                    if self.is_idle():
                        self.print_message('Exiting Idle mode')
                        self.idle_event.clear()
                        self.metrics.idle_time.labels(self.instance_id).set(time.time() - self.idle_time_offset)
                    
                    if self.reload_config_event.is_set():
                        self.ds = self.create_rds_session()
                        self.reload_config_event.clear()
                        self.print_message('Configuration reloaded')

                    with self.metrics.task_processing_latency.labels(self.instance_id).time():
                        if self.ds.check_connection() is False:
                            raise Exception('RDS database connection lost, exiting worker!')
                        if self.table_partitions is None and not self.config.rds.partition_column is None:
                            self.table_partitions = self.ds.get_table_partitions(self.config.rds.table)
                        if self.config.rds.use_transaction is True:
                            self.ds.start_transaction()

                        task = self.queue.get()

                        self.init_task_offsets(task)

                        try:
                            while self._page_record_offset < self._current_task_count:
                                if self.is_aborted():
                                    break

                                if self.is_stopped():
                                    break
                                
                                if self.is_paused():
                                    time.sleep(1)
                                    continue

                                with self.metrics.write_latency.labels(self.instance_id).time():
                                    record = task[0][self._page_record_offset]

                                    data = json.loads(self.rename_columns(record.json))

                                    process_record = self.prepare_next_record(data)

                                    if process_record == True:
                                        data = self.convert_columns(data)
                                        data['json'] = record.json
                                        self.insert(data)
                                    else:
                                        self.metrics.ignored.labels(self.instance_id).inc()
                                        
                                    self.metrics.processed.labels(self.instance_id).inc()
                                    
                                    self.update_record_offset()

                                time.sleep(0.001)

                            self.queue.task_done()
                            self.update_task_offset()
                            # free task
                            del task

                        except (Exception) as e:
                            self.last_error = str(e)
                            self.print_error(self.last_error)
                            if self.config.rds.use_transaction is True:
                                self.ds.rollback()
                        if self.config.rds.use_transaction is True:
                            self.ds.end_transaction()


                    self.metrics.tasks.labels(self.instance_id).inc()
                    self.metrics.update_end_time()
                    # Free unused resources
                    gc.collect()
                else:
                    if not self.is_idle():
                        self.print_message('Entering Idle mode')
                        self.idle()
                        self.idle_time_offset = time.time()

            except (Exception, UnicodeDecodeError) as e:
                self.last_error = str(e)
                self.print_error(self.last_error)
                self.idle()
                self.pause()
                # Force flush on error
                if self.config.rds.use_transaction is True and self.ds is not None:
                    self.ds.end_transaction()

            time.sleep(.100)

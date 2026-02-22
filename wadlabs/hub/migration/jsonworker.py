# External imports
import json
from datetime import datetime
import uuid
import time
import gc

from worker import Worker
from worker import WorkerType

class JSONWorker(Worker):
    def __init__(self, data, config, logger, cache=None):
        Worker.__init__(self, WorkerType.JSON, config, logger, cache)
        self.queueTask(data)
        self.fp=self.create_output_file()
        self.add_separator=False

    def create_output_file(self):
        fp = open(
            self.config.json.filename,
            self.config.json.mode
        )
        if not self.config.json.header is None:
            fp.write(self.config.json.header)
        return fp

    def stop(self):
        if not self.config.json.footer is None:
            self.fp.write(self.config.json.footer)
        self.fp.close()
        Worker.stop(self)

    def write_object(self, data):
        if not self.config.json.template is None:
            content = self.config.json.template % (data)
        else:
            content = json.dumps(data)

        if self.add_separator:
            self.fp.write(',')
        else:
            self.add_separator = True

        self.fp.write(content)

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
                        task = self.queue.get()

                        self.init_task_offsets(task)
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
                                    self.write_object(data)
                                else:
                                    self.metrics.ignored.labels(self.instance_id).inc()

                                self.metrics.processed.labels(self.instance_id).inc()

                                self.update_record_offset()
                            
                            time.sleep(0.001)
                        
                        self.update_task_offset()
                        # free task
                        del task

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

            time.sleep(.100)

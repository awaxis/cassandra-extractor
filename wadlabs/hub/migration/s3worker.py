# External imports
import boto3
import json
from datetime import datetime
import uuid
import time
import gc

from worker import Worker
from worker import WorkerType

class S3Worker(Worker):
    def __init__(self, data, config, logger, cache=None):
        Worker.__init__(self, WorkerType.S3, config, logger, cache)
        self.queueTask(data)
        self.s3 = boto3.client('s3')

    def put_object(self, data):
        key = self.config.s3.object_key % (data)
        if not self.config.s3.template is None:
            body = self.config.s3.template % (data)
        else:
            body = json.dumps(data)
        self.s3.put_object(Bucket=self.config.s3.bucket, Body=body, Key=key)

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
                                    if not 'id' in data:
                                        data['id'] = str(uuid.uuid1())
                                    self.put_object(data)
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


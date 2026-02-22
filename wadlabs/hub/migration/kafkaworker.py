# External imports
from confluent_kafka import Producer
import json
from datetime import datetime
import uuid
import time
import gc

from worker import Worker
from worker import WorkerType
import roundrobin

KAFKA_BROKER_HOST = '127.0.0.1:9092'

class KafkaWorker(Worker):
    def __init__(self, data, config, logger, cache=None, partition=None):
        Worker.__init__(self, WorkerType.KAFKA, config, logger, cache)
        self.queueTask(data)
        self.partition=partition
        self.last_flush_timestamp=time.time()
        self.producer=self.create_producer()
        self._last_reported_record = 0
 
    def create_producer(self):
        producer=Producer({
            'bootstrap.servers': ','.join(self.config.kafka.hosts),
            #api_version=(0, 11),
            'acks': int(self.config.kafka.acks),
            'linger.ms':self.config.kafka.flush_latency or 0.5,
            'request.timeout.ms':self.config.kafka.request_timeout_ms or 5000,
            'message.max.bytes':self.config.kafka.max_request_size or 1048576,
            'security.protocol':(self.config.kafka.security_protocol or 'PLAINTEXT'),
            #'ssl.endpoint.identification.algorithm': 'https' if self.config.kafka.ssl_check_hostname else 'none',
            'ssl.ca.location':self.config.kafka.ssl_cafile,
            'ssl.certificate.location':self.config.kafka.ssl_certfile,
            'ssl.key.location':self.config.kafka.ssl_keyfile,
            'ssl.key.password':self.config.kafka.ssl_password,
            'error_cb': self.on_error
        })
        if self.config.kafka.partitions is not None:
            if isinstance(self.config.kafka.partitions, str):
                # convert str to array of tuple
                partitions = []
                parts = self.config.kafka.partitions.split(',')
                for partition in parts:
                    id, weight = partition.split(':', 2)
                    partitions.append((id, int(weight)))
                self.config.kafka.partitions = roundrobin.smooth(partitions)

        return producer

    def get_next_partition(self):
        if self.config.kafka.partitions is not None:
            return int(self.config.kafka.partitions())
        return None

    def post_messages(self, data, key=None, topic=None, headers=None, partition=None):
        if self.producer is None or topic is None:
            return 0

        count = 0
        if not self.config.kafka.template is None:
            message = self.config.kafka.template % (data)
        else:
            message = json.dumps(data)
    
        if not self.config.kafka.topics_prefix is None:
            topic = self.config.kafka.topics_prefix + topic

        self.producer.produce(
            topic=topic,
            key=key,
            value=message.encode('utf-8'),
            headers=headers,
            partition=(partition or self.partition or 0),
            on_delivery=lambda err, msg, page=self._page_offset, record=self._page_record_offset: self.on_delivery(err, msg, page, record))

        # Force flush
        self.flush_messages(self.config.kafka.auto_flush)

        count += 1

        return count

    def flush_messages(self, force):
        # Force flush
        flush = force
        if flush is False:
            # Force flush if request_timeout is reached
            if (time.time() - self.last_flush_timestamp) >= self.config.kafka.request_timeout_ms:
                flush = True
            else:
                # Force flush on last record
                if self._current_task_counter >= self._current_task_count:
                    flush = True
        if self.producer is not None:
            try:
                if flush is True:
                    self.producer.flush()
                    self.last_flush_timestamp=time.time()
                else:
                    # poll() serves delivery reports (on_delivery) from previous produce() calls.
                    self.producer.poll(0)

            except Exception as e:
                self.print_error(str(e))

    def on_delivery(self, error, message, page, record):
        if error is not None:
            self.last_error = "Failed to deliver message (page [%d], record [%d]): %s" % (page, record, error)
            self.print_error(self.last_error)
            if message is not None:
                self.print_debug("Failed message page [%d] record [%d] topic [%s] partition [%s] @ offset [%d] key [%s] value [%s]" % 
                    (page, record, message.topic(), message.partition(), message.offset() or 0, message.key() or '', message.value() or '') )
            self.pause()
        else:
            self.print_debug("Message delivered (page [%d], record [%d])" % (page, record))
            self._last_reported_record = max(self._last_reported_record, record)
            if self._last_reported_record == record:
                self.update_record_offset(page, self._last_reported_record)

    def on_error(self, error):
        if error is not None:
            self.last_error = "Producer error: %s" % (error)
            self.print_error(self.last_error)
            self.pause()

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
                        self.producer = self.create_producer()
                        self.reload_config_event.clear()
                        self.print_message('Configuration reloaded')
                    
                    with self.metrics.task_processing_latency.labels(self.instance_id).time():
                        task = self.queue.get()

                        self.init_task_offsets(task)
                        self._last_reported_record = 0
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
                                    key = None
                                    if not self.config.kafka.message_key is None:
                                        key = self.config.kafka.message_key % (data)
                                        key = key.encode('utf-8')
                                    
                                    headers = list()
                                    if not self.config.kafka.headers is None:
                                        for k,v in self.config.kafka.headers.items():
                                            headers.append((k, (v % (data)).encode('utf-8')))

                                    for topic in self.config.kafka.topics:
                                        self.post_messages(data, key=key, headers=headers, topic=topic, partition=self.get_next_partition())
                                else:
                                    self.metrics.ignored.labels(self.instance_id).inc()
                                
                                self.metrics.processed.labels(self.instance_id).inc()

                                # Done in on_delivery callback
                                #self.update_record_offset()

                            time.sleep(0.001)
                        self.queue.task_done()
                        self.update_task_offset()

                        self.print_debug("Last reported message: %d" % self._last_reported_record)
                        # free task
                        del task

                    self.metrics.tasks.labels(self.instance_id).inc()
                    self.metrics.update_end_time()
                    # Force flush on each task
                    self.flush_messages(True)
                    # Free unused resources
                    gc.collect()
                else:
                    if not self.is_idle():
                        self.print_message('Entering Idle mode')
                        self.idle()
                        self.idle_time_offset = time.time()

            except (Exception, UnicodeDecodeError) as e:
                # on error, print error message and hold activity (idle mode and paused)
                self.last_error = str(e)
                self.print_error(self.last_error)
                self.idle()
                self.pause()
                # Force flush on error
                self.flush_messages(True)

            time.sleep(0.001)

# External imports
import json
from datetime import datetime
import uuid
import time
from threading import Thread, Event
import queue

from metrics import WorkerMetrics

EXPRESSION_GLOBALS={'__builtins__': None, 'int': int, 'str': str}

class WorkerType:
    S3 = "s3worker"
    KAFKA = "kafkaworker"
    RDS = "rdsworker"
    DATACRAWLER = "datacrawler"
    JSON = "json"

class Worker(Thread):
    def __init__(self, worker_type, config, logger, cache=None):
        Thread.__init__(self)        
        self.id=uuid.uuid1()
        self.type=worker_type
        self.config=config.copy()
        self.logger=logger
        self.queue=queue.Queue()
        self.stop_event = Event()
        self.idle_event = Event()
        self.abort_event = Event()
        self.pause_event = Event()
        self.reload_config_event = Event()
        self.instance_id= "%s_%s_%s" % (worker_type, config.cassandra.table, str(self.id)[0:8])
        self.metrics=WorkerMetrics(self.instance_id)
        self._current_task_count = 0
        self._current_task_counter = 0
        self._page_offset = 0
        self._page_record_offset = 0
        self._page_last_record_offset = 0
        self._page_last_record_cache_key = None
        self._page_state_cache_key = None
        self.idle_time_offset = 0
        self.cache_session = cache
        self.last_error = None

    def stop(self):
        self.print_message("Shutdown consumer...")
        self.stop_event.set()
        self.resume()
        self.join()

    def is_stopped(self):
        return self.stop_event.is_set()

    def abort(self):
        if self.is_paused():
            self.resume()
        self.abort_event.set()

    def is_aborted(self):
        return self.abort_event.is_set()

    def pause(self):
        if not self.is_paused():
            self.pause_event.set()

    def is_paused(self):
        return self.pause_event.is_set()

    def resume(self):
        self.pause_event.clear()

    def idle(self):
        if not self.is_idle():
            self.idle_event.set()

    def is_idle(self):
        return self.idle_event.is_set()

    def queueTask(self, task):
        self.queue.put(task)
        amount = self.metrics.total.labels(self.instance_id)._value.get()
        self.metrics.total.labels(self.instance_id).set((1 if not isinstance(task[0], list) else len(task[0])) + amount)
    
    def format_message(self, msg):
        return '[id=%s][task=%d][rows=%d/%d] %s' % (
            self.instance_id,
            self.metrics.tasks.labels(self.instance_id)._value.get(),
            self.metrics.processed.labels(self.instance_id)._value.get(),
            self.metrics.total.labels(self.instance_id)._value.get(),
            msg
        )
    
    def print_error(self, err):
        self.logger.error(self.format_message(err))
    
    def print_warning(self, err):
        self.logger.warning(self.format_message(err))
    
    def print_debug(self, msg):
        self.logger.debug(self.format_message(msg))
    
    def print_message(self, msg):
        self.logger.info(self.format_message(msg))

    def reload_config(self, config):
        self.config = config.copy()
        self.reload_config_event.set()

    def rename_columns(self, row):
        if self.config.cassandra.table_specs is None:
            return row

        if not 'columns' in self.config.cassandra.table_specs:
            return row

        for column in self.config.cassandra.table_specs['columns']:
            if 'renameTo' in column:
                row = row.replace(column['name'], column['renameTo'])
        
        return row

    def convert_columns(self, row):
        if self.config.cassandra.table_specs is None:
            return row

        if not 'columns' in self.config.cassandra.table_specs:
            return row

        for column in self.config.cassandra.table_specs['columns']:
            try:
                if 'convertTo' in column:
                    col = column['renameTo'] if 'renameTo' in column else column['name']
                    if not col in row or row[col] is None:
                        continue
                    if column['convertTo'] == 'object':
                        row[col] = json.loads(row[col])
                    elif column['convertTo'] == 'list':
                        new_list = list()
                        for list_item in row[col]:
                            new_list.append(json.loads(list_item))
                        row[col] = new_list
                    elif column['convertTo'] == 'map':
                        for key, value in row[col].items():
                            row[col][key] = json.loads(value)
                    elif column['convertTo'] == 'timestamp':
                        fmt = column['timestamp_format'] if 'timestamp_format' in column else self.config.cassandra.timestamp_format
                        row[col] = int(datetime.strptime(row[col].replace('Z', '+0000'), fmt).timestamp()*1000)
                    elif column['convertTo'][0:8] == 'template' and len(column['convertTo']) > 9:
                        row[col] = eval(column['convertTo'][9:], EXPRESSION_GLOBALS, {'row': row, 'conf': self.config, 'col': col, 'wt': self.type, 'wid': self.id, 'WT': WorkerType})
                    else:
                        pass
                elif 'remove' in column and column['remove'] is True:
                    if column['name'] in row:
                        del row[column['name']]
                else:
                    pass
            except Exception as e:
                self.last_error = str(e)
                pass

        return row

    def init_task_offsets(self, task):
        self._current_task_count = len(task[0])
        self._current_task_counter = 0
        
        # retreive last page cached offest
        self._page_record_offset = 0
        self._page_last_record_offset = 0
        # last page offest
        self._page_offset = task[1]
        if not self.cache_session is None:
            self._page_last_record_cache_key = '%s-%s-page-%d-last-record'%(self.config.cassandra.table, self.type, self._page_offset)
            self._page_last_record_offset = self.cache_session.get(self._page_last_record_cache_key)
            self._page_last_record_offset = 0 if self._page_last_record_offset is None else int(self._page_last_record_offset)
            # page state
            self._page_state_cache_key = '%s-%s-page-%d-state'%(self.config.cassandra.table, self.type, self._page_offset)

    def update_task_offset(self, state=None):
        # cache page status
        if not self.cache_session is None:
            self.cache_session.set(self._page_state_cache_key, state or '1')

    def update_record_offset(self, page=None, record=None):
        # cache last page record offset
        if not self.cache_session is None:
            self.cache_session.set(self._page_last_record_cache_key, record or self._page_record_offset)

    def prepare_next_record(self, record):
        self._page_record_offset += 1
        process_record = self._page_record_offset > self._page_last_record_offset
        
        if not self.config.cassandra.modified_timestamp_filter is None:
            if 'modified_timestamp' in record:
                ts = int(datetime.strptime(record['modified_timestamp'].replace('Z', '+0000'), self.config.cassandra.timestamp_format).timestamp()*1000)
                if ts >= self.config.cassandra.modified_timestamp_filter:
                    process_record = False

        self._current_task_counter += 1
        return process_record

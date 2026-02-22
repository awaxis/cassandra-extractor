# External imports
from prometheus_client import Counter, Summary, Info, Gauge, Histogram, generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST
from datetime import datetime

class WorkerMetrics:
    def __init__(self, instance_id):
        self.instance_id=instance_id
        self.registry = CollectorRegistry()
        self.total = Gauge('total_records', 'Total records to process', ['instance'], registry=self.registry)
        self.processed = Counter('processed_records', 'Processed records', ['instance'], registry=self.registry)
        self.ignored = Counter('ignored_records', 'Ignored records', ['instance'], registry=self.registry)
        self.tasks = Counter('processed_tasks', 'Processed tasks', ['instance'], registry=self.registry)
        self.task_processing_latency = Histogram('task_processing_latency', 'Processing latency', ['instance'], registry=self.registry)
        self.write_latency =  Histogram('write_latency', 'Write latency', ['instance'], registry=self.registry)
        self.idle_time = Gauge('idle_time', 'Idle time', ['instance'], registry=self.registry)
        self.start_time = Gauge('start_time', 'Worker start time', ['instance'], registry=self.registry)
        self.end_time = Gauge('end_time', 'Worker end time', ['instance'], registry=self.registry)
        self.idle_time_offset = datetime.utcnow()
        self.update_start_time()
    
    def metric(self, name, labels=None):
        return self.registry.get_sample_value(name, self.instance_id)

    def update_start_time(self):
        self.start_time.labels(self.instance_id).set(datetime.utcnow().timestamp()*1000)

    def update_end_time(self):
        self.end_time.labels(self.instance_id).set(datetime.utcnow().timestamp()*1000)

class AppMetrics:
    def __init__(self):
    # Create a metric to track time spent and requests made.
        self.registry = CollectorRegistry()
        self.activity =Summary('program_activity_time', 'Program activity time', registry=self.registry)
        self.info = Info('program_info', 'Program Info', registry=self.registry)

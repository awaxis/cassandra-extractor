import json
import copy

class Serializable(object):
    def __str__(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=False, indent=4)

class WSGIConfig(Serializable):
    def __init__(self):
        self.port = None

class LogConfig(Serializable):
    def __init__(self):
        self.directory = None
        self.file_name = None
        self.level = None
        self.format = None
        self.module_name = None

class CassandraConfig(Serializable):
    def __init__(self):
        self.keyspace = None
        self.hosts = None
        self.port = None
        self.username = None
        self.password = None
        self.connect_timeout = None
        self.fetch_size = None
        self.table = None
        self.table_specs_files_location = None
        self.table_specs_file = None
        self.table_specs = None
        self.filter = None
        self.sort_by = None
        self.paging_mode = None
        self.columns = None
        self.pages = None
        self.timestamp_format = None
        self.modified_timestamp_filter = None
        self.estimated_total_rows = None
        self.consistency_level = None
 
class RDSConfig(Serializable):
    def __init__(self):
        self.host = None
        self.port = None
        self.username = None
        self.password = None
        self.schema = None
        self.table = None
        self.columns = None
        self.template = None
        self.use_transaction = None
        self.partition_column = None

class RedisConfig(Serializable):
    def __init__(self):
        self.host = None
        self.port = None
        self.password = None
        self.db = None

class KafkaConfig(Serializable):
    def __init__(self):
        self.hosts = None
        self.topics = None
        self.topics_prefix = None
        self.group_id = None
        self.auto_flush = None
        self.flush_latency = None
        self.request_timeout_ms = None
        self.template = None
        self.message_key = None
        self.headers = None
        self.security_protocol = None
        self.ssl_check_hostname = None
        self.ssl_cafile = None
        self.ssl_certfile = None
        self.ssl_keyfile = None
        self.ssl_password = None
        self.partitions = None
        self.acks = None
        self.max_request_size = None
    
class S3Config(Serializable):
    def __init__(self):    # Load S3 configuration from environment variables first
        self.bucket = None
        self.object_key = None

class JSONConfig(Serializable):
    def __init__(self):    # Load S3 configuration from environment variables first
        self.filename = None
        self.mode = None
        self.header = None
        self.footer = None
        self.template = None

class Config(Serializable):
    def __init__(self):
        self.enable_kafka=False
        self.enable_rds=False
        self.enable_s3=False
        self.enable_json=False
        self.max_worker_threads = None
        self.auto_start_workers = None
        self.exit_on_data_crawling_ends = None
        self.default_client_id = None
        self.stateless = None
        self.log = LogConfig()
        self.cassandra = CassandraConfig()
        self.kafka = KafkaConfig()
        self.s3 = S3Config()
        self.wsgi = WSGIConfig()
        self.rds = RDSConfig()
        self.redis = RedisConfig()
        self.json = JSONConfig()
 
    def copy(self):
        config  = Config()
        config.log = self.log
        config.cassandra = copy.copy(self.cassandra)
        config.kafka = copy.copy(self.kafka)
        config.s3 = copy.copy(self.s3)
        config.wsgi = copy.copy(self.wsgi)
        config.rds = copy.copy(self.rds)
        config.redis = copy.copy(self.redis)
        config.json = copy.copy(self.json)
        config.enable_kafka = copy.copy(self.enable_kafka)
        config.enable_rds = copy.copy(self.enable_rds)
        config.enable_s3 = copy.copy(self.enable_s3)
        config.enable_json = copy.copy(self.enable_json)
        config.max_worker_threads = copy.copy(self.max_worker_threads)
        config.auto_start_workers = copy.copy(self.auto_start_workers)
        config.exit_on_data_crawling_ends = copy.copy(self.exit_on_data_crawling_ends)
        config.default_client_id = copy.copy(self.default_client_id)
        config.stateless = copy.copy(self.stateless)
        config.max_memory_usage = copy.copy(self.max_memory_usage)
        config.max_memory_usage_resume_threshold = copy.copy(self.max_memory_usage_resume_threshold)
        config.memory_usage_check_delay = copy.copy(self.memory_usage_check_delay)

        return config

    def to_json(self):
        config  = self.copy()
        if not config.cassandra.password is None:
            config.cassandra.password = '********'
        if not config.rds.password is None:
            config.rds.password = '********'
        if not config.redis.password is None:
            config.redis.password = '********'
        if not config.kafka.ssl_password is None:
            config.kafka.ssl_password = '********'
        return str(config)

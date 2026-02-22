import os
import glob
import socket
from logging import handlers
import sys
import signal
from threading import Event
import logging
from gelfformatter import GelfFormatter
import time
import configparser
from datetime import datetime
from io import StringIO
import argparse
import uuid
import json
from prometheus_client import Counter, Summary, Info, Gauge, Histogram, generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST
import redis

# local imports
from rdsdatabase import RDSDatabase
from fakesectionhead import FakeSectionHead
from workermanager import WorkerType
from workermanager import WorkerManager
from datacrawler import DataCrawler, PagingMode
from metrics import AppMetrics, WorkerMetrics
from config import Config
from extractor import Extractor
from management import Actuator

##### SETTINGS ###################################
HOSTNAME = socket.gethostname()
PROJECT_NAME = 'Cassandra Migration'
VERSION = '2.3.7'
NAMESPACE = os.environ.get("NAMESPACE") or 'DEV'
LOGGER = None
ARGS_PARSER = argparse.ArgumentParser()

class App():
    EXIT_EVENT = Event()
    def __init__(self, config, logger):
        self.extractors = list()
        self.actuator = None
        self.config = config
        self.logger = logger
        self.metrics = AppMetrics()
        self.exit_message = 'Ctrl+C fired, exiting'
        self.metrics.activity.time()
        self.metrics.info.info({'name': PROJECT_NAME, 'version': VERSION})
 
    # Global functions
    @staticmethod
    def exit_gracefully(signum, frame):
        App.EXIT_EVENT.set()

    def create_extractor(self, table):
        if table.strip() == '':
            return None
        conf = load_table_config(table=table, properties_file=table+'.properties', global_config=self.config, logger=self.logger)
        if conf is None:
            return None
        log = setup_logging_to_file(conf)
        return Extractor(table, conf, log)

    def register_extractor(self, extractor):
        self.extractors.append(extractor)

    def unregister_extractor(self, extractor):
        self.extractors.remove(extractor)

    def load_table_config(self, table, raw_properties):
        return load_table_config(table=table, properties_file=None, raw_properties=raw_properties, global_config=self.config, logger=self.logger)

    def main(self):
        for table in self.config.cassandra.tables:
            extractor = self.create_extractor(table)
            if not extractor is None:
                self.register_extractor(extractor)

        for extractor in self.extractors:
            extractor.start()

        self.actuator = Actuator(self, self.config, self.logger)
        self.actuator.start()

        for extractor in self.extractors:
            if extractor.config.auto_start_workers is True:
                extractor.queueTask(({'action':'start'}, 0))

        try:
            while True:
                time.sleep(1)
                self.metrics.activity.observe(1)
                if App.EXIT_EVENT.is_set():
                    logger.info(self.exit_message)
                    break
        except Exception as e:
            logger.info(str(e))
            App.EXIT_EVENT.set()

        for extractor in self.extractors:
            extractor.stop()
        
        self.actuator.stop()

class LogContextFilter(logging.Filter):
    def filter(self, record):
        # Add any number of arbitrary additional fields
        record.app = PROJECT_NAME
        record.version = VERSION
        record.host = HOSTNAME
        record.namespace_name = NAMESPACE

        return True
def get_option(properties, section, option, default=None):
    try:
        if section != configparser.DEFAULTSECT and properties.has_section(section):
            return properties.get(section, option, fallback=default)

        if section == configparser.DEFAULTSECT:
            return properties.get(configparser.DEFAULTSECT, option, fallback=default)
        else:
            return properties.get(configparser.DEFAULTSECT, section.lower()+'_'+option, fallback=default)
    except (configparser.NoOptionError, configparser.NoSectionError) as _e:
        return default

def get_option_int(properties, section, option, default=None):
    try:
        if section != configparser.DEFAULTSECT and properties.has_section(section):
            return properties.getint(section, option, fallback=default)

        if section == configparser.DEFAULTSECT:
            return properties.getint(configparser.DEFAULTSECT, option, fallback=default)
        else:
            return properties.getint(configparser.DEFAULTSECT, section.lower()+'_'+option, fallback=default)
    except (configparser.NoOptionError, configparser.NoSectionError) as _e:
        return default

def get_option_boolean(properties, section, option, default=None):
    try:
        if section != configparser.DEFAULTSECT and properties.has_section(section):
            return properties.getboolean(section, option, fallback=default)

        if section == configparser.DEFAULTSECT:
            return properties.getboolean(configparser.DEFAULTSECT, option, fallback=default)
        else:
            return properties.getboolean(configparser.DEFAULTSECT, section.lower()+'_'+option, fallback=default)
    except (configparser.NoOptionError, configparser.NoSectionError) as _e:
        return default

def get_env(name, cli_value=None, default=None, as_list=False, list_sep=','):
    if cli_value is not None:
        return cli_value
    else:
        value = os.environ.get(name)
        if value is not None:
            if as_list:
                return value.split(list_sep)
            return value
    return default

def get_int_env(name, cli_value=None, default=None):
    value = get_env(name=name, cli_value=cli_value, default=default)
    if value is not None:
        return int(value)
    return default

def get_boolean_env(name, cli_value=None, default=None):
    value = get_env(name=name, cli_value=cli_value, default=default)
    if value is not None:
        return str2bool(value)
    return default

def get_property(properties, section, option, default=None, env=None, cli_value=None, as_list=False, list_sep=','):
    value = None
    if env is not None:
        value = get_env(name=env, cli_value=cli_value, as_list=False, list_sep=list_sep)
    if value is None:
        value = get_option(properties, section, option, default)
    if value is not None and as_list:
        value = value.split(list_sep)
    return value

def get_int_property(properties, section, option, default=None, env=None, cli_value=None):
    value = None
    if env is not None:
        value = get_int_env(name=env, cli_value=cli_value)
    if value is None:
        value = get_option_int(properties, section, option, default)
    return value

def get_boolean_property(properties, section, option, default=None, env=None, cli_value=None):
    value = None
    if env is not None:
        value = get_boolean_env(name=env, cli_value=cli_value)
    if value is None:
        value = get_option_boolean(properties, section, option, default)
    return value

def str2bool(value):
    if isinstance(value, bool):
       return value
    if value.lower() in ('yes', 'true', 'on', 't', 'y', '1'):
        return True
    elif value.lower() in ('no', 'false', 'off', 'f', 'n', '0'):
        return False
    else:
        return False

def log_message(level, message):
    print("{timestamp} {module} {level} {message}".format(
        timestamp= datetime.utcnow(),
        module= "CassandraMigration.app",
        level= level,
        message= message
    ))

def setup_logging_to_file(config):
    """
    Logging configuration
    :param config:
    :return: Configured logger instance
    """
    # Already exists
    if config.log.module_name in logging.Logger.manager.loggerDict:
        return logging.getLogger(config.log.module_name)

    log = logging.getLogger(config.log.module_name)
    log.setLevel(config.log.level)
    log.addFilter(LogContextFilter())
    
    if config.log.enable_gelf_format is True:
        formatter = GelfFormatter(allowed_reserved_attrs=config.log.extra_attrs)
    else:
        formatter = logging.Formatter(config.log.format)
    # STDOUT handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    log.addHandler(sh)
    # File handler
    fh = handlers.TimedRotatingFileHandler('%s/%s' % (config.log.directory, os.path.basename(config.log.file_name)), when="D", interval=1, backupCount=30)
    fh.setFormatter(formatter)
    log.addHandler(fh)
    return log

def load_global_config(properties_file = None):
    config = Config()
    args = ARGS_PARSER.parse_args()

    args.config_file = get_env('CONFIG_FILE', cli_value=args.config_file)

    script_basename = os.path.basename(sys.argv[0][:-3])

    properties = configparser.ConfigParser(allow_no_value=True, strict=False)

    if not args.config_file is None:
        properties_file = args.config_file

    if properties_file is None:
        prop_file = script_basename + '.properties'
        if os.path.isfile(prop_file):
            properties_file = prop_file
        else:
            prop_file = '/etc/wad-labs/' + prop_file
            if os.path.isfile(prop_file):
                properties_file = prop_file

    if not properties_file is None:
        if os.path.isfile(properties_file):
            log_message('INFO', "Loading properties file '%s'" % properties_file)
            properties.read_file(FakeSectionHead(properties_file))
        else:
            log_message('WARNING', "Invalid properties file '%s'" % properties_file)

    config.default_client_id = get_property(properties, 'DEFAULT', 'client_id', '00000000-0000-0000-0000-000000000000', env='DEFAULT_CLIENT_ID', cli_value=args.default_client_id)
    config.stateless = get_boolean_property(properties, 'DEFAULT', 'stateless', True, env='STATELESS_CRAWLER', cli_value=args.stateless)
    config.wsgi.port = get_int_property(properties, 'WSGI', 'port', 8000, env='WSGI_PORT', cli_value=args.wsgi_port)

    config.enable_kafka = get_boolean_property(properties, 'DEFAULT', 'enable_kafka', False, env='ENABLE_KAFKA', cli_value=args.enable_kafka)
    config.enable_rds = get_boolean_property(properties, 'DEFAULT', 'enable_rds', False, env='ENABLE_RDS', cli_value=args.enable_rds)
    config.enable_s3 = get_boolean_property(properties, 'DEFAULT', 'enable_s3', False, env='ENABLE_S3', cli_value=args.enable_s3)
    config.enable_json = get_boolean_property(properties, 'DEFAULT', 'enable_json', False, env='ENABLE_JSON', cli_value=args.enable_json)

    config.max_worker_threads = get_int_property(properties, 'DEFAULT', 'max_worker_threads', 1, env='MAX_WORKER_THREADS', cli_value=args.max_worker_threads)
    config.auto_start_workers = get_boolean_property(properties, 'DEFAULT', 'auto_start_workers', False, env='AUTO_START_WORKERS', cli_value=args.auto_start_workers)
    config.max_memory_usage = get_int_property(properties, 'DEFAULT', 'max_memory_usage', 256, env='MAX_MEMORY_USAGE', cli_value=args.max_memory_usage)
    config.max_memory_usage_resume_threshold = get_int_property(properties, 'DEFAULT', 'max_memory_usage_resume_threshold', config.max_memory_usage-16, env='MAX_MEMORY_USAGE_RESUME_THRESHOLD', cli_value=args.max_memory_usage_resume_threshold)
    config.memory_usage_check_delay = get_int_property(properties, 'DEFAULT', 'memory_usage_check_delay', 20, env='MEMORY_USAGE_CHECK_DELAY', cli_value=args.memory_usage_check_delay)

    # Load Redis configuration from environment variables first
    config.redis.host = get_property(properties, 'REDIS', 'host', None, env='REDIS_HOST', cli_value=args.redis_host)
    config.redis.port = get_int_property(properties, 'REDIS', 'port', None if config.redis.host is None else 6379, env='REDIS_PORT', cli_value=args.redis_port)
    config.redis.password = get_property(properties, 'REDIS', 'password', None, env='REDIS_PASSWORD', cli_value=args.redis_password)
    config.redis.db = get_int_property(properties, 'REDIS', 'db', None, env='REDIS_DB', cli_value=args.redis_db)

    # Load database configuration from environment variables first
    # Otherwise, load database configuration from properties file
    config.cassandra.keyspace = get_property(properties, 'CASSANDRA', 'keyspace', 'hub', env='CASSANDRA_KEYSPACE', cli_value=args.keyspace)
    config.cassandra.hosts = get_property(properties, 'CASSANDRA', 'hosts', 'localhost', as_list=True, env='CASSANDRA_HOSTS', cli_value=args.hosts)
    config.cassandra.port = get_int_property(properties, 'CASSANDRA', 'port', 9042, env='CASSANDRA_PORT', cli_value=args.port)
    config.cassandra.username = get_property(properties, 'CASSANDRA', 'username', None, env='CASSANDRA_USERNAME', cli_value=args.username)
    config.cassandra.password = get_property(properties, 'CASSANDRA', 'password', None, env='CASSANDRA_PASSWORD', cli_value=args.password)
    config.cassandra.connect_timeout = get_int_property(properties, 'CASSANDRA', 'connect_timeout', 200, env='CASSANDRA_CONNECT_TIMEOUT', cli_value=args.connect_timeout)
    config.cassandra.fetch_size = get_int_property(properties, 'CASSANDRA', 'fetch_size', 10000, env='CASSANDRA_FETCH_SIZE', cli_value=args.fetch_size)
    config.cassandra.consistency_level = get_property(properties, 'CASSANDRA', 'consistency_level', 'ONE', env='CASSANDRA_CONSISTENCY_LEVEL', cli_value=args.consistency_level)
    config.cassandra.pages = get_int_property(properties, 'CASSANDRA', 'pages', None, env='CASSANDRA_PAGES', cli_value=args.pages)
    config.cassandra.timestamp_format = get_property(properties, 'CASSANDRA', 'timestamp_format', '%Y-%m-%d %H:%M:%S.%f%z', env='CASSANDRA_TIMESTAMP_FORMAT', cli_value=args.timestamp_format)
    config.cassandra.modified_timestamp_filter = get_int_property(properties, 'CASSANDRA', 'modified_timestamp_filter', None, env='CASSANDRA_TIMESTAMP_FORMAT_FILTER', cli_value=args.modified_timestamp_filter)
    config.cassandra.tables = get_property(properties, 'CASSANDRA', 'tables', '', as_list=True, env='CASSANDRA_TABLES', cli_value=args.tables)
    config.cassandra.table_specs_files_location = get_property(properties, 'CASSANDRA', 'table_specs_files_location', 'conf', env='CASSANDRA_TABLE_SPECS_FILES_LOCATION', cli_value=args.table_specs_files_location)

    # Load logging configuration from environment variables first
    # Otherwise, load log configuration from properties file
    config.log.directory = get_property(properties, 'LOG', 'directory', './', env='LOG_DIRECTORY', cli_value=args.log_directory)
    config.log.file_name = get_property(properties, 'LOG', 'file_name', script_basename + '.log', env='LOG_FILE_NAME', cli_value=args.log_file_name)
    config.log.level = get_property(properties, 'LOG', 'level', 'DEBUG', env='LOG_LEVEL', cli_value=args.log_level)
    config.log.format = get_property(properties, 'LOG', 'format', '%(asctime)s %(name)s %(levelname)s %(message)s', env='LOG_FORMAT', cli_value=args.log_format)
    config.log.module_name = get_property(properties, 'LOG', 'module_name', PROJECT_NAME.replace(' ', '') + '.' + script_basename, env='LOG_MODULE_NAME', cli_value=args.log_module_name)
    config.log.enable_gelf_format = get_boolean_property(properties, 'LOG', 'enable_gelf_format', False, env='LOG_ENABLE_GELF_FORMAT', cli_value=args.log_enable_gelf_format)
    config.log.extra_attrs = get_property(properties, 'LOG', 'extra_attrs', 'message,lineno,module,name,thread,levelname', as_list=True, env='LOG_EXTRA_ATTRS', cli_value=args.log_extra_attrs)

    # Load RDS datasource configuration from environment variables first
     # Otherwise, load RDS datasource configuration from properties file
    config.rds.host = get_property(properties, 'RDS', 'host', 'localhost', env='RDS_HOST', cli_value=args.rds_host)
    config.rds.port = get_int_property(properties, 'RDS', 'port', 5432, env='RDS_PORT', cli_value=args.rds_port)
    config.rds.username = get_property(properties, 'RDS', 'username', 'dbuser', env='RDS_USERNAME', cli_value=args.rds_username)
    config.rds.password = get_property(properties, 'RDS', 'password', None, env='RDS_PASSWORD', cli_value=args.rds_password)
    config.rds.schema = get_property(properties, 'RDS', 'schema', None, env='RDS_SCHEMA', cli_value=args.rds_schema)

    # Load kafka broker configuration from environment variables first
    # Otherwise, load kafka broker configuration from properties file
    config.kafka.hosts = get_property(properties, 'KAFKA', 'hosts', None, as_list=True, env='KAFKA_HOSTS', cli_value=args.kafka_hosts)
    config.kafka.security_protocol = get_property(properties, 'KAFKA', 'security_protocol', None, env='KAFKA_SECURITY_PROTOCOL', cli_value=args.kafka_security_protocol)
    config.kafka.ssl_check_hostname = get_boolean_property(properties, 'KAFKA', 'ssl_check_hostname', None, env='KAFKA_SSL_CHECK_HOSTNAME', cli_value=args.kafka_ssl_check_hostname)
    config.kafka.ssl_cafile = get_property(properties, 'KAFKA', 'ssl_cafile', None, env='KAFKA_SSL_CAFILE', cli_value=args.kafka_ssl_cafile)
    config.kafka.ssl_certfile = get_property(properties, 'KAFKA', 'ssl_certfile', None, env='KAFKA_SSL_CERTFILE', cli_value=args.kafka_ssl_certfile)
    config.kafka.ssl_keyfile = get_property(properties, 'KAFKA', 'ssl_keyfile', None, env='KAFKA_SSL_KEYFILE', cli_value=args.kafka_ssl_keyfile)
    config.kafka.ssl_password = get_property(properties, 'KAFKA', 'ssl_password', None, env='KAFKA_SSL_PASSWORD', cli_value=args.kafka_ssl_password)
    config.kafka.topics_prefix = get_property(properties, 'KAFKA', 'topics_prefix', None, env='KAFKA_TOPIC_PREFIX', cli_value=args.kafka_topics_prefix)
    config.kafka.auto_flush = get_boolean_property(properties, 'KAFKA', 'auto_flush', False, env='KAFKA_AUTO_FLUSH', cli_value=args.kafka_auto_flush)
    config.kafka.flush_latency = get_int_property(properties, 'KAFKA', 'flush_latency', 0, env='KAFKA_FLUSH_LATENCY', cli_value=args.kafka_flush_latency)
    config.kafka.request_timeout_ms = get_int_property(properties, 'KAFKA', 'request_timeout_ms', 30000, env='KAFKA_REQUEST_TIMEOUT_MS', cli_value=args.kafka_request_timeout_ms)
    config.kafka.message_key = get_property(properties, 'KAFKA', 'message_key', None, env='KAFKA_MESSAGE_KEY', cli_value=args.kafka_message_key)
    config.kafka.headers = get_property(properties, 'KAFKA', 'headers', None, env='KAFKA_HEADERS', cli_value=args.kafka_headers)
    config.kafka.partitions = get_property(properties, 'KAFKA', 'partitions', None, env='KAFKA_PARTITIONS', cli_value=args.kafka_partitions)
    config.kafka.acks = get_property(properties, 'KAFKA', 'acks', '-1', env='KAFKA_ACKS', cli_value=args.kafka_acks)
    config.kafka.max_request_size = get_int_property(properties, 'KAFKA', 'max_request_size', None, env='KAFKA_MAX_REQUEST_SIZE', cli_value=args.kafka_max_request_size)
    if not config.kafka.headers is None:
        config.kafka.headers = json.loads(config.kafka.headers)

    # Load S3 configuration from environment variables first
    # Otherwise, load S3 configuration from properties file
    config.s3.bucket = get_property(properties, 'S3', 'bucket', None, env='S3_BUCKET', cli_value=args.s3_bucket)

    # Load JSON configuration from environment variables first
    # Otherwise, load JSON configuration from properties file
    config.json.filename = get_property(properties, 'JSON', 'filename', None, env='JSON_FILENAME', cli_value=args.json_filename)
    config.json.mode = get_property(properties, 'JSON', 'mode', None, env='JSON_MODE', cli_value=args.json_mode)
    config.json.header = get_property(properties, 'JSON', 'header', None, env='JSON_HEADER', cli_value=args.json_header)
    config.json.footer = get_property(properties, 'JSON', 'footer', None, env='JSON_FOOTER', cli_value=args.json_footer)
    config.json.template = get_property(properties, 'JSON', 'template', None, env='JSON_TEMPLATE', cli_value=args.json_template)
 
    return config

def load_table_config(table, properties_file = None, raw_properties = None, global_config=None, logger=None):
    if properties_file is None and raw_properties is None:
        return None
    config = Config()

    properties = configparser.ConfigParser(allow_no_value=True, strict=False)

    if raw_properties is not None:
        logger.info("Loading raw properties")
        if raw_properties[0:9] != '[DEFAULT]':
            raw_properties = '[DEFAULT]\n'+raw_properties
        properties.read_string(raw_properties)
    else:
        if not os.path.isfile(properties_file):
            prop_file =  global_config.cassandra.table_specs_files_location + '/' + properties_file
            if os.path.isfile(prop_file):
                properties_file = prop_file
            else:
                prop_file = '/etc/wad-labs/' + properties_file
                if os.path.isfile(prop_file):
                    properties_file = prop_file
                else:
                    prop_file = '/etc/wad-labs/specs/' + properties_file
                    if os.path.isfile(prop_file):
                        properties_file = prop_file
                    else:
                        prop_file = '/data/config/' + properties_file
                        if os.path.isfile(prop_file):
                            properties_file = prop_file
                        else:
                            logger.warning("Invalid properties file '%s'" % properties_file)
                            return None
    
        logger.info("Loading properties file '%s'" % properties_file)
        properties.read_file(FakeSectionHead(properties_file))

    table_name = table.upper()

    config.default_client_id = global_config.default_client_id
    config.wsgi.port = global_config.wsgi.port
    config.stateless = get_boolean_property(properties, 'DEFAULT', 'stateless', global_config.stateless, env=((table_name+'_STATELESS_CRAWLER') if raw_properties is None else None))

    config.enable_kafka = get_boolean_property(properties, 'DEFAULT', 'enable_kafka', global_config.enable_kafka, env=((table_name+'_ENABLE_KAFKA') if raw_properties is None else None))
    config.enable_rds = get_boolean_property(properties, 'DEFAULT', 'enable_rds', global_config.enable_rds, env=((table_name+'_ENABLE_RDS') if raw_properties is None else None))
    config.enable_s3 = get_boolean_property(properties, 'DEFAULT', 'enable_s3', global_config.enable_s3, env=((table_name+'_ENABLE_S3') if raw_properties is None else None))
    config.enable_json = get_boolean_property(properties, 'DEFAULT', 'enable_json', global_config.enable_json, env=((table_name+'_ENABLE_JSON') if raw_properties is None else None))
    config.max_worker_threads = get_int_property(properties, 'DEFAULT', 'max_worker_threads', global_config.max_worker_threads, env=((table_name+'_MAX_WORKER_THREADS') if raw_properties is None else None))
    config.auto_start_workers = get_boolean_property(properties, 'DEFAULT', 'auto_start_workers', global_config.auto_start_workers, env=((table_name+'_AUTO_START_WORKERS') if raw_properties is None else None))
    config.max_memory_usage = get_int_property(properties, 'DEFAULT', 'max_memory_usage', global_config.max_memory_usage, env=((table_name+'_MAX_MEMORY_USAGE') if raw_properties is None else None))
    config.max_memory_usage_resume_threshold = get_int_property(properties, 'DEFAULT', 'max_memory_usage_resume_threshold', global_config.max_memory_usage_resume_threshold if global_config.max_memory_usage_resume_threshold<config.max_memory_usage else (config.max_memory_usage-16), env=((table_name+'_MAX_MEMORY_USAGE_RESUME_THRESHOLD') if raw_properties is None else None))
    config.memory_usage_check_delay = get_int_property(properties, 'DEFAULT', 'memory_usage_check_delay', global_config.memory_usage_check_delay, env=((table_name+'_MEMORY_USAGE_CHECK_DELAY') if raw_properties is None else None))

    # Load Redis configuration from environment variables first
    # Load Redis configuration from global config
    config.redis.host = get_property(properties, 'REDIS', 'host', global_config.redis.host, env=((table_name+'_REDIS_HOST') if raw_properties is None else None))
    config.redis.port = get_int_property(properties, 'REDIS', 'port', global_config.redis.port, env=((table_name+'_REDIS_PORT') if raw_properties is None else None))
    config.redis.password = get_property(properties, 'REDIS', 'password', global_config.redis.password, env=((table_name+'_REDIS_PASSWORD') if raw_properties is None else None))
    config.redis.db = get_int_property(properties, 'REDIS', 'db', global_config.redis.db, env=((table_name+'_REDIS_DB') if raw_properties is None else None))

    # Load database configuration from environment variables
    # Otherwise, load log configuration from properties file
    config.log.directory = get_property(properties, 'LOG', 'directory', global_config.log.directory, env=((table_name+'_LOG_DIRECTORY') if raw_properties is None else None))
    config.log.level = get_property(properties, 'LOG', 'level', global_config.log.level, env=((table_name+'_LOG_LEVEL') if raw_properties is None else None))
    config.log.format = get_property(properties, 'LOG', 'format', global_config.log.format, env=((table_name+'_LOG_FORMAT') if raw_properties is None else None))
    config.log.file_name = get_property(properties, 'LOG', 'file_name', table + '.log', env=((table_name+'_LOG_FILE_NAME') if raw_properties is None else None))
    config.log.module_name = get_property(properties, 'LOG', 'module_name', PROJECT_NAME.replace(' ', '') + '.' + table, env=((table_name+'_LOG_MODULE_NAME') if raw_properties is None else None))
    config.log.enable_gelf_format = get_boolean_property(properties, 'LOG', 'enable_gelf_format', global_config.log.enable_gelf_format, env=((table_name+'_LOG_ENABLE_GELF_FORMAT') if raw_properties is None else None))
    config.log.extra_attrs = get_property(properties, 'LOG', 'extra_attrs', ','.join(global_config.log.extra_attrs), as_list=True, env=((table_name+'_LOG_EXTRA_ATTRS') if raw_properties is None else None))

    # Load database configuration from environment variables first
    # Otherwise, load database configuration from properties file
    config.cassandra.table = table
    config.cassandra.keyspace = get_property(properties, 'CASSANDRA', 'keyspace', global_config.cassandra.keyspace, env=((table_name+'_CASSANDRA_KEYSPACE') if raw_properties is None else None))
    config.cassandra.hosts = get_property(properties, 'CASSANDRA', 'hosts', ','.join(global_config.cassandra.hosts), as_list=True, env=((table_name+'_CASSANDRA_HOSTS') if raw_properties is None else None))
    config.cassandra.port = get_int_property(properties, 'CASSANDRA', 'port', global_config.cassandra.port, env=((table_name+'_CASSANDRA_PORT') if raw_properties is None else None))
    config.cassandra.username = get_property(properties, 'CASSANDRA', 'username', global_config.cassandra.username, env=((table_name+'_CASSANDRA_USERNAME') if raw_properties is None else None))
    config.cassandra.password = get_property(properties, 'CASSANDRA', 'password', global_config.cassandra.password, env=((table_name+'_CASSANDRA_PASSWORD') if raw_properties is None else None))
    config.cassandra.connect_timeout = get_int_property(properties, 'CASSANDRA', 'connect_timeout', global_config.cassandra.connect_timeout, env=((table_name+'_CASSANDRA_CONNECT_TIMEOUT') if raw_properties is None else None))
    config.cassandra.fetch_size = get_int_property(properties, 'CASSANDRA', 'fetch_size', global_config.cassandra.fetch_size, env=((table_name+'_CASSANDRA_FETCH_SIZE') if raw_properties is None else None))
    config.cassandra.consistency_level = get_property(properties, 'CASSANDRA', 'consistency_level', global_config.cassandra.consistency_level, env=((table_name+'_CASSANDRA_CONSISTENCY_LEVEL') if raw_properties is None else None))
    config.cassandra.filter = get_property(properties, 'CASSANDRA', 'filter', None, env=((table_name+'_CASSANDRA_FILTER') if raw_properties is None else None))
    config.cassandra.sort_by = get_property(properties, 'CASSANDRA', 'sort_by', None, env=((table_name+'_CASSANDRA_SORT_BY') if raw_properties is None else None))
    config.cassandra.paging_mode = get_property(properties, 'CASSANDRA', 'paging_mode', PagingMode.DRIVER, env=((table_name+'_CASSANDRA_PAGING_MODE') if raw_properties is None else None))
    config.cassandra.columns = get_property(properties, 'CASSANDRA', 'columns', '*', as_list=True, env=((table_name+'_CASSANDRA_COLUMNS') if raw_properties is None else None))
    config.cassandra.pages = get_int_property(properties, 'CASSANDRA', 'pages', global_config.cassandra.pages, env=((table_name+'_CASSANDRA_PAGES') if raw_properties is None else None))
    config.cassandra.table_specs = get_property(properties, 'CASSANDRA', 'table_specs', None, env=((table_name+'_CASSANDRA_TABLE_SPECS') if raw_properties is None else None))
    config.cassandra.table_specs_file = get_property(properties, 'CASSANDRA', 'table_specs_file', None, env=((table_name+'_CASSANDRA_TABLE_SPECS_FILE') if raw_properties is None else None))
    config.cassandra.table_specs_files_location = get_property(properties, 'CASSANDRA', 'table_specs_files_location', global_config.cassandra.table_specs_files_location, env=((table_name+'_CASSANDRA_TABLE_SPECS_FILES_LOCATION') if raw_properties is None else None))
    config.cassandra.timestamp_format = get_property(properties, 'CASSANDRA', 'timestamp_format', global_config.cassandra.timestamp_format, env=((table_name+'_CASSANDRA_TIMESTAMP_FORMAT') if raw_properties is None else None))
    config.cassandra.modified_timestamp_filter = get_int_property(properties, 'CASSANDRA', 'modified_timestamp_filter', None, env=((table_name+'_CASSANDRA_MODIFIED_TIMESTAMP_FILTER') if raw_properties is None else None))
    config.cassandra.estimated_total_rows = get_int_property(properties, 'CASSANDRA', 'estimated_total_rows', None, env=((table_name+'_CASSANDRA_ESTIMATED_TOTAL_ROWS') if raw_properties is None else None))

    if config.cassandra.table_specs_file is None:
        table_specs_file = config.cassandra.table_specs_files_location + '/' + table + '.json'
        if os.path.isfile(table_specs_file):
            config.cassandra.table_specs_file = table_specs_file
        else:
            table_specs_file = '/etc/wad-labs/' + table + '.json'
            if os.path.isfile(table_specs_file):
                config.cassandra.table_specs_file = table_specs_file
            else:
                table_specs_file = '/etc/wad-labs/specs/' + table + '.json'
                if os.path.isfile(table_specs_file):
                    config.cassandra.table_specs_file = table_specs_file
                else:
                    table_specs_file = '/data/config/' + table + '.json'
                    if os.path.isfile(table_specs_file):
                        config.cassandra.table_specs_file = table_specs_file

    if not config.cassandra.table_specs_file is None:
        try:
            logger.info("Loading Cassandra table specs file '%s'" % config.cassandra.table_specs_file)
            config.cassandra.table_specs = json.load(open(config.cassandra.table_specs_file))
        except Exception as e:
            logger.info("error loading Cassandra table specs file '%s': %s" % (config.cassandra.table_specs_file, str(e)))

    if not config.cassandra.table_specs is None and isinstance(config.cassandra.table_specs, str):
        config.cassandra.table_specs = json.loads(config.cassandra.table_specs)

    # Load RDS datasource configuration from environment variables first  
    # Otherwise, load RDS datasource configuration from properties file
    config.rds.host = get_property(properties, 'RDS', 'host', global_config.rds.host, env=((table_name+'_RDS_HOST') if raw_properties is None else None))
    config.rds.port = get_int_property(properties, 'RDS', 'port', global_config.rds.port, env=((table_name+'_RDS_PORT') if raw_properties is None else None))
    config.rds.username = get_property(properties, 'RDS', 'username', global_config.rds.username, env=((table_name+'_RDS_USERNAME') if raw_properties is None else None))
    config.rds.password = get_property(properties, 'RDS', 'password', global_config.rds.password, env=((table_name+'_RDS_PASSWORD') if raw_properties is None else None))
    config.rds.schema = get_property(properties, 'RDS', 'schema', global_config.rds.schema, env=((table_name+'_RDS_SCHEMA') if raw_properties is None else None))
    config.rds.table = get_property(properties, 'RDS', 'table', None, env=((table_name+'_RDS_TABLE') if raw_properties is None else None))
    config.rds.columns = get_property(properties, 'RDS', 'columns', None, env=((table_name+'_RDS_COLUMNS') if raw_properties is None else None))
    config.rds.template = get_property(properties, 'RDS', 'template', None, env=((table_name+'_RDS_TEMPLATE') if raw_properties is None else None))
    config.rds.use_transaction = get_boolean_property(properties, 'RDS', 'use_transaction', False, env=((table_name+'_RDS_USE_TRANSACTION') if raw_properties is None else None))
    config.rds.partition_column = get_property(properties, 'RDS', 'partition_column', None, env=((table_name+'_RDS_PARTITION_COLUMN') if raw_properties is None else None))
    if not config.rds.template is None:
        config.rds.template = config.rds.template.replace('[DQ]', '"')

    # Load kafka broker configuration from environment variables first
    # Otherwise, load kafka broker configuration from properties file
    config.kafka.hosts = get_property(properties, 'KAFKA', 'hosts', global_config.kafka.hosts, env=((table_name+'_KAFKA_HOSTS') if raw_properties is None else None))
    config.kafka.security_protocol = get_property(properties, 'KAFKA', 'security_protocol', global_config.kafka.security_protocol, env=((table_name+'_KAFKA_SECURITY_PROTOCOL') if raw_properties is None else None))
    config.kafka.ssl_check_hostname = get_boolean_property(properties, 'KAFKA', 'ssl_check_hostname', global_config.kafka.ssl_check_hostname, env=((table_name+'_KAFKA_SSL_CHECK_HOSTNAME') if raw_properties is None else None))
    config.kafka.ssl_cafile = get_property(properties, 'KAFKA', 'ssl_cafile', global_config.kafka.ssl_cafile, env=((table_name+'_KAFKA_SSL_CAFILE') if raw_properties is None else None))
    config.kafka.ssl_certfile = get_property(properties, 'KAFKA', 'ssl_certfile', global_config.kafka.ssl_certfile, env=((table_name+'_KAFKA_SSL_CERTFILE') if raw_properties is None else None))
    config.kafka.ssl_keyfile = get_property(properties, 'KAFKA', 'ssl_keyfile', global_config.kafka.ssl_keyfile, env=((table_name+'_KAFKA_SSL_KEYFILE') if raw_properties is None else None))
    config.kafka.ssl_password = get_property(properties, 'KAFKA', 'ssl_password', global_config.kafka.ssl_password, env=((table_name+'_KAFKA_SSL_PASSWORD') if raw_properties is None else None))
    config.kafka.topics = get_property(properties, 'KAFKA', 'topics', '', as_list=True, env=((table_name+'_KAFKA_TOPICS') if raw_properties is None else None))
    config.kafka.topics_prefix = get_property(properties, 'KAFKA', 'topics_prefix', global_config.kafka.topics_prefix, env=((table_name+'_KAFKA_TOPIC_PREFIX') if raw_properties is None else None))
    config.kafka.auto_flush = get_boolean_property(properties, 'KAFKA', 'auto_flush', global_config.kafka.auto_flush, env=((table_name+'_KAFKA_AUTO_FLUSH') if raw_properties is None else None))
    config.kafka.flush_latency = get_int_property(properties, 'KAFKA', 'flush_latency', global_config.kafka.flush_latency, env=((table_name+'_KAFKA_FLUSH_LATENCY') if raw_properties is None else None))
    config.kafka.request_timeout_ms = get_int_property(properties, 'KAFKA', 'request_timeout_ms', global_config.kafka.request_timeout_ms, env=((table_name+'_KAFKA_REQUEST_TIMEOUT_MS') if raw_properties is None else None))
    config.kafka.template = get_property(properties, 'KAFKA', 'template', None, env=((table_name+'_KAFKA_TEMPLATE') if raw_properties is None else None))
    config.kafka.message_key = get_property(properties, 'KAFKA', 'message_key', global_config.kafka.message_key, env=((table_name+'_KAFKA_MESSAGE_KEY') if raw_properties is None else None))
    config.kafka.headers = get_property(properties, 'KAFKA', 'headers', None, env=((table_name+'_KAFKA_HEADERS') if raw_properties is None else None))
    config.kafka.partitions = get_property(properties, 'KAFKA', 'partitions', global_config.kafka.partitions, env=((table_name+'_KAFKA_PARTITIONS') if raw_properties is None else None))
    config.kafka.acks = get_property(properties, 'KAFKA', 'acks', global_config.kafka.acks, env=((table_name+'_KAFKA_ACKS') if raw_properties is None else None))
    config.kafka.max_request_size = get_int_property(properties, 'KAFKA', 'max_request_size', global_config.kafka.max_request_size, env=((table_name+'_KAFKA_MAX_REQUEST_SIZE') if raw_properties is None else None))
    if not config.kafka.headers is None:
        config.kafka.headers = json.loads(config.kafka.headers)
    else:
        config.kafka.headers = global_config.kafka.headers
    if not config.kafka.template is None:
        config.kafka.template = config.kafka.template.replace('[DQ]', '"')

    # Load S3 configuration from environment variables first
    # Otherwise, load S3 configuration from properties file
    config.s3.bucket = get_property(properties, 'S3', 'bucket', global_config.s3.bucket, env=((table_name+'_S3_BUCKET') if raw_properties is None else None))
    config.s3.object_key = get_property(properties, 'S3', 'object_key', '%(id)s', env=((table_name+'_S3_OBJECT_KEY') if raw_properties is None else None))
    config.s3.template = get_property(properties, 'S3', 'template', None, env=((table_name+'_S3_TEMPLATE') if raw_properties is None else None))

    # Load JSON configuration from environment variables first
    # Otherwise, load JSON configuration from properties file
    config.json.filename = get_property(properties, 'JSON', 'filename', global_config.json.filename, env=((table_name+'_JSON_FILENAME') if raw_properties is None else None))
    config.json.mode = get_property(properties, 'JSON', 'mode', global_config.json.mode, env=((table_name+'_JSON_MODE') if raw_properties is None else None))
    config.json.header = get_property(properties, 'JSON', 'header', global_config.json.header, env=((table_name+'_JSON_HEADER') if raw_properties is None else None))
    config.json.footer = get_property(properties, 'JSON', 'footer', global_config.json.footer, env=((table_name+'_JSON_FOOTER') if raw_properties is None else None))
    config.json.template = get_property(properties, 'JSON', 'template', global_config.json.template, env=((table_name+'_JSON_TEMPLATE') if raw_properties is None else None))

    return config

#### MAIN
if __name__ == '__main__':
    # Global settings
    ARGS_PARSER.add_argument('--config-file', '-c', dest='config_file', type=str, default=None, help='Config filename, Env. : CONFIG_FILE')
    ARGS_PARSER.add_argument('--keyspace', '-k', dest='keyspace', type=str, default=None, help='Cassandra keyspace, Env. : CASSANDRA_KEYSPACE')
    ARGS_PARSER.add_argument('--hosts', '-H', dest='hosts', type=str, default=None, help='Cassandra host, Env. : CASSANDRA_HOSTS')
    ARGS_PARSER.add_argument('--port', '-P', dest='port', type=int, default=None, help='Cassandra port, Env. : CASSANDRA_PORT')
    ARGS_PARSER.add_argument('--tables', '-T', dest='tables', type=str, default=None, help='Cassandra tables to extract, Env. : CASSANDRA_TABLES')
    ARGS_PARSER.add_argument('--username', '-u', dest='username', type=str, default=None, help='Cassandra username, Env. : CASSANDRA_USERNAME')
    ARGS_PARSER.add_argument('--password', '-p', dest='password', type=str, default=None, help='Cassandra password, Env. : CASSANDRA_PASSWORD')
    ARGS_PARSER.add_argument('--connect-timeout', '-t', type=int, dest='connect_timeout', default=None, help='Cassandra connect timeout, Env. : CASSANDRA_CONNECT_TIMEOUT')
    ARGS_PARSER.add_argument('--default-client-id', dest='default_client_id', type=str, default=None, help='Default client id, Env. : DEFAULT_CLIENT_ID')
    ARGS_PARSER.add_argument('--stateless', dest='stateless', type=str2bool, default=None, help='Stateless data crawling, Env. : STATELESS_CRAWLER')
    ARGS_PARSER.add_argument('--wsgi-port', type=int, dest='wsgi_port', default=None, help='The WSGI listen port, Env. : WSGI_PORT')
    ARGS_PARSER.add_argument('--redis-host', dest='redis_host', type=str, default=None, help='REDIS host, Env. : REDIS_HOST')
    ARGS_PARSER.add_argument('--redis-port', dest='redis_port', type=int, default=None, help='REDIS port, Env. : REDIS_PORT')
    ARGS_PARSER.add_argument('--redis-password', dest='redis_password', type=str, default=None, help='REDIS password, Env. : REDIS_PASSWORD')
    ARGS_PARSER.add_argument('--redis-db', dest='redis_db', type=int, default=None, help='REDIS db, Env. : REDIS_DB')

    # Default Cassandra table settings
    ARGS_PARSER.add_argument('--enable-kafka', dest='enable_kafka', type=str2bool, default=None, help='Enable KAFKA, Env. : ENABLE_KAFKA')
    ARGS_PARSER.add_argument('--enable-s3', dest='enable_s3', type=str2bool, default=None, help='Enabled S3, Env. : ENABLE_S3')
    ARGS_PARSER.add_argument('--enable-rds', dest='enable_rds', type=str2bool, default=None, help='Enabled RDS PostgreSQL, Env. : ENABLE_RDS')
    ARGS_PARSER.add_argument('--enable-json', dest='enable_json', type=str2bool, default=None, help='Enabled JSON output, Env. : ENABLE_JSON')

    ARGS_PARSER.add_argument('--max-worker-threads', '-w', dest='max_worker_threads', type=int, default=None, help='Max worker threads, Env. : MAX_WORKER_THREADS')
    ARGS_PARSER.add_argument('--auto-start-workers', dest='auto_start_workers', type=str2bool, default=None, help='Auto start workers, Env. : AUTO_START_WORKERS')
    ARGS_PARSER.add_argument('--max-memory-usage', dest='max_memory_usage', type=int, default=None, help='Max memory usage (Mb), Env. : MAX_MEMORY_USAGE')
    ARGS_PARSER.add_argument('--max-memory-usage-resume-threshold', dest='max_memory_usage_resume_threshold', type=int, default=None, help='Max memory usage resume threshold (Mb), Env. : MAX_MEMORY_USAGE_RESUME_THRESHOLD')
    ARGS_PARSER.add_argument('--memory-usage-check-delay', dest='memory_usage_check_delay', type=int, default=None, help='Memory usage check delay (sec), Env. : MEMORY_USAGE_CHECK_DELAY')

    ARGS_PARSER.add_argument('--fetch-size', '-f', type=int, dest='fetch_size', default=None, help='Cassandra query fetch size, Env. : CASSANDRA_FETCH_SIZE')
    ARGS_PARSER.add_argument('--consistency-level', type=str, dest='consistency_level', default=None, help='Cassandra consistency level, Env. : CASSANDRA_CONSISTENCY_LEVEL')
    ARGS_PARSER.add_argument('--pages', type=int, dest='pages', default=None, help='Total pages to fetch, Env. : CASSANDRA_PAGES')
    ARGS_PARSER.add_argument('--columns', type=str, dest='columns', default=None, help='Dataset columns, Env. : CASSANDRA_COLUMNS')
    ARGS_PARSER.add_argument('--sort-by', type=str, dest='sort_by', default=None, help='Dataset sort fields, Env. : CASSANDRA_SORT_BY')
    ARGS_PARSER.add_argument('--filter', type=str, dest='filter', default=None, help='Dataset SolR filter, Env. : CASSANDRA_FILTER')
    ARGS_PARSER.add_argument('--modified-timestamp-filter', type=int, dest='modified_timestamp_filter', default=None, help='Dataset Modified Timestamp filter, Env. : CASSANDRA_MODIFIED_TIMESTAMP_FILTER')
    ARGS_PARSER.add_argument('--paging-mode', type=str, dest='paging_mode', choices=[PagingMode.DRIVER, PagingMode.CURSOR], default=None, help='Dataset paging mode, Env. : CASSANDRA_PAGING_MODE')
    ARGS_PARSER.add_argument('--table-specs', type=str, dest='table_specs', default=None, help='Dataset table specs, Env. : CASSANDRA_TABLE_SPECS')
    ARGS_PARSER.add_argument('--table-specs-file', type=str, dest='table_specs_file', default=None, help='Dataset table specs file, Env. : CASSANDRA_TABLE_SPECS_FILE')
    ARGS_PARSER.add_argument('--table-specs-files-location', type=str, dest='table_specs_files_location', default=None, help='Dataset table specs files location, Env. : CASSANDRA_TABLE_SPECS_FILE_LOCATION')
    ARGS_PARSER.add_argument('--timestamp-format', type=str, dest='timestamp_format', default=None, help='Dataset timestamp format, Env. : CASSANDRA_TIMESTAMP_FORMAT')

    ARGS_PARSER.add_argument('--kafka-hosts', dest='kafka_hosts', type=str, default=None, help='Kafka server host, Env. : KAFKA_HOSTS')
    ARGS_PARSER.add_argument('--kafka-security-protocol', dest='kafka_security_protocol', type=str, default=None, help='KAFKA security protocol, Env. : KAFKA_SECURITY_PROTOCOL')
    ARGS_PARSER.add_argument('--kafka-ssl-check-hostname', dest='kafka_ssl_check_hostname', type=str2bool, default=None, help='KAFKA ssl check hostname, Env. : KAFKA_SSL_CHECK_HOSTNAME')
    ARGS_PARSER.add_argument('--kafka-ssl-cafile', dest='kafka_ssl_cafile', type=str, default=None, help='KAFKA ssl cafile, Env. : KAFKA_SSL_CAFILE')
    ARGS_PARSER.add_argument('--kafka-ssl-certfile', dest='kafka_ssl_certfile', type=str, default=None, help='KAFKA ssl certfile, Env. : KAFKA_SSL_CERTFILE')
    ARGS_PARSER.add_argument('--kafka-ssl-keyfile', dest='kafka_ssl_keyfile', type=str, default=None, help='KAFKA ssl keyfile, Env. : KAFKA_SSL_KEYFILE')
    ARGS_PARSER.add_argument('--kafka-ssl-password', dest='kafka_ssl_password', type=str, default=None, help='KAFKA ssl keyfile password, Env. : KAFKA_SSL_PASSWORD')
    ARGS_PARSER.add_argument('--kafka-topics', dest='kafka_topics', type=str, default=None, help='Kafka topics, Env. : KAFKA_TOPICS')
    ARGS_PARSER.add_argument('--kafka-topics-prefix', dest='kafka_topics_prefix', type=str, default=None, help='Kafka topics prefix, Env. : KAFKA_TOPIC_PREFIX')
    ARGS_PARSER.add_argument('--kafka-group-id', dest='kafka_group_id', type=str, default=None, help='Kafka client Group Id, Env. : KAFKA_GROUP_ID')
    ARGS_PARSER.add_argument('--kafka-flush-latency', dest='kafka_flush_latency', type=int, default=None, help='Kafka flush latency (ms), Env. : KAFKA_FLUSH_LATENCY')
    ARGS_PARSER.add_argument('--kafka-request-timeout-ms', dest='kafka_request_timeout_ms', type=int, default=None, help='Kafka request timeout (ms), Env. : KAFKA_REQUEST_TIMEOUT_MS')
    ARGS_PARSER.add_argument('--kafka-auto-flush', dest='kafka_auto_flush', type=str2bool, default=None, help='Kafka auto flush, Env. : KAFKA_AUTO_FLUSH')
    ARGS_PARSER.add_argument('--kafka-template', dest='kafka_template', type=str, default=None, help='KAFKA message template, Env. : KAFKA_TEMPLATE')
    ARGS_PARSER.add_argument('--kafka-message-key', dest='kafka_message_key', type=str, default=None, help='KAFKA message key, Env. : KAFKA_MESSAGE_KEY')
    ARGS_PARSER.add_argument('--kafka-headers', dest='kafka_headers', type=str, default=None, help='KAFKA message headers, Env. : KAFKA_HEADERS')
    ARGS_PARSER.add_argument('--kafka-partitions', dest='kafka_partitions', type=str, default=None, help='KAFKA topic partitions, Env. : KAFKA_PARTITIONS')
    ARGS_PARSER.add_argument('--kafka-acks', dest='kafka_acks', type=str, default=None, help='KAFKA send acknowlege, Env. : KAFKA_ACKS')
    ARGS_PARSER.add_argument('--kafka-max-request-size', dest='kafka_max_request_size', type=int, default=None, help='Kafka max request size (bytes), Env. : KAFKA_MAX_REQUEST_SIZE')

    ARGS_PARSER.add_argument('--s3-bucket', dest='s3_bucket', type=str, default=None, help='S3 Bucket name, Env. : S3_BUCKET')
    ARGS_PARSER.add_argument('--s3-object-key', dest='s3_object_key', type=str, default=None, help='S3 Object Key, Env. : S3_OBJECT_KEY')
    ARGS_PARSER.add_argument('--s3-template', dest='s3_template', type=str, default=None, help='S3 Object content template, Env. : S3_TEMPLATE')
    
    ARGS_PARSER.add_argument('--log-directory', dest='log_directory', type=str, default=None, help='Log output directory, Env. : LOG_DIRECTORY')
    ARGS_PARSER.add_argument('--log-file-name', dest='log_file_name', type=str, default=None, help='Log output file name, Env. : LOG_FILE_NAME')
    ARGS_PARSER.add_argument('--log-level', dest='log_level', type=str, default=None, help='Log level, Env. : LOG_LEVEL')
    ARGS_PARSER.add_argument('--log-format', dest='log_format', type=str, default=None, help='Log format, Env. : LOG_FORMAT')
    ARGS_PARSER.add_argument('--log-module-name', dest='log_module_name', type=str, default=None, help='Log module name, Env. : LOG_MODULE_NAME')
    ARGS_PARSER.add_argument('--log-enable-gelf-format', dest='log_enable_gelf_format', type=str, default=None, help='Log enable GELF format, Env. : LOG_ENABLE_GELF_FORMAT')
    ARGS_PARSER.add_argument('--log-extra-attrs', dest='log_extra_attrs', type=str, default=None, help='Log extra attributes for GELF formatter, Env. : LOG_EXTRA_ATTRS')
    
    ARGS_PARSER.add_argument('--rds-host', dest='rds_host', type=str, default=None, help='RDS PostgreSQL host, Env. : RDS_HOST')
    ARGS_PARSER.add_argument('--rds-port', dest='rds_port', type=int, default=None, help='RDS PostgreSQL port, Env. : RDS_PORT')
    ARGS_PARSER.add_argument('--rds-username', dest='rds_username', type=str, default=None, help='RDS PostgreSQL username, Env. : RDS_USERNAME')
    ARGS_PARSER.add_argument('--rds-password', dest='rds_password', type=str, default=None, help='RDS PostgreSQL password, Env. : RDS_PASSWORD')
    ARGS_PARSER.add_argument('--rds-schema', dest='rds_schema', type=str, default=None, help='RDS PostgreSQL schema, Env. : RDS_SCHEMA')
    ARGS_PARSER.add_argument('--rds-table', dest='rds_table', type=str, default=None, help='RDS PostgreSQL table, Env. : RDS_TABLE')
    ARGS_PARSER.add_argument('--rds-columns', dest='rds_columns', type=str, default=None, help='RDS columns names, Env. : RDS_COLUMNS')
    ARGS_PARSER.add_argument('--rds-template', dest='rds_template', type=str, default=None, help='RDS SQL template, Env. : RDS_TEMPLATE')
    ARGS_PARSER.add_argument('--rds-use-transaction', dest='rds_use_transaction', type=str2bool, default=None, help='RDS Transaction mode, Env. : RDS_USE_TRANSACTION')
    ARGS_PARSER.add_argument('--rds-partition-column', dest='rds_partition_column', type=str, default=None, help='RDS Table partition column, Env. : RDS_PARTITION_COLUMN')

    ARGS_PARSER.add_argument('--json-filename', dest='json_filename', type=str, default=None, help='JSON Filename, Env. : JSON_FILENAME')
    ARGS_PARSER.add_argument('--json-mode', dest='json_mode', type=str, default=None, help='JSON write mode, Env. : JSON_MODE')
    ARGS_PARSER.add_argument('--json-header', dest='json_header', type=str, default=None, help='JSON File header, Env. : JSON_HEADER')
    ARGS_PARSER.add_argument('--json-footer', dest='json_footer', type=str, default=None, help='JSON File footer, Env. : JSON_FOOTER')
    ARGS_PARSER.add_argument('--json-template', dest='json_template', type=str, default=None, help='JSON template, Env. : JSON_TEMPLATE')

    global_config = load_global_config()

    logger = setup_logging_to_file(global_config)
    logger.info("Start of program : " + str(datetime.utcnow()))
    logger.debug("Loaded configuration : \n%s" % global_config.to_json())

    app = App(global_config, logger)

    signal.signal(signal.SIGINT, app.exit_gracefully)
    signal.signal(signal.SIGTERM, app.exit_gracefully)

    app.main()

    # end analyse
    logger.info("End of program: SUCCESS {0}".format(datetime.utcnow()))

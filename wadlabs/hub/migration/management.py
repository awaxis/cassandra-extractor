from threading import Thread, Event
from datetime import datetime
from wsgiref.simple_server import make_server
from prometheus_client import generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST
import json
from cgi import parse_qs, escape
import psutil
import os

# Monitoring interface
class Actuator(Thread):
    def __init__(self, app, config, logger):
        Thread.__init__(self)
        self.app = app
        self.config=config.copy()
        self.logger=logger
        self.stop_event = Event()
        self.httpd = None

    def stop(self):
        self.logger.info("Shutdown monitoring webserver...")
        self.stop_event.set()
        self.httpd.shutdown()
        self.join()

    def forbidden_response(self):
        return ('403 Forbidden', [('Content-type', 'text/html')],'<h1>Forbidden</h1>\n')

    def webapp(self, environ, start_response):        
        path    = environ['PATH_INFO']
        method  = environ['REQUEST_METHOD']
        status = '200 OK'
        if method == 'GET':
            qs = parse_qs(environ['QUERY_STRING'])
            eid = None if qs is None else (qs.get('e', None) or qs.get('id', None) or qs.get('instance', None))
            if path[:8] == '/metrics':
                response = generate_latest(self.app.metrics.registry)
                length = 0
                for extractor in self.app.extractors:
                    if not eid is None and extractor.table != eid[0]:
                        continue
                    response += generate_latest(extractor.registry)
                    response += generate_latest(extractor.data_crawler.metrics.registry)

                    for key in extractor.worker_manager.workers:
                        for worker in extractor.worker_manager.workers[key]['workers']:
                            response += generate_latest(worker.metrics.registry)

                headers = [
                    ('Content-type', CONTENT_TYPE_LATEST),
                    ('Content-Length', str(len(response)))
                ]
                start_response(status, headers)
                return [response]
            elif path[:6] == '/stats':
                process = psutil.Process(os.getpid())
                memory_info = process.memory_full_info()
                response = {
                    'general': {
                        'info': self.app.metrics.info._value,
                        'start_time': str(self.app.metrics.activity._created),
                        'activity_time': self.app.metrics.activity._sum.get(),
                        'memory_usage': memory_info.uss / 1e6
                    },
                    'extractors': list()
                }
                for extractor in self.app.extractors:
                    if not eid is None and extractor.table != eid[0]:
                        continue
                    extractor_info = {
                        'id': str(extractor.id),
                        'table': extractor.table,
                        'assigned_tasks': extractor.worker_manager.counter._value.get(),
                        'workers': list()
                    }
                    progress = 0
                    total = extractor.data_crawler.metrics.total.labels(extractor.data_crawler.instance_id)._value.get()
                    ignored = extractor.data_crawler.metrics.ignored.labels(extractor.data_crawler.instance_id)._value.get()
                    processed = extractor.data_crawler.metrics.processed.labels(extractor.data_crawler.instance_id)._value.get()
                    samples = extractor.data_crawler.metrics.fetch_latency.labels(extractor.data_crawler.instance_id)._samples()
                    latency_count = samples[len(samples)-3][2]
                    latency_sum = samples[len(samples)-2][2]
                    if total > 0:
                        progress = processed*100/total
                    extractor_info['workers'].append({
                        'id': str(extractor.data_crawler.id),
                        'type': extractor.data_crawler.type,
                        'status': 'Idle' if extractor.data_crawler.is_idle() else 'Paused' if extractor.data_crawler.is_paused() else 'Working',
                        'processed_records': processed,
                        'ignored_records': ignored,
                        'total_records': total,
                        'fetch_latency': latency_sum if int(latency_count)<=1 else (latency_sum/latency_count),
                        'tasks': extractor.data_crawler.metrics.tasks.labels(extractor.data_crawler.instance_id)._value.get(),
                        'start_time': extractor.data_crawler.metrics.start_time.labels(extractor.data_crawler.instance_id)._value.get(),
                        'end_time': extractor.data_crawler.metrics.end_time.labels(extractor.data_crawler.instance_id)._value.get(),
                        'idle_time': extractor.data_crawler.metrics.idle_time.labels(extractor.data_crawler.instance_id)._value.get(),
                        'last_error': extractor.data_crawler.last_error,
                        'progress': progress
                    })
                    for key in extractor.worker_manager.workers:
                        for worker in extractor.worker_manager.workers[key]['workers']:
                            progress = 0
                            total = worker.metrics.total.labels(worker.instance_id)._value.get()
                            ignored = worker.metrics.ignored.labels(worker.instance_id)._value.get()
                            processed = worker.metrics.processed.labels(worker.instance_id)._value.get()
                            samples = worker.metrics.write_latency.labels(worker.instance_id)._samples()
                            latency_count = samples[len(samples)-3][2]
                            latency_sum = samples[len(samples)-2][2]
                            if total > 0:
                                progress = processed*100 / total
                            extractor_info['workers'].append({
                                'id': str(worker.id),
                                'type': worker.type,
                                'status': 'Idle' if worker.is_idle() else 'Paused' if worker.is_paused() else 'Working',
                                'processed_records': processed,
                                'ignored_records': ignored,
                                'total_records': total,
                                'write_latency': latency_sum if int(latency_count)<=1 else (latency_sum/latency_count),
                                'tasks': worker.metrics.tasks.labels(worker.instance_id)._value.get(),
                                'start_time': worker.metrics.start_time.labels(worker.instance_id)._value.get(),
                                'end_time': worker.metrics.end_time.labels(worker.instance_id)._value.get(),
                                'idle_time': worker.metrics.idle_time.labels(worker.instance_id)._value.get(),
                                'last_error': worker.last_error,
                                'progress': progress
                            })
                    response['extractors'].append(extractor_info)

                headers = [('Content-type', 'application/json')]
                response = json.dumps(response) + '\n'
            elif path[:7] == '/config':
                response = {
                    'extractors': []
                }
                for extractor in self.app.extractors:
                    if not eid is None and extractor.table != eid[0]:
                        continue
                    extractor_config = {
                        'id': str(extractor.id),
                        'table': extractor.table,
                        'config': json.loads(extractor.data_crawler.config.to_json())
                    }
                    response['extractors'].append(extractor_config)
                headers = [('Content-type', 'application/json')]
                response = json.dumps(response) + '\n'
            elif path[:6] == '/cache':
                pattern = None if qs is None else qs.get('pattern', None)
                response = {
                    'extractors': []
                }
                for extractor in self.app.extractors:
                    if not eid is None and extractor.table != eid[0]:
                        continue
                    extractor_config = {
                        'id': str(extractor.id),
                        'table': extractor.table,
                        'cache': extractor.data_crawler.get_cache(None if pattern is None else pattern[0])
                    }
                    response['extractors'].append(extractor_config)
                headers = [('Content-type', 'application/json')]
                response = json.dumps(response) + '\n'
            else:
                headers = [('Content-type', 'text/html')]
                response = '<h1>It works</h1>\n'
        elif method == 'POST':
            req = {}
            try:
                request_body_size = int(environ.get('CONTENT_LENGTH', 0))
                request_content_type = environ.get('CONTENT_TYPE')
                # When the method is POST the variable will be sent
                # in the HTTP request body which is passed by the WSGI server
                # in the file like wsgi.input environment variable.
                request_body = environ['wsgi.input'].read(request_body_size)
                if not request_body is None and request_content_type == 'application/json':
                    req = json.loads(request_body)
            except (ValueError):
                request_body_size = 0

            if 'instance' in req:
                instances = req['instance']
            elif 'id' in req:
                instances = req['id']
            else:
                instances = None
            worker = req['worker'] if 'worker' in req else False
            crawler = req['crawler'] if 'crawler' in req else False
            if path == '/start':
                headers = [('Content-type', 'application/json')]
                if not instances is None:
                    extractor_list = list()
                    for instance in instances:
                        extractor_status = {
                            'instance': instance
                        }
                        for extractor in self.app.extractors:
                            if extractor.table == instance or str(extractor.id) == instance:
                                extractor.data_crawler.queueTask(({'action':'start'}, 0))
                                extractor_status['status'] = 'started'
                                break
                        if not 'status' in extractor_status:
                            extractor_status['status'] = 'Not started, consider call Create'
                        extractor_list.append(extractor_status)
                    response = '{"status": "OK", "message": ' + json.dumps(extractor_list) + '}\n'
                else:
                    response = '{"status": "ERROR", "message": "Unknown extractor instance"}\n'
            elif path == '/abort':
                headers = [('Content-type', 'application/json')]
                if not instances is None:
                    extractor_list = list()
                    for instance in instances:
                        extractor_status = {
                            'instance': instance
                        }
                        for extractor in self.app.extractors:
                            if extractor.table == instance or str(extractor.id) == instance:
                                extractor_status['status'] = {
                                    'crawler': '',
                                    'worker': ''
                                }
                                if crawler:
                                    extractor.data_crawler.abort()
                                    extractor_status['status']['crawler'] = 'aborted'
                                if worker:
                                    extractor.worker_manager.abort()
                                    extractor_status['status']['worker'] = 'aborted'
                                break
                        if not 'status' in extractor_status:
                            extractor_status['status'] = 'Not started, consider call Create'
                        extractor_list.append(extractor_status)
                    response = '{"status": "OK", "message": ' + json.dumps(extractor_list) + '}\n'
                else:
                    response = '{"status": "ERROR", "message": "Unknown extractor instance"}\n'
            elif path == '/pause':
                headers = [('Content-type', 'application/json')]
                if not instances is None:
                    extractor_list = list()
                    for instance in instances:
                        extractor_status = {
                            'instance': instance
                        }
                        for extractor in self.app.extractors:
                            if extractor.table == instance or str(extractor.id) == instance:
                                extractor_status['status'] = {
                                    'crawler': '',
                                    'worker': ''
                                }
                                if crawler:
                                    extractor.data_crawler.pause()
                                    extractor_status['status']['crawler'] = 'paused'
                                if worker:
                                    extractor.worker_manager.pause()
                                    extractor_status['status']['worker'] = 'paused'
                                break
                        if not 'status' in extractor_status:
                            extractor_status['status'] = 'Not started, consider call Create'
                        extractor_list.append(extractor_status)
                    response = '{"status": "OK", "message": ' + json.dumps(extractor_list) + '}\n'
                else:
                    response = '{"status": "ERROR", "message": "Unknown extractor instance"}\n'
            elif path == '/resume':
                headers = [('Content-type', 'application/json')]
                if not instances is None:
                    extractor_list = list()
                    for instance in instances:
                        extractor_status = {
                            'instance': instance
                        }
                        for extractor in self.app.extractors:
                            if extractor.table == instance or str(extractor.id) == instance:
                                extractor_status['status'] = {
                                    'crawler': '',
                                    'worker': ''
                                }
                                if crawler:
                                    extractor.data_crawler.resume()
                                    extractor_status['status']['crawler'] = 'resumed'
                                if worker:
                                    extractor.worker_manager.resume()
                                    extractor_status['status']['worker'] = 'resumed'
                                break
                        if not 'status' in extractor_status:
                            extractor_status['status'] = 'Not started, consider call Create'
                        extractor_list.append(extractor_status)
                    response = '{"status": "OK", "message": ' + json.dumps(extractor_list) + '}\n'
                else:
                    response = '{"status": "ERROR", "message": "Unknown extractor instance"}\n'
            elif path == '/shutdown':
                headers = [('Content-type', 'application/json')]
                if not instances is None:
                    extractor_list = list()
                    for instance in instances:
                        extractor_status = {
                            'instance': instance
                        }
                        for extractor in self.app.extractors:
                            if extractor.table == instance or str(extractor.id) == instance:
                                extractor.stop()
                                self.app.unregister_extractor(extractor)
                                extractor_status['status'] = 'shutdown'
                                break
                        if not 'status' in extractor_status:
                            extractor_status['status'] = 'Not started, consider call Create'
                        extractor_list.append(extractor_status)
                    response = '{"status": "OK", "message": ' + json.dumps(extractor_list) + '}\n'
                else:
                    response = '{"status": "ERROR", "message": "Unknown extractor instance"}\n'
            elif path == '/create':
                headers = [('Content-type', 'application/json')]
                if not instances is None:
                    extractor_list = list()
                    for instance in instances:
                        exists = False
                        extractor_status = {
                            'instance': instance
                        }
                        for extractor in self.app.extractors:
                            if extractor.table == instance:
                                exists = True
                                break
                        if exists is False:
                            extractor = self.app.create_extractor(instance)
                            if extractor is None:
                                extractor_status['status'] = 'Invalid extractor configuration'
                            else:
                                self.app.register_extractor(extractor)
                                extractor.start()
                                extractor_status['status'] = 'Created'
                        else:
                            extractor_status['status'] = 'Already exists'

                        extractor_list.append(extractor_status)
                    response = '{"status": "OK", "message": ' + json.dumps(extractor_list) + '}\n'
                else:
                    response = '{"status": "ERROR", "message": "Unknown extractor instance"}\n'
            elif path == '/scaleup':
                headers = [('Content-type', 'application/json')]
                if not instances is None:
                    extractor_list = list()
                    for instance in instances:
                        extractor_status = {
                            'instance': instance
                        }
                        for extractor in self.app.extractors:
                            if extractor.table == instance or str(extractor.id) == instance:
                                extractor_status['status'] = 'scaled up to %d' % extractor.scale_up()
                                break
                        if not 'status' in extractor_status:
                            extractor_status['status'] = 'Not started, consider call Create'
                        extractor_list.append(extractor_status)
                    response = '{"status": "OK", "message": ' + json.dumps(extractor_list) + '}\n'
                else:
                    response = '{"status": "ERROR", "message": "Unknown extractor instance"}\n'
            elif path == '/scaledown':
                headers = [('Content-type', 'application/json')]
                if not instances is None:
                    extractor_list = list()
                    for instance in instances:
                        extractor_status = {
                            'instance': instance
                        }
                        for extractor in self.app.extractors:
                            if extractor.table == instance or str(extractor.id) == instance:
                                extractor_status['status'] = 'scaled down to %d' % extractor.scale_down()
                                break
                        if not 'status' in extractor_status:
                            extractor_status['status'] = 'Not started, consider call Create'
                        extractor_list.append(extractor_status)
                    response = '{"status": "OK", "message": ' + json.dumps(extractor_list) + '}\n'
                else:
                    response = '{"status": "ERROR", "message": "Unknown extractor instance"}\n'
            elif path == '/set-config':
                headers = [('Content-type', 'application/json')]
                if not instances is None:
                    extractor_list = list()
                    for instance in instances:
                        extractor_status = {
                            'instance': instance
                        }
                        for extractor in self.app.extractors:
                            if extractor.table == instance['table']:
                                if 'config' in instance and len(instance['config']) > 0:
                                    config_data = '\n'.join(instance['config'])
                                    config = self.app.load_table_config(table=extractor.table, raw_properties=config_data)
                                    extractor.set_config(config)
                                    extractor_status['status'] = 'Config reloaded'
                                else:
                                    extractor_status['status'] = 'Invalid table config'
                                break
                        if not 'status' in extractor_status:
                            extractor_status['status'] = 'Not started, consider call Create'
                        extractor_list.append(extractor_status)
                    response = '{"status": "OK", "message": ' + json.dumps(extractor_list) + '}\n'
                else:
                    response = '{"status": "ERROR", "message": "Unknown extractor instance"}\n'
            elif path == '/cache/clear':
                headers = [('Content-type', 'application/json')]
                if not instances is None:
                    pattern = None
                    if 'pattern' in req:
                        pattern = req['pattern']
                    extractor_list = list()
                    for instance in instances:
                        extractor_status = {
                            'instance': instance
                        }
                        for extractor in self.app.extractors:
                            if extractor.table == instance or str(extractor.id) == instance:
                                extractor.data_crawler.clear_cache(pattern)
                                extractor_status['status'] = 'cache cleared'
                                break
                        if not 'status' in extractor_status:
                            extractor_status['status'] = 'Not started, consider call Create'
                        extractor_list.append(extractor_status)
                    response = '{"status": "OK", "message": ' + json.dumps(extractor_list) + '}\n'
                else:
                    response = '{"status": "ERROR", "message": "Unknown extractor instance"}\n'
            else:
                (status, headers, response) = self.forbidden_response()
        else:
            (status, headers, response) = self.forbidden_response()

        start_response(status, headers)
        return [response.encode()]

    def run(self):
        self.logger.info("Starting monitoring service")
        self.httpd = make_server('', self.config.wsgi.port, self.webapp)
        self.logger.info("Monitoring service running on port %d...", self.config.wsgi.port)
        self.httpd.serve_forever()

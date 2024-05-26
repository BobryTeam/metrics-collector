from threading import Thread
from threading import Timer
from queue import Queue

import redis

from prometheus_api_client import PrometheusConnect

from events.event import *
from events.kafka_event import *

from metrics.metrics import Metrics

from microservice.microservice import Microservice


class MetricsCollector(Microservice):
    '''
    Класс отвечающий за представление Metrics Collector
    Его задача -- по запросу от Observer'a собирать метрики из Prometheus и отправлять их в кэш
    '''

    def __init__(self, event_queue: Queue, prometheus_url: str,redis_host: str, redis_port: int):
        '''
        Инициализация класса:
        - `self.timer - таймер для сбора метрик`
        - `prometheus_url` - url Prometheus сервера.
        - `redis_host` - хост Redis.
        - `redis_port` - порт Redis.
        Поля класса:
        - `self.prometheus_url` - url Prometheus сервера.
        - `self.redis_host` - хост Redis.
        - `self.redis_port` - порт Redis
        '''
        self.timer = None
        self.requests = ['sum(rate(node_cpu_seconds_total{mode!="idle", job="scaling_target"}[1m]))/' + 
                         'count(rate(node_cpu_seconds_total{mode!="idle", job="scaling_target"}[1m]) > bool 0.05)',
                         '1 - ((avg(avg_over_time(node_memory_MemFree_bytes{job="scaling_target"}[1m])) +' + 
                         'avg(avg_over_time(node_memory_Cached_bytes{job="scaling_target"}[1m])) +' +
                         'avg(avg_over_time(node_memory_Buffers_bytes{job="scaling_target"}[1m]))) /' +
                         'avg(avg_over_time(node_memory_MemTotal_bytes{job="scaling_target"}[1m])))',
                         'avg(rate(node_network_receive_bytes_total{job="scaling_target"}[1m])) * 8 / 1024 / 1024',
                         'avg(rate(node_network_transmit_bytes_total{job="scaling_target"}[1m])) * 8 / 1024 / 1024', 
        ]
        self.redis_host = redis_host
        self.prometheus_url = prometheus_url
        self.redis_port = redis_port
        self.number_metrics = 1


        return super().__init__(event_queue, writers)

    def handle_event(self, event: Event):
        '''
        Обработка ивентов
        '''
        target_function = None

        match event.type:
            case EventType.GetMetrics:
                target_function = self.handle_event_get_metrics
            case _:
                pass

        if target_function is not None:
            Thread(target=target_function, args=(self.number_metrics)).start()

    def handle_event_get_metrics(self, number_metrics: int):
        self.timer = Timer(60, self.get_metrics, number_metrics)
        self.timer.start()


    def get_metrics(self, number_metrics: int):
        prom = PrometheusConnect(url=self.prometheus_url)
        metric_values = []
        for request in self.requests:
            result = prom.query(request)
            metric_values.append(result['value'])
        if len(metric_values) == 4:
            metrics = Metrics(*metric_values)
        self.save_metrics_to_redis(metrics, number_metrics)
        
    def save_metrics_to_redis(self, metrics: Metrics, number_metrics: int):
        json_data = str(metrics)
        redis_client = redis.Redis(host=self.redis_host, port = self.redis_port, db=0)
        key = f'metrics_{number_metrics}'
        redis_client.set(key, json_data)
        if (number_metrics == 5):
            return
        number_metrics =+ 1
        self.handle_event_get_metrics(number_metrics)


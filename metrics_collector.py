from typing import Dict
from queue import Queue

from threading import Thread

from prometheus_api_client import PrometheusConnect
from redis import Redis

import time

from events.event import *
from events.kafka_event import *

from metrics.metrics import Metrics

from microservice.microservice import Microservice


class MetricsCollector(Microservice):
    '''
    Класс отвечающий за представление Metrics Collector
    Его задача -- по запросу от Observer'a собирать метрики из Prometheus и отправлять их в кэш
    '''

    # collect metrics every TIMER_COLLECT_METRCS seconds
    TIMER_COLLECT_METRCS = 10.0
    # collect metrics COLLECT_METRICS_TIMES times
    COLLECT_METRICS_TIMES = 5


    def __init__(self, event_queue: Queue, writers: Dict[str, KafkaEventWriter], prometheus_connection: PrometheusConnect, redis: Redis):
        '''
        Инициализация класса:
        - `prometheus_connection` - подключение к Prometheus
        - `redis` - класс redis
        Поля класса:
        - `self.requests` - реквесты на PromQL
        - `self.prometheus_connection` - подключение к Prometheus
        - `self.redis` - подключение к базе данных Redis
        '''
        self.requests = ['sum(rate(node_cpu_seconds_total{mode!="idle", job="scaling_target"}[1m]))/' + 
                         'count(rate(node_cpu_seconds_total{mode!="idle", job="scaling_target"}[1m]) > bool 0.05)',

                         '1 - ((avg(avg_over_time(node_memory_MemFree_bytes{job="scaling_target"}[1m])) +' + 
                         'avg(avg_over_time(node_memory_Cached_bytes{job="scaling_target"}[1m])) +' +
                         'avg(avg_over_time(node_memory_Buffers_bytes{job="scaling_target"}[1m]))) /' +
                         'avg(avg_over_time(node_memory_MemTotal_bytes{job="scaling_target"}[1m])))',

                         'avg(rate(node_network_receive_bytes_total{job="scaling_target"}[1m])) * 8 / 1024 / 1024',
                         'avg(rate(node_network_transmit_bytes_total{job="scaling_target"}[1m])) * 8 / 1024 / 1024', 
        ]

        self.prometheus_connection = prometheus_connection
        self.redis = redis

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
            Thread(target=target_function, args=(event.data,)).start()

    def handle_event_get_metrics(self, _event_data):
        '''
        Собираем метрики COLLECT_METRICS_TIMES раз
        '''
        metrics: list[Metrics] = [self.get_metrics(0)]

        for current_metrics_count in range(1, self.COLLECT_METRICS_TIMES):
            time.sleep(self.TIMER_COLLECT_METRCS)
            metrics.append(self.get_metrics(current_metrics_count))

        self.save_metrics_to_redis(metrics)
        # ура метрики собраны!
        self.writers['om'].send_event(Event(EventType.GotMetrics, ''))


    def get_metrics(self, metrics_index: int) -> Metrics:
        '''
        Собираем метрики единоразово
        '''
        print('get metrics!')
        metric_values = []

        for request in self.requests:
            result = self.prometheus_connection.custom_query(request)
            if len(result) == 0: raise RuntimeError(f'No such metric in Prometheus: {request}')
            metric_values.append(result[0]['value'])

        return Metrics(*metric_values)

    def save_metrics_to_redis(self, metrics: list[Metrics]):
        for i in range(len(metrics)):
            json_metrics = str(metrics[i])
            key = f'{i}'
            self.redis.set(key, json_metrics)


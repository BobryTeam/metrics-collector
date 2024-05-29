from typing import Dict
from queue import Queue

from threading import Thread

from prometheus_api_client import PrometheusConnect
from redis import Redis

import time

from event import *
from kafka_event import *
from metrics import Metrics
from microservice import Microservice


class MetricsCollector(Microservice):
    '''
    Класс отвечающий за представление Metrics Collector
    Его задача -- по запросу от Observer'a собирать метрики из Prometheus и отправлять их в кэш
    '''

    # collect metrics every TIMER_COLLECT_METRCS seconds
    TIMER_COLLECT_METRCS = 60.0
    # collect metrics COLLECT_METRICS_TIMES times
    COLLECT_METRICS_TIMES = 5


    def __init__(self, event_queue: Queue, writers: Dict[str, KafkaEventWriter], prometheus_connection: PrometheusConnect, redis: Redis, target_job: str):
        '''
        Инициализация класса:
        - `prometheus_connection` - подключение к Prometheus
        - `redis` - класс redis
        Поля класса:
        - `self.requests` - запросы метрик на PromQL
        - `self.prometheus_connection` - подключение к Prometheus
        - `self.redis` - подключение к базе данных Redis
        '''

        self.requests = Metrics.PROMQL_REQUESTS(target_job)
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
        metrics: list[Metrics] = [self.get_metrics()]

        for _ in range(1, self.COLLECT_METRICS_TIMES):
            time.sleep(self.TIMER_COLLECT_METRCS)
            metrics.append(self.get_metrics())

        self.save_metrics_to_redis(metrics)
        # ура метрики собраны!
        self.writers['om'].send_event(Event(EventType.GotMetrics, ''))


    def get_metrics(self) -> Metrics:
        '''
        Собираем метрики единоразово
        '''
        metric_values = []

        for request in self.requests:
            result = self.prometheus_connection.custom_query(request)
            if len(result) == 0: raise RuntimeError(f'No such metric in Prometheus: {request}')
            metric_values.append(result[0]['value'][1]) # ['value'] = (unix_timestamp, actual_value)

        return Metrics(*metric_values)

    def save_metrics_to_redis(self, metrics: list[Metrics]):
        for i in range(len(metrics)):
            json_metrics = str(metrics[i])
            key = f'{i}'
            self.redis.set(key, json_metrics)


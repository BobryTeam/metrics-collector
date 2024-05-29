import os
from queue import Queue

from redis import Redis
from prometheus_api_client import PrometheusConnect

from metrics_collector import MetricsCollector

from event import *
from kafka_event import *

from kafka import KafkaConsumer, KafkaProducer


# put the values in a k8s manifest
kafka_bootstrap_server: str | None = os.environ.get('KAFKA_BOOTSTRAP_SERVER')
prometheus_url: str | None = os.environ.get('PROMETHEUS_URL')
scaling_job: str | None = os.environ.get('SCALING_JOB')
redis_host: str | None = os.environ.get('REDIS_HOST')
redis_port: str | None = os.environ.get('REDIS_PORT')

if kafka_bootstrap_server is None:
    print(f'Environment variable KAFKA_BOOTSTRAP_SERVER is not set')
    exit(1)

if prometheus_url is None:
    print(f'Environment variable PROMETHEUS_URL is not set')
    exit(1)

if scaling_job is None:
    print(f'Environment variable SCALING_JOB is not set')
    exit(1)

if redis_host is None:
    print(f'Environment variable REDIS_HOST is not set')
    exit(1)

if redis_port is None:
    print(f'Environment variable REDIS_PORT is not set')
    exit(1)


event_queue = Queue()

# share the queue with a reader
reader = KafkaEventReader(
    KafkaConsumer(
        'mc',
        bootstrap_servers=kafka_bootstrap_server,
        auto_offset_reset='latest',
    ),
    event_queue
)

# writers to other microservices
writers = {
    'om': KafkaEventWriter(
        KafkaProducer(
            bootstrap_servers=kafka_bootstrap_server
        ),
        'om'
    ),
}

# init the microservice
metrics_collector = MetricsCollector(
    event_queue, writers,
    PrometheusConnect(
        url=prometheus_url
    ),
    Redis(
        host=redis_host,
        port=int(redis_port),
    ),
    scaling_job
)


# Testing
# import time
# writer = KafkaEventWriter(
#     KafkaProducer(
#         bootstrap_servers=kafka_bootstrap_server,
#     ),
#     'mc'
# )
# for i in range(50):
#     print('new event!')
#     writer.send_event(Event(EventType.GetMetrics, ''))
#     time.sleep(600)



metrics_collector.running_thread.join()

from metrics_collector import MetricsCollector

from redis import Redis

from events.kafka_event import *
from events.event import *

import os

# put the value in a k8s manifest
kafka_bootstrap_server = os.environ.get('KAFKA_BOOTSTRAP_SERVER')
prometheus_url = os.environ.get('PROMETHEUS_URL')
redis_host = os.environ.get('REDIS_HOST')
redis_port = os.environ.get('REDIS_PORT')


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
    event_queue, writers, prometheus_url,
    Redis(
        host=redis_host,
        port=redis_port,
    )
)

metrics_collector.running_thread.join()
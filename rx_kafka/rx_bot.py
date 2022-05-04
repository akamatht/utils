
from kafka import KafkaConsumer
import rx
from rx.scheduler import ThreadPoolScheduler


AWS_KAFKA_BOOTSTRAP_SERVERS = ["b-1.kafka01.mb3vi8.c4.kafka.us-east-1.amazonaws.com:9092",
                           "b-2.kafka01.mb3vi8.c4.kafka.us-east-1.amazonaws.com:9092",
                           "b-3.kafka01.mb3vi8.c4.kafka.us-east-1.amazonaws.com:9092"]


def get_kafka_consumer():
    return KafkaConsumer(bootstrap_servers=AWS_KAFKA_BOOTSTRAP_SERVERS, group_id="local_test_group",
                         auto_offset_reset='earliest', enable_auto_commit=True,
                         auto_commit_interval_ms=1000
                         )

kafka_topic = 'dev-trader-curve-definitions-ak'


def get_data(observer, scheduler):
    kafka_consumer = get_kafka_consumer()
    kafka_consumer.subscribe(topics=[kafka_topic])

    for msg in kafka_consumer:
        key = msg.key.decode('utf-8')
        value = msg.value.decode('utf-8')
        observer.on_next(value)


def process_definition_messages(msg:dict):
    print(f'recvd {msg}')


def start():
    rx.Observable(get_data).subscribe(on_next=process_definition_messages, scheduler=ThreadPoolScheduler())


if __name__ == "__main__":
    start()


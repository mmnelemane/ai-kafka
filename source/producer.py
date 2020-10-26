from pykafka import KafkaClient





client = KafkaClient(hosts="127.0.0.1:9092, 127.0.0.1:9093, ...")
client.topics

topic = client.topics['my.test']

with topic.get_sync_producer() as producer:
    for in in range(4):
        producer.produce('test message ' + str(i ** 2))


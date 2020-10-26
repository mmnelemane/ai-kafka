from pykafka import KafkaClient

client = KafkaClient(hosts="127.0.0.1:9092, 127.0.0.1:9093, ...")

topic = client.topics['my.test']

consumer = topic.get_simple_consumer()
for message in consumer:
    if message is not None:
        print message.offset, message.value


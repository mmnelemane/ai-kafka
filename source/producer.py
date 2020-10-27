# This script connects to Kafka and send a few messages

from kafka import KafkaProducer

cert_path = "../certs"

producer = KafkaProducer(
    bootstrap_servers = "kafka-mmn-gcp-snmohan83-3313.aivencloud.com:18652",
    security_protocol = "SSL",
    ssl_cafile = f'{cert_path}/kafka_ca.pem',
    ssl_certfile = f'{cert_path}/service.cert',
    ssl_keyfile = f'{cert_path}/service.key',
)

for i in range(1, 4):
    message = "message number {}".format(i)
    print("Sending: {}".format(message))
    producer.send("demo-topic", message.encode("utf-8"))

# Force sending of all messages

producer.flush()


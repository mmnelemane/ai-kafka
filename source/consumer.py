# This script receives messages from a Kafka topic

from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor
import psycopg2
import configparser

config = configparser.ConfigParser()
config.read('../ai-kafka.conf')

db_user = config.get('postgresql', 'username')
db_pass = config.get('postgresql', 'password')
db_server = config.get('postgresql', 'server_name')
db_port = config.get('postgresql', 'db_port')
db_path = config.get('postgresql', 'db_path')
db_sslmode = config.get('postgresql', 'db_sslmode')

db_uri = f'postgres://{db_user}:{db_pass}@{db_server}:{db_port}/{db_path}?sslmode={db_sslmode}'
db_conn = psycopg2.connect(db_uri)

# TODO: Change to a more standard way of using CERTS

def _create_pg_database(db_conn):
    pass


def run_consumer(kafka_server,
                 kafka_port,
                 sec_protocol,
                 cert_path):

    consumer = KafkaConsumer(
        "topic-webmetrics",
        auto_offset_reset="earliest",
        bootstrap_servers="%s:%s" % (kafka_server,kafka_port),
        client_id="webmetrics-client-1",
        group_id="webmetrics-group",
        security_protocol=sec_protocol,
        ssl_cafile="%s/kafka/%s" % (cert_path, "ca.pem"),
        ssl_certfile="%s/kafka/%s" % (cert_path, "service.cert"),
        ssl_keyfile="%s/kafka/%s" % (cert_path, "service.key")
    )

    # Call poll twice. First call will just assign partitions for our
    # consumer without actually returning anything
    
    for _ in range(5):
        raw_msgs = consumer.poll(timeout_ms=1000)
        for tp, msgs in raw_msgs.items():
            for msg in msgs:
                print("Received: {}".format(msg.value))
    
    # Commit offsets so we won't get the same messages again
    consumer.commit()

def _test_db():
    c = db_conn.cursor(cursor_factory=RealDictCursor)
    c.execute("SELECT 1 = 1")
    result = c.fetchone()

def _print_config(config_file):
    conf = dict(default = {}, kafka = {}, postgresql = {})

    config = configparser.ConfigParser()
    config.read(config_file)

    print("Reading defaults Section\n")
    print("Sections: %s\n" % config.sections())

    for sec in config.options('default'):
        conf['default'][sec] = config.get('default', sec)

    for sec in config.options('kafka'):
        conf['kafka'][sec] = config.get('kafka', sec)

    for sec in config.options('postgresql'):
        conf['postgresql'][sec] = config.get('postgresql', sec)

    print("------------------------------------------------\n")
    print(conf)
    print("\n------------------------------------------------\n")


if __name__ == '__main__':
    _print_config('../ai-kafka.conf')

    ssl_cafile = config.get('kafka', 'ssl_cafile')
    ssl_certfile = config.get('kafka', 'ssl_certfile')
    ssl_keyfile = config.get('kafka', 'ssl_keyfile')
    kafka_server = config.get('kafka', 'server_name')
    kafka_port = config.get('kafka', 'server_port')
    sec_protocol = config.get('kafka', 'security_protocol')
    
    cert_path = config.get('default', 'cert_path')

    run_consumer(kafka_server,
                 kafka_port,
                 sec_protocol,
                 cert_path)


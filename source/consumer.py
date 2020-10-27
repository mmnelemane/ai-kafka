# This script receives messages from a Kafka topic

from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor
import psycopg2

db_user = "avnadmin"
db_pass = "xnnaoq445hq74yod"
db_server = "pg-55a8e64-snmohan83-3313.aivencloud.com"
db_port = 18650
db_path = "defaultdb"
db_sslmode = "require"
db_uri = f'postgres://{db_user}:{db_pass}@{db_server}:{db_port}/{db_path}?sslmode={db_sslmode}'

db_conn = psycopg2.connect(db_uri)

# TODO: Change to a more standard way of using CERTS
cert_path = "../certs"

def _test_db():
    c = db_conn.cursor(cursor_factory=RealDictCursor)
    c.execute("SELECT 1 = 1")
    result = c.fetchone()

consumer = KafkaConsumer(
    "demo-topic",
    auto_offset_reset="earliest",
    bootstrap_servers="server-name:port",
    client_id="demo-client-1",
    group_id="demo-group",
    security_protocol="SSL",
    ssl_cafile=f'{cert_path}/kafka_ca.pem',
    ssl_certfile=f'{cert_path}/service.cert',
    ssl_keyfile=f'{cert_path}service.key',
)


# Call poll twice. First call will just assign partitions for our
# consumer without actually returning anything

for _ in range(2):
    raw_msgs = consumer.poll(timeout_ms=1000)
    for tp, msgs in raw_msgs.items():
        for msg in msgs:
            print("Received: {}".format(msg.value))

# Commit offsets so we won't get the same messages again

consumer.commit()


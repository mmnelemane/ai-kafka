# This script receives messages from a Kafka topic

import psycopg2
import configparser
import logging
from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor


logger = logging.getLogger("kafka_consumer")
# TODO: Change to a more standard way of using CERTS

def _create_pg_database(db_cursor):
    query = """
        CREATE TABLE IF NOT EXISTS web_metrics (
            event_id INTEGER PRIMARY KEY,
            url VARCHAR(256),
            status_code INTEGER,
            status_message VARCHAR(256),
            response_time_microsec INTEGER,
            search_text VARCHAR(256),
            search_result INTEGER
        );
        """

    logger.debug("CREATE QUERY %s" % query)
    try:    
        db_cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def _insert_db_record(db_cursor,
                   event_id,
                   url,
                   status_code, 
                   status_message,
                   response_time,
                   search_text,
                   search_result):

    insert_query = """
        INSERT INTO web_metrics (event_id, status_code, status_message, 
            response_time_microsec, search_text, search_result)
            VALUES(%s, %s, %s, %s, %s, %s);
        """
    logger.debug("INSERT QUERY %s" % insert_query)
    try:
        db_cursor.execute(insert_query, 
                          (event_id, status_code, status_message,
                           response_time, search_text, search_result))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def _update_db_record(db_cursor,
                   event_id,
                   url,
                   status_code, 
                   status_message,
                   response_time,
                   search_text,
                   search_result):

    update_query = """
        UPDATE web_metrics set event_id = %s, status_code = %s,
            status_message = %s, response_time_microsec = %s,
            search_text = %s, search_result = %s) WHERE
            url = %s;
        """
    logger.debug("UPDATE QUERY %s" % update_query)
    try:
        db_cursor.execute(update_query, 
                          (event_id, status_code, status_message,
                           response_time, search_text, search_result, url))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def _get_record_by_url(db_cursor, url):
    exists = False
    select_query = """
        SELECT event_id, url FROM web_metrics WHERE url = %s
        """
    try:
        db_cursor.execute(select_query, url)
        row = db_cursor.fetchall()
        if row.count > 0:
            exists = True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return exists

def _get_all_records(db_cursor):
    select_query = """
        SELECT event_id, url, status_code, status_message,
               response_time_microsec, search_text,
               search_result FROM web_metrics;
        """
    logger.debug("SELECT QUERY: %s" % select_query)
    try:
        db_cursor.execute(select_query)
        row = db_cursor.fetchone()
        while row is not None:
            print(row)
            row = db_cursor.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def run_consumer(kafka_server,
                 kafka_port,
                 sec_protocol,
                 cert_path,
                 db_cursor):

    consumer = KafkaConsumer(
        "topic-webmetrics",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
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
    
    while True:
        raw_msgs = consumer.poll(timeout_ms=1000)
        for tp, msgs in raw_msgs.items():
            for msg in msgs:
                print("Received: {}".format(msg.value))
                record = eval(msg.value)
                print ("Record: %s" % str(record))
                if _get_record_by_url(db_cursor, record['url']):
                    _update_db_record(db_cursor,
                                  event_id = record['msg_id'],
                                  url = record['url'],
                                  status_code = record['status_code'], 
                                  status_message = record['status_message'],
                                  response_time = record['resp_time'],
                                  search_text = record['text'],
                                  search_result = record['search'])
                else:
                    _insert_db_record(db_cursor,
                                  event_id = record['msg_id'],
                                  url = record['url'],
                                  status_code = record['status_code'], 
                                  status_message = record['status_message'],
                                  response_time = record['resp_time'],
                                  search_text = record['text'],
                                  search_result = record['search'])
    # Commit offsets so we won't get the same messages again
    consumer.commit()

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
    config = configparser.ConfigParser()
    config.read('../ai-kafka.conf')
    
    db_user = config.get('postgresql', 'username')
    db_pass = config.get('postgresql', 'password')
    db_server = config.get('postgresql', 'server_name')
    db_port = config.get('postgresql', 'db_port')
    db_path = config.get('postgresql', 'db_path')
    db_sslmode = config.get('postgresql', 'db_sslmode')
    
    db_uri = "postgres://%s:%s@%s:%s/%s?sslmode=%s" % (db_user, db_pass, db_server, db_port, db_path, db_sslmode)
    db_conn = psycopg2.connect(db_uri)
    db_cursor = db_conn.cursor()

    _create_pg_database(db_cursor)

    db_conn.commit()

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
                 cert_path,
                 db_cursor)

    db_conn.commit()

    print("DB Records after writing them\n")
    print("---------------------------------------------------------------\n")
    _get_all_records(db_cursor)
    print("\n--------------------------------------------------------------\n")

    if db_cursor is not None:
        db_cursor.close()

    if db_conn is not None:
        db_conn.close()


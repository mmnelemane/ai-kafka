# This script receives messages from a Kafka topic

import time
import psycopg2
import configparser
import logging
from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor

logging.basicConfig(filename="aikafka.log", level=logging.DEBUG)
logger = logging.getLogger("kafka_consumer")
# TODO: Change to a more standard way of using CERTS


class AivenKafkaConsumer(object):
    def __init__(self, interval, db_user, db_pass, db_server,
                 db_port, db_path, db_sslmode):
        self.interval = int(interval)
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_server = db_server
        self.db_port = db_port
        self.db_path = db_path
        self.db_sslmode = db_sslmode


    def _create_pg_database(self, db_cursor):
        logger.debug("create_pg_database")
        web_metrics = """
            CREATE TABLE IF NOT EXISTS web_metrics (
                event_id VARCHAR(128) PRIMARY KEY,
                url VARCHAR(256),
                status_code INTEGER,
                status_message VARCHAR(256),
                response_time_microsec INTEGER,
                search_text VARCHAR(256),
                search_result INTEGER
            )
            """
    
        logger.debug("CREATE QUERY %s" % web_metrics)
        try:    
            db_cursor.execute(web_metrics)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    def _insert_db_record(self,
                       db_cursor,
                       event_id,
                       url,
                       status_code, 
                       status_message,
                       response_time,
                       search_text,
                       search_result):
        logger.debug("Inside Insert")
    
        insert_query = """
            INSERT INTO web_metrics (event_id, url, status_code, status_message, 
                response_time_microsec, search_text, search_result)
                VALUES(%s, %s, %s, %s, %s, %s, %s);
            """

        query_str = insert_query % (event_id, url, status_code, status_message,
                                    response_time, search_text, search_result)
        logger.debug("INSERT QUERY %s" % query_str)
        try:
            db_cursor.execute(insert_query, 
                             (event_id, url, status_code, status_message,
                              response_time, search_text, search_result))
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
    def _update_db_record(self,
                       db_cursor,
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
    
    
    def get_record_by_url(self, db_cursor, url):
        print ("INSIDE get_record_by_url %s" % url)
        exists = False
        select_query = """
            SELECT event_id, url FROM web_metrics WHERE url = %s
            """
        try:
            db_cursor.execute(select_query, url)
            row = db_cursor.fetchall()
            print("ROW  in get_record_by_url: ", row)
            if row.count > 0:
                exists = True
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    
        return exists
    
    def get_all_records(self, db_cursor):
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
    
    
    def run_consumer(self,
                     kafka_server,
                     kafka_port,
                     sec_protocol,
                     topic,
                     cert_path):

        # Create a DB connection with PostGreSQL service
        db_conn = self.get_db_connection()
        db_cursor = db_conn.cursor()

        logger.debug("Creating Tables")
        if db_cursor is not None:
            self._create_pg_database(db_cursor)
        else:
            print("Cannot create Database.")
            sys.exit(2)
        
        logger.debug("Starting Consumer Thread")
        consumer = KafkaConsumer(
            topic,
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
                    rec_exists = self.get_record_by_url(db_cursor, record['url'])
                    if rec_exists:
                        self._update_db_record(db_cursor,
                                      event_id = record['msg_id'],
                                      url = record['url'],
                                      status_code = record['status_code'], 
                                      status_message = record['status_message'],
                                      response_time = record['resp_time'],
                                      search_text = record['text'],
                                      search_result = record['search'])
                    else:
                        self._insert_db_record(db_cursor,
                                      event_id = record['msg_id'],
                                      url = record['url'],
                                      status_code = record['status_code'], 
                                      status_message = record['status_message'],
                                      response_time = record['resp_time'],
                                      search_text = record['text'],
                                      search_result = record['search'])
                    db_conn.commit()
            # Commit offsets so we won't get the same messages again
            consumer.commit()
            time.sleep(self.interval)

        # TODO: Need a graceful exit in case of errors.
        if db_cursor is not None:
            db_cursor.close()
        if db_conn is not None:
            db_conn.close()

    
    def get_db_connection(self):
        db_uri = "postgres://%s:%s@%s:%s/%s?sslmode=%s" % (self.db_user, self.db_pass, self.db_server, self.db_port, self.db_path, self.db_sslmode)
        try:
            db_conn = psycopg2.connect(db_uri)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in Connection to Database!: ", error)
            return None
    
        return db_conn


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('../ai-kafka.conf')
   

    aconsumer = AivenKafkaConsumer(config.get('default', 'check_interval'),
                                   config.get('postgresql', 'username'),
                                   config.get('postgresql', 'password'),
                                   config.get('postgresql', 'server_name'),
                                   config.get('postgresql', 'db_port'),
                                   config.get('postgresql', 'db_path'),
                                   config.get('postgresql', 'db_sslmode'))

    db_conn = aconsumer.get_db_connection()
    db_cursor = db_conn.cursor()

    aconsumer._create_pg_database(db_cursor)

    db_conn.commit()

    ssl_cafile = config.get('kafka', 'ssl_cafile')
    ssl_certfile = config.get('kafka', 'ssl_certfile')
    ssl_keyfile = config.get('kafka', 'ssl_keyfile')
    kafka_server = config.get('kafka', 'server_name')
    kafka_port = config.get('kafka', 'server_port')
    sec_protocol = config.get('kafka', 'security_protocol')
    topic = config.get('kafka', 'topic')

    cert_path = config.get('default', 'cert_path')
    interval = config.get('default', 'check_interval')

    run_consumer(kafka_server,
                 kafka_port,
                 sec_protocol,
                 topic,
                 cert_path)

    db_conn.commit()

    print("DB Records after writing them\n")
    print("---------------------------------------------------------------\n")
    get_all_records(db_cursor)
    print("\n--------------------------------------------------------------\n")

    if db_cursor is not None:
        db_cursor.close()

    if db_conn is not None:
        db_conn.close()


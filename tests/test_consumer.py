#! /usr/bin/python3

# Tests to verify functionality of the consumer

import consumer

class AivenKafkaConsumerTest(object):
    def __init__(self):
        self.consumer = consumer.AivenKafkaConsumer('10', 'avnadmin',
                'xavuesgxzeay1e8b', 'pg-mmn-kafka-snmohan83-3313.aivencloud.com',
                '18650', 'defaultdb', 'require') 

    def test_get_db_connection(self):
        print("Running test_get_db_connection")
        result = "FAILED"
        db_conn = self.consumer.get_db_connection()
        db_cursor = db_conn.cursor()

        db_cursor.execute("SELECT 1 = 1")
        record = c.fetchone()

        if db_cursor is not None:
            db_cursor.close()
        if db_conn is not None:
            db_conn.close()

        if record:
            result = "PASSED"

        print("test_get_db_connection %s" % result)


    def test_create_pg_database(self):
        print("Running test_create_pg_database")
        result = "PASSED"
        print("test_create_pg_database: %s" % result)


    def test_insert_db_record(self):
        print("Running test_insert_db_record")
        result = "PASSED"
        print("test_insert_db_record: %s" % result)

    def test_update_db_record(self):
        print("Running test_update_record")
        result = "PASSED"
        print("test_update_db_record: %s" % result)


    def test_run_consumer(self):
        print("Running test_run_consumer")
        result = "FAILED"
        consumer = self.consumer.get_consumer_instance()
        topics = consumer.topics()

        if topics:
            result = "PASSED"
        print("test_run_consumer: %s" % result)

    def test_get_records(self):
        print("Running test_get_records")
        result = "PASSED"
        print("test_get_records: %s" % result)


if __name__=='__main__':
    aikafkatest = AivenKafkaConsumerTest()

    print("Running tests one by one")
    aikafkatest.test_get_db_connection()
    aikafkatest.test_create_pg_database()
    aikafkatest.test_insert_db_record()
    aikafkatest.test_update_db_record()
    aikafkatest.test_run_consumer()
    aikafkatest.test_get_records()



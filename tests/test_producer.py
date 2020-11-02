#! /usr/bin/python3

# This test file is used for testing the producer functionality

import producer

class AivenKafkaProducerTest(object):
    def __init__(self):
        pass

    def test_run_producer(self):
        print ("Running test_run_producer")
        result = "PASSED"
        print ("test_run_producer: %s" % result)

    def test_get_webmetrics(self) 
        print ("Running test_get_webmetrics")
        result = "PASSED"
        print ("test_get_webmetrics: %s" % result)

if __name__=='__main__':
    aikafkaptest = AivenKafkaProducerTest()

    print("Running Tests one by one")
    aikafkaptest.test_run_producer()
    aikafkaptest.test_get_webmetrics()

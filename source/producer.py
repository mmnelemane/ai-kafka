# This script connects to Kafka and send a few messages

import json
import time
import uuid
import configparser
import requests
import logging
from kafka import KafkaProducer

logging.basicConfig(filename="aikafka.log", level=logging.DEBUG)
logger = logging.getLogger("kafka-producer")

class AivenKafkaProducer(object):
    def __init__(self, interval=10):
        self.interval = int(interval)

    def _compose_kafka_message(self,
                               url,
                               response_time,
                               error_code,
                               search_result):
        return "%s;%s;%s;%s;" % (url, response_time, error_code, search_result)
        
    def get_webmetrics(self, url, search_text):
        status = -1
        response_time = -1
        error = "None"
        find_result = -1
    
        try:
            response = requests.get(url)
            response_time = response.elapsed.microseconds
    
            status = response.status_code
            error = response.reason
    
            find_result = response.text.find(search_text)
    
        except:
            logging.error('Response has errors')
    
        return dict(status_code=status, 
                    resp_time=response_time,
                    status_message=error,
                    search=find_result,
                    text=search_text)
    
        
    def run_producer(self,
                     kafka_server,
                     kafka_port,
                     sec_protocol,
                     cert_path,
                     urlfile):
        logger.debug("starting Producer Thread")
    
        producer = KafkaProducer(
            bootstrap_servers = "%s:%s" % (kafka_server, kafka_port),
            security_protocol = sec_protocol,
            ssl_cafile = "%s/kafka/%s" % (cert_path, 'ca.pem'),
            ssl_certfile = "%s/kafka/%s" % (cert_path, 'service.cert'),
            ssl_keyfile = "%s/kafka/%s" % (cert_path, 'service.key')
        )
    
        while True:
            urls = []
            with open(urlfile) as ufile:
                urls = json.load(ufile)
    
            message = dict()
            for url in urls['sites']:
                web_metrics = self.get_webmetrics(url['url'], url['text'])
                message['msg_id'] = "%s" % str(uuid.uuid1().hex)
                message['url'] = url['url']
                message.update(web_metrics)
                producer.send("topic-webmetrics", str(message).encode("utf-8"))
    
            print("Producing data after %s seconds" % self.interval)
            time.sleep(self.interval)
    
            # Force sending of all messages
        
            producer.flush()

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('../ai-kafka.conf')

    interval = config.get('default', 'check_interval')
    aproducer = AivenKafkaProducer(interval)

    aproducer.run_producer(config.get('kafka', 'server_name'),
                           config.get('kafka', 'server_port'),
                           config.get('kafka', 'security_protocol'),
                           config.get('default', 'cert_path'),
                           '../weburls.txt')
    


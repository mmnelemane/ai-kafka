# This script connects to Kafka and send a few messages

from kafka import KafkaProducer
import configparser
import requests
import json


def _compose_kafka_message(url,
                           response_time,
                           error_code,
                           search_result):
    return "%s;%s;%s;%s;" % (url, response_time, error_code, search_result)
    
def get_webmetrics(url, search_text):
    availability = False
    response_time = -1
    error = "None"
    find_result = -1

    try:
        response = requests.get(url)
        response_time = response.elapsed.microseconds

        if response.status_code == 200:
            availability = True
            error = "None"
        else: 
            error = response.reason

        find_result = response.text.find(search_text)
    except:
        logging.error('Response has errors')

    return dict(avail=availability, 
                resp_time=response_time,
                err=error,
                search=find_result)

    
def run_producer(kafka_server,
                 kafka_port,
                 sec_protocol,
                 cert_path,
                 cafile,
                 certfile,
                 keyfile,
                 urlfile):

    print("CA FILE: %s/%s" % (cert_path, cafile))
    producer = KafkaProducer(
        bootstrap_servers = "%s:%s" % (kafka_server, kafka_port),
        security_protocol = sec_protocol,
        ssl_cafile = "%s/kafka/%s" % (cert_path, cafile),
        ssl_certfile = "%s/kafka/%s" % (cert_path, certfile),
        ssl_keyfile = "%s/kafka/%s" % (cert_path, keyfile)
    )

    urls = []
    with open(urlfile) as ufile:
        urls = json.load(ufile)

    msg_no = 0
    message = dict()
    for url in urls['sites']:
        msg_no = msg_no + 1
        web_metrics = get_webmetrics(url['url'], url['text'])
        message['msg_id'] = msg_no
        message.update(web_metrics)
        producer.send("topic-webmetrics", str(message).encode("utf-8"))

    # Force sending of all messages
    
    producer.flush()


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('../ai-kafka.conf')

    run_producer(config.get('kafka', 'server_name'),
                 config.get('kafka', 'server_port'),
                 config.get('kafka', 'security_protocol'),
                 config.get('default', 'cert_path'),
                 config.get('kafka', 'ssl_cafile'),
                 config.get('kafka', 'ssl_certfile'),
                 config.get('kafka', 'ssl_keyfile'),
                 '../weburls.txt')
    
    


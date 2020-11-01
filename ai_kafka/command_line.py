#! /usr/bin/python3

# This script starts both consumer and producer functions
# of the application as daemons and waits on user for 
# further operations and tests

import os, time
import sys, getopt
from ai_kafka import producer
from ai_kafka import consumer
import pprint
import getopt
import threading
import argparse
import logging
import configparser

logging.basicConfig(filename="aikafka.log", level=logging.DEBUG)
logger = logging.getLogger("start_aikafka")


def print_config(config_file):
    pp = pprint.PrettyPrinter(indent=4)

    conf = dict(default = {}, kafka = {}, postgresql = {})

    config = configparser.ConfigParser()
    config.read(config_file)

    for sec in config.options('default'):
        conf['default'][sec] = config.get('default', sec)

    for sec in config.options('kafka'):
        conf['kafka'][sec] = config.get('kafka', sec)

    for sec in config.options('postgresql'):
        conf['postgresql'][sec] = config.get('postgresql', sec)

    print("------------------------------------------------\n")
    pp.pprint(conf)
    print("\n------------------------------------------------\n")


def main(args=None):

    configfile = ""
    inputfile = ""

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", type=str, help="Config File with full path and name")
    parser.add_argument("-i", "--inputfile", type=str, help="Input File with full path and name")
    parser.add_argument("-p", "--printconfig", help="Print current running configuration", action="store_true")
    args = parser.parse_args()
    
    if not args.configfile or not args.inputfile:
        print("Error: Config File and input file are needed")
        parser.print_help()
        sys.exit(2)

    configfile = args.configfile
    inputfile = args.inputfile

    if not os.path.isfile(inputfile):
        print("Error: Input file does not exist!")
        sys.exit(2)
        

    if not os.path.isfile(configfile):
        print("Error: Config file does not exist!")
        sys,exit(2)


    if args.printconfig:
        print_config(configfile)
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(configfile)

    # Create an instance of the Consumer
    aconsumer = consumer.AivenKafkaConsumer(config.get('default', 'check_interval'),
                                            config.get('postgresql', 'username'),
                                            config.get('postgresql', 'password'),
                                            config.get('postgresql', 'server_name'),
                                            config.get('postgresql', 'db_port'),
                                            config.get('postgresql', 'db_path'),
                                            config.get('postgresql', 'db_sslmode'))

    logger.debug("Starting Consumer Thread")

    # Start the Consumer as a daemon
    consumer_thread = threading.Thread(target=aconsumer.run_consumer,
                                       args = (config.get('kafka', 'server_name'),
                                              config.get('kafka', 'server_port'),
                                              config.get('kafka', 'security_protocol'),
                                              config.get('kafka', 'topic'),
                                              config.get('default', 'cert_path')))
    consumer_thread.daemon = True
    consumer_thread.start()

    # wait for consumer thread to start before creating Producer Instance
    time.sleep(10)

    aproducer = producer.AivenKafkaProducer(interval=config.get('default', 'check_interval'))

    # Start the Producer as a daemon
    producer_thread = threading.Thread(target=aproducer.run_producer, 
                                       args = (config.get('kafka', 'server_name'),
                                              config.get('kafka', 'server_port'),
                                              config.get('kafka', 'security_protocol'),
                                              config.get('default', 'cert_path'),
                                              inputfile))

    producer_thread.daemon = True
    producer_thread.start()

    consumer_thread.join()
    producer_thread.join()

if __name__=='__main__':
    sys.exit(main())

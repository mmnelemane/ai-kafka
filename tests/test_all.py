import pytest
import consumer
import producer

@pytest.fixture
def example_url_data():
    return {
        "sites": [
             { 
                 "url": "https://www.python.org",
                 "text": "Python is a programming language"
             },
             { 
                 "url": "https://www.kernel.org",
                 "text": "The Linux Kernel Archives"
             }
         ]
    }


def test_database():
    
    config.set 
    db_conn = consumer.get_db_connection()
    db_server = "postgres://avnadmin:xnnaoq445hq74yod@pg-55a8e64-snmohan83-3313.aivencloud.com:18650/defaultdb?sslmode=require"
    
def test_consumer():
    pass

def test_metrics_integration():
    pass


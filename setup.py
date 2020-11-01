import os
from setuptools import setup
from nvpy import nvpy

setup(
    name = "ai-kafka",
    version = "0.1",
    author = "Madhu Mohan Nelemane",
    author_email = "snmohan83@gmail.com",
    description = "An example of using Aiven Kafka and Aiven PostgreSQL services",
    license = "Creative Commons",
    url = "https://github.com/mmnelemane/ai-kafka",
    packages=['myscript'],
    entry_points = {
        'consumer' : ['consumer = ai-kafka.consumer:main'],
        'producer' : ['producer = ai-kafka.producer:main']
    },
    data_files = [
        ('./', ['weburls.txt'])
    ],
    classifiers=[
        "License ::  :: BSD License",
    ],
)

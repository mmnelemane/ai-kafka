import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name = "ai_kafka",
    version = "0.1",
    author = "Madhu Mohan Nelemane",
    author_email = "snmohan83@gmail.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    description = "An example of using Aiven Kafka and Aiven PostgreSQL services",
    license = "Madhu Mohan Nelemane",
    url = "https://github.com/mmnelemane/ai-kafka",
    packages=setuptools.find_packages(),
    entry_points = {
        "console_scripts" : [
            "aikafka=ai_kafka.command_line:main"
        ]
    },
    data_files = [
        ('./', ['weburls.json'])
    ],
    classifiers=[
        "License :: Other/Proprietary ",
    ],
    python_requires='>=3.6',
)

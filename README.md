# ai-kafka

An implementation using Kafka as a Service

# Application Description

This is a system that:
 * monitors website availability over the network,
 * produces metrics about the website availability,
 * persists the events passing through an Aiven Kafka instance into an Aiven PostgreSQL database.

For this, it implements a Kafka producer which periodically checks the target websites and sends 
the check results to a Kafka topic. A Kafka consumer storing the data to an Aiven PostgreSQL database.
For this local setup, these components run in the same machine but in production use similar components
would run in different systems.

The website checker should perform the checks periodically and collect the HTTP response time,
error code returned, as well as optionally checking the returned page contents for a regexp pattern
that is expected to be found on the page.

For the database writer we expect to see a solution that records the check results into one or more
database tables and could handle a reasonable amount of checks performed over a longer period of time.

Even though this is a small concept program, returned homework should include tests and proper packaging. If your tests require Kafka and PostgreSQL services, for simplicity your tests can assume those are already running, instead of integrating Aiven service creation and deleting.
Aiven is a Database as a Service vendor and the homework requires using our services. Please register to Aiven at https://console.aiven.io/signup.html at which point you'll automatically be given $300 worth of credits to play around with. The credits should be enough for a few hours of use of our services. If you need more credits to complete your homework, please contact us.

The solution would NOT include using any of the following:

    • Database ORM libraries - use a Python DB API compliant library and raw SQL queries instead
    • Extensive container build recipes - rather focus on the Python code, tests, documentation and ease of execution.
      
# Criteria for evaluation (TODO: Remove this section after completion)
    • Code formatting and clarity. We value readable code written for other developers, not for a
      tutorial, or as one-off hack.
    • We appreciate demonstrating your experience and knowledge, but also utilizing  existing libraries.
      There is no need to re-invent the wheel.
    • Practicality of testing. 100% test coverage may not be practical, and also having 100% coverage
      but no validation is not very useful.
    • Automation. We like having things work automatically, instead of multi-step instructions to run
      misc commands to set up things. Similarly, CI is a relevant thing for automation.
    • Attribution. If you take code from Google results, examples etc., add attributions. We all know
      new things are often written based on search results.
    • "Open source ready" repository. It's very often a good idea to pretend the homework assignment
      in Github is used by random people (in practice, if you want to, you can delete/hide the repository
      as soon as we have seen it).
      


# Pre-Implementation installations

## Installing psycopg2 as a client to run SQL querries with PostGreSQL
* sudo apt-get install postgresql
* sudo apt-get install libpq-dev
* sudo pip3 install psycopg2

# References

* Basic parts of the producer and consumer code were taken from: 
 - https://help.aiven.io/en/articles/489572-getting-started-with-aiven-kafka
* Basic parts of the postgresql client was taken from:
 - https://help.aiven.io/en/articles/489573-getting-started-with-aiven-postgresql

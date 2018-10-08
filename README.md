# Tempus Data Engineer Challenge Project

### Two [Apache Airflow](https://airflow.apache.org) data pipeline were developed which fetch news data from a [News REST API](https://newsapi.org), store the data on the local filesystem, and perform a series of [ETL tasks](https://en.wikipedia.org/wiki/Extract,_transform,_load) that extract the top headlines; transform them into a CSV tabular structure; and upload the transformations to a given [Amazon S3](https://aws.amazon.com/s3/) bucket.

---
### Project Status

**Build**: [![Build Status](https://travis-ci.org/davidolorundare/tempus_de_challenge.svg?branch=master)](https://travis-ci.org/davidolorundare/tempus_de_challenge)

**Coverage**: [![Coverage Status](https://coveralls.io/repos/github/davidolorundare/tempus_de_challenge/badge.svg?branch=master)](https://coveralls.io/github/davidolorundare/tempus_de_challenge?branch=master)

---
### Getting Started: Pipeline Overview (with Screenshots)

Discuss how the goals were broken into the two pipelines.

#### DAG Pipeline 1

The first pipeline does
image of pipe1


#### DAG Pipeline 2
The second pipeline does
image of pipe2


---
### Prerequisites 

What things you need to install the software and how to install them.

1. [Python](http://www.python.org) and [Virtualenv](https://virtualenv.pypa.io/en/stable/)
	* author's Python and virtualenv versions are 3.6 and 16.0.0 respectively.
2. [Docker](https://www.docker.com)
	* docker versions are docker 18.06.1-ce and docker-compose 1.22.0


---
### Setup

How to setup and install.
A step by step series of examples that tell you how to get a development env running.

1. Clone a copy of the github repo into your working directory or Download it as a zip file.

2. 

---
### Running Code and Usage

How to run code via `make run`


End with an example of getting some data out of the system or using it for a little demo

---
### Running Tests (Unit and Integration)

How to run unit and integration tests via `make test`
Explain how to run the automated tests for this project.
Break down into end to end tests + 
Explain what these tests test and why + coding style tests.

---
### Packages Used/ Built With

1. [Apache Airflow CLI](https://airflow.apache.org/cli.html)
2. [Amazon Python SDK (boto) library](http://boto3.readthedocs.io/en/latest/guide/resources.html)
3. [PostgreSQL Python library](https://wiki.postgresql.org/wiki/Psycopg2)
4. [Python Requests library](http://docs.python-requests.org)
5. [Python Data Analysis library (Pandas)](https://pandas.pydata.org/)
6. [Pytest](https://docs.pytest.org/en/latest/)
7. [Flake8 - Python Pep-8 Style Guide Enforcement](http://flake8.pycqa.org/en/latest/)
8. [News API](https://newsapi.org/)

---
### Working Footnotes

- Where to store the data at rest: locally as files or using a database ? What are the tradeoffs ? Note size of json returned objects.

- For the bonus challenge, on experimenting with the News API it was discovered that
using all four keywords in the same api-request returned 0 hits. Hence, four separate api-request calls will be made; for each individual keyword.

- Note error-handling needs for each of the functions, e.g. the news api endpoint (ok code versus not ok codes), handling connection failures, etc

- Need to decide on whether to use the news api client libray or not.e.g. pip install newsapi-python

- Use json and csv libs for parsing. url-encode the requests ?

- Note security concern of hardcoding the apikey in requests. Might want to encrypt api? - use Airflow Variable 'apikey' (programmatically create this)

- No S3 bucket link was given in the requirements, I will have to create my own S3 bucket then.

- Need to write unit tests and apply a TDD approach.

- Need to write integration tests (try using Travis CI ? and badge), use mocking for external services. Must also make the whole solution startable via 'make run'

- Do I need to put any screenshots ?


---
### Approach to Project

- two full days of learning and experimentation with new topics: Working with Airflow, RESTFul APIs, Docker, AWS Python Boto libraries, Python Integration Testing, Python Test Doubles, and applying Test-Driven Development in practice.

- one full day of coding, testing, and developing the solution.
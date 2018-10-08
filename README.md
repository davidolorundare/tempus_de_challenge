# Tempus Data Engineer Challenge Project

### Two [Apache Airflow](https://airflow.apache.org) data pipelines were developed which fetch news data from a [News REST API](https://newsapi.org), store the data on the local filesystem, and perform a series of [ETL tasks](https://en.wikipedia.org/wiki/Extract,_transform,_load) that extract the top headlines; transform them into a CSV tabular structure; and upload the transformations to a given [Amazon S3](https://aws.amazon.com/s3/) bucket.

---
### Project Status

**Build**: [![Build Status](https://travis-ci.org/davidolorundare/tempus_de_challenge.svg?branch=master)](https://travis-ci.org/davidolorundare/tempus_de_challenge)

**Coverage**: [![Coverage Status](https://coveralls.io/repos/github/davidolorundare/tempus_de_challenge/badge.svg?branch=master)](https://coveralls.io/github/davidolorundare/tempus_de_challenge?branch=master)

---
### Getting Started: Pipeline Overview (with Screenshots)

Discusses the breakdown of the project goals into the two pipelines.

#### DAG Pipeline 1

The first pipeline, named 'tempus_challenge_dag' is scheduled to run once a day at 12AM, and consists of eight tasks. Its structure is shown below:

image of pipe1

The pipeline tasks are as follows:
- The first task is an Airflow DummyOperator which does nothing and is used merely to visually indicate the beginning of the pipeline. 

- Next, using a predefined Airflow PythonOperator, it calls a python function to create three datastore folders for storing the intermediary data for the 'tempus_challenge_dag' that is later on downloaded and transformed. 
The 'news', 'headlines', and 'csv' folders are created under the parent 'tempdata' directory which is made relative to the airflow home directory.
	
- The third task involves a defined Airflow SimpleHTTPOperator makes an HTTP GET request to the News API's 'sources' endpoint with the assigned API Key, to fetch all english news sources. A Python callback function is defined with this operator, and handles the returned Response object, storing the JSON news data as a file in the 'news' folder.

- The fourth task involves a defined Airflow FileSensor detects when the JSON news data is in its appropriate directory and kicks off the ETL stage of the pipeline.

- The fifth task, the Extraction task, involves a defined Airflow PythonOperator, which calls a predefined python function that reads the news JSON data from its folder and uses the JSON library to extract the top-headlines from it, storing the result in the 'headlines' folder.

- The sixth task, the Transformation task, involves a defined Airflow PythonOperator, which calls a predefined python function that reads the top-headlines data from the 'headlines' folder, and using Pandas flattens the JSON data into CSV. The converted CSV data is stored in the 'csv' folder.

- The seventh task, the Load task, involves a defined Custom Airflow Operator, as Airflow does not have an existing Operator for transferring data directly from the local filesystem to Amazon S3. Our custom operator makes use of the Amazon Python Boto library to move the transformed data from the 'csv' to an S3 bucket already setup by the author.

- The final task is an Airflow DummyOperator which does nothing and is used merely to signify the end of the pipeline.


#### DAG Pipeline 2
The second pipeline, named 'tempus_bonus_challenge_dag' is similar to the first; also consisting of eight tasks. It is scheduled to run once a day at 1AM. Its structure is shown below:

image of pipe2

The pipeline tasks are identical to that of the first. The only difference is in the third task:

- Four Airflow SimpleHTTPOperators are defined, which make separate HTTP GET requests to the News API's 'everything' endpoint with the assigned API Key and a query for specific keywords: 'Tempus Labs', 'Eric Lefokosky', 'Cancer', and Immunotherapy. This fetches data on each of these keywords. The Python callback function which handles the return Response object stores the them as four JSON files in the 'news' folder, created in an earlier step, for the 'tempus_bonus_challenge_dag'.


---
### Prerequisites 

What things you need to install the software and how to install them.

1. [Python](http://www.python.org) and [Virtualenv](https://virtualenv.pypa.io/en/stable/)
	* author's Python and virtualenv versions are 3.6 and 16.0.0 respectively.
2. [Docker](https://www.docker.com)
	* docker versions are docker 18.06.1-ce and docker-compose 1.22.0
3. Register for a [News API key](https://newsapi.org/register)	
4. Register for an [Amazon Web Services](http://aws.amazon.com/) account. This is required for authenticating to S3 using the boto Python SDK library.


---
### Setup

How to setup and install.
A step by step series of examples that tell you how to get a development env running.

1. Clone a copy of the github repo into your working directory or Download it as a zip file and extract its content into your working directory.

2. Open a command line terminal and navigate to the root of the repo directory.

3. Run the command `make init` ; this downloads all of the project's dependencies.

4. Run the command `make test` ; this runs all the unit and integration tests for the project and ensures they are passing.

5. Run the command `make run` ; this starts up Docker reads in the Dockerfile and configures Airflow to begin running. 
	- After a few seconds, Airflow's webserver starts up and the User interface and Admin Console becomes accessible. Open a web browser a navigate to http://localhost:9090 to access the Console.
	- The two data pipelines "tempus_challenge_dag" and "tempus_bonus_challenge_dag" will have been loaded and are visible.
	- The pipeline are preconfigured to run already, 1hour apart. Their respective logs can be viewed from their [Task Instance Context Menus](https://airflow.readthedocs.io/en/latest/ui.html#task-instance-context-menu)

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
### Packages Used

1. [Apache Airflow CLI](https://airflow.apache.org/cli.html)
2. [Amazon Python SDK (boto) library](http://boto3.readthedocs.io/en/latest/guide/resources.html)
3. [PostgreSQL Python library](https://wiki.postgresql.org/wiki/Psycopg2)
4. [Python Requests library](http://docs.python-requests.org)
5. [Python Data Analysis library (Pandas)](https://pandas.pydata.org/)
6. [Python JSON library](https://docs.python.org/3/library/json.html)
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
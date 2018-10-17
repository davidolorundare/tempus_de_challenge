# Tempus Data Engineer Challenge Project

### Two [Apache Airflow](https://airflow.apache.org) data pipelines were developed which fetch news data from a [News REST API](https://newsapi.org), store the data on the local filesystem, and perform a series of [ETL tasks](https://en.wikipedia.org/wiki/Extract,_transform,_load) that extract the top headlines; transform them into a CSV tabular structure; and upload the transformations to a given [Amazon S3](https://aws.amazon.com/s3/) bucket.

---
### Project Status

**Build**: [![Build Status](https://travis-ci.org/davidolorundare/tempus_de_challenge.svg?branch=master)](https://travis-ci.org/davidolorundare/tempus_de_challenge)

**Coverage**: [![Coverage Status](https://coveralls.io/repos/github/davidolorundare/tempus_de_challenge/badge.svg?branch=master)](https://coveralls.io/github/davidolorundare/tempus_de_challenge?branch=master)


---
### Prerequisites 

1. [Python](http://www.python.org) and [Virtualenv](https://virtualenv.pypa.io/en/stable/)
	* author's Python and virtualenv versions are `3.6` and `16.0.0` respectively.
2. [PyPI](https://pypi.org/project/pip/)
	* author's PIP version is `18.1`
3. [Docker](https://www.docker.com)
	* docker versions are `docker 18.06.1-ce` and `docker-compose 1.22.0`
4. Register for a free [News API key](https://newsapi.org/register)	in order to use their News RESTFul API service.
5. Register for a free [Amazon Web Services](http://aws.amazon.com/) account. This is required for authenticating to S3 using the boto Python SDK library.
	* Before beginning to use the Boto library, you should set up authentication credentials. Credentials for your AWS account can be found in the [IAM Console](https://console.aws.amazon.com/iam/home). You can create or use an existing user. Go to manage access keys and generate a new set of keys. These are needed required for the project to make S3 bucket-upload requests. For more details on this see the [documentation here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html)
	* Create two AWS S3 buckets with your account. Name them `tempus-challenge-csv-headlines` and `tempus-bonus-challenge-csv-headlines`. These buckets will hold the final csv transformations and the project-code *expects* these two buckets to already exist, as it **does not** programmatically create them before
	uploading and will throw errors if they are detected not to exist in S3.


---
### Setup

1. Clone a copy of the github repo into your working directory or Download it as a zip file and extract its contents into your working directory.

2. Open a command line terminal and navigate to the root of the repo directory.

	* In this terminal export, as an [environmental variable](https://en.wikipedia.org/wiki/Environment_variable), the generated News API Key you obtained after registration. Name the environmental variable `NEWS_API_KEY` and set its value to be the key you obtained.

	* Run the command `make init` ; this downloads all of the project's dependencies.

	* Run the command `make test` ; this runs all the unit and integration tests for the project and ensures they are passing.

	* Run the command `make run` ; this starts up Docker, reads in the Dockerfile, and configures Airflow to begin running. 
		- After a few seconds, Airflow's webserver starts up and the User interface and Admin Console becomes accessible. Open a web browser a navigate to http://localhost:9090 to access the Console.
		- The two data pipelines "tempus_challenge_dag" and "tempus_bonus_challenge_dag" will have been loaded and are visible.
		- The pipeline are preconfigured to run already, 1hour apart. Their respective logs can be viewed from their [Task Instance Context Menus](https://airflow.readthedocs.io/en/latest/ui.html#task-instance-context-menu)
		- In the Console UI (shown below) click on the toggle next to each pipeline name to activate them, and click on the the play button icon on the right to start each. The steps are numbered in order.

	![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/master/readme_images/airflow_ui_console.jpeg "Airflow Console UI - Activate and Trigger Dags")

---
### Getting Started: Pipeline Overview 

Discusses the breakdown of the project goals into the two pipelines.

#### DAG Pipeline 1

The first pipeline, named 'tempus_challenge_dag' is scheduled to run once a day at 12AM, and consists of eight tasks (five of which are the core). Its structure is shown below:

![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/master/readme_images/tempus_dag_pipeline-1_image.jpeg "Image of Pipeline-1 structure")

The pipeline tasks are as follows:
- The first task is an [Airflow DummyOperator](https://airflow.apache.org/code.html#airflow.operators.dummy_operator.DummyOperator) which does nothing and is used merely to visually indicate the beginning of the pipeline. 

- Next, using a predefined [Airflow PythonOperator](https://airflow.apache.org/code.html#airflow.operators.python_operator.PythonOperator), it calls a python function to create three datastore folders for storing the intermediary data for the 'tempus_challenge_dag' that is later on downloaded and transformed. 
The 'news', 'headlines', and 'csv' folders are created under the parent 'tempdata' directory which is made relative to the airflow home directory.
	
- The third task involves a defined [Airflow SimpleHTTPOperator](https://airflow.apache.org/code.html#airflow.operators.http_operator.SimpleHttpOperator) makes an HTTP GET request to the News API's 'sources' endpoint with the assigned API Key, to fetch all english news sources. A Python callback function is defined with this operator, and handles the returned Response object, storing the JSON news data as a file in the 'news' folder.

- The fourth task involves a defined [Airflow FileSensor](https://airflow.apache.org/code.html#airflow.contrib.sensors.file_sensor.FileSensor) detects whenever the JSON news data has landed in its appropriate directory and kicks off the subsequent ETL stages of the pipeline.

- The fifth task, the Extraction task, involves a defined [Airflow PythonOperator](https://airflow.apache.org/code.html#airflow.operators.python_operator.PythonOperator), which calls a predefined python function that reads the news JSON data from its folder and uses the JSON library to extract the top-headlines from it, storing the result in the 'headlines' folder.

- The sixth task, the Transformation task, involves a defined [Airflow PythonOperator](https://airflow.apache.org/code.html#airflow.operators.python_operator.PythonOperator), which calls a predefined python function that reads the top-headlines data from the 'headlines' folder, and using Pandas flattens the JSON data into CSV. The converted CSV data is stored in the 'csv' folder.

- The seventh task, the Upload task, involves a defined Custom Airflow Operator, as Airflow does not have an existing Operator for transferring data directly from the local filesystem to Amazon S3. Our custom operator is built ontop of the [Airflow S3 Hook](https://airflow.apache.org/code.html#airflow.hooks.S3_hook.S3Hook) and the Amazon Python Boto library; to move the transformed data from the 'csv' folder to an S3 bucket already setup by the author.
Two Amazon S3 buckets were setup by the author:
	* [`tempus-challenge-csv-headlines`](http://tempus-challenge-csv-headlines.s3.amazonaws.com/) 
	* [`tempus-bonus-challenge-headlines`](http://tempus-bonus-challenge-csv-headlines.s3.amazonaws.com/) 
to store the flattened csv files from the 'tempus_challenge_dag' and 'tempus_bonus_challenge_dag' pipeline respectively. Navigating to these bucket links from any web browser returns any XML list of all their contents. 

![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/master/readme_images/s3_challenge_bucket.jpeg "Image of Pipeline-1 S3 Bucket")

---

![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/master/readme_images/s3_bonus_challenge_bucket.jpeg "Image of Pipeline-2 S3 Bucket")

By default a `dummy.txt` file is all that exists in the buckets. To view or download any of the files in the bucket, append the name of that document to the end of the aforementioned links.

---

- The final task is an [Airflow DummyOperator](https://airflow.apache.org/code.html#airflow.operators.dummy_operator.DummyOperator) which does nothing and is used merely to signify the end of the pipeline.


#### DAG Pipeline 2
The second pipeline, named 'tempus_bonus_challenge_dag' is similar to the first; also consisting of eight tasks. It is scheduled to run once a day at 1AM. Its structure is shown below:

![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/master/readme_images/tempus_dag_pipeline-2_image.jpeg "Image of Pipeline-2 structure")

The pipeline tasks are identical to that of the first. The only difference is in the third task of calling the News API:

- Four [Airflow SimpleHTTPOperators](https://airflow.apache.org/code.html#airflow.operators.http_operator.SimpleHttpOperator) are defined, which make separate parallel HTTP GET requests to the News API's 'top-headlines' endpoint directly with the assigned API Key and a query for specific keywords: 'Tempus Labs', 'Eric Lefokosky', 'Cancer', and Immunotherapy. This fetches data on each of these keywords. The Python callback function which handles the return Response object stores them as four JSON files in the 'headlines' folder, created in an earlier step, for the 'tempus_bonus_challenge_dag'.

#### Transformations Notes
The end transformations are stored in the respective `csv` datastore folders of the respective pipelines.

##### 				Pipeline 1  CSV: Tempus Challenge DAG
For the 'tempus_challenge_dag' pipeline all the news headlines from all the english sources are flattened and transformed into one single csv file, the pipeline execution date is appended to the end transformed csv. It is of the form: `pipeline_execution_date_headlines.csv`

##### 				Pipeline 2 CSVs: Tempus Bonus Challenge DAG
For each the four keywords queries of the 'tempus_bonus_challenge_dag' - 'Tempus Labs', 'Eric Lefkofsky', 'Cancer', 'Immunotheraphy' - the result is four separate csv files, each representing all the headlines about that particular keyword. The pipeline execution date is appended to the end transformed csv's. The keyword headline files are of form:`pipeline_execution_date_keyword_headlines.csv`

---
### Running Code and Usage

Two running demos are shown below; of both Pipelines.
GIF OF CODE RUNNING/USAGE

---
### Running Tests (Unit and Integration)

* This project's unit and integration tests can be found in the `tests` folder in the root directory; with the unit tests in the `unit` subdirectory and the integration tests in the `integration` subdirectory. 
	- Running `make test` from the command line runs all the tests for the associated Python functions used in the project.
* The project uses [Flake8](http://flake8.pycqa.org/en/latest/) as its Python Linter, ensuring code conformance to the [Python PEP-8 standards](http://pep8.org/). It is also setup with [Travis CI](http://travis-ci.com/) to remotely run all the tests and this can be further integrated in a [Continuous Build/Integration](https://en.wikipedia.org/wiki/Continuous_integration)/Delivery pipeline later on if needed.

The **unit tests** consists of five test suites corresponding to the five core tasks in the two data pipelines. They are split into python files with the prefix `test_xxxxx`, where xxxxx is the name of the kind of functionality being tested.
The tests make use of [Pytest](https://docs.pytest.org/en/latest/) for unit testing and test coverage checks, as well as the [Python Mocking library](https://docs.python.org/dev/library/unittest.mock.html) and [PyFakeFS](https://pypi.org/project/pyfakefs/) for simulating I/O dependencies such as functions interacting with the filesystem or making external network calls. The test suites are:
- *TestFileStorage* which runs tests on the task involving creation of the datastore folders and actions on them.
- *TestNetworkOperations* which run tests on the task involving HTTP calls to the News API.
- *TestExtractOperations* which run tests on the task involving extracting headlines from the news data.
- *TestTransformOperations* which run tests on the task involving conversion of the news headlines JSON data into CSV.
- *TestUploadOperations* which run tests on the task involving data-transfer of the flattened CSVs to a predefined Amazon S3 bucket.

The **integration tests** exercise the overall combination of the tasks in the pipelines, particularly their interaction with the two main external services used: the News API and Amazon S3.

---
### Packages Used

1. [Amazon Python SDK (boto 3) library](http://boto3.readthedocs.io/en/latest/guide/resources.html)
2. [Apache Airflow CLI](https://airflow.apache.org/cli.html)
3. [Flake8 - Python Pep-8 Style Guide Enforcement](http://flake8.pycqa.org/en/latest/)
4. [News API](https://newsapi.org/)
5. [PostgreSQL Python library](https://wiki.postgresql.org/wiki/Psycopg2)
6. [Pyfakefs](https://pypi.org/project/pyfakefs/)
7. [Pytest](https://docs.pytest.org/en/latest/)
8. [Python Data Analysis library (Pandas)](https://pandas.pydata.org/)
9. [Python JSON library](https://docs.python.org/3/library/json.html)
10. [Python Requests library](http://docs.python-requests.org) 

---
### Footnotes

- Where to store the data at rest: locally as files or using a database ? There are a number of tradeoffs using either, but given the scope of the project I decided to use a local filesystem. 
	* Although Airflow has an inter-task mechanism (called XCom) for passing data between tasks, from reading the Airflow documentation and research on the topic it was generally not recommended Xcoms be used for transferring large data between tasks (though the json data in this project is rather smaller). Hence why the data-at-rest decision was narrowed down to only the filesystem or database options.
	* In a production environment I would rather configure a real database, like PostgreSQL, to serve as a data warehouse for the News data retrieved, as well as setup tempoary data-staging areas for the intermediate data created during the ETL tasks.

- For the bonus challenge, on experimenting with the News API it was discovered that
using all four keywords in the same api-request returned 0 hits. Hence, I decided four separate api-request calls would made; for each individual keyword.

- To reduce the number of calls to the News API in the task of DAG pipeline 1 `tempus_challenge_dag`, to retrieve the source headlines, the list of sources from the previous upstream task can be batched up and fed as a comma-separated string of identifiers to the `sources` parameter of the `headlines` endpoint. 
	* However, the returned Response object will be very large can consist of a mix of headlines from all these news sources, which can be very confusing to parse programmatically (without some ample patience for writing more unit tests to extensively validate the returned data). 
	* The alternative then, which was what I chose, was to make the calls to the News API `headlines` endpoint be separate for each news source. While this amounts to more calls to endpoint, it is easier and more understandable to parse the returned response object, programmatically.

- Note security concern of hardcoding the News API apikey the functions used for the http requests. 
	* After doing some research on the topic of `api key storage and security`, I decide based on reading some discussions online for example from [here](https://12factor.net/config), [here](https://github.com/geosolutions-it/evo-odas/issues/159), [here](https://github.com/geosolutions-it/evo-odas/issues/118) and [here](https://issues.apache.org/jira/browse/AIRFLOW-45) - to store the key in an environmental variable that is then accessed in Airflow and Python at runtime. 
	* Airflow has an option of storing keys in a [Variable](https://airflow.apache.org/concepts.html#variables) but it based on the Airflow documentation it doesn't seem to be a very secure approach. Might want to look into better ways of api key encryption ?

- No S3 bucket link was given in the requirements, thus I created my own S3 bucket.

- Added `pip install --upgrade pip` and `pip install --upgrade setuptools` commands to the Makefile, under `init`

#### Versioning Issues

- The Apache Airflow version in the `requirements.txt` file was changed to `1.10.0` (from the original `1.9.0`) this was because the support for the FileSensor operator, used in one of the pipeline tasks, was only added in `1.10.0`
	* When installing `1.10.0` it throws a RuntimeError:
	>RuntimeError: By default one of Airflow's dependencies installs a GPL dependency (unidecode). To avoid this dependency set SLUGIFY_USES_TEXT_UNIDECODE=yes in your environment when you install or upgrade Airflow. To force installing the GPL version set AIRFLOW_GPL_UNIDECODE.
	* More details are discussed [here](https://medium.com/datareply/apache-airflow-1-10-0-released-highlights-6bbe7a37a8e1), [here](https://github.com/apache/incubator-airflow/blob/master/UPDATING.md#airflow-110) [here](https://github.com/pypa/pipenv/issues/2791), and [here](https://stackoverflow.com/questions/52203441/error-while-install-airflow-by-default-one-of-airflows-dependencies-installs-a) 
	* The solution to this error involves setting either `AIRFLOW_GPL_UNIDECODE=yes` OR `SLUGIFY_USES_TEXT_UNIDECODE=yes` as one of the environment variables in the Docker config file, so that it is available to Airflow during installation.
	* Running version `1.10.0` gives empty logs (when you click on a task node log in its Task Instance Context Menu) in the UI. The solution to this (found [here](https://github.com/apache/incubator-airflow/blob/master/UPDATING.md#airflow-110)) is to change this line in the airflow.cfg file:
	`task_log_reader = file.task` to `task_log_reader = task`
- The Airflow Community contributed [`airflow.contrib.sensor.file_sensor`](https://airflow.apache.org/_modules/airflow/contrib/sensors/file_sensor.html) and [`airflow.contrib.hooks.fs_hook`](https://airflow.apache.org/_modules/airflow/contrib/hooks/fs_hook.html#FSHook) classes were found to be *very* buggy, especially when trying to configure and test them in a DAG task pipeline.




---
### Project Plan/Initial Approach

- A week of learning and experimentation with new topics: Working with Airflow, RESTFul APIs, Docker, AWS Python Boto library, Python Integration Testing, Using Python Test Doubles, and applying Python Test-Driven Development in practice.

- two full days of coding, testing, and developing the solution.
# [Tempus Data Engineer Challenge](https://github.com/tempuslabs/challenges/tree/master/data-engineer-airflow-challenge) Project

### Two [Apache Airflow](https://airflow.apache.org) data pipelines were developed which fetch news data from a [News REST API](https://newsapi.org), store the data on the local filesystem, and perform a series of [ETL tasks](https://en.wikipedia.org/wiki/Extract,_transform,_load) that extract the top headlines; transform them into a CSV tabular structure; and upload the transformations to a given [Amazon S3](https://aws.amazon.com/s3/) bucket.

---
### Project Status

**Build**: [![Build Status](https://travis-ci.org/davidolorundare/tempus_de_challenge.svg?branch=project-with-moto-integration)](https://travis-ci.org/davidolorundare/tempus_de_challenge)

**Coverage**: [![codecov](https://codecov.io/gh/davidolorundare/tempus_de_challenge/branch/project-with-moto-integration/graph/badge.svg)](https://codecov.io/gh/davidolorundare/tempus_de_challenge)


---
### <a name="prereqs">Prerequisites</a> 

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

### <a name="optional-prereqs">Optional Prerequisites</a> - for running Integration test with a Fake Amazon S3 server
1. [RubyGems](https://rubygems.org/) [installation](https://rubygems.org/pages/download)
2. Register for a free [FakeS3 server license](https://supso.org/projects/fake-s3)
3. [Install FakeS3](https://github.com/jubos/fake-s3#installation)

---
### Getting Started: Setup

1. Clone a copy of the github repo into your working directory or Download it as a zip file and extract its contents into your working directory.

2. Open a command line terminal and navigate to the root of the repo directory.

3. Setup a Python virtual environment with the command `virtualenv ENV` where ENV is the directory path to where the virtual environment will be created.

4. The application uses [environmental variables](https://en.wikipedia.org/wiki/Environment_variable) to access the api keys needed for the News API and Amazon S3 usage. These keys are read from an `.env` file and `.aws` directory respectively, in the root directory of the repo, which you **must** create (and place in that directory) before proceeding to the next step. During Docker build-time, these files are copied into the container and made available to the application.
	* In the terminal run the command `export AIRFLOW_GPL_UNIDECODE=yes`, this resolves a dependency issue with the Airflow version used in this project (version 1.10.0). This command **needs to be run before** `make init` in the next step, so that this environmental variable is available to Airflow prior to its installation.
	* An example of an `.env` is shown below, the generated News API Key you obtained after registration is given the environmental variable name `NEWS_API_KEY` and its value should be set to the key you obtained.
	![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/project-with-moto-integration/readme_images/configure_newsapi_key_image.jpeg "Configuring API Keys")
	* An example of the `.aws` directory, `config` and `credentials` files are shown below.
	---
	![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/project-with-moto-integration/readme_images/configure_project_directory_image.jpeg "Creating the AWS directory")
	---
	![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/project-with-moto-integration/readme_images/configure_aws_directory_image.jpeg ".aws directory contents")
	---
	![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/project-with-moto-integration/readme_images/configure_aws_config_image.jpeg "AWS config file creation")
	---
	![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/project-with-moto-integration/readme_images/configure_aws_credentials_image.jpeg "AWS credentials file creation")

---

5. Run the command `make init` ; this downloads all of the project's dependencies.
	- `make init` installs the Amazon Python (Boto) SDK library. **Ensure your AWS account credentials are setup**, to use the SDK, after this step. See [here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html) for more details.

6. Run the command `make test` and `make integration-test` ; this runs all the unit and integration tests, respectively, for the project and ensures they are passing.

7. Run the command `make run` ; this starts up Docker, reads in the Dockerfile, and configures the container with Airflow to begin running. 
	- This takes a few seconds to about three minutes; for the container images to be downloaded and setup. Thereafter, Airflow's scheduler and webserver start up and the User interface and Admin Console becomes accessible. Open a web browser a navigate to http://localhost:9090 to access the Console.
	- The two data pipelines "tempus_challenge_dag" and "tempus_bonus_challenge_dag" will have been loaded and are visible.
	- In the Console UI (shown below) click on the toggle next to each pipeline name to activate them, and click on the the play button icon on the right to start each. The steps are numbered in order.

	![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/master/readme_images/airflow_ui_console.jpeg "Airflow Console UI - Activate and Trigger Dags")
	---
	- By default, Airflow loads DAGs *paused*, clicking on toggle described previously will unpause them. For convenience the pipelines are preconfigured to be unpaused when Airflow starts, and thereafter to run at their prescheduled times of 12AM and 1AM each day (i.e. 1hour apart). They *can* be run immediately, however, by clicking on the "Trigger Dag" icon, described previously and shown above in the Airflow UI. Their respective logs can be viewed from their [Task Instance Context Menus](https://airflow.readthedocs.io/en/latest/ui.html#task-instance-context-menu)

---
### Getting Started: Pipeline Overview 

Discusses the breakdown of the project goals into the two pipelines.

#### DAG Pipeline 1

The first pipeline, named 'tempus_challenge_dag' is scheduled to run once a day at 12AM, and consists of eight tasks (six of which are the core). Its structure is shown below:

![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/project-with-moto-integration/readme_images/tempus_dag_pipeline-1-success_image.jpeg "Image of Pipeline-1 structure")

The pipeline tasks are as follows:
- The first task is an [Airflow DummyOperator](https://airflow.apache.org/code.html#airflow.operators.dummy_operator.DummyOperator) which does nothing and is used merely to visually indicate the beginning of the pipeline. 

- Next, using a predefined [Airflow PythonOperator](https://airflow.apache.org/code.html#airflow.operators.python_operator.PythonOperator), it calls a python function to create three datastore folders for storing the intermediary data for the 'tempus_challenge_dag' that is later on downloaded and transformed. 
The 'news', 'headlines', and 'csv' folders are created under the parent 'tempdata' directory which is made relative to the airflow home directory.
	
- The third task involves a defined [Airflow SimpleHTTPOperator](https://airflow.apache.org/code.html#airflow.operators.http_operator.SimpleHttpOperator) making an HTTP GET request to the News API's 'sources' endpoint with the assigned API Key, to fetch all English news sources. A Python callback function is defined with this operator, and handles processing of the returned Response object, storing the JSON news data as a file in the pipeline's 'news' datastore folder.

- The fourth task involves a defined [Airflow FileSensor](https://airflow.apache.org/code.html#airflow.contrib.sensors.file_sensor.FileSensor) detects whenever the JSON news files have landed in the appropriate directory, this kicks off the subsequent ETL stages of the pipeline.

- The fifth task - Extraction - involves a defined [Airflow PythonOperator](https://airflow.apache.org/code.html#airflow.operators.python_operator.PythonOperator) which reads from the news sources directory and for each source in the JSON file it makes a remote api call to get the latest headlines; then using JSON and Pandas libraries extracts the top-headlines from it, storing the result in the 'headlines' folder.

- The sixth task, extraction and transformation of the headlines take place and it involves a separate predefined [Airflow PythonOperator](https://airflow.apache.org/code.html#airflow.operators.python_operator.PythonOperator) using a python function that reads the top-headlines JSON data from the 'headlines' folder, and using Pandas converts it into an intermidiary DataFrame object which is flattened into CSV. The flattened CSV files are stored in the 'csv' folder. If **no news articles were found** in the data **then no CSV file is created**, the application logs this csv-file absence to the Airflow Logs.

- The seventh task, the Upload task, involves a defined custom Airflow PythonOperator, as Airflow does not have an existing Operator for transferring data directly from the local filesystem to Amazon S3. The Operator is built ontop of the Amazon Python Boto library, using [preexisting credentials](#prereqs) already setup, and moves the transformed data from the 'csv' folder to an S3 bucket already setup by the author.
Two Amazon S3 buckets were setup by the author:
	* [`tempus-challenge-csv-headlines`](http://tempus-challenge-csv-headlines.s3.amazonaws.com/) 
	* [`tempus-bonus-challenge-headlines`](http://tempus-bonus-challenge-csv-headlines.s3.amazonaws.com/) 
---

to store the flattened csv files from the 'tempus_challenge_dag' and 'tempus_bonus_challenge_dag' pipeline respectively. Navigating to these bucket links from any web browser returns any XML list of all their contents. 

![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/master/readme_images/s3_challenge_bucket.jpeg "Image of Pipeline-1 S3 Bucket")

---

![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/master/readme_images/s3_bonus_challenge_bucket.jpeg "Image of Pipeline-2 S3 Bucket")

By default a `dummy.txt` file is all that exists in the buckets. To view or download any of the files in the bucket, append the name of that document to the end of the aforementioned links.

---

- The eighth and final task is an [Airflow DummyOperator](https://airflow.apache.org/code.html#airflow.operators.dummy_operator.DummyOperator) which does nothing and is used merely to signify the end of the pipeline.


#### DAG Pipeline 2
The second pipeline, named 'tempus_bonus_challenge_dag' is similar to the first; but consisting of seven tasks. It is scheduled to run once a day at 1AM. Its structure is shown below:

![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/project-with-moto-integration/readme_images/tempus_dag_pipeline-2-success_image.jpeg "Image of Pipeline-2 structure")

The pipeline tasks are identical to that of the first. The only main difference is in the third task of calling the News API:

- Four [Airflow SimpleHTTPOperators](https://airflow.apache.org/code.html#airflow.operators.http_operator.SimpleHttpOperator) are defined which make separate, but parallel, HTTP GET requests to the News API's 'top-headlines' endpoint directly with the assigned API Key and a query for specific keywords: 'Tempus Labs', 'Eric Lefokosky', 'Cancer', and Immunotherapy. This fetches data on each of these keywords. The Python callback function which handles the return Response object stores them as four JSON files in the 'headlines' folder, created in an earlier step, for the 'tempus_bonus_challenge_dag'.

- In its fifth task, extraction and transformation sub-operations take place in this task, named `flatten_to_csv_kw_task`, this is similar to Pipeline 1's sixth task.

#### Transformations Notes
The end transformations are stored in the respective `csv` datastore folders of the respective pipelines.

##### 				Pipeline 1  CSV: Tempus Challenge DAG
For the 'tempus_challenge_dag' pipeline all the news headlines from all the english sources are flattened and transformed into one single csv file, the pipeline execution date is appended to the end transformed csv. It is of the form: `pipeline_execution_date_headlines.csv`

##### 				Pipeline 2 CSVs: Tempus Bonus Challenge DAG
For each of the four keywords queries of the 'tempus_bonus_challenge_dag' - 'Tempus Labs', 'Eric Lefkofsky', 'Cancer', 'Immunotheraphy' - the result is four separate csv files, each representing all the headlines about that particular keyword. The pipeline execution date is appended to the end transformed csv's. The keyword headline files are of form:`pipeline_execution_date_keyword_headlines.csv`

---
### Running Usage

![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/master/readme_images/tempus_de_challenge.gif "Activating and Triggering a Pipeline Run")

---

![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/master/readme_images/tempus_de_challenge_graph-view.gif "Graph View of Pipeline")

---
### Running Tests (Unit and Integration)

* This project's unit and integration tests can be found in the `tests` folder in the root directory.
	- Running `make test` and `make integration-test` from the command line run all the tests for the associated Python functions used in the project.
* The project uses [Flake8](http://flake8.pycqa.org/en/latest/) as its Python Linter, ensuring code conformance to the [Python PEP-8 standards](http://pep8.org/). It is also setup with [Travis CI](http://travis-ci.com/) to remotely run all the tests and [Codecov](https://Codecov.io/) reports test-coverage for the project; these can be further integrated in a [Continuous Build/Integration](https://en.wikipedia.org/wiki/Continuous_integration)/Delivery pipeline later on if needed.

The **unit tests** consists of six test suites corresponding to the core tasks in the two data pipelines. They are split into python files with the prefix `test_xxxxx`, where xxxxx is the name of the kind of functionality being tested.
The tests make use of [Pytest](https://docs.pytest.org/en/latest/) for unit testing and test coverage checks, as well as the [Python Mocking library](https://docs.python.org/dev/library/unittest.mock.html) and [PyFakeFS](https://pypi.org/project/pyfakefs/) for simulating I/O dependencies such as functions interacting with the filesystem or making external network calls. The test core suites are:
- *TestFileStorage* which runs tests on the task involving creation of the datastore folders and actions on them.
- *TestNetworkOperations* which run tests on the task involving HTTP calls to the News API.
- *TestExtractOperations* which run tests on the task involving extracting headlines from the news data.
- *TestTransformOperations* which run tests on the task involving conversion of the news headlines JSON data into CSV.
- *TestUploadOperations* which run tests on the task involving data-transfer of the flattened CSVs to a predefined Amazon S3 bucket.
- *TestNewsInfoDTO* which run tests on NewsInfoDTO, a Data Transfer Object Python class, used by many of the other python class and module functions for moving information about news data between processing functions.

**Integration tests** exercise the overall combination of components interacting with each and other and external services. This implies that for tasks in the pipelines it would particularly test their interaction with the two main external services used: the News API and Amazon S3. Integration tests were written only for the UploadOperation interaction with an external Amazon S3 server; using Moto. [Moto](http://docs.getmoto.org/en/latest/) an (embedded) Amazon S3 Mock Server, is used to mock/simulate the behavior of running the project's csv-file upload operations (the last main task in each pipeline) interacting with the external Amazon S3 storage service.

The Amazon S3 integrations mock tests were done with the moto library standalone, as well as with a live running [FakeS3 server](https://github.com/jubos/fake-s3). The **test with the FakeS3 server** is *by default skipped* in the test suite. Details on how to run the integration test with the FakeS3 server are describe below:

---

![alt text](https://github.com/davidolorundare/tempus_de_challenge/blob/project-with-moto-integration/readme_images/fakes3_server_integration.jpeg "Image of using moto s3 server integration")

---

- With [FakeS3 installed](#optional-prereqs) already, in the terminal navigate to a directory of your choice and run the following command: `fakes3 -r . -p 4567 --license YOUR_LICENSE_KEY`. This starts the Fake Amazon S3 server.
- In the `tests` directory of the project open the `test_upload_operations.py`
file, remove the `@pytest.mark.skip` line on the `test_upload_csv_to_s3_fakes3_integration_succeeds` test.
- Run `make integration-test` to execute the test case, which invokes live calls to the fake Amazon S3 server.
- To stop the Fake Amazon S3 server, return to the previous terminal and press `Ctrl+C` to stop it.

---
### Packages Used

1. [Amazon Python SDK (boto 3) library](http://boto3.readthedocs.io/en/latest/guide/resources.html)
2. [Apache Airflow CLI](https://airflow.apache.org/cli.html)
3. [Codecov](https://codecov.io/)
4. [FakeS3](https://github.com/jubos/fake-s3)
5. [Flake8 - Python Pep-8 Style Guide Enforcement](http://flake8.pycqa.org/en/latest/)
6. [Moto Amazon S3 Mock Server](http://docs.getmoto.org/en/latest/) 
7. [News API](https://newsapi.org/)
8. [PostgreSQL Python library](https://wiki.postgresql.org/wiki/Psycopg2)
9. [Pyfakefs](https://pypi.org/project/pyfakefs/)
10. [Pytest](https://docs.pytest.org/en/latest/)
11. [Python Data Analysis library (Pandas)](https://pandas.pydata.org/)
12. [Python JSON library](https://docs.python.org/3/library/json.html)
13. [Python Requests library](http://docs.python-requests.org) 

---
### Footnotes

- Where to store the data at rest: locally as files or using a database ? There are a number of tradeoffs using either, but given the scope of the project I decided to use a local filesystem. 
	* Although Airflow has an inter-task mechanism (called XCom) for passing data between tasks, from reading the Airflow documentation and research on the topic it was generally not recommended Xcoms be used for transferring large data between tasks (though the news json data in this project is small it could become large if the news data returned from the newsapi increases). Hence why, ultimately, the data-at-rest decision was narrowed down to only the filesystem or database options.
	* In a production environment I would rather configure a real database, like PostgreSQL, to serve as a data warehouse for the News data retrieved, as well as setup tempoary data-staging areas for the intermediate data created during the ETL tasks.

- For the bonus challenge, on experimenting with the News API, it was discovered that
using all four keywords in the same api-request returned 0 hits. Hence, I decided four separate api-request calls would made; for each individual keyword.

- To reduce the number of calls to the News API in the task of DAG pipeline 1 `tempus_challenge_dag`, to retrieve the source headlines, the list of sources from the previous upstream task can be batched up and fed as a comma-separated string of identifiers to the `sources` parameter of the `headlines` endpoint. 
	* However, the returned Response object will be very large and would can consist of a mix of headlines from all these news sources, which can be very confusing to parse programmatically (without some ample patience for writing more unit tests to extensively validate the behaviors. 
	* The alternative then, which was what I chose, was to make the calls to the News API `headlines` endpoint be separate for each news source. While this amounts to more calls to endpoint, it is easier and more understandable to parse the returned response object, programmatically.

- Note security concern of hardcoding the News API apikey the functions used for the http requests. 
	* After doing some research on the topic of 'api key storage and security', I decide based on reading some discussions online - for example from [here](https://12factor.net/config), [here](https://github.com/geosolutions-it/evo-odas/issues/159), [here](https://github.com/geosolutions-it/evo-odas/issues/118) and [here](https://issues.apache.org/jira/browse/AIRFLOW-45) - to store the key in an environmental variable that is injected into the Docker container and then accessed in the Airflow instance and Python at runtime. 
	* Airflow has an option of storing keys in a [Variable](https://airflow.apache.org/concepts.html#variables) but it based on the Airflow documentation it doesn't seem to be a very secure approach. Might want to look into better ways of api key encryption ?

- No S3 bucket link was given in the project requirements, thus I created my own S3 bucket. The project implementation was designed such that anyone could use their own preexisting S3 buckets when running the code locally, as long as their bucket names corresponded to the two developed for this project: `
tempus-challenge-csv-headlines` and `tempus-bonus-challenge-csv-headlines`.
- I added `pip install --upgrade pip` and `pip install --upgrade setuptools` commands to the Makefile, under `init`, to ensure an up to date version of pip is always used when the code is run. Though, in hindsight, this *could potentially* cause build-breaking issues; if there are new changes in pip to the python packages used in the project that weren't supported.

- It was observed that in some instances the Amazon Boto library doesn't detect the AWS API keys when set from within the Docker container environmental variables. The workaround was to create an `.aws` directory inside the container during Docker build-time and inject the `config` and `credentials` files with the keys. The dockerfile was modified for this purpose. Due to the obvious security concerns around this approach **these two files are never kept** in git. 

#### Versioning Issues

- The Apache Airflow version in the `requirements.txt` file was changed to `1.10.0` (from the original `1.9.0`) this was because the support for the FileSensor operator, used in one of the pipeline tasks, was only added in `1.10.0`
	* When installing `1.10.0` it throws a RuntimeError:
	>RuntimeError: By default one of Airflow's dependencies installs a GPL dependency (unidecode). To avoid this dependency set SLUGIFY_USES_TEXT_UNIDECODE=yes in your environment when you install or upgrade Airflow. To force installing the GPL version set AIRFLOW_GPL_UNIDECODE.
	* More details are discussed [here](https://medium.com/datareply/apache-airflow-1-10-0-released-highlights-6bbe7a37a8e1), [here](https://github.com/apache/incubator-airflow/blob/master/UPDATING.md#airflow-110) [here](https://github.com/pypa/pipenv/issues/2791), and [here](https://stackoverflow.com/questions/52203441/error-while-install-airflow-by-default-one-of-airflows-dependencies-installs-a) 
	* The solution to this error involved setting either `AIRFLOW_GPL_UNIDECODE=yes` OR `SLUGIFY_USES_TEXT_UNIDECODE=yes` as one of the environment variables in the Docker config file - creating the variable at build-time - so that it would be available to Airflow during its installation.
	* Running version `1.10.0` gives empty logs (when you click on a task node log in its Task Instance Context Menu) in the UI. The solution to this (found [here](https://github.com/apache/incubator-airflow/blob/master/UPDATING.md#airflow-110)) is to change this line in the airflow.cfg file:
	`task_log_reader = file.task` to `task_log_reader = task`
- The Airflow Community contributed [`airflow.contrib.sensor.file_sensor`](https://airflow.apache.org/_modules/airflow/contrib/sensors/file_sensor.html) and [`airflow.contrib.hooks.fs_hook`](https://airflow.apache.org/_modules/airflow/contrib/hooks/fs_hook.html#FSHook) modules were found to be *very* buggy to use, especially when trying to configure and test them in a DAG task pipeline.

- There are known issues with using the [Moto Server](http://docs.getmoto.org/en/latest/docs/getting_started.html#stand-alone-server-mode) in stand-alone server mode when testing a locally created url endpoint. See [here](https://github.com/spulec/moto/issues/1026) for more details.
- Moto breaks the CI builds (e.g. Travis-CI, Circle-CI) when those CI tools make pull requests to build this project within their environments. This is due a Moto dependency on a google-compute-engine module that is non-existent in the environment and fails to download. More details on this issue with Travis-CI are [here](https://github.com/travis-ci/travis-ci/issues/7940) and its fix [here](https://github.com/travis-ci/travis-ci/issues/7940#issuecomment-310759657) and [here](https://github.com/spulec/moto/issues/1771).


---
### Project Plan/Initial Approach

- A week of learning and experimentation with completely new topics: Working with Airflow, RESTFul APIs, Docker, AWS Python Boto library, Python Integration Testing, Using Python Test Doubles, and applying Python Test-Driven Development in practice.
- A week of coding, testing, and developing the solution.
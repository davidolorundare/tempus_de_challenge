# Tempus Data Engineer Challenge Project

## A solution to the Tempus Data Engineer Challenge


---
### Status

| --- | --- |
|**Build****|[![Build Status](https://travis-ci.org/davidolorundare/tempus_de_challenge.svg?branch=master)](https://travis-ci.org/davidolorundare/tempus_de_challenge)|

---
### Solution Goals

Writing this, will update soon.

---
### Approach

- one full day of learning and experimentation with new topics: Working with Airflow, RESTFul APIs, Docker, AWS Python Boto lib, and applying TDD in practice.

- second full day of coding, testing, and developing the solution.

---
### Working Notes

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
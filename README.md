# Tempus Data Engineer Challenge Project

## A solution to the Tempus Data Engineer Challenge

---

### Solution Goals

---
### Approach

- one full day of learning and experimentation with new topics: Working with Airflow, RESTFul APIs, Docker, AWS Python Boto lib, and applying TDD in practice.

- second full day of coding, testing, and developing the solution.

---
### Notes

- On experimenting with News API it was discovered that
using all four keywords in the same api-request returned
0 hits. Hence, four separate api-request calls will be made;
for each keyword.

- Note error handling for api endpoint (ok versus not ok)

- Need to decide on whether to use the client lib/ not.e.g. pip install newsapi-python

- Use json and csv libs for parsing

- Note security concern of hardcoding the apikey in requests. Might want to encrypt api

- Need to write unit tests. Apply a TDD approach.

- Need to write integration tests (try using Travis CI and badge), must be startable via 'make run'
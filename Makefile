SHELL = /bin/bash
MODULE = dags

init:
	pip install -r requirements-setup.txt
	pip install -r requirements-test.txt
	pip install -r requirements.txt

run: clean
	@echo
	@echo --- Running Dockerized Airflow ---
	docker-compose -f docker/docker-compose.yml up --build

lint:
	@echo
	@echo --- Lint ---
	python -m flake8 ${MODULE}/

test:
	@echo
	@echo --- Test ---
	python -m pytest --cov=${MODULE} --cov-branch tests/

clean:
	@echo
	@echo --- Clean ---
	python setup.py clean
	find ${MODULE} | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf
	find tests | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf
	if [ -d ".pytest_cache" ]; then rm -r .pytest_cache; fi
	if [ -d ".coverage" ]; then rm  .coverage; fi

.PHONY: test

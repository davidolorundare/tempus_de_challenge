#!/usr/bin/env python
import os
from setuptools import setup


def read(fname):
    """ Reads a file; Used everywhere.

    :param fname: The name of a file relative to the root path
    :return: The string contents of the file
    """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='Data Eng Challenge',
    version='1.0',
    description='Data Engineer Airflow Challenge',
    long_description=read('README.md'),
    author='Alberto Rios',
    author_email='alberto.rios@tempus.com',
    url='https://www.tempus.com/',
    packages=['dags'],
    install_requires=read('requirements.txt'),
    setup_requires=read('requirements-setup.txt'),
)

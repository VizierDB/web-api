#! /usr/bin/env python

from setuptools import setup

setup(
    name='vizier-webapi',
    version='0.3.0',
    description='API to query and manipulate Vizier DB data curation projects and workflows',
    keywords='data curation ',
    license='apache-2.0',
    packages=['vizier'],
    package_data={'': ['LICENSE.txt']},
    install_requires=[
	'Flask~=0.12',
	'flask-cors',
	'pyyaml',
	'py4j>=0.10.6',
	'spylon>=0.3.0'
    ]
)

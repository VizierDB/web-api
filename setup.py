#! /usr/bin/env python

from setuptools import setup

setup(
    name='vizier-webapi',
    version='0.3.0',
    description='API to query and manipulate Vizier DB data curation projects and workflows',
    keywords='data curation ',
    license='GPLv3',
    packages=['vizier'],
    package_data={'': ['LICENSE']},
    install_requires=[
    ]
)

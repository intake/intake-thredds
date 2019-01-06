#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages
import versioneer
from os.path import exists

readme = open("README.rst").read() if exists("README.rst") else ""



setup(
    name='intake-siphon',
    description='Siphon to Intake translation layer',
    long_description=readme,
    maintainer='Anderson Banihirwe',
    maintainer_email='abanihi@ucar.edu',
    url='https://github.com/NCAR/intake-siphon',
    packages=find_packages(),
    package_dir={'intake-siphon': 'intake-siphon'},
    include_package_data=True,
    install_requires=[
    ],
    license='Apache-2.0 license',
    zip_safe=False,
    keywords='intake-siphon',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)
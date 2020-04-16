#!/usr/bin/env python

"""The setup script."""

from os.path import exists

from setuptools import find_packages, setup

with open('requirements.txt') as f:
    install_requires = f.read().strip().split('\n')

if exists('README.rst'):
    with open('README.rst') as f:
        long_description = f.read()
else:
    long_description = ''

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Intended Audience :: Science/Research',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Topic :: Scientific/Engineering',
]


setup(
    name='intake-thredds',
    description='Thredds to Intake translation layer',
    long_description=long_description,
    python_requires='>=3.6',
    maintainer='NCAR XDev Team',
    maintainer_email='xdev@ucar.edu',
    url='https://github.com/NCAR/intake-thredds',
    packages=find_packages(),
    package_dir={'intake-thredds': 'intake-thredds'},
    include_package_data=True,
    install_requires=install_requires,
    license='Apache 2.0',
    zip_safe=False,
    keywords='intake thredds siphon',
    use_scm_version={'version_scheme': 'post-release', 'local_scheme': 'dirty-tag'},
    setup_requires=['setuptools_scm', 'setuptools>=30.3.0'],
    classifiers=CLASSIFIERS,
)

#!/usr/bin/env python3

"""The setup script."""


from setuptools import find_packages, setup

with open('requirements.txt') as f:
    install_requires = f.read().strip().split('\n')

with open('README.md') as f:
    long_description = f.read()


CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Intended Audience :: Science/Research',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Topic :: Scientific/Engineering',
]


setup(
    name='intake-thredds',
    description='Intake interface to THREDDS data catalogs.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    python_requires='>=3.7',
    maintainer='NCAR XDev Team',
    maintainer_email='xdev@ucar.edu',
    url='https://github.com/intake/intake-thredds',
    project_urls={
        'Documentation': 'https://github.com/intake/intake-thredds',
        'Source': 'https://github.com/intake/intake-thredds',
        'Tracker': 'https://github.com/intake/intake-thredds/issues',
    },
    packages=find_packages(),
    package_dir={'intake-thredds': 'intake-thredds'},
    entry_points={
        'intake.drivers': [
            'thredds_cat = intake_thredds.cat:ThreddsCatalog',
            'thredds_merged = intake_thredds.source:THREDDSMergedSource',
        ]
    },
    include_package_data=True,
    install_requires=install_requires,
    license='Apache 2.0',
    zip_safe=False,
    keywords='intake thredds siphon catalogs',
    use_scm_version={'version_scheme': 'post-release', 'local_scheme': 'dirty-tag'},
    classifiers=CLASSIFIERS,
)

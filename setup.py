#!/usr/bin/env python3

from setuptools import setup, Extension

setup(name='pydbs',
      version='0.1',
      description='Python interface to Direct Block Store',
      author='Antony Chazapis',
      ext_modules=[Extension('pydbs',
                             sources=['pydbs.c', 'dbs.c'])],
      python_requires='>=3.6',
      classifiers=['Development Status :: 4 - Beta',
                   'Environment :: Console',
                   'Programming Language :: Python :: 3.6',
                   'Programming Language :: C'
                   'Operating System :: OS Independent',
                   'Topic :: Software Development :: Libraries :: Python Modules'])

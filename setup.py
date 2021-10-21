#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013 - 2021 Pytroll

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>
#   Adam Dybbroe

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Setup for modis-dr-runner.
"""
from setuptools import setup, find_packages

try:
    # HACK: https://github.com/pypa/setuptools_scm/issues/190#issuecomment-351181286
    # Stop setuptools_scm from including all repository files
    import setuptools_scm.integration
    setuptools_scm.integration.find_files = lambda _: []
except ImportError:
    pass

description = 'Pytroll runner for MODIS level-1 processing'

try:
    with open('./README', 'r') as fd:
        long_description = fd.read()
except IOError:
    long_description = ''


NAME = "modis_runner"

setup(name=NAME,
      description=description,
      author='Adam Dybroe',
      author_email='adam.dybroe@smhi.se',
      classifiers=["Development Status :: 3 - Alpha",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: GNU General Public License v3 " +
                   "or later (GPLv3+)",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Scientific/Engineering"],
      url="https://github.com/pytroll/pytroll-modis-runner",
      long_description=long_description,
      license='GPLv3',
      packages=find_packages(),
      scripts=['bin/modis_dr_runner.py',
               'bin/seadas_modis_runner.py', ],
      data_files=[],
      install_requires=['posttroll', 'trollsift', 'nwcsafpps_runner', ],
      python_requires='>=3.6',
      zip_safe=False,
      setup_requires=['posttroll', 'setuptools_scm', 'setuptools_scm_git_archive'],
      use_scm_version=True
      )

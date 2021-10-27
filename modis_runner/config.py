#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>

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

"""
"""
import os
import yaml


def load_config_from_file(filepath):
    """Load the yaml config from file, given the file-path"""
    with open(filepath, 'r') as fp_:
        config = yaml.load(fp_, Loader=yaml.FullLoader)

    return config


def get_config_from_yamlfile(configfile):
    """Get the configuration from file."""

    config = load_config_from_file(configfile)

    options = {}
    for item in config:
        if not isinstance(config[item], dict):
            options[item] = config[item]

    return options


def get_config_yaml(configfile):
    """Get the configuration from file."""
    config = load_config_from_file(configfile)

    options = {}
    for item in config:
        if not isinstance(config[item], dict):
            options[item] = config[item]

    if isinstance(options.get('subscribe_topics'), str):
        subscribe_topics = options.get('subscribe_topics').split(',')
        for item in subscribe_topics:
            if len(item) == 0:
                subscribe_topics.remove(item)
        options['subscribe_topics'] = subscribe_topics

    return options


def get_config(configfile):

    filetype = os.path.splitext(configfile)[1]
    if filetype == '.yaml':
        return get_config_yaml(configfile)
    else:
        print("%s is not a valid extension for the config file" % filetype)
        print("Pleas use .yaml")
        return []

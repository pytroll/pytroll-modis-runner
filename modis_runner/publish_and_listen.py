#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Pytroll Developers

# Author(s):

#   Adam Dybbroe <Firstname.Lastname at smhi.se>

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

"""Helper functions for listening for incoming messages and publish posttroll
messages when processing is ready.

"""

import socket
import logging
from six.moves.urllib.parse import urlparse
from posttroll.address_receiver import get_local_ips
from nwcsafpps_runner.publish_and_listen import FileListener
from nwcsafpps_runner.publish_and_listen import FilePublisher

LOG = logging.getLogger(__name__)


class MODISFilePublisher(FilePublisher):
    """A publisher for the MODIS level-1 files. 

    Picks up the return value from the ctype_composite_worker when ready, and
    publishes the files via posttroll.
    """

    def __init__(self, queue, publish_topic):
        super(MODISFilePublisher, self).__init__(queue, publish_topic,
                                                 runner_name='modis_dr_runner')


class MODISFileListener(FileListener):
    """A file listener class, to listen for incoming messages with a 
    relevant file for further processing"""

    def __init__(self, queue, subscribe_topics, eos_satellites):
        super(MODISFileListener, self).__init__(queue, subscribe_topics)
        self.eos_satellites = eos_satellites

    def check_message(self, msg):
        if not msg:
            return False
        if msg.type not in ('file', 'collection', 'dataset'):
            return False

        urlobj = urlparse(msg.data['uri'])
        server = urlobj.netloc
        url_ip = socket.gethostbyname(urlobj.netloc)
        if server and (url_ip not in get_local_ips()):
            LOG.warning("Server %s not the current one: %s", str(server),
                        socket.gethostname())
            return False

        if ('platform_name' not in msg.data or 'orbit_number' not in msg.data
                or 'start_time' not in msg.data):
            LOG.info("Message is lacking crucial fields...")
            return False

        if msg.data['platform_name'] not in self.eos_satellites:
            LOG.info(
                str(msg.data['platform_name']) + ": " +
                "Not an EOS satellite. Continue...")
            return False

        sensor = msg.data.get('sensor', None)
        if not isinstance(sensor, list):
            sensor = [sensor]
        if any([s not in ['modis', 'gbad'] for s in sensor]):
            LOG.debug("Not MODIS or GBAD data, skip it...")
            return False

        LOG.debug("Ok: message = %s", str(msg))
        return True

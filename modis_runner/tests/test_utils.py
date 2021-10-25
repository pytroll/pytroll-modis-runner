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

"""Unit testing the modis-dr-runner utils functions
"""

from unittest.mock import patch
import unittest
from datetime import datetime
import tempfile
import os

from modis_runner.utils import scene_processed_recently
from modis_runner.utils import ready2run
from modis_runner.utils import get_geo_command_line_list


OPTIONS = {'subscribe_topics': ['/PDS/0/nkp/dev/polar/direct_readout', '/XLBANDANTENNA/AQUA/ISP', '/XLBANDANTENNA/TERRA/ISP'], 'publish_topic': '/EOS', 'level0_home': '/local_disk/data/polar_in/direct_readout/modis', 'ephemeris_home': '/local_disk/data/polar_out/direct_readout/modis', 'attitude_home': '/local_disk/data/polar_out/direct_readout/modis', 'level1b_home': '/local_disk/data/polar_out/direct_readout/modis', 'filetype_aqua': 'P1540064AAAAAAAAAAAAAA%y%j%H%M%S001.PDS', 'globfile_aqua': 'P1540064AAAAAAAAAAAAAA*001.PDS', 'packetfile_aqua': 'P154095715409581540959%y%j%H%M%S001.PDS', 'geofile_aqua': 'MYD03_A%y%j_%H%M%S', 'level1a_aqua': 'Aqua_MODIS_l1a_%y%j_%H%M%S', 'level1b_aqua': 'MYD021km_A%y%j_%H%M%S', 'level1b_250m_aqua': 'MYD02Qkm_A%y%j_%H%M%S', 'level1b_500m_aqua': 'MYD02Hkm_A%y%j_%H%M%S', 'filetype_terra': 'P0420064AAAAAAAAAAAAAA%y%j%H%M%S001.PDS', 'globfile_terra': 'P0420064AAAAAAAAAAAAAA*001.PDS',
           'geofile_terra': 'MOD03_A%y%j_%H%M%S', 'level1a_terra': 'Terra_MODIS_l1a_%y%j_%H%M%S', 'level1b_terra': 'MOD021km_A%y%j_%H%M%S', 'level1b_250m_terra': 'MOD02Qkm_A%y%j_%H%M%S', 'level1b_500m_terra': 'MOD02Hkm_A%y%j_%H%M%S', 'working_dir': '/local_disk/src/modis_lvl1proc/work', 'spa_config_file': '/local_disk/opt/MODISL1DB_SPA/current/gbad/smhi_configfile', 'url_modis_navigation': 'http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/', 'days_keep_old_etc_files': 90, 'days_between_url_download': 7, 'log_rotation_days': 1, 'log_rotation_backup': 5, 'ocssw_env': '/san1/opt/SeaDAS/8.1/ocssw/OCSSW_bash.env', 'seadas_home': '/san1/opt/SeaDAS/8.1', 'leapsec_dir': '/san1/opt/SeaDAS/8.1/ocssw/var/modis', 'modis_l1a_script': '/san1/opt/SeaDAS/8.1/ocssw/bin/modis_L1A', 'modis_l1b_script': '/san1/opt/SeaDAS/8.1/ocssw/bin/modis_L1B', 'modis_geo_script': '/san1/opt/SeaDAS/8.1/ocssw/bin/modis_GEO'}


class TestModisRunnerSceneChecks(unittest.TestCase):
    """Test various aspects on checking the scene messages before launching the actual processing."""

    def setUp(self):
        self.scene_id1 = "EOS-Aqua_3423_202110131328"
        self.scene_id2 = "EOS-Terra_16029_202110111943"
        self.job1 = {'EOS-Aqua_3423_202110131328': datetime(2021, 10, 13, 13, 41, 24, 193399)}

        self.msg_data1 = {"uid": "P0420064AAAAAAAAAAAAAA21284194359001.PDS", "format": "PDS", "orbit_number": 16029, "start_time": "2021-10-11T19:43:59", "variant": "DR",
                          "uri": "ssh://localhost/tmp/P0420064AAAAAAAAAAAAAA21284194359001.PDS", "platform_name": "EOS-Terra", "end_time": "2021-10-11T19:57:47", "type": "binary", "sensor": "modis", "data_processing_level": "0"}

        self.msg_data2 = {"uid": "P0420064AAAAAAAAAAAAAA21284194359000.PDS", "format": "PDS", "orbit_number": 16029, "start_time": "2021-10-11T19:43:59", "variant": "DR",
                          "uri": "ssh://localhost/tmp/P0420064AAAAAAAAAAAAAA21284194359000.PDS", "platform_name": "EOS-Terra", "end_time": "2021-10-11T19:57:47", "type": "binary", "sensor": "modis", "data_processing_level": "0"}

        self.msg_data_aqua1 = {"uid": "P15409571540958154095921286132853001.PDS", "format": "PDS", "orbit_number": 3423, "start_time": "2021-10-13T13:28:53", "variant": "DR",
                               "uri": "ssh://localhost/tmp/P15409571540958154095921286132853001.PDS", "platform_name": "EOS-Aqua", "end_time": "2021-10-13T13:40:35", "type": "binary", "sensor": "gbad", "data_processing_level": "0"}
        self.msg_data_aqua2 = {"uid": "P1540064AAAAAAAAAAAAAA21286132853001.PDS", "format": "PDS", "orbit_number": 3423, "start_time": "2021-10-13T13:28:53", "variant": "DR",
                               "uri": "ssh://localhost/tmp/P1540064AAAAAAAAAAAAAA21286132853001.PDS", "platform_name": "EOS-Aqua", "end_time": "2021-10-13T13:40:35", "type": "binary", "sensor": "modis", "data_processing_level": "0"}

        self.options = OPTIONS

    def test_scene_processed_recently(self):
        """Test the function to check if a given scene has been processed before."""
        job_register = self.job1

        res = scene_processed_recently(job_register, self.scene_id1)
        self.assertTrue(res)

        res = scene_processed_recently(job_register, self.scene_id2)
        self.assertFalse(res)

    def test_ready2run_terra_scene(self):
        """Test the function checking if a Terra scene is ready for processing."""
        with tempfile.TemporaryDirectory() as tmpdirname:

            filename1 = "P0420064AAAAAAAAAAAAAA21284194359001.PDS"
            filename2 = "P0420064AAAAAAAAAAAAAA21284194359000.PDS"

            file1 = os.path.join(tmpdirname, filename1)
            file2 = os.path.join(tmpdirname, filename2)

            msg1data = self.msg_data1.copy()
            msg2data = self.msg_data2.copy()

            msg1data['uri'] = "ssh://localhost" + file1
            msg2data['uri'] = "ssh://localhost" + file2

            with open(file1, 'w') as _:
                pass
            with open(file2, 'w') as _:
                pass

            eosfiles = {}
            res = ready2run(msg1data, eosfiles, self.scene_id2, self.options)
            assert res

            res = ready2run(msg2data, eosfiles, self.scene_id2, self.options)
            assert res

    def test_ready2run_aqua_scene(self):
        """Test the function checking if a Aqua scene is ready for processing."""
        with tempfile.TemporaryDirectory() as tmpdirname:

            filename1 = "P15409571540958154095921286132853001.PDS"
            filename2 = "P1540064AAAAAAAAAAAAAA21286132853001.PDS"

            file1 = os.path.join(tmpdirname, filename1)
            file2 = os.path.join(tmpdirname, filename2)

            msg1data = self.msg_data_aqua1.copy()
            msg2data = self.msg_data_aqua2.copy()

            msg1data['uri'] = "ssh://localhost" + file1
            msg2data['uri'] = "ssh://localhost" + file2

            with open(file1, 'w') as _:
                pass
            with open(file2, 'w') as _:
                pass

            eosfiles = {}
            res = ready2run(msg1data, eosfiles, self.scene_id1, self.options)

            assert (res is False)

            res = ready2run(msg2data, eosfiles, self.scene_id1, self.options)
            assert res


class TestPrepareSeaDASCalls(unittest.TestCase):
    """Testing the preparation of SeaDAS command line calls."""

    def setUp(self):
        self.options = {'modis_geo_script': '/san1/opt/SeaDAS/8.1/ocssw/bin/modis_GEO',
                        'modis_geo_options_terra': ['--verbose',
                                                    '--disable-download'],
                        'modis_geo_options_aqua': ['--verbose',
                                                   '--enable-dem',
                                                   '--disable-download']}
        self.mod01_file = "/path/to/eos/level1/files/my_mod01_filename"
        self.mod03_file = "my_mod03_filename.hdf"

    def test_get_geo_command_line_list_terra(self):
        """Test getting the command line options used when launching the Terra/modis Geo location processing."""

        res = get_geo_command_line_list(self.options, self.mod01_file, self.mod03_file)

        expected = ['/san1/opt/SeaDAS/8.1/ocssw/bin/modis_GEO',
                    '--verbose',
                    '--disable-download',
                    '-omy_mod03_filename.hdf',
                    '/path/to/eos/level1/files/my_mod01_filename']

        self.assertListEqual(res, expected)

    def test_get_geo_command_line_list_aqua(self):
        """Test getting the command line options used when launching the Aqua/modis Geo location processing."""

        att_filepath = '/path/to/eos/level1/files/P15409571540958154095921295233937001.att'
        eph_filepath = '/path/to/eos/level1/files/P15409571540958154095921295233937001.eph'
        res = get_geo_command_line_list(self.options, self.mod01_file, self.mod03_file,
                                        attitude=att_filepath,
                                        ephemeris=eph_filepath)

        expected = ['/san1/opt/SeaDAS/8.1/ocssw/bin/modis_GEO',
                    '--verbose',
                    '--enable-dem',
                    '--disable-download',
                    '--att1=/path/to/eos/level1/files/P15409571540958154095921295233937001.att',
                    '--eph1=/path/to/eos/level1/files/P15409571540958154095921295233937001.eph',
                    '-omy_mod03_filename.hdf', '/path/to/eos/level1/files/my_mod01_filename']

        self.assertListEqual(res, expected)

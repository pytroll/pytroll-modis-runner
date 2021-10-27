#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Pytroll

# Author(s):

#   Adam Dybbroe <Firstname.Lastname@smhi.se>

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

from glob import glob
from datetime import datetime, timedelta
import os
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from socket import timeout
import logging
from six.moves.urllib.parse import urlparse

LOG = logging.getLogger(__name__)

NAVIGATION_HELPER_FILES = ['utcpole.dat', 'leapsec.dat']

PACKETFILE_AQUA_PRFX = "P154095715409581540959"
MODISFILE_AQUA_PRFX = "P1540064AAAAAAAAAAAAAA"
MODISFILE_TERRA_PRFX = "P0420064AAAAAAAAAAAAAA"


def check_working_dir(working_dir):
    """Check if working_dir exists and try create if not. Return dir or a tmp-dir"""
    if not os.path.exists(working_dir):
        try:
            os.makedirs(working_dir)
        except OSError:
            LOG.error("Failed creating working directory %s", working_dir)
            LOG.info("Will use /tmp")
            return '/tmp'

    return working_dir


def clean_utcpole_and_leapsec_files(etc_dir, thr_days=60):
    """Clean any old *leapsec.dat* and *utcpole.dat* backup files, older than
    *thr_days* old

    """
    now = datetime.utcnow()
    deltat = timedelta(days=int(thr_days))

    # Make the list of files to clean:
    flist = glob(os.path.join(etc_dir, '*.dat_*'))
    for filename in flist:
        lastpart = os.path.basename(filename).split('dat_')[1]
        tobj = datetime.strptime(lastpart, "%Y%m%d%H%M")
        if (now - tobj) > deltat:
            LOG.info("File too old, cleaning: %s", filename)
            os.remove(filename)

    return


def check_utcpole_and_leapsec_files(leapsec_dir, thr_days=14):
    """Check if the files *leapsec.dat* and *utcpole.dat* are available in the
    etc directory and check if they are fresh.
    Return True if fresh/new files exists, otherwise False

    """
    now = datetime.utcnow()
    tdelta = timedelta(days=int(thr_days))

    files_ok = True
    for bname in NAVIGATION_HELPER_FILES:
        LOG.info("File " + str(bname) + "...")
        filename = os.path.join(leapsec_dir, bname)
        if os.path.exists(filename):
            # Check how old it is:
            realpath = os.path.realpath(filename)
            # Get the timestamp in the file name:
            try:
                tstamp = os.path.basename(realpath).split('.dat_')[1]
            except IndexError:
                files_ok = False
                break
            tobj = datetime.strptime(tstamp, "%Y%m%d%H%M")

            if (now - tobj) > tdelta:
                LOG.info("File too old! File=%s " % filename)
                files_ok = False
                break
        else:
            LOG.info("No navigation helper file: %s" % filename)
            files_ok = False
            break

    return files_ok


def update_utcpole_and_leapsec_files(options):
    """
    Function to update the ancillary data files *leapsec.dat* and
    *utcpole.dat* used in the navigation of MODIS direct readout data.

    These files need to be updated at least once every 2nd week, in order to
    achieve the best possible navigation.

    """

    # Start cleaning any possible old files:
    leapsec_dir = options['leapsec_dir']
    clean_utcpole_and_leapsec_files(leapsec_dir,
                                    options.get('days_keep_old_etc_files', 60))
    url_modis_navigation = options['url_modis_navigation']
    try:
        usock = urlopen(url_modis_navigation, timeout=10)
    except URLError:
        LOG.warning('Failed opening url: %s', str(url_modis_navigation))
        return
    except timeout:
        LOG.error('socket timed out - URL %s', url_modis_navigation)
        return
    else:
        usock.close()

    LOG.info("Start downloading....")
    now = datetime.utcnow()
    timestamp = now.strftime('%Y%m%d%H%M')
    for filename in NAVIGATION_HELPER_FILES:
        try:
            usock = urlopen(url_modis_navigation + filename, timeout=10)
        except HTTPError:
            LOG.warning("Failed opening file " + filename)
            continue
        except timeout:
            LOG.error('socket timed out - URL %s', url_modis_navigation)
            continue

        data = usock.read()
        usock.close()
        LOG.info("Data retrieved from url...")

        # I store the files with a timestamp attached, in order not to remove
        # the existing files. In case something gets wrong in the download, we
        # can handle this by not changing the sym-links below:
        newname = filename + '_' + timestamp
        outfile = os.path.join(leapsec_dir, newname)
        linkfile = os.path.join(leapsec_dir, filename)
        with open(outfile, 'wb') as fd_:
            fd_.write(data)

        LOG.info("Data written to file " + outfile)
        # Here we could make a check on the sanity of the downloaded files:
        # TODO!

        # Update the symlinks (assuming the files are okay):
        LOG.debug("Adding symlink %s -> %s", linkfile, outfile)
        if os.path.islink(linkfile) or os.path.isfile(linkfile):
            LOG.debug("Unlinking %s", linkfile)
            os.unlink(linkfile)

        try:
            os.symlink(outfile, linkfile)
        except OSError as err:
            LOG.warning(str(err))

    return


def scene_processed_recently(job_register, sceneid):
    """Check if scene has been processed recently."""

    LOG.debug("Scene identifier = " + str(sceneid))
    LOG.debug("Job register = " + str(job_register))
    if sceneid in job_register and job_register[sceneid]:
        LOG.debug("Processing of scene %s has already been launched...", str(sceneid))
        return True

    return False


def ready2run(message_data, eosfiles, sceneid, options):
    """Check if we have got all the input lvl0 files and that we are
    ready to process MODIS lvl1 data.

    """
    if sceneid not in eosfiles:
        eosfiles[sceneid] = {}

    urlobj = urlparse(message_data['uri'])
    sensor = message_data['sensor']
    if not isinstance(sensor, list):
        sensor = [sensor]

    if (message_data['platform_name'] == "EOS-Terra" and message_data['sensor'] == 'modis'):
        path, fname = os.path.split(urlobj.path)
        LOG.debug("path " + str(path) + " filename = " + str(fname))

        modisfile_terra_prfx = options.get('modisfile_terra_prfx', MODISFILE_TERRA_PRFX)
        modisfile_terra_postfx = options.get('modisfile_terra_postfx', '001.PDS')

        LOG.debug("fname {}".format(fname))
        LOG.debug("modisfile_terra_prfx {}".format(modisfile_terra_prfx))
        LOG.debug("modisfile_terra_postfx {}".format(modisfile_terra_postfx))
        if fname.startswith(modisfile_terra_prfx) and fname.endswith(modisfile_terra_postfx):

            # Check if the file exists:
            if not os.path.exists(urlobj.path):
                LOG.warning("File is reported to be dispatched " +
                            "but is not there! File = " + urlobj.path)
                return False

            eosfiles[sceneid]['modisfile'] = urlobj.path
            eosfiles[sceneid]['packetfile'] = ''

    elif (message_data['platform_name'] == "EOS-Aqua" and
          all([s in ['modis', 'gbad'] for s in sensor])):

        path, fname = os.path.split(urlobj.path)
        LOG.debug("path " + str(path) + " filename = " + str(fname))
        modisfile_aqua_prfx = options.get('modisfile_aqua_prfx', MODISFILE_AQUA_PRFX)

        packetfile_aqua_prfx = options.get('packetfile_aqua_prfx', PACKETFILE_AQUA_PRFX)
        modisfile_aqua_postfx = options.get('modisfile_aqua_postfx', '001.PDS')
        packetfile_aqua_postfx = options.get('packetfile_aqua_postfx', '001.PDS')

        LOG.debug("modis prfx {} modis postfx {} packet prfx {} packet postfx {}".format(modisfile_aqua_prfx,
                                                                                         modisfile_aqua_postfx,
                                                                                         packetfile_aqua_prfx,
                                                                                         packetfile_aqua_postfx))
        if ((fname.find(modisfile_aqua_prfx) == 0 or
             fname.find(packetfile_aqua_prfx) == 0) and (fname.endswith(modisfile_aqua_postfx) or
                                                         fname.endswith(packetfile_aqua_postfx))):

            # Check if the file exists:
            if not os.path.exists(urlobj.path):
                LOG.warning("File is reported to be dispatched " +
                            "but is not there! File = " + urlobj.path)
                return False

            LOG.debug("find prfx: {}".format(fname.find(modisfile_aqua_prfx)))
            LOG.debug("find postfx: {}".format(fname.endswith(modisfile_aqua_postfx)))
            if ((fname.find(modisfile_aqua_prfx) == 0) and fname.endswith(modisfile_aqua_postfx)):
                eosfiles[sceneid]['modisfile'] = urlobj.path
            else:
                LOG.debug("Not modisfile %s", str(urlobj.path))
            if ((fname.find(packetfile_aqua_prfx) == 0) and fname.endswith(packetfile_aqua_postfx)):
                eosfiles[sceneid]['packetfile'] = urlobj.path
            else:
                LOG.debug("Not packetfile %s", str(urlobj.path))

    if 'modisfile' in eosfiles[sceneid] and 'packetfile' in eosfiles[sceneid]:
        LOG.info("Files ready for MODIS level-1 runner: " +
                 str(eosfiles[sceneid]))
        return True
    else:
        return False


def get_geo_command_line_list(options, mod01_file, mod03_file, **kwargs):
    """Get command line for the SeaDAS Geolocation script."""

    modis_geo_script = options['modis_geo_script']
    cmdl = [modis_geo_script, ]
    if 'attitude' in kwargs and 'ephemeris' in kwargs:
        cmd_opts = options.get('modis_geo_options_aqua')
    else:
        cmd_opts = options.get('modis_geo_options_terra')

    if cmd_opts:
        cmdl = cmdl + cmd_opts

    if 'attitude' in kwargs and 'ephemeris' in kwargs:
        aqua_opts = ["--att1=%s" % kwargs['attitude'],
                     "--eph1=%s" % kwargs['ephemeris']]
        cmdl = cmdl + aqua_opts + ["-o%s" % (os.path.basename(mod03_file)),
                                   mod01_file]
    else:
        cmdl = cmdl + ["-o%s" % (os.path.basename(mod03_file)), mod01_file]

    return cmdl

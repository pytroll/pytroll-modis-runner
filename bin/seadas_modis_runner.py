#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016 - 2021 PyTroll

# Author(s):

#   Adam Dybbroe <Firstname.Lastname@smhi.se>
#   Trygve Aspenes, MET Norway

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
"""Runner for MODIS level-1 processing using SeaDAS. Needs the gbad software
from DRL as well, in order to generate attitude and ephemeris data for Aqua.

"""

import logging
import os
import shutil
import argparse
from datetime import datetime
from posttroll.message import Message
from multiprocessing import Pool, Manager
import threading
from queue import Empty
from trollsift import Parser, compose
from subprocess import Popen, PIPE

from modis_runner.utils import update_utcpole_and_leapsec_files
from modis_runner.utils import check_working_dir
from modis_runner.utils import check_utcpole_and_leapsec_files
from modis_runner.utils import ready2run
from modis_runner.utils import scene_processed_recently
from modis_runner.utils import get_geo_command_line_list

from modis_runner.publish_and_listen import MODISFileListener, MODISFilePublisher

from modis_runner.config import get_config
from modis_runner.logger import setup_logging

LOG = logging.getLogger(__name__)

EOS_SATELLITES = ['EOS-Terra', 'EOS-Aqua']
TERRA = 'EOS-Terra'
AQUA = 'EOS-Aqua'
MISSIONS = {'T': 'terra', 'A': 'aqua'}

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

DESTRIPE_HOME = os.environ.get('MODIS_DESTRIPING_HOME', '')
SPA_HOME = os.environ.get("SPA_HOME", '')


def reset_job_registry(objdict, eosfiles, key):
    """Remove job key from registry"""
    LOG.debug("Release/reset job-key %s from job registry", str(key))
    if key in objdict:
        objdict.pop(key)
    else:
        LOG.warning("Nothing to reset/release - Job registry didn't contain any entry matching: %s", str(key))

    LOG.debug("Release/reset key %s from eosfiles registry", str(key))
    if key in eosfiles:
        eosfiles.pop(key)
    else:
        LOG.warning("Nothing to reset/release - EOS-files registry didn't contain any entry matching: %s", str(key))
    return


def modis_live_runner(options):
    """Listens and triggers processing"""

    LOG.info("*** Start the runner for the MODIS level-1 processing")
    LOG.debug("os.environ = " + str(os.environ))

    # Start checking and dowloading the luts (utcpole.dat and
    # leapsec.dat):
    LOG.info("Checking the modis luts and updating from internet if necessary!")

    fresh = check_utcpole_and_leapsec_files(options.get('leapsec_dir'),
                                            options.get('days_between_url_download', 14))
    if fresh:
        LOG.info("Files in etc dir are fresh! No url downloading....")
    else:
        LOG.warning("Files in etc are non existent or too old. " +
                    "Start url fetch...")
        update_utcpole_and_leapsec_files(options)

    pool = Pool(processes=6, maxtasksperchild=1)
    manager = Manager()
    listener_q = manager.Queue()
    publisher_q = manager.Queue()

    pub_thread = MODISFilePublisher(publisher_q, options.get('publish_topic'))
    pub_thread.start()

    subscribe_topics = options.get('subscribe_topics')
    eos_satellites = EOS_SATELLITES
    listen_thread = MODISFileListener(listener_q, subscribe_topics, eos_satellites)
    listen_thread.start()

    eos_files = {}
    jobs_dict = {}
    while True:
        try:
            msg = listener_q.get()
        except Empty:
            LOG.debug("Empty listener queue...")
            continue

        LOG.debug("Number of threads currently alive: %s", str(threading.active_count()))
        LOG.info("EOS files: %s", str(eos_files))
        LOG.debug("\tMessage:")
        LOG.debug(msg)

        start_time = None
        if 'start_time' in msg.data:
            start_time = msg.data['start_time']
        else:
            LOG.warning("start_time not in message!")

        end_time = None
        if 'end_time' in msg.data:
            end_time = msg.data['end_time']
        else:
            LOG.warning("No end_time in message!")

        platform_name = msg.data['platform_name']
        orbit_number = int(msg.data['orbit_number'])
        sensor = msg.data.get('sensor', None)

        keyname = (str(platform_name) + '_' + str(orbit_number) + '_' + str(
            start_time.strftime('%Y%m%d%H%M')))
        # Check if we have all the files before processing can start:

        if scene_processed_recently(jobs_dict, keyname):
            continue

        status = ready2run(msg.data, eos_files, keyname, options)
        if status:
            # Register scene:
            jobs_dict[keyname] = datetime.utcnow()

            # Run
            LOG.info("Ready to run...")
            LOG.debug("Modisfile = %s", eos_files[keyname]['modisfile'])
            LOG.debug("Packetfile = %s", eos_files[keyname]['packetfile'])

            scene = {
                'platform_name': platform_name,
                'orbit_number': orbit_number,
                'starttime': start_time,
                'endtime': end_time,
                'sensor': sensor,
                'modisfilename': eos_files[keyname]['modisfile'],
                'packetfilename': eos_files[keyname]['packetfile']
            }

            if platform_name in [TERRA, AQUA]:
                # Do processing:
                LOG.info("Level-0 to lvl1 processing on Terra/Aqua MODIS: Start... Start time = %s",
                         str(start_time))
                pool.apply_async(run_terra_aqua_l0l1,
                                 (options, scene, msg, jobs_dict[keyname], publisher_q))
                LOG.debug("Terra/Aqua lvl1 processing sent to pool worker...")
            else:
                LOG.debug("Platform %s not supported yet...",
                          str(platform_name))

            # Block any future run on this scene for x minutes from now
            # x = 5 minutes
            thread_job_registry = threading.Timer(
                5 * 60.0,
                reset_job_registry,
                args=(jobs_dict, eos_files, keyname))
            thread_job_registry.start()

        LOG.debug("Eos-file registry: %s", str(eos_files))

    pool.close()
    pool.join()

    pub_thread.stop()
    listen_thread.stop()


def create_message(mda, filename, level):
    LOG.debug("mda: = " + str(mda))
    LOG.debug("type(mda): " + str(type(mda)))
    to_send = mda.copy()
    if isinstance(filename, (list, tuple, set)):
        del to_send['uri']
        del to_send['uid']
        to_send['dataset'] = [{
            'uri': 'file://' + fname,
            'uid': os.path.basename(fname)
        } for fname in filename]
        mtype = 'dataset'
    else:
        to_send['uri'] = ('file://' + filename)
        to_send['uid'] = os.path.basename(filename)
        mtype = 'file'
    to_send['format'] = 'EOS'
    to_send['data_processing_level'] = level
    to_send['type'] = 'HDF4'
    to_send['sensor'] = 'modis'

    station = OPTIONS.get('station', 'unknown')
    message = Message(
        '/'.join(('', str(to_send['format']),
                  str(to_send['data_processing_level']), station, 'polar'
                  'direct_readout')), mtype, to_send).encode()

    return message


def run_aqua_gbad(obs_time, end_time=None, orbit_number=None, process_time=None, uid=None, ftype=None):
    """Run the gbad for aqua"""

    working_dir = check_working_dir(OPTIONS['working_dir'])

    level0_home = OPTIONS['level0_home']
    if (end_time and orbit_number):
        _data = {}
        _data['start_time'] = obs_time
        _data['end_time'] = end_time
        _data['orbit_number'] = orbit_number
        _data['process_time'] = process_time
        _data['uid'] = uid
        _data['type'] = ftype
        packetfile = os.path.join(level0_home, compose(OPTIONS['packetfile_aqua'], _data))
    else:
        packetfile = os.path.join(level0_home,
                                  obs_time.strftime(OPTIONS['packetfile_aqua']))

    att_dir = OPTIONS['attitude_home']
    eph_dir = OPTIONS['ephemeris_home']
    spa_config_file = os.path.join(SPA_HOME, "smhi_configfile")
    att_file = os.path.basename(packetfile).split('.PDS')[0] + '.att'
    att_file = os.path.join(att_dir, att_file)
    eph_file = os.path.basename(packetfile).split('.PDS')[0] + '.eph'
    eph_file = os.path.join(eph_dir, eph_file)
    LOG.info("eph-file = " + eph_file)

    wrapper_home = SPA_HOME + "/wrapper/gbad"

    cmdl = ["%s/run" % wrapper_home, "aqua.gbad.pds",
            packetfile, "aqua.gbad_att", att_file,
            "aqua.gbad_eph", eph_file
            # "configurationfile", spa_config_file
            ]
    if os.path.exists(spa_config_file):
        cmdl.append("configurationfile")
        cmdl.append(spa_config_file)
    else:
        LOG.warning("SPA config file: {} does not exist. Skip this."
                    "If this is not what you want, fix your config".format(spa_config_file))

    LOG.info("Command: " + str(cmdl))
    # Run the command:
    modislvl1b_proc = Popen(
        cmdl, shell=False, cwd=working_dir, stderr=PIPE, stdout=PIPE)

    while True:
        line = modislvl1b_proc.stdout.readline()
        if not line:
            break
        LOG.info(line.rstrip())

    while True:
        errline = modislvl1b_proc.stderr.readline()
        if not errline:
            break
        LOG.info(errline.rstrip())

    modislvl1b_proc.poll()
    modislvl1b_status = modislvl1b_proc.returncode
    LOG.debug("Return code from modis lvl1b proc = " + str(modislvl1b_status))
    if modislvl1b_status != 0:
        LOG.error("Failed in the Aqua gbad processing!")
        return None, None

    return att_file, eph_file


def run_terra_aqua_l0l1(options, scene, message, job_id, publish_q):
    """Process Terra/Aqua MODIS level 0 PDS data to level 1a/1b"""

    # from subprocess import Popen, PIPE
    from glob import glob

    try:

        LOG.debug("Inside run_terra_aqua_l0l1...")

        working_dir = check_working_dir(options['working_dir'])
        LOG.debug("Working dir = %s", str(working_dir))

        if scene['platform_name'] == TERRA:
            mission = 'T'
        else:
            mission = 'A'

        startnudge = int(options.get('startnudge', 5))
        endnudge = int(options.get('endnudge', 5))

        modis_destripe = options.get('modis_destripe_exe')
        terra_modis_destripe_coeff = options.get('terra_modis_destripe_coeff')
        aqua_modis_destripe_coeff = options.get('aqua_modis_destripe_coeff')
        if options.get('apply_destriping'):
            destripe_on = True
        else:
            destripe_on = False

        level1b_home = options['level1b_home']
        LOG.debug("level1b_home = %s", level1b_home)
        filetype_terra = options['filetype_terra']
        LOG.debug("filetype_terra = %s", options['filetype_terra'])
        geofiles = {}
        geofiles[mission] = options['geofile_%s' % MISSIONS[mission]]
        level1a_terra = options['level1a_terra']
        level1b_terra = options['level1b_terra']
        level1b_250m_terra = options['level1b_250m_terra']
        level1b_500m_terra = options['level1b_500m_terra']

        filetype_aqua = options['filetype_aqua']
        LOG.debug("filetype_aqua = %s", str(filetype_aqua))
        level1a_aqua = options['level1a_aqua']
        level1b_aqua = options['level1b_aqua']
        level1b_250m_aqua = options['level1b_250m_aqua']
        level1b_500m_aqua = options['level1b_500m_aqua']

        # Get the observation time from the filename as a datetime object:
        LOG.debug("modis filename = %s", scene['modisfilename'])
        bname = os.path.basename(scene['modisfilename'])
        process_time = None
        uid = None
        ftype = None
        if mission == 'T':
            p__ = Parser(filetype_terra)
            res = p__.parse(bname)
            obstime = res['start_time']  # datetime.strptime(bname, filetype_terra)
            end_time = None
            orbit_number = None
        else:
            p__ = Parser(filetype_aqua)
            res = p__.parse(bname)
            obstime = res.get('start_time')
            end_time = res.get('end_time')
            orbit_number = res.get('orbit_number')
            process_time = res.get('process_time')
            uid = res.get('uid')
            ftype = res.get('type')
        LOG.debug("bname = %s obstime = %s", str(bname), str(obstime))

        # level1_home
        proctime = datetime.now()
        lastpart = proctime.strftime("%Y%j%H%M%S.hdf")
        if mission == 'T':
            firstpart = obstime.strftime(level1b_terra)
        else:
            firstpart = obstime.strftime(level1b_aqua)
        mod021km_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)

        if mission == 'T':
            firstpart = obstime.strftime(level1b_250m_terra)
        else:
            firstpart = obstime.strftime(level1b_250m_aqua)
        mod02qkm_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)

        if mission == 'T':
            firstpart = obstime.strftime(level1b_500m_terra)
        else:
            firstpart = obstime.strftime(level1b_500m_aqua)
        mod02hkm_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)

        lastpart = proctime.strftime("%Y%j%H%M%S.hdf")
        if mission == 'T':
            firstpart = obstime.strftime(level1a_terra)
        else:
            firstpart = obstime.strftime(level1a_aqua)
        mod01_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)

        firstpart = obstime.strftime(geofiles[mission])
        mod03_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)

        retv = {
            'mod021km_file': mod021km_file,
            'mod02hkm_file': mod02hkm_file,
            'mod02qkm_file': mod02qkm_file,
            'level1a_file': mod01_file,
            'geo_file': mod03_file
        }

        LOG.debug("Do a file globbing to check for existing level-1b files:")
        mod01files = glob("%s/%s*hdf" % (level1b_home, firstpart))
        if len(mod01files) > 0:
            LOG.warning("Level 1 file for this scene already exists: %s",
                        mod01files[0])

        LOG.info("Level-1 filename: " + str(mod01_file))

        modis_l1a_script = options['modis_l1a_script']
        cmdl = [modis_l1a_script,
                "--verbose",
                "--mission=%s" % mission,
                "--startnudge=%d" % startnudge,
                "--stopnudge=%d" % endnudge,
                "-o%s" % (os.path.basename(mod01_file)),
                scene['modisfilename']]

        LOG.debug("Run command: " + str(cmdl))
        my_env = os.environ.copy()
        modislvl1b_proc = Popen(cmdl, shell=False,
                                cwd=working_dir,
                                stderr=PIPE, stdout=PIPE, env=my_env)

        while True:
            line = modislvl1b_proc.stdout.readline()
            if not line:
                break
            LOG.info(line.rstrip())

        while True:
            errline = modislvl1b_proc.stderr.readline()
            if not errline:
                break
            LOG.info(errline.rstrip())

        modislvl1b_proc.poll()
        modislvl1b_status = modislvl1b_proc.returncode
        LOG.debug(
            "Return code from modis lvl-1a processing = " + str(modislvl1b_status))
        if modislvl1b_status != 0 and modislvl1b_status is not None:
            LOG.error("Failed in the Terra/Aqua MODIS level-1 processing!")
            return None

        fname_orig = os.path.join(working_dir, os.path.basename(mod01_file))
        if os.path.exists(fname_orig):
            shutil.move(fname_orig, mod01_file)

            l1a_file = retv['level1a_file']
            pubmsg = create_message(message.data, l1a_file, "1A")
            LOG.info("Sending: %s", pubmsg)
            publish_q.put(pubmsg)
        else:
            LOG.warning("Missing level-1a file! %s", fname_orig)

        if mission == 'A':
            # Get ephemeris and attitude names
            attitude, ephemeris = run_aqua_gbad(obstime, end_time, orbit_number,
                                                process_time=process_time, uid=uid, ftype=ftype)
            if not attitude or not ephemeris:
                LOG.error(
                    "Failed producing the attitude and/or the ephemeris file(s)"
                )
                return None

        # Next run the geolocation and the level-1b file:
        # Mission T: modis_GEO.py --verbose --enable-dem --entrained
        # --disable-download $level1a_file
        # Mission A: modis_GEO.py --verbose --enable-dem
        # --disable-download -a aqua.att -e aqua.eph $level1a_file
        if mission == 'T':
            cmdl = get_geo_command_line_list(options, mod01_file, mod03_file)
        else:
            cmdl = get_geo_command_line_list(options, mod01_file, mod03_file,
                                             attitude=attitude, ephemeris=ephemeris)

        LOG.debug("Run command: %s", str(cmdl))
        modislvl1b_proc = Popen(
            cmdl, shell=False, cwd=working_dir, stderr=PIPE, stdout=PIPE)

        while True:
            line = modislvl1b_proc.stdout.readline()
            if not line:
                break
            LOG.info(line.rstrip())

        while True:
            errline = modislvl1b_proc.stderr.readline()
            if not errline:
                break
            LOG.info(errline.rstrip())

        modislvl1b_proc.poll()
        modislvl1b_status = modislvl1b_proc.returncode
        LOG.debug("Return code from modis geo-loc processing = " +
                  str(modislvl1b_status))
        # Apparently a return code of 1 and None is okay...
        # Verify which return codes are ok! FIXME!
        if modislvl1b_status not in [0, 1, None]:
            LOG.error("Failed in the Terra/Aqua MODIS level-1 processing!")
            return None

        l1b_files = []
        fname_orig = os.path.join(working_dir,
                                  os.path.basename(retv['geo_file']))
        fname_dest = retv['geo_file']
        if os.path.exists(fname_orig):
            shutil.move(fname_orig, fname_dest)
            l1b_files.append(fname_dest)
        else:
            LOG.warning("Missing file: %s", fname_orig)

        # modis_L1B.py --verbose $level1a_file $geo_file
        modis_l1b_script = options['modis_l1b_script']
        cmdl = [modis_l1b_script,
                "--okm=%s" % os.path.basename(mod021km_file),
                "--hkm=%s" % os.path.basename(mod02hkm_file),
                "--qkm=%s" % os.path.basename(mod02qkm_file), mod01_file,
                mod03_file
                ]

        LOG.debug("Run command: " + str(cmdl))
        modislvl1b_proc = Popen(
            cmdl, shell=False, cwd=working_dir, stderr=PIPE, stdout=PIPE)

        while True:
            line = modislvl1b_proc.stdout.readline()
            if not line:
                break
            LOG.info(line.rstrip())

        while True:
            errline = modislvl1b_proc.stderr.readline()
            if not errline:
                break
            LOG.info(errline.rstrip())

        modislvl1b_proc.poll()
        modislvl1b_status = modislvl1b_proc.returncode

        LOG.debug(
            "Return code from modis lvl1b processing = " + str(modislvl1b_status))
        if modislvl1b_status != 0 and modislvl1b_status is not None:
            LOG.error("Failed in the Terra level-1 processing!")
            return None

        if destripe_on:
            LOG.info("Apply destriping...")
            # Perform the modis destriping:
            # MOD_PRDS_DB.exe in_hdf in_coeff
            cmdl = [
                os.path.join(DESTRIPE_HOME, 'bin/%s' % modis_destripe),
                os.path.basename(mod021km_file)
            ]
            if mission == 'T':
                cmdl.append(
                    os.path.join(DESTRIPE_HOME,
                                 'coeff/%s' % terra_modis_destripe_coeff))
            else:
                cmdl.append(
                    os.path.join(DESTRIPE_HOME,
                                 'coeff/%s' % aqua_modis_destripe_coeff))

            LOG.debug("Run command: %s", str(cmdl))
            modislvl1b_proc = Popen(
                cmdl, shell=False, cwd=working_dir, stderr=PIPE, stdout=PIPE)

            while True:
                line = modislvl1b_proc.stdout.readline()
                if not line:
                    break
                LOG.info(line.rstrip())

            while True:
                errline = modislvl1b_proc.stderr.readline()
                if not errline:
                    break
                LOG.info(errline.rstrip())

            modislvl1b_proc.poll()
            modislvl1b_status = modislvl1b_proc.returncode

            LOG.debug("Return code from modis destriping = " +
                      str(modislvl1b_status))
            if modislvl1b_status != 0:
                LOG.error(
                    "Failed in the Terra level-1 (destriping) processing!")
                return None
        else:
            LOG.info("Destriping will not be applied!")

        # for key in ['mod021km_file',
        #            'mod02hkm_file',
        #            'mod02qkm_file']:
        # metno-adaptions
        for key in [
                'geo_file', 'mod021km_file', 'mod02hkm_file', 'mod02qkm_file'
        ]:

            fname_orig = os.path.join(working_dir, os.path.basename(retv[key]))
            fname_dest = retv[key]
            if os.path.exists(fname_orig):
                shutil.move(fname_orig, fname_dest)
                l1b_files.append(fname_dest)
            else:
                LOG.warning("Missing file: %s", fname_orig)

        pubmsg = create_message(message.data, l1b_files, '1B')
        LOG.info("Sending: %s", pubmsg)
        publish_q.put(pubmsg)

        if isinstance(job_id, datetime):
            dt_ = datetime.utcnow() - job_id
            LOG.info("Terra MODIS level-1b scene " + str(job_id) +
                     " finished. It took: " + str(dt_))
        else:
            LOG.warning("Job entry is not a datetime instance: " + str(job_id))

        # Start checking and dowloading the luts (utcpole.dat and
        # leapsec.dat):
        LOG.info("Checking the modis luts and updating from internet if necessary!")
        fresh = check_utcpole_and_leapsec_files(options.get('leapsec_dir'),
                                                options.get('days_between_url_download', 14))
        if fresh:
            LOG.info("Files in etc dir are fresh! No url downloading....")
        else:
            LOG.warning("Files in etc are non existent or too old. Start url fetch...")
            update_utcpole_and_leapsec_files(options)

    except:
        LOG.exception('Failed in run_terra_aqua_l0l1...')
        raise

    LOG.debug("Leaving run_terra_aqua_l0l1")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log-config",
                        help="Log config file to use instead of the standard logging.")
    parser.add_argument("-c", "--config",
                        help="YAML config file to use.",
                        required=True)
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")

    cmd_args = parser.parse_args()
    setup_logging(cmd_args)

    OPTIONS = get_config(cmd_args.config)
    modis_live_runner(OPTIONS)

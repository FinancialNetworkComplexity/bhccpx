#!/usr/bin/env python3

# -----------------------------------------------------------------------------
# This file is part of the BHC Complexity Toolkit.
#
# The BHC Complexity Toolkit is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# The BHC Complexity Toolkit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with the BHC Complexity Toolkit.  If not, 
# see <https://www.gnu.org/licenses/>.
# -----------------------------------------------------------------------------
# Copyright 2019, Mark D. Flood
#
# Author: Mark D. Flood
# Last revision: 16-Jul-2019
# -----------------------------------------------------------------------------

import os
import logging
import time
import wget
from configparser import ConfigParser

import progressbar as pb

# The config object defines key user-controlled variables
# If running from the command line, these choices may be overridden below
import bhc_datautil as UTIL

CONFIG = UTIL.read_config()
logger = logging.getLogger(__file__.split(os.path.sep)[-1].split('.')[0])


# This is not used anywhere, and is essentially just zip2xml::unzip_nic_file but with the wget call
# TODO: remove or refactor
def download_data(config: ConfigParser, zipfilename):
    wget.download(url, out=output_directory)
    zipfilepath = os.path.join(config['zip2xml']['indir'], zipfilename)
    logger.debug('ZIP file found:    '+zipfilepath+
              ' ('+str(os.path.getsize(zipfilepath)) +' bytes)',
              extra={'src':'unzip_nic_file.zip_file'})
    zipf = zipfile.ZipFile(zipfilepath, 'r')
    xmlfilename = zipf.namelist()[0]
    zipf.extract(xmlfilename, config['zip2xml']['outdir'])
    zipf.close()
    logger.debug('XML file extracted: %s in directory: %s', xmlfilename, config.get('zip2xml','outdir'))



def make_dirs(config):
    # NIC dir
    today = int(time.strftime("%Y%m%d"))
    qtrend = UTIL.rcnt_qtrend(today)
    nic_subdir = UTIL.stringify_qtrend(qtrend)
    nic_dir = os.path.join(config.get('www2dat', 'nic_dir'), nic_subdir)
    os.makedirs(nic_dir, exist_ok=True)
    logger.info('NIC dir: %s', nic_dir)

    # FDIC CB dir
    fdic_cb_dir = config.get('www2dat','fdic_cb_dir')
    os.makedirs(fdic_cb_dir, exist_ok=True)
    logger.info('FDIC CB dir: %s', fdic_cb_dir)

    # FDIC Fail dir
    fdic_fail_dir = config.get('www2dat','fdic_fail_dir')
    os.makedirs(fdic_fail_dir, exist_ok=True)
    logger.info('FDIC Fail dir: %s', fdic_fail_dir)
            
    
def main(argv=None):
    config = UTIL.parse_command_line(argv, CONFIG, __file__)
    UTIL.print_config(config, __file__)
    make_dirs(config)
    
if __name__ == "__main__":
    main()
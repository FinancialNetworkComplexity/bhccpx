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
import zipfile
import progressbar as pb
from configparser import ConfigParser

# The config object defines key user-controlled variables
# If running from the command line, these choices may be overridden below
import bhc_datautil as UTIL
CONFIG = UTIL.read_config()

logger = logging.getLogger(__file__.split(os.path.sep)[-1].split('.')[0])


# Unzip a specific file
def unzip_nic_file(config: ConfigParser, zipfilename: str):
    zipfilepath = os.path.join(config.get('zip2xml', 'indir'), zipfilename)
    logger.debug('ZIP file found: %s (%s bytes)', zipfilepath, str(os.path.getsize(zipfilepath)))
    zipf = zipfile.ZipFile(zipfilepath, 'r')
    xmlfilename = zipf.namelist()[0]
    zipf.extract(xmlfilename, config.get('zip2xml', 'outdir'))
    zipf.close()
    logger.debug('XML file extracted: %s in directory: %s', xmlfilename, config.get('zip2xml', 'outdir'))


# This critical function unzips all five *.zip files, as
# indentified in the configuration object
def unzip_nic(config: ConfigParser):
    UTIL.print_config(config, __file__)
    zipfiles = [
        config.get('zip2xml', 'attributesactive'),
        config.get('zip2xml', 'attributesbranch'),
        config.get('zip2xml', 'attributesclosed'),
        config.get('zip2xml', 'relationships'),
        config.get('zip2xml', 'transformations'),
    ]
    if (logger.getEffectiveLevel()<logging.WARNING):
        for z in pb.progressbar(zipfiles, redirect_stdout=True):
            unzip_nic_file(config, z)
    else:
        for z in zipfiles:
            unzip_nic_file(config, z)
    logger.warning('**** Processing complete ****')
            
    
def main(argv=None):
    config = UTIL.parse_command_line(argv, CONFIG, __file__)
    unzip_nic(config)
    
if __name__ == "__main__":
    main()
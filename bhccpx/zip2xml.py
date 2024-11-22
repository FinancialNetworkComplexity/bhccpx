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

# The config object defines key user-controlled variables
# If running from the command line, these choices may be overridden below
import bhc_datautil as UTIL

CONFIG = UTIL.read_config()
LOG = logging.getLogger(__file__.split(os.path.sep)[-1].split('.')[0])


# Unzip a specific file
def unzip_nic_file(config, zipfilename):
    zipfilepath = os.path.join(config['zip2xml']['indir'], zipfilename)
    LOG.debug('ZIP file found:    '+zipfilepath+
              ' ('+str(os.path.getsize(zipfilepath)) +' bytes)',
              extra={'src':'unzip_nic_file.zip_file'})
    zipf = zipfile.ZipFile(zipfilepath, 'r')
    xmlfilename = zipf.namelist()[0]
    zipf.extract(xmlfilename, config['zip2xml']['outdir'])
    zipf.close()
    LOG.debug('XML file extracted: '+xmlfilename+' in directory: '+
              CONFIG['zip2xml']['outdir'],
              extra={'src':'unzip_nic_file.xml_file'})


# This critical function unzips all five *.zip files, as
# indentified in the configuration object
def unzip_nic(config):
    UTIL.print_config(config, __file__)
    zipfiles = [
        config['zip2xml']['attributesactive'],
        config['zip2xml']['attributesbranch'],
        config['zip2xml']['attributesclosed'],
        config['zip2xml']['relationships'],
        config['zip2xml']['transformations'] ]
    if (LOG.getEffectiveLevel()<logging.WARNING):
        for z in pb.progressbar(zipfiles, redirect_stdout=True):
            unzip_nic_file(config, z)
    else:
        for z in zipfiles:
            unzip_nic_file(config, z)
    LOG.warning('**** Processing complete ****', 
                extra={'src':'unzip_nic.proc_complete'})
            
    
# The main function controls execution when running from the command line
def main(argv=None):
    config = UTIL.parse_command_line(argv, CONFIG, __file__)
    unzip_nic(config)
    
# This tests whether the module is being run from the command line
if __name__ == "__main__":
    main()
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
# Last revision: 22-Jun-2019
# -----------------------------------------------------------------------------

import sys
import os
import re
import logging
from tqdm.auto import tqdm
from configparser import ConfigParser
from io import TextIOWrapper
import ast
import bhc_datautil


def write_head(config: ConfigParser, outfile: TextIOWrapper, template):
    """ Writes the header row in the CSV output file."""
    if config.get('xml2csv', 'delim') == '<TAB>':
        delim = '\t'  
    else:
        delim = config.get('xml2csv', 'delim')
    outfile.write(delim.join(template) + '\n')
    

def write_elem(config: ConfigParser, elem, outfile: TextIOWrapper, template, logger=logging):
    """Writes an individual observation (elem) row in the CSV output file."""
    if config.get('xml2csv', 'delim') == '<TAB>':
        delim = '\t' 
    else:
        delim = config.get('xml2csv', 'delim')
    
    keyvals = {key:None for key in template}

    # Carve off the first XML tag as opentag, keeping the rest as elem
    opentag, elem = elem.split('>',1)

    # Now extract attributes in opentag as key-value pairs:  '<KEY>value'
    opentag = opentag.replace('<','')
    open_pairs = opentag.split(' ')
    open_pairs.pop(0)
    for i, p in enumerate(open_pairs):
        open_pairs[i] = '<'+p.replace('="', '>').replace('"','')

    # Replace element end tags with the CSV delimiter
    elem_mod = re.sub('</[A-Z_0-9]+>', delim, elem)
    # Strip off meaningless trailing blanks to avoid parsing errors
    elem_mod = elem_mod.rstrip()

    # Split elem into a list of key-value pairs, each of the form: '<KEY>value'
    keyval_pairs = open_pairs + elem_mod.split(delim) 

    # Populate the keyvals dict with values parsed from elem
    for kv in keyval_pairs:
        try:
            key_val = kv.split('>',1)
            key = key_val[0].replace('<','')
            val = key_val[1]
            keyvals[key] = val
        except Exception:
            logger.error('Cannot parse key-value pair: %s, %s', kv, elem)
    outfile.write(delim.join(['' if keyvals[k] is None else keyvals[k] for k in template]) + '\n')


def get_template(config: ConfigParser, xmlfilepath: str) -> tuple[str, str]:
    """Inspect the XML file to find the element type; return it and obtain the template"""
    with open(xmlfilepath, 'r') as infile:
        chunk = infile.read(1024).upper()
    if chunk.find('<ATTRIBUTES') >= 0:
        elemtype = 'ATTRIBUTES'
        template = ast.literal_eval(config.get('xml2csv', 'attributestemplate'))
    elif chunk.find('<RELATIONSHIP') >= 0:
        elemtype = 'RELATIONSHIP'
        template = ast.literal_eval(config.get('xml2csv', 'relationshipstemplate'))
    elif chunk.find('<TRANSFORMATION') >= 0:
        elemtype = 'TRANSFORMATION'
        template = ast.literal_eval(config.get('xml2csv', 'transformationstemplate'))
    else:
        raise Exception('XML data in file %s not recognized as any of: ATTRIBUTES, RELATIONSHIP, TRANSFORMATION', xmlfilepath)
    return elemtype, template


def clean_and_write_elem(
    config: ConfigParser, elem: str, template: list[str],
    outfile: TextIOWrapper, logger=logging
):
    """Cleans the XML element and writes it to the CSV output file."""
    elem = elem.replace('&AMP;', '&')
    elem = elem.replace('&LT;', '<')
    elem = elem.replace('&GT;', '>')
    write_elem(config, elem, outfile, template, logger)


def parse_nic_file(config: ConfigParser, xmlfilename: str, logger=logging):
    xmlfilepath = os.path.join(config.get('xml2csv', 'indir'), xmlfilename)
    chunksize = config.getint('xml2csv', 'chunksize')
    elemtype, template = get_template(config, xmlfilepath)
    logger.info('NIC element type for XML file %s: %s', xmlfilename, elemtype)

    # Read from XML and write to CSV
    needs_csv_head, infileEOF = True, False
    chunk = ''
    start_tag, end_tag = f'<{elemtype}', f'</{elemtype}>'
    chunksize_processed = 0
    xmlfilesize = os.path.getsize(xmlfilepath)
    sys.stdout.flush()
    pbar = tqdm(total=xmlfilesize, desc=xmlfilename)

    # Open files to read XML and write CSV
    outfilename = os.path.splitext(xmlfilename)[0] + config.get('xml2csv', 'outfileext')
    outfilepath = os.path.join(config.get('xml2csv', 'outdir'), outfilename)
    with open(xmlfilepath, 'r') as infile:
        with open(outfilepath, 'w', 1) as outfile:
            while not infileEOF:
                chunk = chunk + infile.read(chunksize)
                delta = min(chunksize, xmlfilesize-chunksize_processed)
                pbar.update(delta)
                chunksize_processed += delta

                if needs_csv_head:
                    # first row not done yet, need to skip frontmatter
                    start_idx = chunk.upper().find(start_tag)
                    chunk = chunk[start_idx:len(chunk)]
                
                # This next block means we have found the end point (end_tag) of
                # the XML element (elem), and can proceed to parse and write it out
                while chunk.upper().find(end_tag) >= 0:
                    endelem = chunk.upper().find(end_tag) + len(end_tag)
                    elem = chunk[0:endelem].upper()
                    chunk = chunk[endelem:len(chunk)]
                    if needs_csv_head:
                        write_head(config, outfile, template)
                        needs_csv_head = False
                    clean_and_write_elem(config, elem, template, outfile, logger)
                
                # When we find the XML end tag (</DATA>), shut things down
                if chunk.upper().find('</DATA>') >= 0:
                    infileEOF = True
    
    pbar.close()
    if not infileEOF:
        logger.error('Did not find </DATA> end tag in XML file: %s', xmlfilepath)


def parse_nic(config, xmlfiles: list | None = None, logger=logging):
    """Parses NIC XML files. If xmlfiles is provided, parse those; otherwise use config defaults."""
    if not xmlfiles:
        xmlfiles = [
            config.get('xml2csv', 'attributesactive'),
            config.get('xml2csv', 'attributesbranch'),
            config.get('xml2csv', 'attributesclosed'),
            config.get('xml2csv', 'relationships'),
            config.get('xml2csv', 'transformations')
        ]
    for xfile in xmlfiles:
        parse_nic_file(config, xfile, logger)
        logger.info('xml2csv conversion for file %s complete', xfile)


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description='Convert NIC XML files to CSV')
    parser.add_argument('xmlfiles', nargs='*', help='List of XML files to convert (uses config defaults if none provided)')
    args, remaining = parser.parse_known_args(argv)
    
    config = bhc_datautil.read_config()
    config = bhc_datautil.parse_command_line(remaining, config, __file__)
    logger = logging.getLogger("xml2csv")
    
    parse_nic(config, args.xmlfiles if args.xmlfiles else None, logger)


if __name__ == "__main__":
    main()

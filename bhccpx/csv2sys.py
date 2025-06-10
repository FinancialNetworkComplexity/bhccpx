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

#import pandas as pd
import networkx as nx
import pickle as pkl
import multiprocessing as mp
import progressbar as pb

import bhca
import bhc_datautil
from configparser import ConfigParser

import logging


def clear_cache(cachedir, YQ0, YQ1):
    """
    Deletes any files in the cache corresponding to the range of dates implied by YQ0 and YQ1
    """
    asof_list = bhc_datautil.assemble_asofs(YQ0, YQ1)
    for asofdate in asof_list:
        sysfilename = 'NIC_'+'_'+str(asofdate)+'.pkl'
        sysfilepath = os.path.join(cachedir, sysfilename)
        if os.path.isfile(sysfilepath):
            os.remove(sysfilepath)
            

def find_highholder(config, BankSys, rssd, logger=logging):
    """Finds an entity's high-holder within a banking system 
    
    Examines a directed graph representing the full banking system to find 
    the high-holder for a given starting node (identified by rssd).
    Returns the first node found to have zero parents, among all the ancestors 
    of rssd. Typically, this parent-less ancestor node should be
    unique among all ancestors of rssd (including rssd itself).
    
    It is theoretically possible for a directed graph to have no parent-less
    nodes (a cycle, for example), in which case None is returned, but this 
    should never occur in practice for BHCs. 
    
    On the other hand, it is also possible for a node to have multiple 
    high-holders, for example, if there is a joint venture that bridges 
    two (or more) BHCs. This situation does occasionally occur in practice.
    
    Parameters
    ----------
    BankSys : networkx.DiGraph
        A directed graph representing ownerships in a banking system
    rssd : int
        Entity whose high holder we seek within the banking system
     
    Returns
    -------
    int
        Identifier of the high-holder entity (or first in the list, if 
        multiple high holders are found)
    """
    HH_list = bhca.find_highholders(BankSys, rssd)
    if HH_list[0] is None:
        logger.warning('Entity not in the banking system: %s', str(rssd))
    elif len(HH_list) > 1:
        logger.warning('Multiple high-holders: %s %s', str(rssd), str(HH_list))
    elif len(HH_list) < 1:
        logger.warning('No high-holders: %s', str(rssd))
    return HH_list[0]

           
def make_banksys(config: ConfigParser, asofdate, logger=logging):
    """
    A function to read or create a NetworkX graph of the full banking system
    on a given date. The function looks for an existing graph in a pickle file
    located at sysfilepath (for example, .../cachedir/NIC__YYYYMMDD.pkl),
    where YYYYMMDD is the asofdate. 

    If this file exists, it is unpackeded from the pickle and returned.
    If the file does not (yet) exist, the NetworkX DiGraph is instead created
    from the relationships data and dumped into a new pickle at sysfilepath.

    The graph is a naked directed graph whose nodes are NIC entities and 
    whose edges point from parent nodes to offspring nodes. 
    The function then returns this digraph (either newly created or unpickled). 
    """
    sysfilename = 'NIC_'+'_'+str(asofdate)+'.pkl'
    sysfilepath = os.path.join(config.get('csv2sys', 'outdir'), sysfilename)
    relfilename = config.get('csv2sys', 'relationships')
    csvfilepath = os.path.join(config.get('csv2sys', 'indir'), relfilename)

    BankSys = None
    if os.path.isfile(sysfilepath):
        logger.debug('FOUND: Banking system file path: %s', sysfilepath)
        with open(sysfilepath, 'rb') as f:
            BankSys: nx.DiGraph = pkl.load(f)
    else:
        logger.debug('CREATING: Banking system file path: %s for %s', sysfilepath, asofdate)
        BankSys = nx.DiGraph()
        csvfilepath = os.path.join(config.get('csv2sys', 'indir'), config.get('csv2sys', 'relationships'))
        logger.debug('CSV file path: %s %s', csvfilepath, asofdate)

        RELdf = bhc_datautil.RELcsv2df(csvfilepath, asofdate)
        (ID_RSSD_PARENT, ID_RSSD_OFFSPRING, DT_START, DT_END) = bhc_datautil.REL_IDcols(RELdf)
        for row in RELdf.iterrows():
            date0 = int(row[0][DT_START])
            date1 = int(row[0][DT_END])
            rssd_par = row[0][ID_RSSD_PARENT]
            rssd_off = row[0][ID_RSSD_OFFSPRING]
            if (asofdate < date0 or asofdate > date1):
                logger.info('ASOFDATE, %s out of bounds: %s %s %s %s', asofdate, rssd_par, rssd_off, date0, date1)
                continue   
            BankSys.add_edge(rssd_par, rssd_off)
        
        # Adding in the singleton institutions (no edges in Relationships file)
        logger.debug(
            'System (pre)  as of %s has %s nodes and %s edges',
            str(asofdate), BankSys.number_of_nodes(), BankSys.number_of_edges())
        indir = config.get('csv2sys', 'indir')
        fA = config.get('csv2sys', 'attributesactive')
        fB = config.get('csv2sys', 'attributesbranch')
        fC = config.get('csv2sys', 'attributesclosed')
        ATTdf = bhc_datautil.makeATTs(indir, fA, fB, fC, asofdate, filter_asof=True)
        ATTdf = ATTdf[ATTdf.DT_END >= asofdate]
        ATTdf = ATTdf[ATTdf.DT_OPEN <= asofdate]
        nodes_BankSys = set(BankSys.nodes)
        nodes_ATTdf = set(ATTdf['ID_RSSD'].unique())
        nodes_new = nodes_ATTdf.difference(nodes_BankSys)
        BankSys.add_nodes_from(nodes_new)

        # Storing the new banking system file
        os.makedirs(os.path.dirname(sysfilepath), exist_ok=True)
        with open(sysfilepath, 'wb') as f:
            pkl.dump(BankSys, f)
        logger.debug(
            'System (post) as of %s has %s nodes and %s edges; %s added, out of %s candidates in %s',
            str(asofdate), BankSys.number_of_nodes(), BankSys.number_of_edges(), len(nodes_new), len(nodes_ATTdf), type(ATTdf))
    return BankSys
    

def build_sys(config: ConfigParser, logger=logging):
    """Builds a representation of the full system"""
    if config.getboolean('csv2sys', 'clearcache'):
        # Remove existing banking system pkl files and recreate
        logger.info('Clearing output cache of *.pkl files in the range: %s %s', config.get('csv2sys', 'asofdate0'), config.get('csv2sys', 'asofdate1'))
        clear_cache(config.get('csv2sys', 'outdir'), config.get('csv2sys', 'asofdate0'), config.get('csv2sys', 'asofdate1'))
    
    asof_list = bhc_datautil.assemble_asofs(config.get('csv2sys', 'asofdate0'), config.get('csv2sys', 'asofdate1'))
    logger.debug('List of as-of dates to process: %s Cores: %s %s', asof_list, config.getint('csv2sys', 'parallel'), config.get('csv2sys', 'outdir'))
    if config.getint('csv2sys', 'parallel') > 0:
        logger.info(
            'Beginning parallel processing (%s tasks across %s cores) for each as-of date (process messages may be trapped by parallel threads)',
            str(len(asof_list)), config.getint('csv2sys', 'parallel'))
        
        pcount = min(config.getint('csv2sys', 'parallel'), os.cpu_count(), len(asof_list))
        pool = mp.Pool(processes=pcount)
        for asof in asof_list:
            pool.apply_async(make_banksys, (config, asof, logger))
        pool.close()
        pool.join()

        logger.info('Parallel processing complete')
    else:
        logger.info('Beginning sequential processing for each as-of date')
        for asof in pb.ProgressBar()(asof_list):
             make_banksys(config, asof, logger)
        logger.info('Sequential processing complete')


def main(argv=None):
    config = bhc_datautil.read_config()
    config = bhc_datautil.parse_command_line(argv, config, __file__)
    logger = logging.getLogger("csv2sys")
    build_sys(config, logger)
    
if __name__ == "__main__":
    main()
    

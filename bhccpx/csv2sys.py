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
from bhc_datautil import AsOfDate
from configparser import ConfigParser

import logging


def clear_cache(cachedir: str, YQ0: str, YQ1: str):
    """
    This function removes cached pickle files from the specified cache directory
    that correspond to dates within the range defined by YQ0 and YQ1. The files
    follow the naming convention 'NIC__YYYYMMDD.pkl'.

    :param cachedir: Path to the directory containing cached files
    :type cachedir: str
    :param YQ0: Start date for the range of files to clear
    :type YQ0: str or date-like
    :param YQ1: End date for the range of files to clear
    :type YQ1: str or date-like
    """
    asof_list = bhc_datautil.AsOfDate.make_range_from_YQ_strs(YQ0, YQ1)
    for asofdate in asof_list:
        sysfilename = f"NIC__{asofdate}.pkl"
        sysfilepath = os.path.join(cachedir, sysfilename)
        if os.path.isfile(sysfilepath):
            os.remove(sysfilepath)
            

def find_highholders(
    config: ConfigParser, BankSys: nx.DiGraph,
    rssd: int | None, hc_types: list[str] | None = None,
    logger=logging
) -> list[int]:
    """
    Finds an entity's high-holder within a banking system.
    
    Examines a directed graph representing the full banking system to find 
    the high-holder for a given starting node (identified by rssd).
    Returns the first node found to have zero parents, among all the ancestors 
    of rssd. Typically, this parent-less ancestor node should be
    unique among all ancestors of rssd (including rssd itself).
    
    It is theoretically possible for a directed graph to have no parent-less
    nodes (a cycle, for example), but this should never occur in practice for BHCs.
    On the other hand, it is also possible for a node to have multiple 
    high-holders, for example, if there is a joint venture that bridges 
    two (or more) BHCs. This situation does occasionally occur in practice.
    
    :param BankSys: A directed graph representing ownerships in a banking system
    :type BankSys: networkx.DiGraph
    :param rssd: Entity whose high holder we seek within the banking system
    :type rssd: int
    :returns: Identifier of the high-holder entity (or first in the list, if 
              multiple high holders are found)
    :rtype: int

    .. Examples::

    The examples work with a simple banking system containing two BHCs, 
    each organized as a simple DAG tree containing seven nodes: 
        
      * BHC tree rooted at node 0 and containing nodes 0-6
      * BHC tree rooted at node 7 and containing nodes 7-13
     
        >>> import bhc_testutil as TEST
        >>> BankSys = TEST.BHC_systemDAG()

    Find the high-holder for a node in the first tree
    
        >>> find_highholders(BankSys, 3)
        [0]

    Find the high-holder for a node in the second tree
    
        >>> find_highholders(BankSys, 13)
        [7]
    """
    if rssd is None:
        raw_HHs = [node for node, indeg in BankSys.in_degree() if indeg == 0]
    else:
        if rssd in BankSys:
            raw_HHs: list[int] = [
                node for node in nx.ancestors(BankSys, rssd) | {rssd}
                if len(list(BankSys.predecessors(node))) == 0
            ]
        else:
            logger.warning('Cannot find %s in BankSys', str(rssd))
            return []
    
    if hc_types is None:
        HHs = raw_HHs
        if HHs[0] is None:
            logger.warning('Entity not in the banking system: %s', str(rssd))
        elif len(HHs) > 1:
            logger.warning('Multiple high-holders: %s %s', str(rssd), str(HHs))
        elif len(HHs) < 1:
            logger.warning('No high-holders: %s', str(rssd))
    else:
        HHs = [node for node in raw_HHs if BankSys.nodes[node].get('entity_type') in hc_types]
    return HHs


def make_banksys(config: ConfigParser, asofdate: AsOfDate, logger=None):
    """
    Read or create a NetworkX graph of a full banking system on a given date.
    If a pickle file for this banking system exists, it will load and return it.
    If not, it will use the relationship data to create a naked directed graph whose
    nodes are NIC entities and whose edges point from parents to offspring, based on the
    relationship data available. This directed graph will be saved for future use.

    :param config: Configuration parser containing file paths and settings
    :type config: ConfigParser
    :param asofdate: Date for which to build/load the banking system
    :type asofdate: AsOfDate
    :param logger: Logger instance for debugging and info messages
    :type logger: logging.Logger, optional
    :returns: Directed graph representing the banking system hierarchy
    :rtype: networkx.DiGraph
    :raises: FileNotFoundError if required CSV files are not found
    :raises: KeyError if required configuration keys are missing

    .. note::
        The function creates pickle files in the format 'NIC__YYYYMMDD.pkl' for caching.
    """
    if logger is None:
        logger = logging.getLogger("csv2sys")

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

        RELdf = bhc_datautil.RELcsv2df(csvfilepath)
        ID_RSSD_PARENT, ID_RSSD_OFFSPRING, DT_START, DT_END = bhc_datautil.REL_IDcols(RELdf)
        for row in RELdf.iterrows():
            date0 = AsOfDate.from_int(row[0][DT_START])
            date1 = AsOfDate.from_int(row[0][DT_END])
            rssd_par = row[0][ID_RSSD_PARENT]
            rssd_off = row[0][ID_RSSD_OFFSPRING]
            if asofdate < date0 or asofdate > date1:
                # logger.info('ASOFDATE, %s out of bounds: %s %s %s %s', asofdate, rssd_par, rssd_off, date0, date1)
                continue
            BankSys.add_edge(rssd_par, rssd_off)
        
        # Adding in the singleton institutions (no edges in Relationships file)
        logger.debug(
            'System (pre) asof %s has %s nodes and %s edges',
            asofdate, BankSys.number_of_nodes(), BankSys.number_of_edges())
        indir = config.get('csv2sys', 'indir')
        fA = config.get('csv2sys', 'attributesactive')
        fB = config.get('csv2sys', 'attributesbranch')
        fC = config.get('csv2sys', 'attributesclosed')
        ATTdf = bhc_datautil.makeATTs(indir, fA, fB, fC, filter_asofdate=asofdate)
        ATTdf = ATTdf[ATTdf.DT_END >= int(asofdate)]
        ATTdf = ATTdf[ATTdf.DT_OPEN <= int(asofdate)]
        nodes_BankSys = set(BankSys.nodes)
        nodes_ATTdf = set(ATTdf['ID_RSSD'].unique())
        nodes_new = nodes_ATTdf.difference(nodes_BankSys)
        BankSys.add_nodes_from(nodes_new)

        # Storing the new banking system file
        os.makedirs(os.path.dirname(sysfilepath), exist_ok=True)
        with open(sysfilepath, 'wb') as f:
            pkl.dump(BankSys, f)
        logger.debug(
            'System (post) asof %s has %s nodes and %s edges; %s added, out of %s candidates in %s',
            asofdate, BankSys.number_of_nodes(), BankSys.number_of_edges(), len(nodes_new), len(nodes_ATTdf), type(ATTdf))
    return BankSys


def make_banksys_logged(config: ConfigParser, asofdate: AsOfDate):
    import logging.config
    logging.config.fileConfig(config, disable_existing_loggers=False)
    logger = logging.getLogger("csv2sys")
    return make_banksys(config, asofdate, logger)
    

def build_sys(config: ConfigParser, logger=logging):
    """
    Builds a representation of the full banking system for specified dates.
    Processing can be done either sequentially or in parallel.
    
    :param config: Configuration parser containing processing parameters. The 'csv2sys' section should contain:
                   
                   - clearcache (bool): Whether to clear existing *.pkl cache files
                   - asofdate0 (str): Start date for processing range
                   - asofdate1 (str): End date for processing range  
                   - outdir (str): Output directory path for generated files
                   - parallel (int): Number of parallel processes (0 for sequential)
    :type config: ConfigParser
    :param logger: Logger instance for recording processing information and debug messages
    :type logger: logging.Logger, optional
    
    .. note::
        - Sequential processing shows a progress bar for visual feedback
        - Parallel process count is limited by CPU cores and number of dates to process
        - Cache clearing removes NIC__YYYYMMDD.pkl files within the specified date range
    """
    if config.getboolean('csv2sys', 'clearcache'):
        # Remove existing banking system pkl files and recreate
        logger.info('Clearing output cache of NIC__YYYYMMDD.pkl files in the range: %s %s', config.get('csv2sys', 'asofdate0'), config.get('csv2sys', 'asofdate1'))
        clear_cache(config.get('csv2sys', 'outdir'), config.get('csv2sys', 'asofdate0'), config.get('csv2sys', 'asofdate1'))
    
    asof_list = bhc_datautil.AsOfDate.make_range_from_YQ_strs(config.get('csv2sys', 'asofdate0'), config.get('csv2sys', 'asofdate1'))
    logger.debug('List of as-of dates to process: %s Cores: %s %s', asof_list, config.getint('csv2sys', 'parallel'), config.get('csv2sys', 'outdir'))
    if config.getint('csv2sys', 'parallel') > 0:
        logger.info(
            'Beginning parallel processing (%s tasks across %s cores) for each as-of date (process messages may be trapped by parallel threads)',
            str(len(asof_list)), config.getint('csv2sys', 'parallel'))
        
        pcount = min(config.getint('csv2sys', 'parallel'), os.cpu_count(), len(asof_list))
        pool = mp.Pool(processes=pcount)
        for asof in asof_list:
            pool.apply_async(make_banksys_logged, (config, asof))
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
    

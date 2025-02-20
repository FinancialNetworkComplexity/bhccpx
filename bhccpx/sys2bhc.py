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

import os

import networkx as nx
import numpy as np
import multiprocessing as mp
import progressbar as pb
from configparser import ConfigParser
import pickle as pkl
import ast

import bhc_datautil as UTIL
CONFIG = UTIL.read_config()
import csv2sys

import logging
logger = logging.getLogger(__file__.split(os.path.sep)[-1].split('.')[0])


def add_attributes(config: ConfigParser, DATA, BHC):
    """
    A function to decorate a BHC graph with certain important attibutes.
    - DATA is a list of key information resources, as assembled by makeDATA()
    - BHC is a naked (no attributes) NetworkX DiGraph object for the firm

    For each node in the BHC, a number of important attributes are looked up 
    from the attributes dataframe (in the DATA object), and attached to the node. 
    The decorated BHC graph is returned.
    """
    ATTdf = DATA[UTIL.IDX_Attributes]
    for node in BHC.nodes():
        node_id = np.int32(node)
        try:
            ent = ATTdf.loc[node_id]
            node_dict = {
                'nicsource': ent['NICsource'],
                'entity_type': ent['ENTITY_TYPE'],
                'GEO_JURISD': ent['CNTRY_NM'].strip() +' - '+ ent['STATE_ABBR_NM'],
            }
            # Now add the extra params requested in the config file
            extras: list[str] = ast.literal_eval(config.get('sys2bhc', 'extraattributes'))
            for x in extras:
                node_dict[x.lower()] = ent[x]
        except:
            node_dict = {
                'nicsource': 'XXX',
                'entity_type': 'XXX',
                'GEO_JURISD': 'XXX'
            }
        nx.set_node_attributes(BHC, {node: node_dict})
    return BHC


def remove_branches(config: ConfigParser, DATA, BHC):
    """
    Copies BHC to a new DiGraph object that is identical to BHC, except
    that it lacks any branches or subsidiaries of branches
    """
    removal_set = set()
    ATTdf = DATA[UTIL.IDX_Attributes]
    # Create a copy of the BHC that is editable
    BHC2 = nx.DiGraph()
    BHC2.add_nodes_from(BHC)
    BHC2.add_edges_from(BHC.edges)
    for node in BHC.nodes():
        node_id = np.int32(node)
        nicsource = 'XXX'
        # Copy any of node's existing attributes from BHC to BHC2
        nx.set_node_attributes(BHC2, {node: BHC.nodes()[node]})
        try:
            ent = ATTdf.loc[node_id]
            nicsource = ent['NICsource']
        except:
            pass
        if ('B'==nicsource):
            branchdescendants = nx.algorithms.dag.descendants(BHC,node)
            removal_set.add(node)
            for b in branchdescendants:
                removal_set.add(b)
    for n in removal_set:
        BHC2.remove_node(n)
    return BHC2


def extractBHC(config: ConfigParser, asofdate, rssd, DATA=None):
    """
    A function to extract a single BHC graph from a full banking system 
    graph, starting with the BHC's high-holder RSSD ID    
    - config is a configuration module, containing pointers to key files, etc.
    - asofdate is the point in (historical) time of the desired BHC snapshot
    - rssd is the RSSD ID of a particular BHC high-holder to extract

    The function also decorates the BHC graph with a number of important 
    attributes, by calling the add_attibutes().
    """
    BHC = None
    if config.get('sys2bhc', 'indir') != config.get('csv2sys', 'indir'):
        logger.warning('csv2sys.indir: %s differs from sys2bhc_indir: %s', config.get('csv2sys', 'indir'), config.get('sys2bhc', 'indir'))
    if config.get('sys2bhc', 'outdir') != config.get('csv2sys', 'outdir'):
        logger.warning('csv2sys.outdir: %s differs from sys2bhc_outdir: %s', config.get('csv2sys', 'outdir'), config.get('sys2bhc', 'outdir'))
    BankSys = csv2sys.make_banksys(config, asofdate)

    if DATA is None:
        logger.debug('Fetching DATA (not provided)')
        DATA = UTIL.fetch_DATA(
            outdir=config.get('sys2bhc', 'outdir'),
            asofdate=asofdate,
            indir=config.get('sys2bhc', 'indir'),
            fA=config.get('sys2bhc', 'attributesactive'),
            fB=config.get('sys2bhc', 'attributesbranch'),
            fC=config.get('sys2bhc', 'attributesclosed'),
            fREL=config.get('sys2bhc', 'relationships')
        )

    HHs = DATA[UTIL.IDX_HighHolder]
    if rssd in HHs:
        BHC = populate_bhc(config, BankSys, DATA, rssd)
        logger.debug('BHC: %s %s %s %s', rssd, type(BHC), BHC.number_of_nodes(), BHC.number_of_edges())
        bhcfilename = 'NIC_'+str(rssd)+'_'+str(asofdate)+'.pik'
        bhcfilepath = os.path.join(config.get('sys2bhc', 'outdir'), bhcfilename)
        with open(bhcfilepath, 'wb') as f:
            pkl.dump(BHC, f)
        logger.debug(
            'As of %s, BHC %s has %s nodes and %s edges: %s',
            asofdate, rssd, BHC.number_of_nodes(), BHC.number_of_edges(), BHC.nodes(data=True)[rssd]['nm_lgl']
        )
    else:
        logger.warning('RSSD missing from high-holder list: %s as of %s', rssd, asofdate)
    return BHC


def populate_bhc(config: ConfigParser, BankSys, DATA, rssd) -> nx.DiGraph:
    usebranches = config.getboolean('sys2bhc', 'usebranches')
    bhc_entities = nx.algorithms.dag.descendants(BankSys, rssd)
    bhc_entities.add(rssd)    # Include HH in the BHC too
    BHC = BankSys.subgraph(bhc_entities)
    BHC = add_attributes(config, DATA, BHC)
    if (not(usebranches)):
        BHC = remove_branches(config, DATA, BHC)
    BHC = BHC.to_directed()
    return BHC


def clear_cache(cachedir: str, asof_list):
    """Deletes any DATA_* files in the cache corresponding to the dates in asof_list"""
    for asofdate in asof_list:
        sysfilename = 'DATA_'+str(asofdate)+'.pik'
        sysfilepath = os.path.join(cachedir, sysfilename)
        if os.path.isfile(sysfilepath):
            os.remove(sysfilepath)


def extract_bhcs_ondate(config: ConfigParser, asofdate):
    """
    Loop over all RSSD IDs in the bhclist (in config), loading or creating 
    a cached pik file for each on the asofdate. 
    Returns a list containing those BHCs. 
    """
    BHCs = []
    rssd_lst: list[int] = ast.literal_eval(config.get('sys2bhc', 'bhclist'))
    for rssd in rssd_lst:
        BHC = extractBHC(config, asofdate, rssd)
        BHCs.append(BHC)
    return BHCs

def make_bhcs(config: ConfigParser):
    """
    Loop over all dates in the asoflist (in config). For each date, extract
    all the BHCs in the bhclist.
    """
    asof_list = []
    for YQ in ast.literal_eval(config.get('sys2bhc', 'asoflist')):
        asof_list.append(UTIL.make_asof(YQ)[0])
    if config.getboolean('sys2bhc', 'clearcache'):
        clear_cache(config.get('sys2bhc', 'outdir'), asof_list)
    if config.getint('sys2bhc', 'parallel') > 0:
        logger.info(
            'Beginning parallel processing (%s tasks across %s cores) for each as-of date (process messages may be trapped by parallel threads)',
            str(len(asof_list)), config.getint('sys2bhc', 'parallel'))
        pcount = min(config.getint('sys2bhc', 'parallel'), os.cpu_count(), len(asof_list))
        pool = mp.Pool(pcount)
        for asofdate in asof_list:
            pool.apply_async(extract_bhcs_ondate, (config, asofdate))
        pool.close()
        pool.join()
        logger.info('Parallel processing complete')
    else:
        logger.info('Beginning sequential processing for each asofdate')
        for asof in pb.progressbar(asof_list, redirect_stdout=True):
            extract_bhcs_ondate(config, asof)
        logger.info('Sequential processing complete')


def main(argv=None):
    config = UTIL.parse_command_line(argv, CONFIG, __file__)
    # UTIL.print_config(config, __file__)
    make_bhcs(config)
    
if __name__ == "__main__":
    main()

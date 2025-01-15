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

import bhc_datautil as UTIL
from bhc_datautil import Verbosity
# CONFIG = UTIL.read_bhc_config()
CONFIG = UTIL.read_config()

import csv2sys

# A function to decorate a BHC graph with certain important attibutes.
#  -- DATA is a list of key information resources, as assembled by makeDATA()
#  -- BHC is a naked (no attributes) NetworkX DiGraph object for the firm
# For each node in the BHC, a number of important attributes are looked up 
# from the attributes dataframe (in the DATA object), and attached to the node. 
# The decorated BHC graph is returned.
def add_attributes(config, DATA, BHC):
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
            extras = eval(config['sys2bhc']['extraattributes'])
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


# Copies BHC to a new DiGraph object that is identical to BHC, except
# that it lacks any branches or subsidiaries of branches
def remove_branches(config, DATA, BHC):
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


# A function to extract a single BHC graph from a full banking system 
# graph, starting with the BHC's high-holder RSSD ID    
#  -- config is a configuration module, containing pointers to key files, etc.
#  -- asofdate is the point in (historical) time of the desired BHC snapshot
#  -- rssd is the RSSD ID of a particular BHC high-holder to extract
# The function also decorates the BHC graph with a number of important 
# attributes, by calling the add_attibutes().
def extractBHC(config, asofdate, rssd, DATA=None):
    # (verbose, veryverbose) = UTIL.verbosity(config)
    verbosity = UTIL.verbosity(config)
    BHC = None
    if (config['sys2bhc']['indir'] != config['csv2sys']['indir']):
        print('WARNING: csv2sys.indir:', config['csv2sys']['indir'], 'differs from sys2bhc_indir:', config['sys2bhc']['indir'])
    if (config['sys2bhc']['outdir'] != config['csv2sys']['outdir']):
        print('WARNING: csv2sys.outdir:', config['csv2sys']['outdir'], 'differs from sys2bhc_outdir:', config['sys2bhc']['outdir'])
    BankSys = csv2sys.make_banksys(config, asofdate)
    if (None==DATA):
        if (verbosity == Verbosity.VERYVERBOSE): print('Fetching DATA (not provided)')
        outdir = config['sys2bhc']['outdir']
        indir = config['sys2bhc']['indir']
        fA = config['sys2bhc']['attributesactive']
        fB = config['sys2bhc']['attributesbranch']
        fC = config['sys2bhc']['attributesclosed']
        fREL = config['sys2bhc']['relationships']
        DATA = UTIL.fetch_DATA(outdir, asofdate, indir, fA, fB, fC, fREL)
#        datafilename = 'DATA_'+str(asofdate)+'.pik'
#        datafilepath = os.path.join(config['sys2bhc']['outdir'], datafilename)
#        if (os.path.isfile(datafilepath)):
#            if (veryverbose): print('Reading DATA from cache:', datafilepath)
#            f = open(datafilepath, 'rb')
#            DATA = pik.load(f)
#            f.close()
#        else:
#            if (veryverbose): print('Creating DATA for cache:', datafilepath)
#            fA = config['sys2bhc']['attributesactive']
#            fB = config['sys2bhc']['attributesbranch']
#            fC = config['sys2bhc']['attributesclosed']
#            fREL = config['sys2bhc']['relationships']
#            DATA = UTIL.makeDATA(config['sys2bhc']['indir'], fA, fB, fC, fREL, asofdate)
#            f = open(datafilepath, 'wb')
#            pik.dump(DATA, f)
#            f.close()
    HHs = DATA[UTIL.IDX_HighHolder]
    if (rssd in HHs):
        BHC = populate_bhc(config, BankSys, DATA, rssd)
        if (verbosity == Verbosity.VERYVERBOSE):
            print('BHC:', rssd, type(BHC), BHC.number_of_nodes(), BHC.number_of_edges())
        bhcfilename = 'NIC_'+str(rssd)+'_'+str(asofdate)+'.pik'
        bhcfilepath = os.path.join(config['sys2bhc']['outdir'], bhcfilename)
        nx.write_gpickle(BHC, bhcfilepath)
        if (verbosity == Verbosity.VERYVERBOSE):
            print('As of', asofdate, 'BHC', rssd, 'has', BHC.number_of_nodes(), 'nodes and', BHC.number_of_edges(), 'edges:', BHC.nodes(data=True)[rssd]['nm_lgl'])
    else:
        if (verbosity >= Verbosity.VERBOSE):
            print('WARNING: RSSD missing from high-holder list:', rssd, 'as of', asofdate)
    return BHC


def populate_bhc(config, BankSys, DATA, rssd):
    usebranches = ('TRUE'==config['sys2bhc']['usebranches'].upper())
    bhc_entities = nx.algorithms.dag.descendants(BankSys, rssd)
    bhc_entities.add(rssd)    # Include HH in the BHC too
    BHC = BankSys.subgraph(bhc_entities)
    BHC = add_attributes(config, DATA, BHC)
    if (not(usebranches)):
        BHC = remove_branches (config, DATA, BHC)
    BHC = BHC.to_directed()
    return BHC


# Deletes any DATA_* files in the cache corresponding to the dates in asof_list
def clear_cache(cachedir, asof_list):
    for asofdate in asof_list:
        sysfilename = 'DATA_'+str(asofdate)+'.pik'
        sysfilepath = os.path.join(cachedir, sysfilename)
        if os.path.isfile(sysfilepath):
            os.remove(sysfilepath)


# Loop over all RSSD IDs in the bhclist (in config), loading or creating 
# a cached pik file for each on the asofdate. 
# Returns a list containing those BHCs. 
def extract_bhcs_ondate(config, asofdate):
    BHCs = []
    for rssd in eval(config['sys2bhc']['bhclist']):
        BHC = extractBHC(config, asofdate, rssd)
        BHCs.append(BHC)
    return BHCs

# Loop over all dates in the asoflist (in config). For each date, extract
# all the BHCs in the bhclist.
def make_bhcs(config):
    # (verbose, veryverbose) = UTIL.verbosity(config)
    verbosity = UTIL.verbosity(config)
    if (verbosity == Verbosity.VERYVERBOSE): UTIL.print_config(config, __file__)
    asof_list = []    
    for YQ in eval(config['sys2bhc']['asoflist']):
        asof_list.append(UTIL.make_asof(YQ)[0])
    if ("TRUE"==config['sys2bhc']['clearcache'].upper()):
        clear_cache(config['sys2bhc']['outdir'], asof_list)
    if (int(config['sys2bhc']['parallel']) > 0):
        if (verbosity >= Verbosity.VERBOSE):
            print('Beginning parallel processing for each asofdate (process messages may be trapped by parallel threads)')
        pcount = min(int(config['sys2bhc']['parallel']), os.cpu_count(), len(asof_list))
        pool = mp.Pool(pcount)
        for asofdate in asof_list:
            pool.apply_async(extract_bhcs_ondate, (config, asofdate))
        pool.close()
        pool.join()
        if (verbosity >= Verbosity.VERBOSE):
            print('Parallel processing complete')
    else:
        if (verbosity >= Verbosity.VERBOSE):
            print('Beginning sequential processing for each asofdate')
        for asof in pb.progressbar(asof_list, redirect_stdout=True):
            extract_bhcs_ondate(config, asof)
        if (verbosity >= Verbosity.VERYVERBOSE):
            print('Sequential processing complete')
    if (verbosity >= Verbosity.VERBOSE):
        print('**** Processing complete ****')


# The main function controls execution when running from the command line
def main(argv=None):
    config = UTIL.parse_command_line(argv, CONFIG, __file__)
    make_bhcs(config)
    
# This tests whether the module is being run from the command line
if __name__ == "__main__":
    main()

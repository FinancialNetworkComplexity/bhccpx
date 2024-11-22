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
import _pickle as pik
import multiprocessing as mp
import progressbar as pb

import bhca
import bhc_datautil as UTIL
CONFIG = UTIL.read_bhc_config()


# Deletes any files in the cache corresponding to the range of dates
# implied by YQ0 and YQ1
def clear_cache(cachedir, YQ0, YQ1):
    asof_list = UTIL.assemble_asofs(YQ0, YQ1)
    for asofdate in asof_list:
        sysfilename = 'NIC_'+'_'+str(asofdate)+'.pik'
        sysfilepath = os.path.join(cachedir, sysfilename)
        if os.path.isfile(sysfilepath):
            os.remove(sysfilepath)
            

def find_highholder(config, BankSys, rssd):
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
    (verbose, veryverbose) = UTIL.verbosity(config)
    HH_list = bhca.find_highholders(BankSys, rssd)
    if (None==HH_list[0]):
        if (verbose): print('WARNING: Entity not in the banking system:', rssd)   
    elif (len(HH_list) > 1):
        if (verbose): print('WARNING: Multiple high-holders', rssd, HH_list)    
    elif (len(HH_list) < 1):
        if (verbose): print('WARNING: No high-holders for '+str(rssd))
    return HH_list[0]

           
# A function to read or create a NetworkX graph of the full banking system
# on a given date. The function looks for an existing graph in a pickle file
# located at sysfilepath (for example, .../cachedir/NIC__YYYYMMDD.pik),
# where YYYYMMDD is the asofdate. 
# If this file exists, it is unpackeded from the pickle and returned.
# If the file does not (yet) exist, the NetworkX DiGraph is instead created
# from the relationships data and dumped into a new pickle at sysfilepath.
# The graph is a naked directed graph whose nodes are NIC entities and 
# whose edges point from parent nodes to offspring nodes. 
# The function then returns this digraph (either newly created or unpickled). 
def make_banksys(config, asofdate):
    (verbose, veryverbose) = UTIL.verbosity(config)
    sysfilename = 'NIC_'+'_'+str(asofdate)+'.pik'
    sysfilepath = os.path.join(config['csv2sys']['outdir'], sysfilename)
    relfilename = config['csv2sys']['relationships']
    csvfilepath = os.path.join(config['csv2sys']['indir'], relfilename)
    BankSys = None
    if os.path.isfile(sysfilepath):
        if (veryverbose): print('FOUND: Banking system file path:   ', sysfilepath)
        f = open(sysfilepath, 'rb')
        BankSys = pik.load(f)
        f.close()
    else:
        if (veryverbose): print('CREATING: Banking system file path:', sysfilepath, 'for', asofdate)
        BankSys = nx.DiGraph()
        csvfilepath = os.path.join(config['csv2sys']['indir'], config['csv2sys']['relationships'])
        if (veryverbose): print('CSV file path:', csvfilepath, asofdate)
        RELdf = UTIL.RELcsv2df(csvfilepath, asofdate)
        (ID_RSSD_PARENT, ID_RSSD_OFFSPRING, DT_START, DT_END) = UTIL.REL_IDcols(RELdf)
        for row in RELdf.iterrows():
            date0 = int(row[0][DT_START])
            date1 = int(row[0][DT_END])
            rssd_par = row[0][ID_RSSD_PARENT]
            rssd_off = row[0][ID_RSSD_OFFSPRING]
            if (asofdate < date0 or asofdate > date1):
                if (verbose): print('ASOFDATE,', asofdate, 'out of bounds:', rssd_par, rssd_off, date0, date1)
                continue   
            BankSys.add_edge(rssd_par, rssd_off)
        # Adding in the singleton institutions (no edges in Relationships file)
        if (veryverbose): print('System (pre)  as of '+str(asofdate)+' has', BankSys.number_of_nodes(), 'nodes and', BankSys.number_of_edges(), 'edges')
        indir = config['csv2sys']['indir']
        fA = config['csv2sys']['attributesactive']
        fB = config['csv2sys']['attributesbranch']
        fC = config['csv2sys']['attributesclosed']
        ATTdf = UTIL.makeATTs(indir, fA, fB, fC, asofdate, filter_asof=True)
#        ATTdf = ATTdf[ATTdf.NICsource=='A']
        ATTdf = ATTdf[ATTdf.DT_END >= asofdate]
        ATTdf = ATTdf[ATTdf.DT_OPEN <= asofdate]
    #    ATTdf = ATTdf[ATTdf.DT_OPEN <= asofdate]
    #    ATTdf = ATTdf[ATTdf.DT_END >= asofdate]
        nodes_BankSys = set(BankSys.nodes)
        nodes_ATTdf = set(ATTdf['ID_RSSD'].unique())
        nodes_new = nodes_ATTdf.difference(nodes_BankSys)
        BankSys.add_nodes_from(nodes_new)
        # Storing the new banking system file
        f = open(sysfilepath, 'wb')
        pik.dump(BankSys, f)
        f.close()
    if (veryverbose): print('System (post) as of '+str(asofdate)+' has', BankSys.number_of_nodes(), 'nodes and', BankSys.number_of_edges(), 'edges;', len(nodes_new), 'added, out of', len(nodes_ATTdf), 'candidates in', type(ATTdf))
#    return (BankSys, ATTdf, nodes_BankSys, nodes_ATTdf, nodes_new)
    return BankSys
    

# This critical function builds a representation of the full system
def build_sys(config):
    (verbose, veryverbose) = UTIL.verbosity(config)
    if (veryverbose): UTIL.print_config(config, __file__)
    # If clearcache, then remove existing banking system pik files and recreate
    if ('TRUE'==config['csv2sys']['clearcache'].upper()):
        if (verbose): print('Clearing output cache of *.pik files in the range:', config['csv2sys']['asofdate0'], config['csv2sys']['asofdate1'])
        clear_cache(config['csv2sys']['outdir'], config['csv2sys']['asofdate0'], config['csv2sys']['asofdate1'])
    asof_list = UTIL.assemble_asofs(config['csv2sys']['asofdate0'], config['csv2sys']['asofdate1'])
    if (veryverbose): print('List of as-of dates to process:', asof_list, 'Cores:', config['csv2sys']['parallel'], config['csv2sys']['outdir'])
    if (int(config['csv2sys']['parallel']) > 0):
        if (verbose): print('Beginning parallel processing ('+str(len(asof_list))+' tasks across '+config['csv2sys']['parallel']+' cores) for each as-of date (process messages may be trapped by parallel threads)')
        pcount = min(int(config['csv2sys']['parallel']), os.cpu_count(), len(asof_list))
        pool = mp.Pool(processes=pcount)
        for asof in asof_list:
            pool.apply_async(make_banksys, (config, asof))
        pool.close()
        pool.join()
        if (veryverbose): print('Parallel processing complete')
    else:
        if (verbose): print('Beginning sequential processing for each as-of date')
        for asof in pb.progressbar(asof_list, redirect_stdout=True):
             make_banksys(config, asof)
        if (veryverbose): print('Sequential processing complete')
    if (verbose): print('**** Processing complete ****')


# The main function controls execution when running from the command line
def main(argv=None):
    config = UTIL.parse_command_line(argv, CONFIG, __file__)
    build_sys(config)
    
# This tests whether the module is being run from the command line
if __name__ == "__main__":
    main()
    

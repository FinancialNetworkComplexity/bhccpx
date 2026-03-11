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
from tqdm import tqdm
from configparser import ConfigParser
import pickle as pkl
import ast
import sys
import logging
import bhc_datautil
from bhc_datautil import NICData, AsOfDate
import csv2sys


def add_attributes(config: ConfigParser, DATA: NICData, BHC: nx.DiGraph, logger=logging) -> nx.DiGraph:
    """
    Decorate a BHC graph with important attributes from NIC data.
    For each entity in the BHC graph, we look up relevant attributes from the DATA object
    and attach them to the node.

    Default attributes added to each node include:
    - nicsource: Source identifier from NIC data
    - entity_type: Type of the business entity
    - GEO_JURISD: Geographic jurisdiction (country and state)
    
    Additional attributes can be specified in the configuration file under
    the 'extraattributes' setting in the 'sys2bhc' section.

    :param config: Configuration parser containing settings including extra attributes to add
    :type config: ConfigParser
    :param DATA: NIC data container with attributes dataframe and other key information resources
    :type DATA: NICData
    :param BHC: Bare NetworkX directed graph representing the firm structure without attributes
    :type BHC: nx.DiGraph
    :param logger: Logger instance for debugging and info messages
    :type logger: logging.Logger, optional
    :returns: Enhanced NetworkX directed graph with node attributes added
    :rtype: nx.DiGraph

    .. warning::
        If a node ID is not found in the attributes dataframe, default values
        of 'XXX' are assigned to prevent errors.
    """
    ATTdf = DATA.attributes
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
            logger.debug('Node %s not found in attributes dataframe, assigning default attributes', node_id)
        nx.set_node_attributes(BHC, {node: node_dict})
    return BHC


def remove_branches(config: ConfigParser, DATA: NICData, BHC: nx.DiGraph) -> nx.DiGraph:
    """
    Creates a copy of the input BHC DiGraph and removes all nodes that are identified
    as branches (NICsource == 'B') along with all their descendant nodes in the 
    directed acyclic graph structure.
    
    :param config: Configuration parser object (currently unused in function)
    :type config: ConfigParser
    :param DATA: NIC data object containing attributes dataframe with NICsource information
    :type DATA: NICData
    :param BHC: Bank holding company represented as a directed graph
    :type BHC: nx.DiGraph
    :returns: New directed graph identical to input BHC but without branch nodes and their descendants
    :rtype: nx.DiGraph
    """
    removal_set = set()
    ATTdf = DATA.attributes
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
        if nicsource == 'B':
            branchdescendants = nx.algorithms.dag.descendants(BHC,node)
            removal_set.add(node)
            for b in branchdescendants:
                removal_set.add(b)
    for n in removal_set:
        BHC2.remove_node(n)
    return BHC2


def extractBHC(
    config: ConfigParser, asofdate: AsOfDate, rssd: int,
    DATA: NICData | None = None, BankSys: nx.DiGraph | None = None,
    use_cache: bool = True, logger=logging
) -> nx.DiGraph | None:
    """
    This function extracts a single BHC graph from a full banking system
    graph, as identified by the BHC's high-holder RSSD. It also decoreates the BHC
    graph with various important attributes. Outputs are cached under "BHC_{rssd}_{asofdate}.pkl".

    :param config: Configuration information
    :type config: ConfigParser
    :param DATA: NIC data object containing attributes dataframe with NICsource information.
        Will fetch/generate if not provided.
    :type DATA: NICData
    :param BankSys: Full banking system graph, optional. Will generate if not provided.
    :type BankSys: nx.DiGraph or None, default None
    :param logger: Logger instance for debugging and warnings
    :type logger: logging.Logger, default logging
    :returns: Extracted and decorated BHC graph, or None if extraction fails
    :rtype: nx.DiGraph or None
    
    .. warning::
        Configuration directory mismatches between csv2sys and sys2bhc sections
        will generate warning messages but won't prevent execution.
    .. warning::
        If 'nm_lgl' is not present in the BHC nodes, it will be skipped.

    The function also decorates the BHC graph with a number of important
    attributes, by calling the add_attributes().
    """
    BHC = None
    if config.get('sys2bhc', 'indir') != config.get('csv2sys', 'indir'):
        logger.warning(
            'csv2sys.indir: %s differs from sys2bhc_indir: %s',
            config.get('csv2sys', 'indir'), config.get('sys2bhc', 'indir')
        )
    if config.get('sys2bhc', 'outdir') != config.get('csv2sys', 'outdir'):
        logger.warning(
            'csv2sys.outdir: %s differs from sys2bhc_outdir: %s',
            config.get('csv2sys', 'outdir'), config.get('sys2bhc', 'outdir')
        )
    
    bhcfilename = f"BHC_{rssd}_{asofdate}.pkl"
    bhcfilepath = os.path.join(config.get('sys2bhc', 'outdir'), bhcfilename)
    if use_cache and os.path.exists(bhcfilepath):
        with open(bhcfilepath, 'rb') as f:
            return pkl.load(f)

    if BankSys is None:
        BankSys = csv2sys.make_banksys(config, asofdate, logger)
    if DATA is None:
        logger.debug('Fetching DATA (not provided)')
        DATA = bhc_datautil.fetch_DATA(
            outdir=config.get('sys2bhc', 'outdir'),
            asofdate=asofdate,
            indir=config.get('sys2bhc', 'indir'),
            fA=config.get('sys2bhc', 'attributesactive'),
            fB=config.get('sys2bhc', 'attributesbranch'),
            fC=config.get('sys2bhc', 'attributesclosed'),
            fREL=config.get('sys2bhc', 'relationships'),
            logger=logger
        )

    highholders = DATA.highholders
    if rssd in highholders:
        BHC = populate_bhc(config, BankSys, DATA, rssd, logger)
        logger.debug('BHC: %s %s %s %s', rssd, type(BHC), BHC.number_of_nodes(), BHC.number_of_edges())
        if 'nm_lgl' not in BHC.nodes(data=True)[rssd]:
            logger.debug("RSSD=%s has no legal name, skipping", rssd)
            return
        with open(bhcfilepath, 'wb') as f:
            pkl.dump(BHC, f)
        logger.debug(
            'As of %s, BHC %s has %s nodes and %s edges: %s',
            asofdate, rssd, BHC.number_of_nodes(), BHC.number_of_edges(), BHC.nodes(data=True)[rssd]['nm_lgl']
        )
    else:
        logger.warning('RSSD missing from high-holder list: %s as of %s', rssd, asofdate)
    return BHC


def populate_bhc(config: ConfigParser, BankSys: nx.DiGraph, DATA: NICData, rssd, logger=logging) -> nx.DiGraph:
    """
    This function extract a BHC subgraph from a banking system graph, adds relevant attributes,
    optionally removed branches, and returns a directed graph containing the BHC.

    :param config: Configuration information
    :type config: ConfigParser
    :param BankSys: Directed graph representing the entire banking system structure
    :type BankSys: nx.DiGraph
    :param DATA: Data object containing banking information used for adding attributes
    :type DATA: NICData
    :param rssd: Root RSSD identifier of the holding company to extract
    :type rssd: str or int
    :param logger: Logger instance for recording processing information
    :type logger: logging module, optional
    :returns: Directed graph representing the BHC structure with all descendants of the
              specified RSSD, including added attributes and optional branch removal
    :rtype: nx.DiGraph

    .. note::
    - The function includes the root RSSD (holding company) itself in the BHC graph
    - Branch removal is controlled by the 'usebranches' configuration setting
    - The returned graph is always converted to a directed graph format
    """

    usebranches = config.getboolean('sys2bhc', 'usebranches')
    bhc_entities = nx.algorithms.dag.descendants(BankSys, rssd)
    bhc_entities.add(rssd)  # Include HH in the BHC too
    BHC = BankSys.subgraph(bhc_entities)
    BHC = add_attributes(config, DATA, BHC, logger)
    if not usebranches:
        logger.info(f"Removing branches ({usebranches=}) for high-holder: {rssd}")
        BHC = remove_branches(config, DATA, BHC)
    BHC = BHC.to_directed()
    return BHC


def clear_cache(cachedir: str, asof_list: list[AsOfDate]):
    """Deletes any BHC_* files in the cache corresponding to the dates in asof_list"""
    asof_set = set(asof_list)
    for filename in os.listdir(cachedir):
        if not (filename.startswith("BHC_") and filename.endswith(".pkl")):
            continue
        parts = filename.split("_")
        if len(parts) != 3:
            continue
        asofdate_with_ext = parts[2]
        asofdate = AsOfDate.from_str(asofdate_with_ext.replace(".pkl", ""))
        if asofdate not in asof_set:
            continue
        filepath = os.path.join(cachedir, filename)
        if os.path.isfile(filepath):
            os.remove(filepath)


def extract_bhcs_ondate(config: ConfigParser, asofdate: AsOfDate, logger=logging) -> list[nx.DiGraph | None]:
    """
    Loop over all RSSD IDs in the bhclist (in config), loading or creating 
    a cached pik file for each on the asofdate. 
    Returns a list containing those BHCs. 
    """
    DATA = bhc_datautil.fetch_DATA(
        outdir=config.get('sys2bhc', 'outdir'),
        asofdate=asofdate,
        indir=config.get('sys2bhc', 'indir'),
        fA=config.get('sys2bhc', 'attributesactive'),
        fB=config.get('sys2bhc', 'attributesbranch'),
        fC=config.get('sys2bhc', 'attributesclosed'),
        fREL=config.get('sys2bhc', 'relationships'),
        logger=logger
    )
    rssd_lst: list[int] | None = ast.literal_eval(config.get('sys2bhc', 'bhclist'))
    if rssd_lst is None:
        rssd_lst: list[int] = sorted(list(DATA.highholders))
    BankSys = csv2sys.make_banksys(config, asofdate, logger)

    BHCs = []
    for rssd in rssd_lst:
        BHC = extractBHC(config, asofdate, rssd, DATA, BankSys, logger=logger)
        BHCs.append(BHC)
    return BHCs

def make_bhcs(config: ConfigParser, logger=logging):
    """
    Loop over all dates in the asoflist (in config). For each date, extract
    all the BHCs in the bhclist.
    """
    asof_list = ast.literal_eval(config.get('sys2bhc', 'asoflist'))
    if asof_list is None:
        # Include all dates in range when provided asof_list is None
        asof_list = bhc_datautil.AsOfDate.make_range_from_YQ_strs(config.get('csv2sys', 'asofdate0'), config.get('csv2sys', 'asofdate1'))
    else:
        asof_list = list(map(lambda YQ: bhc_datautil.AsOfDate.from_YQ_str(YQ), asof_list))
    if config.getboolean('sys2bhc', 'clearcache'):
        clear_cache(config.get('sys2bhc', 'outdir'), asof_list)
    if config.getint('sys2bhc', 'parallel') > 0:
        logger.info(
            'Beginning parallel processing (%s tasks across %s cores) for each as-of date (process messages may be trapped by parallel threads)',
            str(len(asof_list)), config.getint('sys2bhc', 'parallel'))
        pcount = min(config.getint('sys2bhc', 'parallel'), os.cpu_count(), len(asof_list))
        pool = mp.Pool(pcount)
        results = [pool.apply_async(extract_bhcs_ondate, (config, asof, logger)) for asof in asof_list]
        with tqdm(total=len(results), desc="Parallel processing for each asofdate") as pbar:
            for r in results:
                r.wait()
                pbar.update(1)
        pool.close()
        pool.join()
        logger.info('Parallel processing complete')
    else:
        logger.info('Beginning sequential processing for each asofdate')
        for asof in tqdm(asof_list, file=sys.stdout):
            extract_bhcs_ondate(config, asof, logger=logger)
        logger.info('Sequential processing complete')


def main(argv=None):
    config = bhc_datautil.read_config()
    config = bhc_datautil.parse_command_line(argv, config, __file__)
    logger = logging.getLogger("sys2bhc")
    make_bhcs(config, logger=logger)
    
if __name__ == "__main__":
    main()

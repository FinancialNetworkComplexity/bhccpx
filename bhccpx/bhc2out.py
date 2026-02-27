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

import ast
from configparser import ConfigParser
import os
import csv

import pandas as pd
import graphviz as gv
from tqdm.auto import tqdm
import multiprocessing as mp
from enum import StrEnum
import networkx as nx
import pickle as pkl
import logging
from logging import Logger

import bhc_datautil
from bhc_datautil import AsOfDate
import csv2sys
import sys2bhc
import bhca


# String constants for metric names
class Metrics(StrEnum):
    BVct = 'Bas_Vertex_count'
    BEct = 'Bas_Edge_count'
    BCrk = 'Bas_Cycle_rank'
    BCmp = 'Bas_Num_CComp'
    EQfxB = 'Ent_Qfull_B1'
    EQhxB = 'Ent_Qhetr_B1'
    EQfcB = 'Ent_Qfcon_B1'
    EQhcB = 'Ent_Qhcon_B1'
    EQecB = 'Ent_edgcn_B1'
    EDHmB = 'Ent_DjHom_B1'
    EDHmM = 'Ent_DjHom_M'
    ENlbl = 'Ent_Nlabl'
    GQfxB = 'Geo_Qfull_B1'
    GQhxB = 'Geo_Qhetr_B1'
    GQfcB = 'Geo_Qfcon_B1'
    GQhcB = 'Geo_Qhcon_B1'
    GQecB = 'Geo_edgcn_B1'
    GDHmB = 'Geo_DjHom_B1'
    GDHmM = 'Geo_DjHom_M'
    GNlbl = 'Geo_Nlabl'


def make_wachwells_comparison(BHCconfigs: list[tuple[int, AsOfDate]], config: ConfigParser, logger: Logger = logging) -> pd.DataFrame:
    """
    A dedicated function that produces the summary comparison of complexity
    measures for the Wachovia-Wells Fargo case study. This appears as Table 2
    in the NBER version of the paper.

    :param BHCconfigs: A list of (rssd, asofdate) tuples to be processed
    :type BHCconfigs: list[tuple[int, int]]
    :param config: Configuration object containing settings for the BHC Complexity Toolkit
    :type config: ConfigParser
    :param logger: Logger object for logging messages, by default logging
    :type logger: Logger, optional
    :return: A DataFrame containing the complexity metrics for Wachovia and Wells Fargo
             at specified as-of dates.
    :rtype: pd.DataFrame
    """
    # Loop to extract all of BHC snapshots defined by BHCconfigs
    metrics = {}
    BHCdict = {}
    logger.info('Creating BHC networks')
    for rssd, asof in tqdm(BHCconfigs, desc="BHC-Quarter pairs"):
        BHC = sys2bhc.extractBHC(config, asof, rssd)
        metrics = complexity_workup(BHC)
        BHCdict[str(rssd)+'_'+str(asof)] = [rssd, str(asof)] + list(metrics.values())
    cols = ['rssd', 'asofdate'] + list(metrics.keys())
    table2 = pd.DataFrame.from_dict(BHCdict, orient='index')
    table2.columns = cols
    table2['rssd'] = table2['rssd'].astype(int)
    table2.sort_values(['rssd','asofdate'],ascending=[True,True],inplace=True)
    logger.debug(table2.iloc[:,2:6])
    logger.debug(table2.iloc[:,6:14])
    logger.debug(table2.iloc[:,14:22])
    logger.info('*** Processing Table2 Complete ***')
    return table2


def complexity_workup(BHC) -> dict[str, int]:
    """Calculates a standard set of complexity metrics for a BHC
    
    Most of the metrics involve quotienting the nodes of the BHC graph.
    The nodes are quotiented by entity type and (separately) geographic
    juridiction. 
    
    The following metrics are calculated:
        
    A. Basic metrics
    
      * Bas_Vertex_count = Number of nodes, original BHC graph 
      * Bas_Edge_count   = Number of edges, BHC graph 
      * Bas_Cycle_rank   = Cycle rank (b1), BHC graph 
      * Bas_Num_CComp    = Number of connected components (b0), BHC graph
      
    B. Entity quotients
    
      * Ent_Qfull_B1 = Cycle rank, full entity quotient
      * Ent_Qhetr_B1 = Cycle rank, heterogeneous entity quotient
      * Ent_Qfcon_B1 = Cycle rank, condensed entity quotient
      * Ent_Qhcon_B1 = Cycle rank, heterogeneous condensed entity quotient
      * Ent_edgcn_B1 = 'Ent_edgcn_B1'
      * Ent_DjHom_B1 = 'Ent_DjHom_B1'
      * Ent_DjHom_M  = 'Ent_DjHom_M'
      * Ent_Nlabl    = 'Ent_Nlabl'
      
    B. Geography quotients
    
      * Geo_Qfull_B1 = 'Geo_Qfull_B1'
      * Geo_Qhetr_B1 = 'Geo_Qhetr_B1'
      * Geo_Qfcon_B1 = 'Geo_Qfcon_B1'
      * Geo_Qhcon_B1 = 'Geo_Qhcon_B1'
      * Geo_edgcn_B1 = 'Geo_edgcn_B1'
      * Geo_DjHom_B1 = 'Geo_DjHom_B1'
      * Geo_DjHom_M  = 'Geo_DjHom_M'
      * Geo_Nlabl    = 'Geo_Nlabl'

    :param BHC: A directed graph representing a bank holding company
    :type BHC: networkx.DiGraph
    :param logger: Logger object for logging messages, by default logging
    :type logger: Logger, optional
    :return: Components in the projection of BHC to a simple undirected graph
    :rtype: dict[str, int]
    """

    metrics = dict()
    # Basic metrics, using the key constants defined above
    metrics[Metrics.BVct] = BHC.number_of_nodes()
    metrics[Metrics.BEct] = bhca.edge_count(BHC)
    metrics[Metrics.BCrk] = bhca.cycle_rank(BHC)
    metrics[Metrics.BCmp] = bhca.number_of_components(BHC)

    # Quotiented by entity type
    DIMEN = 'entity_type'
    QEF = bhca.get_quotient(BHC, DIMEN, bhca.QType.FULL)
    QEH = bhca.get_quotient(BHC, DIMEN, bhca.QType.HETERO)
    QEFC = bhca.get_quotient(BHC, DIMEN, bhca.QType.FULL_COND)
    QEHC = bhca.get_quotient(BHC, DIMEN, bhca.QType.HETERO_COND)
    CE = bhca.get_contraction(BHC, DIMEN).to_undirected()
    DMHE = bhca.get_disjoint_maximal_homogeneous_subgraphs(BHC, DIMEN)
    metrics[Metrics.EQfxB] = bhca.cycle_rank(QEF)
    metrics[Metrics.EQhxB] = bhca.cycle_rank(QEH)
    metrics[Metrics.EQfcB] = bhca.cycle_rank(QEFC)
    metrics[Metrics.EQhcB] = bhca.cycle_rank(QEHC)
    metrics[Metrics.EQecB] = bhca.cycle_rank(CE)
    metrics[Metrics.EDHmB] = bhca.cycle_rank(DMHE)
    metrics[Metrics.EDHmM] = bhca.number_of_components(DMHE)
    metrics[Metrics.ENlbl] = len(bhca.get_labels(BHC, DIMEN))

    # Quotiented by geographic jurisdiction
    DIMEN = 'GEO_JURISD'
    QGF = bhca.get_quotient(BHC, DIMEN, bhca.QType.FULL)
    QGH = bhca.get_quotient(BHC, DIMEN, bhca.QType.HETERO)
    QGFC = bhca.get_quotient(BHC, DIMEN, bhca.QType.FULL_COND)
    QGHC = bhca.get_quotient(BHC, DIMEN, bhca.QType.HETERO_COND)
    CG = bhca.get_contraction(BHC, DIMEN).to_undirected()
    DMHG = bhca.get_disjoint_maximal_homogeneous_subgraphs(BHC, DIMEN)
    metrics[Metrics.GQfxB] = bhca.cycle_rank(QGF)
    metrics[Metrics.GQhxB] = bhca.cycle_rank(QGH)
    metrics[Metrics.GQfcB] = bhca.cycle_rank(QGFC)
    metrics[Metrics.GQhcB] = bhca.cycle_rank(QGHC)
    metrics[Metrics.GQecB] = bhca.cycle_rank(CG)
    metrics[Metrics.GDHmB] = bhca.cycle_rank(DMHG)
    metrics[Metrics.GDHmM] = bhca.number_of_components(DMHG)
    metrics[Metrics.GNlbl] = len(bhca.get_labels(BHC, DIMEN))

    return metrics


# Calculates a full set of complexity metrics for a BHC, quotienting by
# both entity type and geographic jurisdiction, and returns them as a dict.
def test_metrics(metrics: dict[str, int], context: str, logger: Logger = logging):
    # Ensure that the BHC is a single connected component
    if metrics[Metrics.BCmp] != 1:
        logger.warning("BHC is not completely connected. %s: %s, Context: %s", Metrics.BCmp, metrics[Metrics.BCmp], context)
    
    # Confirm that Equation 3 (Euler-Poincare) holds
    if metrics[Metrics.BCrk] != metrics[Metrics.BEct] - metrics[Metrics.BVct] + metrics[Metrics.BCmp]:
        logger.warning(
            'Euler-Poincare fails. %s: %s, %s: %s, %s: %s, %s: %s, Context: %s',
            Metrics.BCrk, metrics[Metrics.BCrk], Metrics.BEct, metrics[Metrics.BEct],
            Metrics.BVct, metrics[Metrics.BVct], Metrics.BCmp, metrics[Metrics.BCmp], context
        )

    # Confirm that Theorem 3.2 Equation 6 (NBER version) holds -- entity type
    if metrics[Metrics.EQfxB] != metrics[Metrics.BCrk] + metrics[Metrics.BVct] - metrics[Metrics.ENlbl]:
        logger.warning(
            'Theorem 3.2 fails. %s: %s, %s: %s, %s: %s, %s: %s, Context: %s',
            Metrics.EQfxB, metrics[Metrics.EQfxB], Metrics.BCrk, metrics[Metrics.BCrk],
            Metrics.BVct, metrics[Metrics.BVct], Metrics.ENlbl, metrics[Metrics.ENlbl], context
        )
    
    # Confirm that Corollary 3.4 (NBER version) holds -- entity type
    if metrics[Metrics.EQhxB] != metrics[Metrics.EDHmM] - metrics[Metrics.ENlbl] + metrics[Metrics.BCrk] - metrics[Metrics.EDHmB]:
        logger.warning(
            'Corollary 3.4 fails. %s: %s, %s: %s, %s: %s, %s: %s, %s: %s, Context: %s',
            Metrics.EQhxB, metrics[Metrics.EQhxB], Metrics.EDHmM, metrics[Metrics.EDHmM], Metrics.ENlbl, metrics[Metrics.ENlbl],
            Metrics.BCrk, metrics[Metrics.BCrk], Metrics.EDHmB, metrics[Metrics.EDHmB], context
        )

    # Confirm that Theorem 3.2 Equation 6 (NBER version) holds -- geography
    if metrics[Metrics.GQfxB] != metrics[Metrics.BCrk] + metrics[Metrics.BVct] - metrics[Metrics.GNlbl]:
        logger.warning(
            'Theorem 3.2 fails (Geographic quotient). %s: %s, %s: %s, %s: %s, %s: %s, Context: %s',
            Metrics.GQfxB, metrics[Metrics.GQfxB], Metrics.BCrk, metrics[Metrics.BCrk],
            Metrics.BVct, metrics[Metrics.BVct], Metrics.GNlbl, metrics[Metrics.GNlbl], context
        )
        
    # Confirm that Corollary 3.4 (NBER version) holds -- geography
    if metrics[Metrics.GQhxB] != metrics[Metrics.GDHmM] - metrics[Metrics.GNlbl] + metrics[Metrics.BCrk] - metrics[Metrics.GDHmB]:
        logger.warning(
            'Corollary 3.4 fails (Geographic quotient). %s: %s, %s: %s, %s: %s, %s: %s, %s: %s, Context: %s',
            Metrics.GQhxB, metrics[Metrics.GQhxB], Metrics.GDHmM, metrics[Metrics.GDHmM], Metrics.GNlbl, metrics[Metrics.GNlbl],
            Metrics.BCrk, metrics[Metrics.BCrk], Metrics.GDHmB, metrics[Metrics.GDHmB], context
        )
        
def makeSVG(config:ConfigParser, BHC:nx.DiGraph, outdir, rssd_hh, asofdate: AsOfDate, partition:str='entity_type', popup=False, logger:Logger=logging):
    """
    Create an SVG image file representing a BHC.
    The file is stored in the outdir, with the filename: RSSD_<rssd_hh>_<asofdate>.svg.

    If popup is set to True, then the function will also launch a browser to
    display the file.
    """
    svg_filename = 'RSSD'+'_'+str(rssd_hh)+'_'+str(asofdate)
    svg_file = outdir + svg_filename
    colormap = ast.literal_eval(config.get('bhc2out', 'colormap'))
    dot = gv.Digraph(comment='RSSD:'+str(rssd_hh), engine='dot')
    dot.attr('node', fontsize='8')
    dot.attr('node', fixedsize='true')
    dot.attr('node', width='0.7')
    dot.attr('node', height='0.3')
    BHC_allnodes = BHC.nodes(data=True)
    for N in BHC_allnodes:
        NM_LGL = ''
        ENTITY_TYPE = 'ZZZ'
        GEO_JURISD = 'ZZZ'
        attribute_error = True
        try:
            NM_LGL = N[1]['nm_lgl'].strip()
            ENTITY_TYPE = N[1]['entity_type']
            GEO_JURISD = N[1]['GEO_JURISD']
            attribute_error = False
        except KeyError as KE:
            logger.warning('Invalid attribute data for RSSD=%s at asofdate=%s', N, asofdate)
        tt = '['+str(N[0])+']'+' '+ENTITY_TYPE+'\\n' +'------------\\n'+ NM_LGL +'\\n' +'------------\\n'+ GEO_JURISD
        if (attribute_error):
            dot.node('rssd'+str(N[0]), str(N[0]), style="filled", fillcolor="red;.5:green", tooltip=tt)
        else:
            dot.node('rssd'+str(N[0]), str(N[0]), style="filled", fillcolor=colormap[ENTITY_TYPE], tooltip=tt)
    for E in BHC.edges():
        col_het = config.get('bhc2out', 'col_het')
        col_hom = config.get('bhc2out', 'col_hom')
        col_nul = config.get('bhc2out', 'col_nul')
        src = 'rssd' + str(E[0])
        tgt = 'rssd' + str(E[1])
        Vs = BHC.nodes[E[0]]
        Vt = BHC.nodes[E[1]]
        col = col_het  # Assume heterogeneous by default
        if (partition not in Vs) or (partition not in Vt):
            col=col_nul
        elif Vs[partition] == Vt[partition]:
            col=col_hom
        dot.edge(src, tgt, arrowsize='0.3', color=col)
    dot.render(filename=svg_file, format='svg')
    dot.save(filename=svg_file+'.dot', directory=outdir)
    if (popup):
        os.system("%s %s" % (config.get('bhc2out', 'browsercmd'), svg_file+'.svg'))


def make_panel(config: ConfigParser, logger: Logger = logging):
    """
    Create a full panel of complexity measures for all BHCs for all quarters
    in the list of as-of dates between asofdate0 and asofdate1.
    """
    asof_list = bhc_datautil.AsOfDate.make_range_from_YQ_strs(config.get('bhc2out', 'asofdate0'), config.get('bhc2out', 'asofdate1'))
    if config.getint('bhc2out', 'parallel') > 0:
        logger.info('Beginning parallel processing for each asofdate (process messages may be trapped by parallel threads)')
        pcount = min(config.getint('bhc2out', 'parallel'), os.cpu_count(), len(asof_list))
        pool = mp.Pool(pcount)
        results: dict[AsOfDate, dict[int, dict[str, int]]] = {
            asof: pool.apply_async(all_bhc_complex, (config, asof, logger))
            for asof in asof_list
        }
        results = {k: v.get() for k, v in results.items()}
        pool.close()
        pool.join()
        logger.debug('Parallel processing complete')
    else:
        logger.info('Beginning sequential processing for each asofdate')
        results = []
        for asofdate in tqdm(asof_list, desc="Processing per as-of date"):
            results.append(all_bhc_complex(config, asofdate, logger))
        logger.debug('Sequential processing complete')
    
    panelfilepath = os.path.join(config.get('bhc2out', 'outdir'), config.get('bhc2out', 'panel_filename'))
    with open(panelfilepath, mode='w') as csvfile:
        fields = ['ASOF', 'RSSD'] + ast.literal_eval(config.get('bhc2out', 'metric_list'))
        csvwriter = csv.DictWriter(csvfile, fieldnames=fields)
        csvwriter.writeheader()
        # TODO: NEED TO SORT results BY ASOF AND RSSD BEFORE SAVING TO CSV
        for asof, res in results.items():
            for rssd, metric_dict in res.items():
                metric_dict['ASOF'] = str(asof)
                metric_dict['RSSD'] = rssd
                csvwriter.writerow(metric_dict)
    csvfile.close()
    logger.info('**** Processing complete ****')


def all_bhc_complex(config: ConfigParser, asofdate: AsOfDate, logger=logging):
    DATA = bhc_datautil.makeDATA(
        indir=config.get('bhc2out', 'indir'),
        fA=config.get('bhc2out', 'attributesactive'),
        fB=config.get('bhc2out', 'attributesbranch'),
        fC=config.get('bhc2out', 'attributesclosed'),
        fREL=config.get('bhc2out', 'relationships'),
        asofdate=asofdate,
        logger=logger
    )
    BankSys = csv2sys.make_banksys(config, asofdate, logger=logger)
    highholders: list[int] | None = ast.literal_eval(config.get('bhc2out', 'bhclist'))
    if highholders is None:
        # Include all RSSDs when HHs is None
        highholders: list[int] = sorted(list(DATA.highholders))
    logger.debug('Identified %s high-holders for %s', str(len(highholders)), str(asofdate))

    BHCs: dict[int, dict[str, int]] = dict()
    for rssd in highholders:
        BHC = sys2bhc.populate_bhc(config, BankSys, DATA, rssd)
        metrics = complexity_workup(BHC)
        if config.getboolean('bhc2out', 'test_metrics'):
            context = f"ASOF={str(asofdate)}, RSSD={str(rssd)}"
            test_metrics(metrics, context)
        BHCs[rssd] = metrics
    return BHCs


# The main function controls execution when running from the command line
def main(argv=None):
    config = bhc_datautil.read_config()
    config = bhc_datautil.parse_command_line(argv, config, __file__)
    logger = logging.getLogger("bhc2out")
    
    if config.getboolean('bhc2out', 'make_panel'):
        make_panel(config)
    
    if config.getboolean('bhc2out', 'make_wachwells_comparison'):
        # Default configs to run
        # RSSD 1073551 is Wachovia Corp.
        # RSSD 1120754 is Wells Fargo & Co.
        BHCconfigs = [
            (1120754,20061231),
            (1073551,20061231),
            (1120754,20080930), 
            (1073551,20080930),
            (1120754,20081231),
            (1120754,20101231)
        ]
        make_wachwells_comparison(BHCconfigs, config, logger=logger)
    
    
if __name__ == "__main__":
    # import doctest
    # doctest.testmod()
    main()

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
import ast
from configparser import ConfigParser
import logging
import csv
import multiprocessing as mp
from enum import StrEnum
import pandas as pd
import graphviz as gv
from tqdm.auto import tqdm
import networkx as nx

from bhc_datautil import AsOfDate, makeDATA
from csv2sys import make_banksys
from sys2bhc import populate_bhc, extractBHC
from bhca import (
	QType, get_labels, edge_count, cycle_rank, number_of_components,
	get_quotient, get_contraction, get_disjoint_maximal_homogeneous_subgraphs
)

logger = logging.getLogger("bhc2out")


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


def parse_metrics_list(config: ConfigParser) -> list[Metrics]:
    """
    Resolves the metrics_list configuration parameter into Metrics members.

    The configured entries are attribute expressions (for example,
    Metrics.BVct), which ast.literal_eval cannot parse, so the metric names
    are resolved explicitly here.

    :param config: Configuration object containing settings for the BHC Complexity Toolkit
    :type config: ConfigParser
    :return: The complexity metrics to assemble, in the order configured
    :rtype: list[Metrics]
    """
    metrics_expr = ast.parse(config.get('bhc2out', 'metrics_list'), mode='eval').body
    if not isinstance(metrics_expr, ast.List):
        raise ValueError('The metrics_list parameter must be a list')
    metrics_list = []
    for entry in metrics_expr.elts:
        if not (isinstance(entry, ast.Attribute) and isinstance(entry.value, ast.Name)
                and entry.value.id == 'Metrics'):
            raise ValueError(f'Invalid metrics_list entry: {ast.unparse(entry)}. Expected format is Metrics.NAME')
        if Metrics[entry.attr] not in metrics_list:
            metrics_list.append(Metrics[entry.attr])
    return metrics_list


def make_bespoke(config: ConfigParser, bhc_asof_list: list[tuple[int, int]] | None = None) -> pd.DataFrame:
    """
    Produces a bespoke summary of complexity measures, with one row for each
    of a given list of BHC-date pairs.

    The metrics to assemble are given by the metrics_list configuration
    parameter. The results are written as a CSV file, named by the
    bespoke_filename parameter, in the configured outdir.

    :param config: Configuration object containing settings for the BHC Complexity Toolkit
    :type config: ConfigParser
    :param bhc_asof_list: A list of (rssd, asofdate) pairs of ints, defaulting to the
             bhc_asof_list configuration parameter. The configured default reproduces
             the Wachovia-Wells Fargo comparison, which appears as Table 2 in the
             NBER version of the paper.
    :type bhc_asof_list: list[tuple[int, int]], optional
    :return: A DataFrame containing the complexity metrics, one row per BHC-date pair
    :rtype: pd.DataFrame
    """
    if bhc_asof_list is None:
        bhc_asof_list = ast.literal_eval(config.get('bhc2out', 'bhc_asof_list')): list[tuple[int, int]]
    BHCconfigs = [(int(rssd), AsOfDate.from_int(int(asof))) for rssd, asof in bhc_asof_list]
    metrics_list = parse_metrics_list(config)
    # Loop to extract all of BHC snapshots defined by BHCconfigs
    metrics = {}
    BHCdict = {}
    logger.info('Creating BHC networks')
    for rssd, asof in tqdm(BHCconfigs, desc="BHC-Quarter pairs"):
        BHC = extractBHC(config, asof, rssd)
        metrics = complexity_workup(BHC, metrics_list)
        BHCdict[str(rssd)+'_'+str(asof)] = [rssd, str(asof)] + list(metrics.values())
    cols = ['rssd', 'asofdate'] + list(metrics.keys())
    bespoke = pd.DataFrame.from_dict(BHCdict, orient='index')
    bespoke.columns = cols
    bespoke['rssd'] = bespoke['rssd'].astype(int)
    bespoke.sort_values(['rssd','asofdate'], ascending=[True,True], inplace=True)
    bespokefilepath = os.path.join(config.get('bhc2out', 'outdir'), config.get('bhc2out', 'bespoke_filename'))
    bespoke.to_csv(bespokefilepath, index=False)
    logger.debug('\n%s', bespoke.to_string())
    logger.info('*** Processing bespoke output complete ***')
    return bespoke


def complexity_workup(BHC, metrics_list: list[Metrics] | None = None) -> dict[str, int]:
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
    :param metrics_list: The subset of the metrics above to calculate,
             defaulting to all of them
    :type metrics_list: list[Metrics], optional
    :param logger: Logger object for logging messages, by default logging
    :type logger: Logger, optional
    :return: Components in the projection of BHC to a simple undirected graph
    :rtype: dict[str, int]
    """

    wanted = set(Metrics if metrics_list is None else metrics_list)
    metrics = dict()
    # Basic metrics, using the key constants defined above
    if Metrics.BVct in wanted:
        metrics[Metrics.BVct] = BHC.number_of_nodes()
    if Metrics.BEct in wanted:
        metrics[Metrics.BEct] = edge_count(BHC)
    if Metrics.BCrk in wanted:
        metrics[Metrics.BCrk] = cycle_rank(BHC)
    if Metrics.BCmp in wanted:
        metrics[Metrics.BCmp] = number_of_components(BHC)

    # Quotiented by entity type
    DIMEN = 'entity_type'
    if Metrics.EQfxB in wanted:
        metrics[Metrics.EQfxB] = cycle_rank(get_quotient(BHC, DIMEN, QType.FULL))
    if Metrics.EQhxB in wanted:
        metrics[Metrics.EQhxB] = cycle_rank(get_quotient(BHC, DIMEN, QType.HETERO))
    if Metrics.EQfcB in wanted:
        metrics[Metrics.EQfcB] = cycle_rank(get_quotient(BHC, DIMEN, QType.FULL_COND))
    if Metrics.EQhcB in wanted:
        metrics[Metrics.EQhcB] = cycle_rank(get_quotient(BHC, DIMEN, QType.HETERO_COND))
    if Metrics.EQecB in wanted:
        metrics[Metrics.EQecB] = cycle_rank(get_contraction(BHC, DIMEN).to_undirected())
    if wanted & {Metrics.EDHmB, Metrics.EDHmM}:
        DMHE = get_disjoint_maximal_homogeneous_subgraphs(BHC, DIMEN)
        if Metrics.EDHmB in wanted:
            metrics[Metrics.EDHmB] = cycle_rank(DMHE)
        if Metrics.EDHmM in wanted:
            metrics[Metrics.EDHmM] = number_of_components(DMHE)
    if Metrics.ENlbl in wanted:
        metrics[Metrics.ENlbl] = len(get_labels(BHC, DIMEN))

    # Quotiented by geographic jurisdiction
    DIMEN = 'GEO_JURISD'
    if Metrics.GQfxB in wanted:
        metrics[Metrics.GQfxB] = cycle_rank(get_quotient(BHC, DIMEN, QType.FULL))
    if Metrics.GQhxB in wanted:
        metrics[Metrics.GQhxB] = cycle_rank(get_quotient(BHC, DIMEN, QType.HETERO))
    if Metrics.GQfcB in wanted:
        metrics[Metrics.GQfcB] = cycle_rank(get_quotient(BHC, DIMEN, QType.FULL_COND))
    if Metrics.GQhcB in wanted:
        metrics[Metrics.GQhcB] = cycle_rank(get_quotient(BHC, DIMEN, QType.HETERO_COND))
    if Metrics.GQecB in wanted:
        metrics[Metrics.GQecB] = cycle_rank(get_contraction(BHC, DIMEN).to_undirected())
    if wanted & {Metrics.GDHmB, Metrics.GDHmM}:
        DMHG = get_disjoint_maximal_homogeneous_subgraphs(BHC, DIMEN)
        if Metrics.GDHmB in wanted:
            metrics[Metrics.GDHmB] = cycle_rank(DMHG)
        if Metrics.GDHmM in wanted:
            metrics[Metrics.GDHmM] = number_of_components(DMHG)
    if Metrics.GNlbl in wanted:
        metrics[Metrics.GNlbl] = len(get_labels(BHC, DIMEN))

    return metrics


# Calculates a full set of complexity metrics for a BHC, quotienting by
# both entity type and geographic jurisdiction, and returns them as a dict.
def test_metrics(metrics: dict[str, int], context: str):
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
        
def makeSVG(config:ConfigParser, BHC:nx.DiGraph, outdir, rssd_hh, asofdate: AsOfDate, partition:str='entity_type', popup=False):
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


def make_panel(config: ConfigParser):
    """
    Create a full panel of complexity measures, with one row for each BHC in
    the bhclist configuration parameter (or every high holder, if bhclist is
    None), for every quarter in the list of as-of dates between asofdate0
    and asofdate1.

    The metrics to assemble are given by the metrics_list configuration
    parameter. The panel is written as a CSV file, named by the
    panel_filename parameter, in the configured outdir.

    :param config: Configuration object containing settings for the BHC Complexity Toolkit
    :type config: ConfigParser
    """
    metrics_list = parse_metrics_list(config)
    asof_list = AsOfDate.make_range(AsOfDate.from_YQ_str(config.get('bhc2out', 'asofdate0')), AsOfDate.from_YQ_str(config.get('bhc2out', 'asofdate1')))
    if config.getint('bhc2out', 'parallel') > 0:
        logger.info('Beginning parallel processing for each asofdate (process messages may be trapped by parallel threads)')
        os_cpu_count = os.cpu_count()
        pcount = min(config.getint('bhc2out', 'parallel'), 1 if os_cpu_count is None else os_cpu_count, len(asof_list))
        pool = mp.Pool(pcount)
        results = {
            asof: pool.apply_async(all_bhc_complex, (config, asof, metrics_list))
            for asof in asof_list
        }
        results = {k: v.get() for k, v in results.items()}
        pool.close()
        pool.join()
        logger.debug('Parallel processing complete')
    else:
        logger.info('Beginning sequential processing for each asofdate')
        results = {}
        for asofdate in tqdm(asof_list, desc="Processing per as-of date"):
            results[asofdate] = all_bhc_complex(config, asofdate, metrics_list)
        logger.debug('Sequential processing complete')
    
    panelfilepath = os.path.join(config.get('bhc2out', 'outdir'), config.get('bhc2out', 'panel_filename'))
    with open(panelfilepath, mode='w') as csvfile:
        fields = ['ASOF', 'RSSD'] + [metric.value for metric in metrics_list]
        csvwriter = csv.DictWriter(csvfile, fieldnames=fields)
        csvwriter.writeheader()
        # TODO: NEED TO SORT results BY ASOF AND RSSD BEFORE SAVING TO CSV
        for asof, res in results.items():
            for rssd, metric_dict in res.items():
                metric_dict['ASOF'] = int(asof)
                metric_dict['RSSD'] = rssd
                csvwriter.writerow(metric_dict)
    csvfile.close()
    logger.info('**** Processing complete ****')


def all_bhc_complex(config: ConfigParser, asofdate: AsOfDate, metrics_list: list[Metrics] | None = None) -> dict[int, dict[str, int]]:
    DATA = makeDATA(
        indir=config.get('bhc2out', 'indir'),
        file_attA=config.get('bhc2out', 'attributesactive'),
        file_attB=config.get('bhc2out', 'attributesbranch'),
        file_attC=config.get('bhc2out', 'attributesclosed'),
        file_rel=config.get('bhc2out', 'relationships'),
        asofdate=asofdate,
    )
    BankSys = make_banksys(config, asofdate)
    highholders: list[int] | None = ast.literal_eval(config.get('bhc2out', 'bhclist'))
    if highholders is None:
        # Include all RSSDs when HHs is None
        highholders = sorted(list(DATA.highholders))
    logger.debug('Identified %s high-holders for %s', str(len(highholders)), str(asofdate))

    BHCs: dict[int, dict[str, int]] = dict()
    for rssd in highholders:
        BHC = populate_bhc(config, BankSys, DATA, rssd)
        metrics = complexity_workup(BHC, metrics_list)
        if config.getboolean('bhc2out', 'test_metrics'):
            context = f"ASOF={str(asofdate)}, RSSD={str(rssd)}"
            test_metrics(metrics, context)
        BHCs[rssd] = metrics
    return BHCs


def process(config):
    if config.getboolean('bhc2out', 'make_panel', fallback=False):
        make_panel(config)
    
    if config.getboolean('bhc2out', 'make_bespoke', fallback=False):
        make_bespoke(config)

def main():
	import argparse
	from bhc_datautil import add_common_args, get_config

	parser = argparse.ArgumentParser(description='Compute complexity metrics for BHCs and generate output tables/files')
	add_common_args(parser)
	args = parser.parse_args()
	config = get_config(args, 'bhc2out')
	
	process(config)
    
if __name__ == "__main__":
    main()

#!/usr/bin/env python
'''
-----------------------------------------------------------------------------
This file is part of the BHC Complexity Toolkit.

The BHC Complexity Toolkit is free software: you can redistribute it and/or
modify it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

The BHC Complexity Toolkit is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with the BHC Complexity Toolkit.  If not, 
see <https://www.gnu.org/licenses/>.
-----------------------------------------------------------------------------
Copyright 2019, Mark D. Flood

Author: Mark D. Flood
Last revision: 22-Jun-2019
-----------------------------------------------------------------------------
'''

import logging
import networkx as nx
import re

Q_FULL = 1          # Constant indicating the full quotient
Q_HETERO = 2        # Constant indicating the heterogeneous quotient
Q_FULL_COND = 3     # Constant indicating the full condensed quotient
Q_HETERO_COND = 4   # Constant indicating the heterogeneous condensed quotient
verbose = False


def get_labels(BHC, dimen):
    '''Gets all possible values associated with a particular key
    
    Parameters
    ==========
    
    BHC : networkx.DiGraph
    A directed graph representing a bank holding company
    
    dimen : str
    Key indicating which node attribute to scan for values
    '''

    rv = set()
    for v in BHC.nodes(data=True):
        if (v[1].has_key(dimen)):
            rv.add(v[1][dimen])
        else:
            print('Missing attribute:', dimen, 'on', v)
    return rv


def get_quotient(BHC, dimen, Qtype):
    # BHCq is the (undirected) quotient graph to be derived from BHC
    BHCq = None
    if (Q_FULL==Qtype):     # Full quotient (multi-edges and self-loops)
        BHCq = nx.MultiGraph()
    elif (Q_HETERO==Qtype):   # Heterogeneous quotient (multi-edges, no self-loops)
        BHCq = nx.MultiGraph()
    elif (Q_FULL_COND==Qtype):   # Condensed (self-loops, but no multi-edges)
        BHCq = nx.Graph()
    elif (Q_HETERO_COND==Qtype):   # Condensed heterogeneous (no multi-edges or self-loops)
        BHCq = nx.Graph()
    else:
        print('Unrecognized Qtype', Qtype)
        return
    # Establish the nodes of the quotient
    entity_types = nx.get_node_attributes(BHC,'entity_type')
    nx.set_node_attributes(BHCq, entity_types)
    # Copy edges from the BHC to the quotient
    for e in BHC.edges(data=True):
        parent = e[0]
        child = e[1]
        e_attrs = e[2]
        if ((dimen in BHC.node[parent]) and (dimen in BHC.node[child])):
            parent_label = BHC.node[parent][dimen]
            child_label = BHC.node[child][dimen]
            BHCq.add_edge(parent_label, child_label, attr_dict=e_attrs)
    # Remove self-loops, as appropriate
    if (2==Qtype or 4==Qtype):
        removals = list(nx.selfloop_edges(BHCq))
        BHCq.remove_edges_from(removals)
    # Wrap up
    if (verbose): print('Summing up:', BHC.number_of_nodes(), BHC.number_of_edges(), BHCq.number_of_nodes(), BHCq.number_of_edges())
    return BHCq
def node_equals(u, v, G, dim):
    testval = False
    if (dim in G.node[u] and dim in G.node[v]):
        testval = (G.node[u][dim] == G.node[v][dim])
    return testval
    
def get_nx_quotient(BHC, dimen):
    equivalence = lambda x, y: node_equals(x, y, BHC, dimen)
    BHCq = nx.quotient_graph(BHC.to_undirected(), equivalence)
    if (verbose): print('Summing up:', BHC.number_of_nodes(), BHC.number_of_edges(), BHCq.number_of_nodes(), BHCq.number_of_edges())
    return BHCq

def contract(BHC, dimen):
    # BHCdup is a deep copy of the BHC graph
    BHCdup = BHC.copy()
    edges_contracted = set()
    edges_uncontract = set()
    remap_edges = dict()
    for e in BHCdup.edges():
        if e in remap_edges:
            parent = remap_edges[e][0]
            child = remap_edges[e][1]
        else:
            parent = e[0]
            child = e[1]
        if parent==child:  # Assumes parent/child values are ints
            if BHC.has_edge(parent,child):
                BHC.remove_edge(parent,child)
            edges_contracted.add(e)
        elif BHC.has_node(parent) and BHC.has_node(child) and node_equals(parent, child, BHC, dimen):
            for coe in BHC.out_edges(child, data=True):
                BHC.add_edge(parent,coe[1],**coe[2])
                remap_edges[(coe[0],coe[1])] = (parent,coe[1])
            for cie in BHC.in_edges(child, data=True):
                BHC.add_edge(cie[1],parent,**cie[2])
                remap_edges[(cie[0],cie[1])] = (cie[0],parent)
            BHC.remove_node(child)
            edges_contracted.add(e)
        else:
            edges_uncontract.add(e)
    if (verbose): print('Summing up:', BHCdup.number_of_edges(), len(edges_contracted), len(edges_uncontract), BHC.number_of_edges(), BHC.number_of_nodes())
    return BHC, len(edges_contracted), len(edges_uncontract)

def get_contraction(BHC, dimen):
    #root = BHC.graph['rootnode']
    number_of_edges_contracted  = -1
    BHCcont = BHC.copy()
    while (number_of_edges_contracted != 0):
        BHCcont, number_of_edges_contracted, n_unc = contract(BHCcont, dimen)
    BHCcont.remove_edges_from(BHCcont.selfloop_edges())
    return BHCcont

def get_disjoint_maximal_homogeneous_subgraphs(BHC, dimen):
    BHCdisj = BHC.copy()
    for e in BHC.edges(data=True):
        parent = e[0]
        child = e[1]
        if ((dimen in BHC.node[parent]) and (dimen in BHC.node[child])):
            if not(node_equals(parent, child, BHC, dimen)):
                BHCdisj.remove_edge(parent, child)
    return BHCdisj

def number_of_components(BHC):
    rv = 0
    BHCu = BHC.to_undirected()
    for c in nx.connected_components(BHCu):
        rv = rv+1
    return rv

def cycle_rank(BHC):
    BHCu = BHC.to_undirected()
    b0 = number_of_components(BHCu)
    rv = BHCu.number_of_edges() - BHCu.number_of_nodes() + b0
    return rv


# The LEI incorporates two checksum digits at the final two positions.
# Checksum digits follow the procedure of ISO 7064 (mod 97-10), applied to
# strings from the character set: "0123456789ABCDEFGHIJKLMNOPQRSTUVWKYZ".
# This function takes a candidate LEI string as input and reports (T/F)
# whether it passes all integrity checks.
def lei_check(lei):
    rv = False
    syntax_flaw = False
    if (len(lei) > 20):
        print('WARNING: LEI value is too long:', lei, len(lei))
        syntax_flaw = True
    if (len(lei) < 20):
        print('WARNING: LEI value is too short:', lei, len(lei))
        syntax_flaw = True
    if (lei.upper() != lei):
        print('WARNING: LEI value is not uppercase:', lei)
        syntax_flaw = True
    if (None==re.search(r'^[0-9A-Z]{18}[0-9]{2}$', lei)):
        print('WARNING: LEI does not match the official format:', lei)
        syntax_flaw = True
    if (lei[len(lei)-2:len(lei)] in ['00', '01', '99']):
        print('WARNING: LEI checkdigits in invalid range [00, 01, 99]:', lei)
        syntax_flaw = True
    if not(syntax_flaw):
        ASC_A = ord('A')
        buff = ''
        for i in range(len(lei)):
            char_i = lei[i]
            asc_i = ord(char_i)
            if (asc_i >= ASC_A):
                # Convert letters to their numeric index: A=10, B=11, C=12, etc.
                buff = buff + str(asc_i-55)
            else:
                # Convert digits (0-9) to their string equivalent
                buff = buff + str(char_i)
        # A valid (digitized) LEI should have modulus 1:
        check = int(buff) % 97
        rv = (1==check)
        if (False==rv):
            print('WARNING: LEI fails checksum:', lei, check)
    return rv

# The LEI incorporates two checksum digits at the final two positions.
# Checksum digits follow the procedure of ISO 7064 (mod 97-10), applied to
# strings from the character set: "0123456789ABCDEFGHIJKLMNOPQRSTUVWKYZ".
# This function takes a candidate LEI string as input and reports (T/F)
# whether it passes all integrity checks.
def lei_check_logged(lei):
    rv = False
    syntax_flaw = False
    if (len(lei) > 20):
        print('WARNING: LEI value is too long:', lei, len(lei))
        syntax_flaw = True
    if (len(lei) < 20):
        print('WARNING: LEI value is too short:', lei, len(lei))
        syntax_flaw = True
    if (lei.upper() != lei):
        print('WARNING: LEI value is not uppercase:', lei)
        syntax_flaw = True
    if (None==re.search(r'^[0-9A-Z]{18}[0-9]{2}$', lei)):
        print('WARNING: LEI does not match the official format:', lei)
        syntax_flaw = True
    if (lei[len(lei)-2:len(lei)] in ['00', '01', '99']):
        print('WARNING: LEI checkdigits in invalid range [00, 01, 99]:', lei)
        syntax_flaw = True
    if not(syntax_flaw):
        ASC_A = ord('A')
        buff = ''
        for i in range(len(lei)):
            char_i = lei[i]
            asc_i = ord(char_i)
            if (asc_i >= ASC_A):
                # Convert letters to their numeric index: A=10, B=11, C=12, etc.
                buff = buff + str(asc_i-55)
            else:
                # Convert digits (0-9) to their string equivalent
                buff = buff + str(char_i)
        # A valid (digitized) LEI should have modulus 1:
        check = int(buff) % 97
        rv = (1==check)
        if (False==rv):
            print('WARNING: LEI fails checksum:', lei, check)
    return rv


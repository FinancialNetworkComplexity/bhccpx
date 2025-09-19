#!/usr/bin/env python2
import networkx as nx
import itertools

SPARSE_TRIADS = ('003', '012', '102')

def find_all_triads(BHC):
    """Examine BHC and return a dict of all the triads it contains"""
    triads = dict()
    for v in sorted(BHC):
        vnbrs = set(BHC.pred[v]) | set(BHC.succ[v])
        for x in itertools.combinations(vnbrs,2):
            (tkey, triad) = triad_key(v, x[0], x[1])
            tcode = nx.algorithms.triads.TRICODE_TO_NAME[
              nx.algorithms.triads._tricode(BHC, triad[0], triad[1], triad[2])]
            if not(tcode in SPARSE_TRIADS) and not(tkey in triads):
                triads[tkey] = tuple([triad, tcode])
    return triads

def triad_key(rssdA, rssdB, rssdC):
    """Returns a single long variable that encodes the identity of a triad"""
    triad = tuple(sorted((rssdA, rssdB, rssdC)))
    tkey = triad[0]*10000000*10000000 + triad[1]*10000000 + triad[2]
    return tkey, triad

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

import getopt
import sys
import os
import logging.config as logcfg
import logging

import numpy as np 
import pandas as pd
import configparser as cp
import json
import pickle as pkl
from dataclasses import dataclass
from functools import total_ordering


@total_ordering
@dataclass
class AsOfDate:
    year: int
    quarter: int
    month: int
    day: int

    @staticmethod
    def _quarter_end_date(quarter: int) -> tuple[int, int]:
        if quarter == 1:
            return (3, 31)
        elif quarter == 2:
            return (6, 30)
        elif quarter == 3:
            return (9, 30)
        elif quarter == 4:
            return (12, 31)
        raise ValueError(f"Invalid quarter: {quarter}. Quarter must be in 1..4.")

    @staticmethod
    def from_YQ(year: int, quarter: int) -> "AsOfDate":
        if quarter not in [1, 2, 3, 4]:
            raise ValueError(f"Invalid quarter: {quarter}. Quarter must be in 1..4.")
        month, day = AsOfDate._quarter_end_date(quarter)
        return AsOfDate(year, quarter, month, day)
    
    @staticmethod
    def from_YQ_str(yq_str: str) -> "AsOfDate":
        if len(yq_str) != 6 or yq_str[4] != 'Q':
            raise ValueError(f"Invalid yq_str format: {yq_str}. Expected format is 'YYYYQQ'.")
        year = int(yq_str[0:4])
        quarter = int(yq_str[5:6])
        return AsOfDate.from_YQ(year, quarter)
    
    def to_YQ_str(self) -> str:
        return f"{self.year:04d}Q{self.quarter}"
    
    @staticmethod
    def from_int(asof_int: int) -> "AsOfDate":
        y, m, d = asof_int // 10000, (asof_int % 10000) // 100, asof_int % 100
        q = ((m - 1) // 3) + 1
        return AsOfDate(y, q, m, d)
    
    def __int__(self) -> int:
        return self.year * 10000 + self.month * 100 + self.day
    
    @staticmethod
    def from_str(asof_str: str) -> "AsOfDate":
        if len(asof_str) != 8:
            raise ValueError(f"Invalid asof_str format: {asof_str}. Expected format is 'YYYYMMDD'.")
        y = int(asof_str[0:4])
        m = int(asof_str[4:6])
        d = int(asof_str[6:8])
        q = ((m - 1) // 3) + 1
        return AsOfDate(y, q, m, d)

    def __str__(self):
        return f"{self.year:04d}{self.month:02d}{self.day:02d}"
    
    def __repr__(self):
        return str(self)
    
    def __eq__(self, other: "AsOfDate"):
        return (self.year, self.month, self.day) == (other.year, other.month, other.day)
    
    def __gt__(self, other: "AsOfDate"):
        return (self.year, self.month, self.day) > (other.year, other.month, other.day)
    
    def __hash__(self):
        return hash(str(self))
    
    def nextq(self) -> "AsOfDate":
        if self.quarter == 4:
            return AsOfDate.from_YQ(self.year + 1, 1)
        else:
            return AsOfDate.from_YQ(self.year, self.quarter + 1)
    
    def prevq(self) -> "AsOfDate":
        if self.quarter == 1:
            return AsOfDate.from_YQ(self.year - 1, 4)
        else:
            return AsOfDate.from_YQ(self.year, self.quarter - 1)
    
    @staticmethod
    def most_recent(year: int, month: int) -> "AsOfDate":
        return AsOfDate.from_YQ(year, ((month - 1) // 3) + 1)
    
    @staticmethod
    def make_range(d0: 'AsOfDate', d1: 'AsOfDate', logger=logging) -> list['AsOfDate']:
        asofs = []
        if d0 > d1:
            logger.error('End date, %s, precedes start date, %s', d0, d1)
        if d0.year == d1.year:
            # Full range is within one year
            for q in range(d0.quarter, d1.quarter+1):
                asofs.append(AsOfDate.from_YQ(d0.year, q))
        else:
            # For the (possibly partial) first year in the range
            for q in range(d0.quarter, 5):
                asofs.append(AsOfDate.from_YQ(d0.year, q))
            # For the interior (full) years in the range
            for y in range(d0.year+1, d1.year):
                for q in range(1,5):
                    asofs.append(AsOfDate.from_YQ(y, q))
            # For the (possibly partial) last year in the range
            for q in range(1,d1.quarter+1):
                asofs.append(AsOfDate.from_YQ(d1.year, q))
        return asofs

    @staticmethod
    def make_range_from_YQ_strs(d0_str: str, d1_str: str, logger=logging) -> list['AsOfDate']:
        d0, d1 = AsOfDate.from_YQ_str(d0_str), AsOfDate.from_YQ_str(d1_str)
        return AsOfDate.make_range(d0, d1, logger=logger)


@dataclass
class NICData:
    attributes: pd.DataFrame
    relationships: pd.DataFrame
    highholders: set[int]
    entities: set[int]
    parents: dict[int, set[int]]
    offspring: dict[int, set[int]]


def parse_command_line(argv, config, modulefile):
    """Parses command-line arguments and overrides items in the config.
    
    This method assumes that the Python ConfigParser has already read in
    a config object (during module import), and that additional arguments
    (argv) are available as command-line parameters. The following 
    parameters (all optional) are recognized:
        
        * -c   Print the config dictionary for this module to stdout
        * -l <loglevel_file>      Set the logging threshold for file output
        * -L <loglevel_console>   Set the logging threshold for console output
        * -C <configfile>   Read custom configuration from a separate file
        * -h | --help   Print usage help and quit
        * -p <paramkey>:<paramval>  Override/set individual config parameters
    """
    usagestring = ('python ' + modulefile +
                  ' [-c]' +
                  ' [-C <configfile>]' +
                  ' [-l <loglevel_file>]' +
                  ' [-L <loglevel_console>]' +
                  ' [-h | --help]' +
                  ' [-p <paramkey>:<paramval>]')
    section = os.path.splitext(os.path.basename(modulefile))[0]
    showconfig = False
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "hcl:L:C:p:", ["help"])
        except getopt.error as msg:
            raise Usage(msg)
        for o, a in opts:
            # Scan through once, to see if a special config is named
            if "-C"==o:
                cfgfile = a
                config = read_config(cfgfile)
        for o, a in opts:
            # Now we have the right config file, get the parameters
            if "-p"==o:
                [paramkey, paramval] = a.split(':')
                config[section][paramkey] = paramval
            if "-C"==o:
                pass
            elif "-c"==o:
                showconfig = True
            elif "-l"==o:
                config['handler_file']['level'] = a
            elif "-L"==o:
                config['handler_console']['level'] = a
            elif o in ("-h", "--help"):
                print(usagestring)
                sys.exit()
            else:
                assert False, "unhandled option: " + o
    except Usage as err:
        print(err.msg, file=sys.stderr)
        print("for help use --help", file=sys.stderr)
        raise err
    
    if showconfig:
        print_config(config, modulefile)
    return config

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg



def read_config(config_file=os.path.join(os.path.dirname(__file__), 'BHCCPX.ini')):
    """Reads the application configuration from the BHCCPX.ini file"""
    config = cp.ConfigParser(interpolation=cp.ExtendedInterpolation())
    config.read(config_file, encoding='utf-8')
    # It is safe to configure logging repeatedly; extra calls get ignored
    log_dir = config.get('handler_file', 'args')
    log_dir = log_dir.split(sep="'")[1]
    log_dir = os.path.split(log_dir)[0]
    os.makedirs(log_dir, exist_ok=True)
    logcfg.fileConfig(config_file)
    return config



def print_config(config, modulefile):
    """
    Simple formatted dump of the config parameters relevant for a given
    configuration section. Useful for debugging.
    """
    section = os.path.splitext(os.path.basename(modulefile))[0]
    print('-------------------------- CONFIG ----------------------------')
    print('Current working directory:', os.getcwd())
    print('[DEFAULT]')
    config_dict = dict(config['DEFAULT'])
    for k,v in sorted(config_dict.items()):
        print(k, '=', v)
    print('['+section+']')
    config_dict = dict(config[section])
    for k,v in sorted(config_dict.items()):
        print(k, '=', v)
    print('--------------------------------------------------------------')



def ATTcsv2df(csvfile, nicsource: str, filter_asofdate: AsOfDate | None = None) -> pd.DataFrame:
    """
    Converts a tab-delimited CSV file containing NIC attributes data into a Pandas DataFrame.
    The DataFrame is indexed on the ID_RSSD column, and includes an additional
    'NICsource' column indicating the nature of the node ('A', 'B', or 'C').

    :param csvfile: An open, readable pointer to a tab-delimited CSV file that
    contains the information from a NIC attributes download
    :type csvfile: TextIOWrapper
    :param nicsource: A single character indicating the nature of the node.
        'A' indicates an "active" or going-concern node.
        'B' indicates a "branch" of an active node; not a distinct entity.
        'C' indicates a "closed" or "inactive" node.
    :type nicsource: str
    :param filter_asofdate: Date to perform filtering by; filtering not performed if None provided
    :type filter_asofdate: AsOfDate | None
    :return: A Pandas DataFrame indexed on ID_RSSD, with an additional 'NICsource' column.
    :rtype: pd.DataFrame
    """
    DTYPES_ATT = {
        'ACT_PRIM_CD': object, 
        'AUTH_REG_DIST_FRS': np.int8, 
        'BHC_IND': np.int8, 
        'BNK_TYPE_ANALYS_CD': np.int8, 
        'BROAD_REG_CD': np.int8, 
        'CHTR_AUTH_CD': np.int8, 
        'CHTR_TYPE_CD':np.int16, 
        'CITY': object, 
        'CNSRVTR_CD': np.int8, 
        'CNTRY_CD': np.int32, 
        'CNTRY_INC_CD': np.int32, 
        'CNTRY_INC_NM': object, 
        'CNTRY_NM': object, 
        'COUNTY_CD': np.int32, 
        'DIST_FRS': np.int8, 
        'DOMESTIC_IND': object, 
        'DT_END': np.int32, 
        'DT_EXIST_CMNC': np.int32, 
        'DT_EXIST_TERM': np.int32, 
        'DT_INSUR': np.int32, 
        'DT_OPEN': np.int32, 
        'DT_START': np.int32, 
        'D_DT_END': object, 
        'D_DT_EXIST_CMNC': object, 
        'D_DT_EXIST_TERM': object, 
        'D_DT_INSUR': object, 
        'D_DT_OPEN': object, 
        'D_DT_START': object, 
        'ENTITY_TYPE': object, 
        'EST_TYPE_CD': np.int8, 
        'FBO_4C9_IND': np.int8, 
        'FHC_IND': np.int8, 
        'FISC_YREND_MMDD':np.int16, 
        'FNCL_SUB_HOLDER': np.int8, 
        'FNCL_SUB_IND': np.int8, 
        'FUNC_REG': np.int8, 
        'IBA_GRNDFTHR_IND': np.int8, 
        'IBF_IND': np.int8, 
        'ID_ABA_PRIM': np.int32, 
        'ID_CUSIP': object, 
        'ID_FDIC_CERT': np.int32, 
        'ID_LEI': object, 
        'ID_NCUA': np.int32, 
        'ID_OCC': np.int32, 
        'ID_RSSD': np.int32, 
        'ID_RSSD_HD_OFF': np.int32, 
        'ID_TAX': np.int32, 
        'ID_THRIFT': np.int32, 
        'ID_THRIFT_HC': object, 
        'INSUR_PRI_CD': np.int8, 
        'MBR_FHLBS_IND': bool,
        'MBR_FRS_IND': bool,
        'MJR_OWN_MNRTY': np.int8, 
        'NM_LGL': object, 
        'NM_SHORT': object, 
        'NM_SRCH_CD': np.int32, 
        'ORG_TYPE_CD': np.int8, 
        'PLACE_CD': np.int32, 
        'PRIM_FED_REG': object, 
        'PROV_REGION': object, 
        'REASON_TERM_CD': np.int8, 
        'SEC_RPTG_STATUS': np.int8, 
        'SLHC_IND': bool,
        'SLHC_TYPE_IND': np.int8,
        'STATE_ABBR_NM': object, 
        'STATE_CD': np.int8, 
        'STATE_HOME_CD': np.int8, 
        'STATE_INC_ABBR_NM': object, 
        'STATE_INC_CD': np.int8, 
        'STREET_LINE1': object, 
        'STREET_LINE2': object, 
        'URL': object, 
        'ZIP_CD': object
    }
    ATTdf = pd.read_csv(csvfile, dtype=DTYPES_ATT)
    ATTdf.rename(columns={'#ID_RSSD': 'ID_RSSD'}, inplace=True)
    ATTdf['rssd'] = ATTdf['ID_RSSD']
    if filter_asofdate is not None:
        ATTdf = ATTdf[ATTdf.DT_END >= int(filter_asofdate)]
        ATTdf = ATTdf[ATTdf.DT_OPEN <= int(filter_asofdate)]
    ATTdf.insert(len(ATTdf.columns), 'NICsource', nicsource, allow_duplicates=True)
    ATTdf.reset_index(inplace=True)
    ATTdf.set_index(['rssd'], inplace=True)
    ATTdf.sort_index(inplace=True)
    return ATTdf


      
def RELcsv2df(csvfile, filter_asofdate: AsOfDate | None = None) -> pd.DataFrame:
    """
    Converts a tab-delimited CSV file containing NIC relationship data into a Pandas DataFrame.
    The DataFrame is indexed on the composite key (ID_RSSD_PARENT, ID_RSSD_OFFSPRING, DT_START, DT_END).
    There is a separate function, REL_IDcols(), for identifying the column
    numbers associated with each of these four index columns.

    :param csvfile: An open, readable pointer to a tab-delimited CSV file.
    :type csvfile: TextIOWrapper
    :param filter_asofdate: Date to perform filtering by; filtering not performed if None provided
    :type filter_asofdate: AsOfDate | None
    :return: A Pandas DataFrame indexed on ID_RSSD, with an additional 'NICsource' column.
    :rtype: pd.DataFrame
    """
    DTYPES_REL = {
        'CTRL_IND': np.int8, 
        'DT_RELN_EST': object, 
        'DT_START': np.int32, 
        'DT_END': np.int32, 
        'D_DT_RELN_EST': object, 
        'D_DT_START': object, 
        'D_DT_END': object, 
        'EQUITY_IND': np.int8, 
        'FC_IND': np.int8, 
        'ID_RSSD_OFFSPRING': np.int32, 
        'ID_RSSD_PARENT': np.int32, 
        'MB_COST': np.float64, 
        'OTHER_BASIS_IND': np.int8, 
        'PCT_EQUITY': np.float64, 
        'PCT_EQUITY_BRACKET': object, 
        'PCT_EQUITY_FORMAT': object, 
        'PCT_OTHER': np.float64, 
        'REASON_ROW_CRTD': np.int8, 
        'REASON_TERM_RELN': np.int8, 
        'REGK_INV': np.int8, 
        'REG_IND': np.int8, 
        'RELN_LVL': np.int8
    }
    RELdf = pd.read_csv(csvfile, dtype=DTYPES_REL)
    RELdf.rename(columns={'#ID_RSSD_PARENT': 'ID_RSSD_PARENT'}, inplace=True)
    if filter_asofdate is not None:
        RELdf = RELdf[RELdf.DT_START <= int(filter_asofdate)]
        RELdf = RELdf[RELdf.DT_END >= int(filter_asofdate)]
    RELdf.reset_index(inplace=True)
    RELdf.set_index(['ID_RSSD_PARENT', 'ID_RSSD_OFFSPRING', 'DT_START', 'DT_END'], inplace=True)
    RELdf.sort_index(inplace=True)
    return RELdf
    

def REL_IDcols(RELdf: pd.DataFrame) -> tuple[int, int, int, int]:
    """
    A convenience function to look up and return the column number for the
    four columns composing the index in the relationships dataframe.
    See the function RELcsv2df for further details.
    """
    ID_RSSD_PARENT = RELdf.index.names.index('ID_RSSD_PARENT')
    ID_RSSD_OFFSPRING = RELdf.index.names.index('ID_RSSD_OFFSPRING')
    DT_START = RELdf.index.names.index('DT_START')
    DT_END = RELdf.index.names.index('DT_END')
    return ID_RSSD_PARENT, ID_RSSD_OFFSPRING, DT_START, DT_END


def FAILcsv2df(csvfile):
    DTYPES_FAIL = {
        'CERT': np.int32, 
        'CHCLASS1': object, 
        'CITYST': object, 
        'COST': object, 
        'FAILDATE': object, 
        'FIN': np.int32, 
        'NAME': object, 
        'QBFASSET': np.int32, 
        'QBFDEP': np.int32, 
        'RESTYPE': object, 
        'RESTYPE1': object, 
        'SAVR': object, 
    }
    faildate_parser = lambda x: pd.datetime.strptime(x, '%m/%d/%y')
    nans = {'COST': [''], 'SAVR': ['***']}
    FAILdf = pd.read_csv(csvfile, dtype=DTYPES_FAIL, sep=',', 
      parse_dates=['FAILDATE'], date_parser=faildate_parser, na_values=nans)
    FAILdf['COST'] = pd.to_numeric(FAILdf.COST)
    FAILdf['cert'] = pd.to_numeric(FAILdf.CERT)
    FAILdf.reset_index(inplace=True)
    FAILdf.set_index(['cert'], inplace=True)
    return FAILdf


def maps_rssd_cert(DATA: NICData):
    rssd2cert = dict()
    cert2rssd = dict()
    ATTdf = DATA.attributes
    ATTdf = ATTdf[ATTdf.ID_FDIC_CERT > 0]
    for idx,row in ATTdf.iterrows():
        rssd = idx
        cert = row['ID_FDIC_CERT']
        rssd2cert[rssd] = cert
        cert2rssd[cert] = rssd
    return (rssd2cert, cert2rssd)

    
def augment_FAILdf(FAILdf, outdir, dataasof: AsOfDate):
    FAILdf.sort_values(by=['FAILDATE'], inplace=True)
    DATA = fetch_DATA(outdir, dataasof)
    ATTdf = DATA.attributes
    ATTdf = ATTdf[ATTdf.ID_FDIC_CERT > 0]
    rssd2cert, cert2rssd = maps_rssd_cert(DATA)
    FAILdf2 = FAILdf.copy(deep=True)
    FAILdf2['RSSD']=-1
    FAILdf2['RSSD_HH']=-1
    FAILdf2['ENTITY_TYPE']=''
    FAILdf2['CNTRY_NM']=''
    FAILdf2['STATE_ABBR_NM']=''
    rcntasof = AsOfDate.from_YQ(-1, 1)
    BankSys = None
    for idx, row in FAILdf.iterrows():
        failasof = FAILdf.loc[idx]['FAILDATE']
        failasof = AsOfDate.most_recent(failasof.year, failasof.month)
        if rcntasof != failasof:
            rcntasof = failasof
            sysfilename = 'NIC_'+str(rcntasof)+'.pkl'
            sysfilepath = os.path.join(outdir, sysfilename)
            with open(sysfilepath, 'rb') as f:
                BankSys = pkl.load(f)
        cert = FAILdf.loc[idx]['CERT']
        rssd = cert2rssd[cert]
        NICdict = ATTdf.loc[rssd].to_dict()
        FAILdf2.loc[cert,('RSSD')] = rssd
        FAILdf2.loc[cert,('RSSD_HH')] = rssd
#        FAILdf2.loc[cert,('DT_OPEN')] = NICdict['DT_OPEN']
#        FAILdf2.loc[cert,('DT_START')] = NICdict['DT_START']
#        FAILdf2.loc[cert,('DT_END')] = NICdict['DT_END']
        FAILdf2.loc[cert,('ENTITY_TYPE')] = NICdict['ENTITY_TYPE']
#        FAILdf2.loc[cert,('CNTRY_CD')] = NICdict['CNTRY_CD']
        FAILdf2.loc[cert,('COUNTRY')] = NICdict['CNTRY_NM'].strip()
        FAILdf2.loc[cert,('STATE')] = NICdict['STATE_ABBR_NM']
    return FAILdf2


def makeDATA(indir, file_attA, file_attB, file_attC, file_rel, asofdate: AsOfDate, logger=logging) -> NICData:
    """A function to assemble the NIC data for given asofdate into a single object."""
    ATTdf = makeATTs(indir, file_attA, file_attB, file_attC)
    csvfilepathR = os.path.join(indir, file_rel)
    RELdf = RELcsv2df(csvfilepathR)
    highholders, entities, parents, offspring = NIC_highholders(RELdf, asofdate, logger=logger)
    return NICData(
        attributes=ATTdf,
        relationships=RELdf,
        highholders=highholders,
        entities=entities,
        parents=parents,
        offspring=offspring
    )


def makeATTs(indir, file_attA, file_attB, file_attC, filter_asofdate: AsOfDate | None = None) -> pd.DataFrame:
    csvfilepathA = os.path.join(indir, file_attA)
    csvfilepathB = os.path.join(indir, file_attB)
    csvfilepathC = os.path.join(indir, file_attC)
    ATTdf_a = ATTcsv2df(csvfilepathA, 'A', filter_asofdate)
    ATTdf_b = ATTcsv2df(csvfilepathB, 'B', filter_asofdate)
    ATTdf_c = ATTcsv2df(csvfilepathC, 'C', filter_asofdate)
    ATTdf = pd.concat([ATTdf_a, ATTdf_b, ATTdf_c])
    return ATTdf


def fetch_DATA(outdir, asofdate: AsOfDate, indir=None, fA=None, fB=None, fC=None, fREL=None, logger=logging) -> NICData:
    DATA = None
    datafilename = f"DATA_{asofdate}.pkl"
    datafilepath = os.path.join(outdir, datafilename)
    nonefiles = (indir is None or fA is None or fB is None or fC is None or fREL is None)
    if os.path.isfile(datafilepath):
        with open(datafilepath, 'rb') as f:
            DATA: NICData = pkl.load(f)
    elif not nonefiles:
        DATA = makeDATA(indir, fA, fB, fC, fREL, asofdate, logger=logger)
        with open(datafilepath, 'wb') as f:
            pkl.dump(DATA, f)
    return DATA


def NIC_highholders(RELdf, asofdate: AsOfDate, logger=logging) -> tuple[set[int], dict[int, set[int]], dict[int, set[int]], set[int]]:
    """
    A function to walk through the rows of the relationships dataframe,
    creating four key derived objects:
        * entities is the set of all NIC nodes appearing the relationships, 
            either as parents or offspring
        * parents is a dictionary, keyed by individual node_ids, of the 
            set of immediate parents of each node
        * offspring is a dictionary, keyed by individual node_ids, of the 
            set of immediate offspring of each node
        * high_holders is the set of all high-holder entities, defined as any
          node with no immediate parent
    Note that a high-holder node will have an entry in the parents dict, but
    this entry will point to an empty set (high holders have no parents)

    :param RELdf: The relationships dataframe
    :type RELdf: pd.DataFrame
    :param asofdate: The as-of date for which the relationships are valid
    :type asofdate: AsOfDate
    :return: A tuple containing (high_holders, entities, parents, offspring)
    :rtype: tuple[set[int], dict[int, set[int]], dict[int, set[int]], set[int]]
    """
    ID_RSSD_PARENT, ID_RSSD_OFFSPRING, DT_START, DT_END = REL_IDcols(RELdf)
    # Create some containers for derived structures
    parents = {}     # Dictionary of immediate parents (a set) for each node
    offspring = {}   # Dictionary of immediate children (a set) for each node
    entities = set()
    high_holders = set()
    # Loop through Relationships to assemble entities, parents, and offspring
    for row in RELdf.iterrows():
        date0 = AsOfDate.from_int(row[0][DT_START])
        date1 = AsOfDate.from_int(row[0][DT_END])
        rssd_par = row[0][ID_RSSD_PARENT]
        rssd_off = row[0][ID_RSSD_OFFSPRING]
        if asofdate < date0 or date1 < asofdate:
            # logger.warning(
            #     'asofdate: %s in out of bounds: %d, %d, %s, %s in NIC_highholders',
            #     asofdate, rssd_par, rssd_off, date0, date1
            # )
            continue   
        entities.add(rssd_par)
        try:
            offspring[rssd_par].add(rssd_off)
        except KeyError:
            offspring[rssd_par] = set()
            offspring[rssd_par].add(rssd_off)
        entities.add(rssd_off)
        try:
            parents[rssd_off].add(rssd_par)
        except KeyError:
            parents[rssd_off] = set()
            parents[rssd_off].add(rssd_par)
    # Filter entities to find the high_holders
    for ent in entities:
        try:
            len(parents[ent])      # Count the parents, if they exist
        except KeyError:
            high_holders.add(ent)  # High holders are those w/zero parents
    return high_holders, entities, parents, offspring


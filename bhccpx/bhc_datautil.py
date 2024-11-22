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
import logging
import logging.config as logcfg

import numpy as np 
import pandas as pd
import configparser as cp
import _pickle as pik 

LOG = logging.getLogger(__file__.split(os.path.sep)[-1].split('.')[0])

# Mnemonic indices for the DATA list, as documented below in makeDATA()
IDX_Attributes = 0
IDX_Relationships = 1
IDX_HighHolder = 2
IDX_Entities = 3
IDX_Parents = 4
IDX_Offspring = 5


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
    usagestring = ('python '+modulefile+
                  ' [-c]'+
                  ' [-C <configfile>]'+
                  ' [-l <loglevel_file>]'+
                  ' [-L <loglevel_console>]'+
                  ' [-h | --help]'+
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
                assert False, "unhandled option: "+o
    except Usage as err:
        print(err.msg, file=sys.stderr)
        print("for help use --help", file=sys.stderr)
        return 2
    if (showconfig):
        print_config(config, modulefile)
    return config
class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg



# Reads the application configuration from the bhc_complex.ini file
def read_config(config_file='bhc_complex.ini'):
    config = cp.ConfigParser(interpolation=cp.ExtendedInterpolation())
    config.read(config_file)
    # It is safe to configure logging repeatedly; extra calls get ignored
    log_dir = config['handler_file']['args']
    print('LLL', log_dir)
    log_dir = log_dir.split(sep="'")[1]
    log_dir = os.path.split(log_dir)[0]
    print('LL2', log_dir)
    os.makedirs(log_dir, exist_ok=True)
    logcfg.fileConfig(config_file)
    return config

# Simple formatted dump of the config parameters relevant for a given 
# configuration section. Useful for debugging.
def print_config(config, modulefile):
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


# Simple formatted dump of the config parameters relevant for a given 
# configuration section. Useful for debugging.
def verbosity(config):
    verbose = ('TRUE'==config['DEFAULT']['verbose'].upper())
    veryverbose = ('TRUE'==config['DEFAULT']['veryverbose'].upper())
    return (verbose, veryverbose)


# This function takes an (open) CSV file and an asofdate as inputs:
#  -- csvfile should be an open, readable pointer to a tab-delimited CSV 
#     file that contains the information from a NIC attributes download
#  -- asofdate is an integer value of the form YYYYMMDD
#  -- nicsource is a single character that indicating the nature of the node:
#        -- 'A' indicates an "active" or going-concern node
#        -- 'B' indicates a "branch" of an active node; not a distinct entity
#        -- 'C' indicates a "closed" or "inactive" node
# Note that NIC downloads start in XML format; you must convert this from
# XML to tab-delimited CSV before using this function.
# The contents of the CSV file are converted to appropriate primitive types
# and stored in a Pandas dataframe, which is returned. 
# The returned dataframe is indexed on one field: ID_RSSD.
# In addition, this function adds a 'NICsource' column (not in the source CSV) 
# to the dataframe, which indicates the nature (A/B/C) of the NIC source.
def ATTcsv2df(csvfile, asofdate, nicsource, filter_asof=False):
    DTYPES_ATT = {
        'ACT_PRIM_CD':object, 
        'AUTH_REG_DIST_FRS':np.int8, 
        'BHC_IND':np.int8, 
        'BNK_TYPE_ANALYS_CD':np.int8, 
        'BROAD_REG_CD':np.int8, 
        'CHTR_AUTH_CD':np.int8, 
        'CHTR_TYPE_CD':np.int16, 
        'CITY':object, 
        'CNSRVTR_CD':np.int8, 
        'CNTRY_CD':np.int32, 
        'CNTRY_INC_CD':np.int32, 
        'CNTRY_INC_NM':object, 
        'CNTRY_NM':object, 
        'COUNTY_CD':np.int32, 
        'DIST_FRS':np.int8, 
        'DOMESTIC_IND':object, 
        'DT_END':np.int32, 
        'DT_EXIST_CMNC':np.int32, 
        'DT_EXIST_TERM':np.int32, 
        'DT_INSUR':np.int32, 
        'DT_OPEN':np.int32, 
        'DT_START':np.int32, 
        'D_DT_END':object, 
        'D_DT_EXIST_CMNC':object, 
        'D_DT_EXIST_TERM':object, 
        'D_DT_INSUR':object, 
        'D_DT_OPEN':object, 
        'D_DT_START':object, 
        'ENTITY_TYPE':object, 
        'EST_TYPE_CD':np.int8, 
        'FBO_4C9_IND':np.int8, 
        'FHC_IND':np.int8, 
        'FISC_YREND_MMDD':np.int16, 
        'FNCL_SUB_HOLDER':np.int8, 
        'FNCL_SUB_IND':np.int8, 
        'FUNC_REG':np.int8, 
        'IBA_GRNDFTHR_IND':np.int8, 
        'IBF_IND':np.int8, 
        'ID_ABA_PRIM':np.int32, 
        'ID_CUSIP':object, 
        'ID_FDIC_CERT':np.int32, 
        'ID_LEI':object, 
        'ID_NCUA':np.int32, 
        'ID_OCC':np.int32, 
        'ID_RSSD':np.int32, 
        'ID_RSSD_HD_OFF':np.int32, 
        'ID_TAX':np.int32, 
        'ID_THRIFT':np.int32, 
        'ID_THRIFT_HC':object, 
        'INSUR_PRI_CD':np.int8, 
        'MBR_FHLBS_IND':bool,        # Boolean
        'MBR_FRS_IND':bool,          # Boolean
        'MJR_OWN_MNRTY':np.int8, 
        'NM_LGL':object, 
        'NM_SHORT':object, 
        'NM_SRCH_CD':np.int32, 
        'ORG_TYPE_CD':np.int8, 
        'PLACE_CD':np.int32, 
        'PRIM_FED_REG':object, 
        'PROV_REGION':object, 
        'REASON_TERM_CD':np.int8, 
        'SEC_RPTG_STATUS':np.int8, 
        'SLHC_IND':bool,             # Boolean
        'SLHC_TYPE_IND':np.int8,
        'STATE_ABBR_NM':object, 
        'STATE_CD':np.int8, 
        'STATE_HOME_CD':np.int8, 
        'STATE_INC_ABBR_NM':object, 
        'STATE_INC_CD':np.int8, 
        'STREET_LINE1':object, 
        'STREET_LINE2':object, 
        'URL':object, 
        'ZIP_CD':object}
    ATTdf = pd.read_csv(csvfile, dtype=DTYPES_ATT, sep='\t')
    ATTdf['rssd'] = ATTdf['ID_RSSD']
    if (filter_asof):
        ATTdf = ATTdf[ATTdf.DT_END >= asofdate]
        ATTdf = ATTdf[ATTdf.DT_OPEN <= asofdate]
    ATTdf.insert(len(ATTdf.columns), 'NICsource', nicsource, allow_duplicates=True)
    ATTdf.reset_index(inplace=True)
    ATTdf.set_index(['rssd'], inplace=True)
    ATTdf.sort_index(inplace=True)
    return ATTdf


# This function takes an (open) csvfile and an asofdate as inputs
#  -- csvfile should be an open, readable pointer to a tab-delimited CSV 
#     file that contains the information from a NIC relationships download
#  -- asofdate is an integer value of the form YYYYMMDD
# NIC downloads typically start in XML format; you must convert this from
# XML to tab-delimited CSV before using this function
# The contents of the CSV file are converted to appropriate primitive types
# and stored in a Pandas dataframe, which RELcsv2df returns. 
# The returned dataframe is indexed (and sorted) on four fields:
#     ID_RSSD_PARENT, ID_RSSD_OFFSPRING, DT_START, and DT_END
# There is a separate function, REL_IDcols(), for identifying the column 
# numbers associated with each of these four index columns.        
def RELcsv2df(csvfile, asofdate, filter_asof=True):
    DTYPES_REL = {
        'CTRL_IND':np.int8, 
        'DT_RELN_EST':object, 
        'DT_START':np.int32, 
        'DT_END':np.int32, 
        'D_DT_RELN_EST':object, 
        'D_DT_START':object, 
        'D_DT_END':object, 
        'EQUITY_IND':np.int8, 
        'FC_IND':np.int8, 
        'ID_RSSD_OFFSPRING':np.int32, 
        'ID_RSSD_PARENT':np.int32, 
        'MB_COST':np.float64, 
        'OTHER_BASIS_IND':np.int8, 
        'PCT_EQUITY':np.float64, 
        'PCT_EQUITY_BRACKET':object, 
        'PCT_EQUITY_FORMAT':object, 
        'PCT_OTHER':np.float64, 
        'REASON_ROW_CRTD':np.int8, 
        'REASON_TERM_RELN':np.int8, 
        'REGK_INV':np.int8, 
        'REG_IND':np.int8, 
        'RELN_LVL':np.int8}
    RELdf = pd.read_csv(csvfile, dtype=DTYPES_REL, sep='\t')
    if (filter_asof):
        RELdf = RELdf[RELdf.DT_START <= asofdate]
        RELdf = RELdf[RELdf.DT_END >= asofdate]
    RELdf.reset_index(inplace=True)
    RELdf.set_index(['ID_RSSD_PARENT', 'ID_RSSD_OFFSPRING', 'DT_START', 'DT_END'], inplace=True)
    RELdf.sort_index(inplace=True)
    return RELdf
    

# A convenience function to look up and return the column number for the 
# four columns composing the index in the relationships dataframe. 
# See the function RELcsv2df for further details. 
def REL_IDcols(RELdf):
    # Get the column numbers to dereference the values packed in the multiindex
    ID_RSSD_PARENT = RELdf.index.names.index('ID_RSSD_PARENT')
    ID_RSSD_OFFSPRING = RELdf.index.names.index('ID_RSSD_OFFSPRING')
    DT_START = RELdf.index.names.index('DT_START')
    DT_END = RELdf.index.names.index('DT_END')
    return ID_RSSD_PARENT, ID_RSSD_OFFSPRING, DT_START, DT_END


def FAILcsv2df(csvfile):
    DTYPES_FAIL = {
        'CERT':np.int32, 
        'CHCLASS1':object, 
        'CITYST':object, 
        'COST':object, 
        'FAILDATE':object, 
        'FIN':np.int32, 
        'NAME':object, 
        'QBFASSET':np.int32, 
        'QBFDEP':np.int32, 
        'RESTYPE':object, 
        'RESTYPE1':object, 
        'SAVR':object, 
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


def maps_rssd_cert(DATA):
    rssd2cert = dict()
    cert2rssd = dict()
    ATTdf = DATA[IDX_Attributes]
    ATTdf = ATTdf[ATTdf.ID_FDIC_CERT > 0]
    for idx,row in ATTdf.iterrows():
        rssd = idx
        cert = row['ID_FDIC_CERT']
        rssd2cert[rssd] = cert
        cert2rssd[cert] = rssd
    return (rssd2cert, cert2rssd)


def stringify_qtrend(asofdate):
    """Converts an as-of date to a YYYYQQ string for the next quarter end
    """
    yyyy = int(asofdate/10000)
    mmdd = asofdate -yyyy*10000
    if (930 < mmdd):
        Nqtr = str(yyyy)+'Q4'
    elif (630 < mmdd):
        Nqtr = str(yyyy)+'Q3'
    elif (331 < mmdd):
        Nqtr = str(yyyy)+'Q2'
    elif (100 < mmdd):
        Nqtr = str(yyyy)+'Q1'
    return Nqtr
        
    
def next_qtrend(asofdate):
    """Constructs the next quarter-end date for a given as-of date
    """
    yyyy = int(asofdate/10000)
    mmdd = asofdate -yyyy*10000
    if (1231 == mmdd):
        MRqtr = asofdate
    elif (930 < mmdd):
        MRqtr = yyyy*10000 + 1231
    elif (630 < mmdd):
        MRqtr = yyyy*10000 + 930
    elif (331 < mmdd):
        MRqtr = yyyy*10000 + 630
    elif (100 < mmdd):
        MRqtr = yyyy*10000 + 331
    return MRqtr

    
def rcnt_qtrend(asofdate):
    """Constructs the next quarter-end date for a given as-of date
    """
    yyyy = int(asofdate/10000)
    mmdd = asofdate -yyyy*10000
    if (1231 == mmdd):
        MRqtr = asofdate
    elif (930 < mmdd):
        MRqtr = yyyy*10000 + 930
    elif (630 < mmdd):
        MRqtr = yyyy*10000 + 630
    elif (331 < mmdd):
        MRqtr = yyyy*10000 + 331
    elif (100 < mmdd):
        MRqtr = (yyyy-1)*10000 + 1231
    return MRqtr

    
def augment_FAILdf(FAILdf, outdir, dataasof):
    FAILdf.sort_values(by=['FAILDATE'], inplace=True)
    DATA = fetch_DATA(outdir, dataasof)
    ATTdf = DATA[IDX_Attributes]
    ATTdf = ATTdf[ATTdf.ID_FDIC_CERT > 0]
    (rssd2cert, cert2rssd) = maps_rssd_cert(DATA)
    FAILdf2 = FAILdf.copy(deep=True)
    FAILdf2['RSSD']=-1
    FAILdf2['RSSD_HH']=-1
    FAILdf2['ENTITY_TYPE']=''
    FAILdf2['CNTRY_NM']=''
    FAILdf2['STATE_ABBR_NM']=''
    rcntasof = -1
    banksys = None
    for idx,row in FAILdf.iterrows():
        failasof = FAILdf.loc[idx]['FAILDATE']
        failasof = failasof.year*10000 + failasof.month*100 + failasof.day
        if (rcntasof != rcnt_qtrend(failasof)):
            rcntasof = rcnt_qtrend(failasof)
            sysfilename = 'NIC_'+'_'+str(rcntasof)+'.pik'
            sysfilepath = os.path.join(outdir, sysfilename)
            f = open(sysfilepath, 'rb')
            banksys = pik.load(f)
            f.close()
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


# A function to assemble the NIC data for given asofdate into a single object.
# The returned DATA object is a list containing pointers to six objects, 
# indexed as follows:
#    IDX_Attributes    = 0
#    IDX_Relationships = 1
#    IDX_HighHolder    = 2
#    IDX_Entities      = 3
#    IDX_Parents       = 4
#    IDX_Offspring     = 5
# The first two elements are pandas dataframes, read from tab-delimited files
def makeDATA(indir, file_attA, file_attB, file_attC, file_rel, asofdate):
    DATA = []
    # First, populate DATA with the raw info from the CSV files:    
#    csvfilepathA = os.path.join(indir, file_attA)
#    csvfilepathB = os.path.join(indir, file_attB)
#    csvfilepathC = os.path.join(indir, file_attC)
#    ATTdf_a = ATTcsv2df(csvfilepathA, asofdate, 'A')
#    ATTdf_b = ATTcsv2df(csvfilepathB, asofdate, 'B')
#    ATTdf_c = ATTcsv2df(csvfilepathC, asofdate, 'C')
    ATTdf = makeATTs(indir, file_attA, file_attB, file_attC, asofdate)
    DATA.insert(IDX_Attributes, ATTdf)
    csvfilepathR = os.path.join(indir, file_rel)
    RELdf = RELcsv2df(csvfilepathR, asofdate)
    DATA.insert(IDX_Relationships, RELdf)
    # Then, add derived structures based on the Relationships data:    
    derived_data = NIC_highholders(RELdf, asofdate)
    DATA.insert(IDX_HighHolder, derived_data[0])
    DATA.insert(IDX_Entities, derived_data[1])
    DATA.insert(IDX_Parents, derived_data[2])
    DATA.insert(IDX_Offspring, derived_data[3])
    return DATA


def makeATTs(indir, file_attA, file_attB, file_attC, asofdate, filter_asof=False):
    csvfilepathA = os.path.join(indir, file_attA)
    csvfilepathB = os.path.join(indir, file_attB)
    csvfilepathC = os.path.join(indir, file_attC)
    ATTdf_a = ATTcsv2df(csvfilepathA, asofdate, 'A', filter_asof)
    ATTdf_b = ATTcsv2df(csvfilepathB, asofdate, 'B', filter_asof)
    ATTdf_c = ATTcsv2df(csvfilepathC, asofdate, 'C', filter_asof)
    ATTdf = pd.concat([ATTdf_a, ATTdf_b, ATTdf_c])
    return ATTdf


def fetch_DATA(outdir, asofdate, indir=None, fA=None, fB=None, fC=None, fREL=None):
    DATA = None
    datafilename = 'DATA_'+str(asofdate)+'.pik'
    datafilepath = os.path.join(outdir, datafilename)
    nonefiles = (None==indir or None==fA or None==fB or None==fC or None==fREL)
    if (os.path.isfile(datafilepath)):
        f = open(datafilepath, 'rb')
        DATA = pik.load(f)
        f.close()
    elif (not(nonefiles)):
        DATA = makeDATA(indir, fA, fB, fC, fREL, asofdate)
        f = open(datafilepath, 'wb')
        pik.dump(DATA, f)
        f.close()
    return DATA


#def fetch_banksys(sysfilepath, csvfilepath, asofdate):
##    DATA = None
##    datafilename = 'DATA_'+str(asofdate)+'.pik'
##    datafilepath = os.path.join(outdir, datafilename)
##    nonefiles = (None==indir or None==fA or None==fB or None==fC or None==fREL)
##    if (os.path.isfile(datafilepath)):
##        f = open(datafilepath, 'rb')
##        DATA = pik.load(f)
##        f.close()
##    elif (not(nonefiles)):
##        DATA = makeDATA(indir, fA, fB, fC, fREL, asofdate)
##        f = open(datafilepath, 'wb')
##        pik.dump(DATA, f)
##        f.close()
##    return DATA
#
#    BankSys = None
#    if os.path.isfile(sysfilepath):
##        if (veryverbose): print('FOUND: Banking system file path:   ', sysfilepath)
#        f = open(sysfilepath, 'rb')
#        BankSys = pik.load(f)
#        f.close()
#    else:
##        if (veryverbose): print('CREATING: Banking system file path:', sysfilepath, asofdate)
#        BankSys = nx.DiGraph()
##        if (veryverbose): print('CSV file path:', csvfilepath, asofdate)
#        RELdf = UTIL.RELcsv2df(csvfilepath, asofdate)
#        (ID_RSSD_PARENT, ID_RSSD_OFFSPRING, DT_START, DT_END) = UTIL.REL_IDcols(RELdf)
#        for row in RELdf.iterrows():
#            date0 = int(row[0][DT_START])
#            date1 = int(row[0][DT_END])
#            rssd_par = row[0][ID_RSSD_PARENT]
#            rssd_off = row[0][ID_RSSD_OFFSPRING]
#            if (asofdate < date0 or asofdate > date1):
#                if (verbose): print('ASOFDATE,', asofdate, 'out of bounds:', rssd_par, rssd_off, date0, date1)
#                continue   
#            BankSys.add_edge(rssd_par, rssd_off)
#        f = open(sysfilepath, 'wb')
#        pik.dump(BankSys, f)
#        f.close()
##    if (veryverbose): print('System as of '+str(asofdate)+' has', BankSys.number_of_nodes(), 'nodes and', BankSys.number_of_edges(), 'edges')
#    return BankSys


# The input here is a string of form YYYYQQ, for example, '1995Q3'
# The function splits this and returns a tuple:
#    yyyymmdd:  An int variable indicating year/mo/day, 19950930
#    y:         An int variable indicating the year, 1995
#    q:         An int variable indicating the quarter, 3
#    Q:         A string variable indicating the quarter, 'Q3'
def make_asof(YYYYQQ):
    MMDDs = [331, 630, 930, 1231]
    y = int(YYYYQQ[0:4])
    q = int(YYYYQQ[5:6])
    Q = YYYYQQ[4:6]
    yyyymmdd = y*10000+MMDDs[q-1]
    return yyyymmdd, y, q, Q
 

# Parses the strings Q0 and Q1, each of the form YYYYQQ (e.g., "1986Q2") into
# asofdate variables (of type int), each of the form YYYYMMDD (e.g., 19860630). 
# Every quarter-end asofdate between Q0 and Q1 (inclusive) is added to 
# the asofs list, which is returned. 
def assemble_asofs(YQ0, YQ1):
    asofs = []
    (yyyymmdd0, Y0, q0, Q0) =  make_asof(YQ0)
    (yyyymmdd1, Y1, q1, Q1) = make_asof(YQ1)
    if (yyyymmdd0 > yyyymmdd1):
        print('ERROR: End date,', yyyymmdd1, 'precedes start date,', yyyymmdd0)
    if (Y0 == Y1):
        # Full range is within one year
        for q in range(q0,q1+1):
            asofs.append(make_asof(str(Y0)+'Q'+str(q))[0])
    else:
        # For the (possibly partial) first year in the range
        for q in range(q0,5):
            asofs.append(make_asof(str(Y0)+'Q'+str(q))[0])
        # For the interior (full) years in the range
        for y in range(Y0+1, Y1):
            for q in range(1,5):
                asofs.append(make_asof(str(y)+'Q'+str(q))[0])
        # For the (possibly partial) last year in the range
        for q in range(1,q1+1):
            asofs.append(make_asof(str(Y1)+'Q'+str(q))[0])
    return asofs


# A function to walk through the rows of the relationships dataframe, 
# creating four key derived objects:
#  -- entities is the set of all NIC nodes appearing the relationships, 
#     either as parents or offspring
#  -- parents is a dictionary, keyed by individual node_ids, of the 
#     set of immediate parents of each node
#  -- offspring is a dictionary, keyed by individual node_ids, of the 
#     set of immediate offspring of each node
#  -- high_holders is the set of all high-holder entities, defined as any 
#     node with no immediate parent 
# Note that a high-holder node will have an entry in the parents dict, but
# this entry will point to an empty set (high holders have no parents)
def NIC_highholders(RELdf, asofdate):
    (ID_RSSD_PARENT, ID_RSSD_OFFSPRING, DT_START, DT_END) = REL_IDcols(RELdf)
    # Create some containers for derived structures
    parents = {}     # Dictionary of immediate parents (a set) for each node
    offspring = {}   # Dictionary of immediate children (a set) for each node
    entities = set()
    high_holders = set()
    # Loop through Relationships to assemble entities, parents, and offspring
    for row in RELdf.iterrows():
        date0 = int(row[0][DT_START])
        date1 = int(row[0][DT_END])
        rssd_par = row[0][ID_RSSD_PARENT]
        rssd_off = row[0][ID_RSSD_OFFSPRING]
        if (asofdate < date0 or asofdate > date1):
            print('WARNING: asofdate:', asofdate, 'in out of bounds:', rssd_par, rssd_off, date0, date1, 'in NIC_highholders')
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


#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import pandas as pd
import numpy as np
from pandas import Timestamp
from zipline import run_algorithm
from zipline.api import order, symbol, record

###@starq69: TBD
#from LW_min_max import LW_min_max as LW

def do_something(pd):
    pass

def initialize(context):

    context.barcount = 0
    context.pdd = pd.DataFrame()
    '''
    context.min_max = pd.dataframe()    ###@starq69
    min_max = {
               'val':None,  # min or max value
               'typ':None,  # type: min or max
               }

    '''

def handle_data(context, data):
    
    d = { 
         'open' : data.current(context.A, 'open'),
         'high' : data.current(context.A, 'high'),
         'low'  : data.current(context.A, 'low'),
         'close': data.current(context.A, 'close')
        }    
    
    idx = data.current(context.A, 'last_traded')
    
    df = pd.DataFrame(data=d, index=[idx])
    context.pdd = context.pdd.append(df)

    context.barcount += 1


def analyze(context=None, results=None):
    
    # do_something(context.pdd)
    pass 

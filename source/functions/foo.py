#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import pandas as pd


def foo (call_istance, subset, metadata={}):
    '''
    return a dict
    '''
    # https://stackoverflow.com/questions/15943769/how-do-i-get-the-row-count-of-a-pandas-dataframe
    #return {call_istance : len(subset.index)}
    #return {call_istance : subset.iloc[-1, 1]} # ultima riga (-1), seconda colonna (1 + 1)
    return {call_istance : subset['Volume'].mean()} # ultima riga (-1), seconda colonna (1 + 1)


def mean (call_istance, subset, metadata={}):

    return {call_istance : subset['Volume'].mean()} 


def gap (call_istance, subset, metadata={}):

    _high_col_index = 2
    _low_col_index = 3
    prev_high = subset.iloc[-2, _high_col_index] #.apply(pd.to_numeric())
    prev_low  = subset.iloc[-2, _low_col_index] #.apply(pd.to_numeric())

    return {call_istance : str(prev_high) + ' - ' + str(prev_low)}
    ''' 
    def i_gap(idx,h,l):
        if l > _high:
            return [idx, (((l - _high) / _high) * 100.0)]
        elif h < _low:
            return [idx, (((h - _low) / _low) * 100.0)]
        else:
            return None
        
    return i_gap
    '''



# callback mapping : lo posso generare su __init__.py ?
#
foo_cbmap = {'foo' : foo, 'gap' : gap, 'mean' : mean}


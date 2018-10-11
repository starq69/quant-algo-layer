#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import pandas as pd


def bar (call_istance, subset, metadata={}):
    '''
    return a dict
    '''
    return {call_istance : 'bar!'}




# callback mapping : lo posso generare su __init__.py ?
#
bar_cbmap = {'bar' : bar}


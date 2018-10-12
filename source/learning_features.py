#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import numpy as np
import pandas as pd
import json, time
from collections import OrderedDict

import functions

BUY = 'BUY'
SELL= 'SELL'

_HIGH_ = 'High'
dty = {   
        'Close': 'float64',
        _HIGH_: 'float64',
        'Low': 'float64',
        'Open': 'float64',
        'Volume': 'int64'
      }


def yeld_events_row (events, features_columns):   # feature_columns

    events_row = OrderedDict() 

    if not events:
        print('YELD EVENT ROW : NOT EVENTS')
        for key in features_columns:
            events_row [key] = None
        return events_row

    # crea l'event_dict (events_row) x il datapoint corrente
    #
    for key in features_columns:

        #print('KEY FIELD is : ' + str(key))
        try:
            if events [key]:
                events_row [key] = events [key]
            else:
                events_row [key] = None

        except KeyError as e:
            events_row [key] = None 

    return events_row


def execution_pool (active_indicators, compact=False, dtype=dty, ext_data={}):
    
    ai           = active_indicators 
    #max_duration = max({_:value['frequency'] for (_,value) in ai.items()}.values())
    ext_data     = ext_data

    def _attach_callback(compact):

        if compact:
            for _function, _istances in ai.items():
                for _istance in _istances:
                    _istance['callback'] = functions._mapping[_function]

        else:
            for k, param in ai.items():
                #print('k={},    v={}'.format(str(k), str(v)))
                if 'callback_function' in param:
                    param['callback'] = functions._mapping[param['callback_function']]
                    #print('k={},    v={}'.format(str(k), str(param)))
                    #print('callback : {}'.format(param['callback']))
                else:
                    print('NO callback function found for item {} ==> item discarded')
                    # tbd del(k)

    
    def _run(pdf, index): ### può essere un rif a pdf

        nonlocal ai, ext_data # period, count
        
        v = None
        events = {'Date' : index}

        for fx, param in ai.items():

            sup = pdf.index.get_loc(index) + 1
            inf = sup - param ['frequency'] 

            if sup >= param ['frequency'] :

                try: 
                    v = param['callback'] (fx, pdf.iloc [inf : sup ]) 

                except KeyError as e:
                    print('KEY ERROR : {}'.format(e))
                    continue

                events.update(v)
        return events


    def _run_compact(pdf, index):

        nonlocal ai
        v = None
        events = {'Date' : index}

        sup = pdf.index.get_loc(index) + 1

        for fx, _istances in ai.items():
            for _istance in _istances:
                inf = sup - _istance['frequency'] 
                if sup >= _istance['frequency']:
                    try: 
                        v = _istance['callback'] (_istance['name'], pdf.iloc [inf : sup ]) 
                    except KeyError as e:
                        print('KEY ERROR : {}'.format(e))
                        continue

                    events.update(v)
        return events


    _attach_callback(compact) 

    return _run_compact if compact else _run


def load_active_indicators (conf):

    with open(conf, "r") as _json:
        ai = json.load(_json) # TBD : ai = check_integrity(ai)
        return ai


def build_columns (ai_compact, index='Date'):

    columns = [index]

    for _function, _istances in ai_compact.items():
        if type(_istances) is list and _istances:
            for _istance in _istances:
                if type(_istance) is dict:
                    if 'name' in _istance:
                        columns.append(_istance['name'])
                    else:
                        print('warning : no name definend for function istance ==> discard')

    return columns


def eval_active_indicators (conf, compact=False, index='Date'):
    ai      = {}
    f_col   = []
    with open(conf, "r") as _json:
        ai = json.load(_json) # TBD : ai = check_integrity(ai)
    if compact:
        f_col= build_columns(ai)
        #print('f_col : {}'.format(f_col))
    else:
        f_col= ['Date', *ai]
    '''
    print ('eval :')
    print('ai : ' + str(ai))
    print('f_col : ' + str(f_col))
    '''
    return ai, f_col


def main():

    compact=True
    #compact=False

    print('............ pandas version is : ' + pd.__version__ )
    print('compact = {}'.format(str(compact)))


    #pdf = pd.read_csv('/home/starq/REP/DATA/FINANCE/Quotazioni/test-ai.csv' \
    pdf = pd.read_csv('/home/starq/REP/DATA/FINANCE/Quotazioni/NVDA.csv' \
                                 ,usecols=['Date','Open', 'High', 'Low', 'Close', 'Volume'] \
                                 ,dtype=dty,parse_dates=['Date'],infer_datetime_format=True \
                                 ,index_col='Date')

    #print(pdf.info(memory_usage='deep'))
    rows = 0
    events_rows = OrderedDict() 


    if compact:
        conf = "ext_active_indicators.json"
    else:
        conf = "active_indicators.json"

    ai, features_columns = eval_active_indicators (conf, compact)

    #print('ai : \n{}'.format(ai))
    print('features_columns : \n{}'.format(features_columns))

    start = time.clock()

    for datapoint in pdf.itertuples():

        if rows :
            events = run_step(pdf, datapoint.Index)      ### timestamp come idx ?
            #print ('main loop : events after calling run_step() are : [' + str(events) + ']')

            if events:
                events_dict = yeld_events_row(events, features_columns)

                #print('main loop : events FOUND')
                #print ('main loop : events passed to yeld : ' + str(events))
                #print ('main loop : events_dict after calling yeld(events) : ' + str(events_dict))

            else:
                events_dict = yeld_events_row({}, features_columns)

                #print('main loop : events NOT FOUND')
                #print ('main loop : events passed to yeld : ' + str(events))
                #print ('main loop : events_dict after calling yeld(events) : ' + str(events_dict))

        else:
            if compact:
                run_step = execution_pool(ai, True)  
            else:
                run_step = execution_pool(ai)  

            events      = run_step(pdf, datapoint.Index)
            events_dict = yeld_events_row(events, features_columns)

            #print('main loop : events_dict FIRST ROW is : ' + str(events_dict))

        for k, v in events_dict.items():
            events_rows.setdefault(k, []).append(v)

        rows += 1

    #print('events_rows is : ' + str(events_rows))
    elapsed = (time.clock() - start)
    print ('elapsed time to compute features : {}'.format(str(elapsed)))

    start = time.clock()
    ext_pdf = pd.DataFrame(events_rows)
    #print(ext_pdf)

    pdf = pdf.join(ext_pdf.set_index('Date'))
    elapsed = (time.clock() - start)
    print ('elapsed time to merge dataframes : {}'.format(str(elapsed)))

    print(pdf.to_string())
    #print(pdf.info(memory_usage='deep'))

    pdf.to_csv('stack_execution.csv')


if __name__ == '__main__':
    main()

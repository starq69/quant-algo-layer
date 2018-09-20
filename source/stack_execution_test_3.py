#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-


import numpy as np
import pandas as pd
import time
from collections import OrderedDict

_HIGH_ = 'High'
dty = {   
        'Close': 'float64',
        _HIGH_: 'float64',
        'Low': 'float64',
        'Open': 'float64',
        'Volume': 'int64'
      }

def foo (subset):
    # return a dict
    return {'foo' : subset.size}

ai = {'foo_call': {
                    'name' : 'foo_test',
                    'frequency' : 2,
                    'callback': foo,
                    'params' : {_HIGH_:dty[_HIGH_], 'Low':dty['Low']},   # axis(ohlcv) 
                    'ext_data' : {} # globals (min,max, histvol, TBD)
                  }
      }

pdf_ext_data_columns = {
                        '0' : 'LW-min',
                        '1' : 'LW-max',
                        '2' : 'GAP',
                        '3' : 'foo'
                        }

_ext_data_columns = ['Date', 'foo', 'LW-min', 'LW-max', 'GAP']


def yeld_events_row (events):

    events_row = OrderedDict() 

    if not events:
        print('YELD EVENT ROW : NOT EVENTS')
        for key in _ext_data_columns:
            events_row [key] = None
        return events_row

    # crea l'event_dict (events_row) x il datapoint corrente
    #
    for key in _ext_data_columns:

        print('KEY FIELD is : ' + str(key))
        try:
            if events [key]:
                events_row [key] = events [key]
            else:
                events_row [key] = None

        except KeyError as e:
            events_row [key] = None 

    return events_row


def execution_pool (active_indicators, dtype=dty, ext_data={}):
    
    ai           = active_indicators 
    max_duration = max({_:value['frequency'] for (_,value) in ai.items()}.values())
    ext_data     = ext_data

    period = max_duration
    count  = 1
    
    def _run(pdf, index): ### può essere un rif a pdf

        nonlocal ai, period, count, ext_data
        #count += 1
        
        v = None
        events = {'Date' : index}
        
        for fx in ai:
            
            if (count >= ai[fx]['frequency']):

                v = ai[fx]['callback'] (pdf.iloc[-ai[fx]['frequency']:]) ### ultimi fx['frequency'] elementi

                events.update(v)
                
        if count >= period: 
            pass

        count += 1
        return events
        
    return _run


def main():

    print('............ pandas version is : ' + pd.__version__ )

    pdf = pd.read_csv('/home/starq/REP/DATA/FINANCE/Quotazioni/test-ai.csv' \
    #pdf = pd.read_csv('/home/starq/REP/DATA/FINANCE/Quotazioni/NVDA.csv' \
                                 ,usecols=['Date','Open', 'High', 'Low', 'Close', 'Volume'] \
                                 ,dtype=dty,parse_dates=['Date'],infer_datetime_format=True \
                                 ,index_col='Date')

    print(pdf.info(memory_usage='deep'))

    rows = 0
    #events_rows = dict()
    events_rows = OrderedDict()
    #start = time.clock()

    for datapoint in pdf.itertuples():

        if rows :
            events = run_step(pdf, datapoint.Index)      ### timestamp come idx ?
            print ('main loop : events after calling run_step() are : [' + str(events) + ']')

            if events:
                print('main loop : events FOUND')
                events_dict = yeld_events_row(events)
                print ('main loop : events_dict after calling yeld(events) : ' + str(events_dict))

            else:
                print('main loop : events NOT FOUND')
                events_dict = yeld_events_row({})
                print ('main loop : events_dict after calling yeld(events) : ' + str(events_dict))

            for k, v in events_dict.items():
                events_rows.setdefault(k, []).append(v)
        else:
            run_step = execution_pool(ai)  

            events = run_step(pdf, datapoint.Index)
            events_dict = yeld_events_row(events)

            #events_dict = yeld_events_row({}) ###

            print('main loop : events_dict FIRST ROW is : ' + str(events_dict))

            for k, v in events_dict.items():
                events_rows.setdefault(k, []).append(v)

        rows += 1

    print('events_rows is : ' + str(events_rows))

    #pdf.insert(loc=0, column=columns, value=events_rows)

    ext_pdf = pd.DataFrame(events_rows)
    print(ext_pdf)
    pdf = pdf.join(ext_pdf.set_index('Date'))

    print(pdf.head())
    print(pdf.tail())
    print(pdf.info(memory_usage='deep'))

    pdf.to_csv('stack_execution.csv')


if __name__ == '__main__':
    main()

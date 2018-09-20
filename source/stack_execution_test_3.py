#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-


import numpy as np
import pandas as pd
import time

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

#
# events normalization:
#
def yeld_events_row (events):

    events_row = {}

    if not events:
        for key in pdf_ext_data_columns:
            events_row [pdf_ext_data_columns[key]] = None
        return events_row

    for key in pdf_ext_data_columns:

        #
        # crea l'event_dict (events_row) x il datapoint corrente
        #
        try:
            if events[pdf_ext_data_columns[key]]:
                events_row [pdf_ext_data_columns[key]] = events[pdf_ext_data_columns[key]]
            else:
                events_row [pdf_ext_data_columns[key]] = None

        except KeyError as e:
            events_row [pdf_ext_data_columns[key]] = None 

    return events_row


def execution_pool (active_indicators, dtype=dty, ext_data={}):
    
    ai           = active_indicators 
    max_duration = max({_:value['frequency'] for (_,value) in ai.items()}.values())
    ext_data     = ext_data

    period = max_duration
    count  = 1
    
    def _run(pdf): ### può essere un rif a pdf

        nonlocal ai, period, count, ext_data
        count += 1
        
        v = None
        events = {}
        
        for fx in ai:
            
            if (count >= ai[fx]['frequency']):

                v = ai[fx]['callback'] (pdf.iloc[-ai[fx]['frequency']:]) ### ultimi fx['frequency'] elementi

                events.update(v)
                
        if count >= period: 
            pass

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
    events_rows = dict()
    #start = time.clock()

    for datapoint in pdf.itertuples():

        print ('datapoint ..........: ' + str(datapoint.Index))

        if rows :
            events = run_step(pdf)      ### timestamp come idx ?

            if events:
                #events_dict = yeld_events_row(run_step(pdf, datapoint.Index)) ## new
                events_dict = yeld_events_row(events)

            else:
                events_dict = yeld_events_row({})

            for k, v in events_dict.items():
                events_rows.setdefault(k, []).append(v)
        else:
            run_step = execution_pool(ai)  

            events_dict = yeld_events_row({})

            for k, v in events_dict.items():
                events_rows.setdefault(k, []).append(v)

        rows += 1

    columns =  list(pdf_ext_data_columns.values())
    print('events_rows....')
    print(str(events_rows))

    #print('pdf index size : ' + str(len(pdf.index)))
    #pdf.insert(loc=0, column=columns, value=events_rows)

    ext_pdf = pd.DataFrame(events_rows)
    print(ext_pdf)

    pdf = pdf.join(ext_pdf)
    #pdf = pdf.join(pd.DataFrame(events_rows))
    #pdf = pd.concat([pdf, pd.DataFrame(events_rows.values()).T], ignore_index=True, axis=1)
    print(pdf.head())
    print(pdf.info(memory_usage='deep'))


if __name__ == '__main__':
    main()

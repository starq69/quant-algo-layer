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

#        for i in range (len(pdf_ext_data_columns)):
#            events_row [pdf_ext_data_columns[str(i)]] = None
#        return events_row


    #for i in range (len(pdf_ext_data_columns)): ### pdf_mapped_events
    for key in pdf_ext_data_columns:

        #
        # crea l'event_dict (events_row) x il datapoint corrente
        #
        try:
            if events[pdf_ext_data_columns[key]]:
                events_row [pdf_ext_data_columns[key]] = events[pdf_ext_data_columns[key]]
            else:
                events_row [pdf_ext_data_columns[key]] = None
            '''
            if events[pdf_ext_data_columns[str(i)]]:
                events_row [pdf_ext_data_columns[str(i)]] = events[pdf_ext_data_columns[str(i)]]
            else:
                events_row [pdf_ext_data_columns[str(i)]] = None
            '''
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
        #events = list()  ### dovrebbe essere un dict ??
        events = {}
        
        for fx in ai:
            
            if (count >= ai[fx]['frequency']):
                #
                print('...running ' + ai[fx]['name'])

                v = ai[fx]['callback'] (pdf.iloc[-ai[fx]['frequency']:]) ### ultimi fx['frequency'] elementi
                print('result is ..... ' + str(v))

                #
                # v potrebbe essere :
                # {'LW-min' : True }

                # LIST
                #events.append(v) ### events.append(v, timestamp) ?
                # DICT
                #events[v.key] = v.value ##########

                # 
                #for key, value in v.items():
                #    if key in events:
                #        events[key].append(value)
                #    else:
                #        events[key]=[value]
                events.update(v)
                
        if count >= period: 
            pass

        print('pool_result is :...............: ' + str(events))
        return events
        #
        # events (LIST) potrebbe essere:
        # [{'LW-min' : True }, {'GAP' : '1.45' }]
        # oppure (DICT)
        # {'LW-min' : True, 'GAP' : '1.45' }
        
    return _run


def main():

    pdf = pd.read_csv('/home/starq/REP/DATA/FINANCE/Quotazioni/test-ai.csv' \
    #pdf = pd.read_csv('/home/starq/REP/DATA/FINANCE/Quotazioni/NVDA.csv' \
                                 ,usecols=['Date','Open', 'High', 'Low', 'Close', 'Volume'] \
                                 ,dtype=dty,parse_dates=['Date'],infer_datetime_format=True \
                                 ,index_col='Date')

    print(pdf.info(memory_usage='deep'))
    print('size : {}'.format(pdf.size))


    rows = 0
    #events_rows = list()
    events_rows = dict()
    start = time.clock()

    #run_step = execution_pool(ai)  
    ### events_row.append(_null_event_row_)
    #events_rows.append(yeld_events_row({}))
    #events_rows.update(yeld_events_row({}))

    for datapoint in pdf.itertuples():
        if rows :
            events = run_step(pdf) ### aggiungere datapoint.timestamp come idx ?
            if events:
                print ('............events :')
                print(events)
                pass ##########################################################################
                #
                # events normalization:
                events_dict = yeld_events_row(events)
                print('EVENTS DICT : ')
                print(str(events_dict))
                #
                # ...qui bisogna - con la lista di tutte colonne utilizzate in ai (pdf_ext_data_columns) -
                # verificare se ogni colonna esiste in events, se non esiste la crea vuota in pdf_ext_data
                # se esiste la copia da events
                #
                # quindi :
                #events_rows.update(events_dict)

                for k, v in events_dict.items():
                    events_rows.setdefault(k, []).append(v)
                #
                #################################################################################           
                
            else:
                events_dict = yeld_events_row({})
                for k, v in events_dict.items():
                    events_rows.setdefault(k, []).append(v)
                #events_rows.update(yeld_events_row({}))
        else:
            run_step = execution_pool(ai)  
            events_dict = yeld_events_row({})
            for k, v in events_dict.items():
                events_rows.setdefault(k, []).append(v)
            #events_row.append(_null_event_row_)
            #events_rows.update(yeld_events_row({}))


        rows += 1

    columns =  list(pdf_ext_data_columns.values())
    print('columns....')
    print(str(columns))
    print('events_rows....')
    print(str(events_rows))

    print('pdf index size : ' + str(len(pdf.index)))
    #pdf.insert(loc=0, column=columns, value=events_rows)
    pdf.info()
    pdf = pdf.join(pd.DataFrame(events_rows))
    print(pdf)


if __name__ == '__main__':
    main()

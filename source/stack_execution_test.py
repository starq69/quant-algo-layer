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
def yeld_event_row (events):
    #
    pdf_ext_data = {}
    if not events:
        for i in range (len(pdf_ext_data_columns)):
            pdf_ext_data [pdf_ext_data_columns[str(i)]] = None
        return pdf_ext_data

    for i in range (len(pdf_ext_data_columns)): ### pdf_mapped_events

        #
        # crea l'event_dict (pdf_ext_data) x il datapoint corrente
        #
        try:
            if events[pdf_ext_data_columns[str(i)]]:
                pdf_ext_data [pdf_ext_data_columns[str(i)]] = events[pdf_ext_data_columns[str(i)]]
            else:
                pdf_ext_data [pdf_ext_data_columns[str(i)]] = None
        except KeyError as e:
            pdf_ext_data [pdf_ext_data_columns[str(i)]] = None

    return pdf_ext_data


def stack_execution (active_indicators, datapoint, dtype=dty, ext_data={}):
    
    ai           = active_indicators 
    max_duration = max({_:value['frequency'] for (_,value) in ai.items()}.values())
    i_stack      = pd.DataFrame()
    i_stack      = i_stack.append([datapoint], ignore_index=True)
    ext_data     = ext_data

    period = max_duration
    count  = 1
    print(i_stack)
    
    def _run(pdf): ### può essere un rif a pdf

        nonlocal ai, i_stack, period, count, ext_data
        count += 1
        
        v = None
        #events = list()  ### dovrebbe essere un dict ??
        events = {}
        
        for fx in ai:
            
            if (count >= ai[fx]['frequency']):
                #
                pass

                v = ai[fx]['callback'] (pdf.iloc[-ai[fx]['frequency']:]) ### ultimi fx['frequency'] elementi

                #
                # v potrebbe essere :
                # {'LW-min' : True }

                #v = ai[fx]['callback'] ()
                # i_stack[row_list, :][:, columns_list]
                #v = fx['callback'] (i_stack[np.ix_(row_list, columns_list)]) ### best solution x nparray (fare un performance test)
                              
                # LIST
                #events.append(v) ### events.append(v, timestamp) ?
                # DICT
                #events[v.key] = v.value ##########
                events.update(v)
                
        if count >= period: 
            pass
            #i_stack = i_stack.drop(i_stack.index[0])   ### non serve più
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
    events_rows = list()
    start = time.clock()
    for datapoint in pdf.itertuples():
        if rows :
            events = run_indicators(pdf) ### aggiungere datapoint.timestamp come idx ?
            if events:
                print ('events :')
                print(events)
                pass ##########################################################################
                #
                # events normalization:
                events_dict = yeld_event_row(events)
                #
                # ...qui bisogna - con la lista di tutte colonne utilizzate in ai (pdf_ext_data_columns) -
                # verificare se ogni colonna esiste in events, se non esiste la crea vuota in pdf_ext_data
                # se esiste la copia da events
                #
                # quindi :
                events_rows.append(events_dict)
                #
                #################################################################################           
                
            else:
                events_rows.append(yeld_event_row({}))
        else:
            run_indicators = stack_execution(ai, datapoint)  
            ### events_row.append(_null_event_row_)
            events_rows.append(yeld_event_row({}))

        rows += 1

    columns =  list(pdf_ext_data_columns.values())
    print('columns....')
    print(str(columns))
    print('events_rows....')
    print(str(events_rows))

    pdf.insert(loc=0, column=columns, value=events_rows)


if __name__ == '__main__':
    main()

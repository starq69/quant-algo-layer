'''
active_indicators = {
                    'name' : string,
                    'frequency' : int,
                    'callback': callable,
                    'computed': []      ## necessario ?
                    'params' : []   # axis(ohlcv) and globals (min,max, histvol, ecc)
                    }

TEMA:
utilizzare una closure con uno stack dimensionato al max previsto dal run_setting 
alla quale passare active_indicators in modo tale da permettere la condivisione di una 
sola copia dello stack storico x tutti gli indicatori che lo utilizzano
'''

#_freq        = {_:value['frequency'] for (_,value) in active_indicators.items()}
#max_duration = max(_freq.values()])
max_duration = max({_:value['freq'] for (_,value) in ai.items()}.values())   ### one row OK

pdf = pd.dataframe(<test csv>)

rows = 0
ext_stack = list()
for datapoint in pdf.itertuples(name=None):
    if rows :
        ext_data = run_indicators(datapoint)
        if ext_data:
            #
            # some ext_data check:
            # ext_data = check(ext_data)
            #
            # posso utilizzare una list e fare il merge alla fine:
            ###(1)
            # ext_stack.append(ext_data)


            pdf.merge(ext_data) #, index=timestamp)
    else:
        run_indicators = stack_execution(active_indicators, max_duration, datapoint)

    rows += 1

###(1)
#pdf.merge(ext_stack)


def stack_execution(active_indicators=_SCHEMA_.AI, max_duration, datapoint, ext_data={}):
    
    ai      = active_indicators    
    #i_stack = np.array(max_duration, fill='None')
    i_stack = np.zeros(max_duration, dtype=ohlcv)
    i_stack.append(datapoint)
    ext_data = ext_data

    for fx in ai:
        fx['computed']    = None    ## necessario ?

    period = max_duration
    count  = 1
    
    def _run(datapoint):

        nonlocal ai, i_stack, period, count, ext_data

        i_stack.append(datapoint)
        count += 1
        #
        v = None
        values = list()
        for fx in ai.keys():
            if (count >= fx['frequency']):
                #
                v = fx['callback'] (i_stack[-fx['frequency':]]) ### ultimi fx['frequency'] elementi

                # https://stackoverflow.com/questions/8386675/extracting-specific-columns-in-numpy-array
                #i_stack[:, [1, 9]]
                #i_stack[:, [fx['params']]
                #...comporre con : v = fx['callback'] (i_stack[-fx['frequency':]]) 

                # v = fx['callback'] (i_stack [-fx['frequency':], [fx['params']] ], ext_data)
                #                
                # ...per ottenere le colonne def. in fx['params'] delle ultime fx['frequency'] righe  
                #
                # vedere anche: (****)
                # https://stackoverflow.com/questions/22927181/selecting-specific-rows-and-columns-from-numpy-array
                # i_stack[row_list, :][:, columns_list]
                # i_stack[np.ix_(row_list, columns_list)]   ### best solution x nparray (fare un performance test)
                
                fx['computed'] = v  ## necessario ?
                #values.append(fx['name']:v)
                values.append[{fx['name']]:v})

        if count >= period: i_stack = i_stack.delete(0)          
        #if (count % period == 0): i_stack = i_stack.delete(0..period elements)
        return values
        
    return _run


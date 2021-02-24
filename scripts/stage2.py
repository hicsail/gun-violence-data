#!/usr/bin/env python3
# stage 2: augmenting each incident with additional fields, again using scraping

import asyncio
import logging as log
import numpy as np
import pandas as pd
import sys
import time

from aiohttp.client_exceptions import ClientResponseError
from argparse import ArgumentParser

from log_utils import log_first_call
from stage2_extractor import NIL_FIELDS
from stage2_session import Stage2Session

from selenium.webdriver import Chrome
from selenium import webdriver
import selenium_utils
from selenium.webdriver.common.by import By

SCHEMA = {
    'congressional_district': np.float64,
    'state_house_district': np.float64,
    'state_senate_district': np.float64,
    'n_guns_involved': np.float64,
}

data_dict = {}
columns = []
OUTPUT_FNAME = None
INPUT_FNAME = None
def parse_args():
    targets_specific_month = False
    if len(sys.argv) > 1:
        parts = sys.argv[1].split('-')
        if len(parts) == 2: # e.g. '02-2014'
            targets_specific_month = True
            del sys.argv[1]

    parser = ArgumentParser()
    if not targets_specific_month:
        parser.add_argument(
            'input_fname',
            metavar='INPUT',
            help="path to input file",
        )
        parser.add_argument(
            'output_fname',
            metavar='OUTPUT',
            help="path to output file. " \
                 "if --amend is specified, this is interpreted as a suffix and output is written to the path (INPUT + OUTPUT)."
        )

    parser.add_argument(
        '-a', '--amend',
        help="amend existing stage2 file by populating missing values",
        action='store_true',
        dest='amend',
    )
    parser.add_argument(
        '-d', '--debug',
        help="show debug information",
        action='store_const',
        dest='log_level',
        const=log.DEBUG,
        default=log.WARNING,
    )
    parser.add_argument(
        '-l', '--limit',
        metavar='NUM',
        help="limit the number of simultaneous connections aiohttp makes to gunviolencearchive.org",
        action='store',
        dest='conn_limit',
        type=int,
        default=20,
    )

    args = parser.parse_args()
    if targets_specific_month:
        month, year = map(int, parts)
        args.input_fname = 'stage1.{:02d}.{:04d}.csv'.format(month, year)
        args.output_fname = 'stage2.{:02d}.{:04d}.csv'.format(month, year)
    return args

def load_input(args):
    log_first_call()
    return pd.read_csv(args.input_fname,
                       dtype=SCHEMA,
                       parse_dates=['date'],
                       encoding='utf-8')

def add_incident_id(df):
    log_first_call()
    def extract_id(incident_url):
        PREFIX = 'http://www.gunviolencearchive.org/incident/'
        assert incident_url.startswith(PREFIX)
        return int(incident_url[len(PREFIX):])

    df.insert(0, 'incident_id', df['incident_url'].apply(extract_id))
    return df

def remove_incident_id_from_source(source_fname, res_fname):
    res_df = pd.read_csv(res_fname) 
    source = pd.read_csv(source_fname)
    source = add_incident_id(source)
    incident_ids = res_df['incident_id'].tolist()
    source.drop(source[source['incident_id'].isin(incident_ids)].index, inplace = True)
    source.drop(['incident_id'], axis=1, inplace = True)
    source.to_csv(source_fname,
              index=False,
              float_format='%g',
              encoding='utf-8')

async def add_fields_from_incident_url(driver, df, args, predicate=None):
    log_first_call()
    def field_name(lst):
        assert len(set([field.name for field in lst])) == 1
        return lst[0].name

    def field_values(lst):
        return [field.value for field in lst]

    subset = df if predicate is None else df.loc[predicate]
    if len(subset) == 0:
        # No work to do
        return df    
    async with Stage2Session(limit_per_host=args.conn_limit) as session: 
        #ip_is_blocked = False
        global columns 
        columns = subset.columns.tolist()        
        for i in range(len(subset)):            
            row = subset.iloc[i]
            row_to_list = row.tolist()
            try:            
                extra_fields = session.get_fields_from_incident_url(row, driver)                      
                if extra_fields:                                    
                    for field_name, field_values in extra_fields:                
                        if i == 0:
                            columns.append(field_name)
                        row_to_list.append(field_values)
                data_dict[i] = row_to_list
            except: #The only exception it raises is IpBlocked 
                #ip_is_blocked = True                
                break

        df = pd.DataFrame.from_dict(data_dict, orient='index', columns=columns) 
        df.to_csv(args.output_fname,
              index=False,
              float_format='%g',
              encoding='utf-8')

        #if ip_is_blocked:            
        remove_incident_id_from_source(args.input_fname, args.output_fname)
    return df

async def main():
    args = parse_args()
    log.basicConfig(level=args.log_level)

    options = webdriver.ChromeOptions()
    options.add_experimental_option('w3c', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(options=options)    

    df = load_input(args)   
    if args.amend:
        output_fname = args.input_fname + args.output_fname
        df = await add_fields_from_incident_url(driver, df, args, predicate=df['incident_url_fields_missing'])
    else:
        global OUTPUT_FNAME
        #global INPUT_FNAME
        output_fname = args.output_fname
        OUTPUT_FNAME = args.output_fname
        #INPUT_FNAME = args.input_fname
        df = add_incident_id(df)
        time1= time.time()        
        df = await add_fields_from_incident_url(driver, df, args)        
        time2 = time.time()
        print(time2- time1)

    df.to_csv(output_fname,
              index=False,
              float_format='%g',
              encoding='utf-8')   

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:        
        loop.run_until_complete(main())
    finally:
        df = pd.DataFrame.from_dict(data_dict, orient='index', columns=columns) 
        df.to_csv(OUTPUT_FNAME,
              index=False,
              float_format='%g',
              encoding='utf-8')                    
        loop.close()

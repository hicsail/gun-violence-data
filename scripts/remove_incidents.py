#!/usr/bin/env python3
# removing rows from source file which have already been succesfully augmented

import pandas as pd
from argparse import ArgumentParser

def parse_args():
    parser = ArgumentParser()    
    parser.add_argument(
        'source_fname',
        metavar='SOURCE',
        help="path to source file that we are going to remove some of the rows from"
    )
    parser.add_argument(
        'result_fname',
        metavar='RESULT',
        help="path to result file containing the incident ids of the augmented rows."       
    )
    args = parser.parse_args()   
    return args

def add_incident_id(df):    
    def extract_id(incident_url):
        PREFIX = 'http://www.gunviolencearchive.org/incident/'
        assert incident_url.startswith(PREFIX)
        return int(incident_url[len(PREFIX):])

    df.insert(0, 'incident_id', df['incident_url'].apply(extract_id))
    return df

def main():
    args = parse_args()

    res_df = pd.read_csv(args.result_fname) 
    source = pd.read_csv(args.source_fname)
    source = add_incident_id(source)
    incident_ids = res_df['incident_id'].tolist()
    source.drop(source[source['incident_id'].isin(incident_ids)].index, inplace = True)
    source.drop(['incident_id'], axis=1, inplace = True)
    source.to_csv(args.source_fname,
              index=False,
              float_format='%g',
              encoding='utf-8')   

if __name__ == '__main__':           
   main()
   

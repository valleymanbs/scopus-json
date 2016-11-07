#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Builds two CSV files from a joined search json file
CSVs are nodes (articles) and edges (citations) of the graph
@author: michele
"""

import argparse
from api.scopus_search import ScopusSearch
from api.scopus_author_retrieval import ScopusAuthorRetrieval
import os
import logging
import time
import pandas as pd
from pandas.io.json import json_normalize
import json

# log config
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
script_log = logging.getLogger(' CitationsGraphBuilder ')
script_log.info("Script started")

# parse arguments from terminal  
parser = argparse.ArgumentParser()
parser.add_argument("folder", help='name of the folder inside data/joined_searches/ where the clean.json file is located')
args = parser.parse_args()

DATA_DIR = os.path.join(os.path.abspath('data'),'joined_searches')
DATA_FILE = os.path.join(DATA_DIR, args.folder, 'clean.json')
OUTPUT_DIR = os.path.join('output', args.folder.replace('/', '_slash_'),'articles_citations',time.strftime("%d%m%Y_%H%M%S"))
NODES_CSV = os.path.join(OUTPUT_DIR, 'nodes.csv')
EDGES_CSV = os.path.join(OUTPUT_DIR, 'edges.csv')

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

with open(DATA_FILE) as f:
    keyword_results_list = json.load(f)
    f.close()

# fill pandas dataframe
keywords_df = json_normalize(keyword_results_list)
# drop rows with null authors
keywords_df = keywords_df.dropna(subset=['author', 'eid']).drop_duplicates(subset=['eid'])


# keywords_df.to_csv('debug/keyword_results_list.csv', sep=',', encoding='utf-8') # save csv to file (debug)
script_log.info('Keyword search: found {} valid results'.format(len(keywords_df.index)))

# clean keywords dataframe from useless columns to free some memory
# var NODES_COLS will also be used later, shouldn't be changed
NODES_COLS = [
                "eid", "dc:title", "dc:creator", "dc:description", "authkeywords", "author", "citedby-count", "dc:identifier",
                "prism:aggregationType", "prism:coverDate", "prism:publicationName", "source-id", "subtype", "subtypeDescription"
            ]
try:
    keywords_df = keywords_df[NODES_COLS]
except KeyError as e:
    script_log.warn("keywords_df KeyError: {}".format(e))
    

# convert the citedby-count column from whatever type it is to int
keywords_df['citedby-count'] = keywords_df['citedby-count'].apply(int)
#keywords_df.to_csv('debug/keyword_clean.csv', sep=',', encoding='utf-8') # save csv to file (debug)


# create nodes dataframe: left outer join between authors_df and affliations_df on key 'afid'
# nodes_df columns are the union of authors_df.columns and affiliations_df.columns
# and information has been joined on afid -> authors now have also affiliation name, city, country
#nodes_df = pd.merge(left=authors, right=affiliations_df, left_on='afid', right_on='afid', how='left')

# select from nodes_df only rows with missing afid and apply function to each row
# on each null-afid row, do an AuthorRetrieval request to Scopus using the authid and fill missing values
# save the resulting dataframe (with filled missing affiliations, if found)
# null_data = nodes_df[nodes_df.isnull().afid].apply(lambda row: get_missing_afid(row),axis=1)
# null_data.to_csv('debug/null.csv', sep=',', encoding='utf-8')
# drop nodes that have been copied into 
#nodes_df = nodes_df.dropna(subset=['afid'])
#nodes_df.to_csv('debug/ok_nodes.csv', sep=',', encoding='utf-8')
# add null_data rows to nodes_df
#nodes_df = pd.concat([nodes_df, null_data], axis=0)
# ...like all above but in a single line of code... 
#nodes_df = pd.concat([nodes_df.dropna(subset=['afid']), nodes_df[nodes_df.isnull().afid].apply(lambda row: get_missing_afid(row),axis=1)], axis=0)

# rename some column to import as a nodes spreadsheed in gephi
#nodes_df.rename(columns={'authid': 'Id', 'authname': 'Label'}).to_csv(NODES_CSV, sep=',', encoding='utf-8')

#script_log.info('Saved nodes.csv file: there are {} authors'.format(len(nodes_df.index)))

with_cit = keywords_df[keywords_df['citedby-count'] > 0]


keywords_df = keywords_df[keywords_df['citedby-count'] == 0]


# with_cit_df.to_csv('debug/with_cit.csv', sep=',', encoding='utf-8')

script_log.info('Searching citations for {} articles'.format(len(with_cit.index)))
script_log.info('There are about {} citations...'.format(int(with_cit["citedby-count"].mean()*len(with_cit.index))))

# create a dataframe representing relations between eid from keywords search and authors:
# first apply a function to the subset of keywords_df made only by rows where citedby-count > 0 (note: column previously converted to int)
# a = with_cit_df.apply(lambda row: build_authors_eid_df(row), axis=1)

citations_search_dict = {}
# test_list = []

# itertuples faster than iterrows, but int indexes: column order matters
start = time.time()
to_be_searched_n = len(with_cit.index)

for row in with_cit.drop_duplicates('eid').itertuples():
    to_be_searched_n -= 1
    eid = row[1]

    citations_search_dict[eid] = ScopusSearch(query='REFEID({})'.format(eid),
                                              items_per_query=100,
                                              view='COMPLETE', # ONLY STANDARD AT HOME
                                              no_log=True
                                              ).valid_results_list
    script_log.info("Ok, still {} articles with citations to be searched".format(to_be_searched_n))

script_log.info("Citing articles search completed in %.3fs" % (time.time() - start))

# create a dataframe from the citations_search_dict
ddf = pd.DataFrame(dict([(k,pd.Series(v)) for k,v in citations_search_dict.items()]))
ddf = ddf.transpose().stack().reset_index(level=1, drop=True)
# create citing articles dataframe
citing_df = ddf.to_frame()
citing_df.reset_index(level=0, inplace=True)

# add columns unpacking dict inside the '0' column of new_df
citing_df = pd.concat([citing_df.drop([0], axis=1).rename(columns={'index': 'external_eid'}), citing_df[0].apply(pd.Series)], axis=1)

# copy edges data selecting only useful columns from citing_df
cols = [
        'eid',
        'external_eid',
        'citedby-count'
        ]
# also rename columns to fit graph CSV file standard
edges_df = citing_df[cols].rename(columns={'eid': 'Source', 'external_eid': 'Target','citedby-count':'source_citedby-count'})
# add a column to say the graph has directed edges from source to target nodes
edges_df['directed'] = pd.Series('true', index=edges_df.index)

# select only useful columns for the nodes


citing_df = citing_df[NODES_COLS]

# create nodes dataframe from all the articles dataframes built during the script
# also drop duplicates by eid and then rename columns to fit CSV graph file standard
nodes_df = pd.concat(
                        [
                            keywords_df,
                            with_cit,
                            citing_df
                        ]
                    ).drop_duplicates(subset=['eid']).rename(columns={'eid': 'Id', 'dc:title': 'Label'})

script_log.info("Writing CSV files...")

nodes_df.to_csv(NODES_CSV, sep=',', encoding='utf-8')
edges_df.to_csv(EDGES_CSV, sep=',', encoding='utf-8')

script_log.info("OK. ALL DONE. Script terminated.")
quit()
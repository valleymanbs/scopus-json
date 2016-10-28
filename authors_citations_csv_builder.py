"""
Builds two CSV files from a joined search json file
CSVs are nodes (authors) and edges (citations) of the graph
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
script_log = logging.getLogger(' CitationsGraphBuilder')
script_log.info("Script started")

# % (time.localtime().tm_hour,time.localtime().tm_min,time.localtime().tm_sec)
def get_current_afid(afid_list):
    if type(afid_list) is list:
        return afid_list[-1]['$']
    else:
        return ''

        
def get_missing_afid(row):
    s = ScopusAuthorRetrieval(authid=row['authid'],view='FULL')
    row['afid'] = s.afid
    row['affilname'] = s.affilname
    row['affiliation-country'] = s.affilname
    row['affiliation-city'] = s.affilcity
    return row

    
def build_authors_eid_series(row):
    output = {}
    output['dc:title'] = row['dc:title']
    output['eid'] = row['eid']
    #output['author'] = []
    output['author'] = [a['authid'] for a in row['author']]
    return pd.Series(output)

# parse arguments from terminal  
parser = argparse.ArgumentParser()
parser.add_argument("folder", help='name of the folder inside data/joined_searches/ where the clean.json file is located')
args = parser.parse_args()

DATA_DIR = os.path.join(os.path.abspath('data'),'joined_searches')
DATA_FILE = os.path.join(DATA_DIR, args.folder, 'clean.json')
OUTPUT_DIR = os.path.join('output', args.folder.replace('/', '_slash_'),'authors_citations',time.strftime("%d%m%Y_%H%M%S"))
NODES_CSV = os.path.join(OUTPUT_DIR, 'nodes.csv')
EDGES_CSV = os.path.join(OUTPUT_DIR, 'edges.csv')

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# read JSON data file from the given folder
with open(DATA_FILE) as f:
    keyword_results_list = json.load(f)
    f.close()

# fill pandas dataframe
keywords_df = json_normalize(keyword_results_list)
keywords_df.to_csv('debug/keyword_raw.csv', sep=',', encoding='utf-8')
# drop rows with null authors
keywords_df = keywords_df.dropna(subset=['author', 'eid'])
#keywords_df.to_csv('debug/keyword_dropautheid.csv', sep=',', encoding='utf-8')


# keywords_df.to_csv('debug/keyword_results_list.csv', sep=',', encoding='utf-8') # save csv to file (debug)
script_log.info('Keyword search: found {} valid results'.format(len(keywords_df.index)))

# create a new pandas.Series with affiliations data before clening keywords_df from useless columns
# one row per each list element inside column affiliation -> Series of dicts
affiliations_df = keywords_df.apply(lambda x: pd.Series(x['affiliation']), axis=1).stack().reset_index(level=1, drop=True)
# make pandas.DataFrame from eries of dicts, drop rows without afid and rows with non-unique afid
affiliations_df = pd.DataFrame(list(affiliations_df)).dropna(subset=['afid']).drop_duplicates(subset=['afid'])
# clean affiliations dataframe from useless columns
cols = ['afid', 'affilname', 'affiliation-city', 'affiliation-country']
affiliations_df = affiliations_df[cols]
# affiliations_df.to_csv('debug/affiliations.csv', sep=',', encoding='utf-8')

# clean keywords dataframe from useless columns to free some memory
NODES_COLS = [
        "eid", "dc:title", "dc:creator", "dc:description", "authkeywords", "author", "citedby-count", "dc:identifier",
        "prism:aggregationType", "prism:coverDate",
        "prism:publicationName", "source-id", "subtype", "subtypeDescription"
        ]
keywords_df = keywords_df[NODES_COLS]

    

# convert the citedby-count column from whatever type it is to int
keywords_df['citedby-count'] = keywords_df['citedby-count'].apply(int)
keywords_df.to_csv('debug/keyword_clean.csv', sep=',', encoding='utf-8') # save csv to file (debug)

# create a new pandas.Series with authors data, will be joined with affiliations to make graph nodes
# one row per each list element inside column author -> Series of dicts
author_series = keywords_df.apply(lambda x: pd.Series(x['author']), axis=1).stack().reset_index(level=1, drop=True)
author_series.name = 'author'

eid_author_df = keywords_df.drop('author', axis=1).join(author_series)
eid_author_df = eid_author_df[["eid", "dc:title", "dc:description", "authkeywords", "author"]]
# author column is a dict, unpack it into many columns
eid_author_df = pd.concat([ eid_author_df.drop(['author'], axis=1), eid_author_df['author'].apply(pd.Series)['authid'] ], axis=1).drop_duplicates(subset=['eid', 'authid'])
eid_author_df.to_csv('debug/eid_author_df.csv', sep=',', encoding='utf-8')

# Series of dicts to list of dicts and then to a dataframe; also drop duplicates in the end
authors = pd.DataFrame(list(author_series)).drop_duplicates(subset=['authid'])
authors.to_csv('debug/authors.csv', sep=',', encoding='utf-8')



# authors['afid'] is a list, we need to keep just the last element of this list (current affiliation)
authors['afid'] = authors.apply(lambda row: get_current_afid(row['afid']),axis=1)
# drop the first two columns, useless. [authors = authors.drop(authors.columns[[0, 1]], axis=1)]
# do this while reordering columns: leave out columns [0:1], make authid [3] first column, afid last
cols = ['authid', 'authname', 'surname', 'given-name', 'initials', 'afid']
authors = authors[cols]
# authors.to_csv('debug/authors.csv', sep=',', encoding='utf-8' # save csv to file (debug)

# create nodes dataframe: left outer join between authors_df and affliations_df on key 'afid'
# nodes_df columns are the union of authors_df.columns and affiliations_df.columns
# and information has been joined on afid -> authors now have also affiliation name, city, country
nodes_df = pd.merge(left=authors, right=affiliations_df, left_on='afid', right_on='afid', how='left')

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
nodes_df = pd.concat([nodes_df.dropna(subset=['afid']), nodes_df[nodes_df.isnull().afid].apply(lambda row: get_missing_afid(row),axis=1)], axis=0)

# rename some column to import as a nodes spreadsheed in gephi
nodes_df.rename(columns={'authid': 'Id', 'authname': 'Label', 'given-name':'firstname', 'affiliation-city':'affilcity','affiliation-country':'affilcountry'}).to_csv(NODES_CSV, sep=',', encoding='utf-8')

script_log.info('Saved nodes.csv file: there are {} authors'.format(len(nodes_df.index)))

with_cit_authid_eid = keywords_df[keywords_df['citedby-count'] > 0]
# with_cit_df.to_csv('debug/with_cit.csv', sep=',', encoding='utf-8')
to_be_searched_n = len(with_cit_authid_eid.index)
script_log.info('Searching citations for {} articles'.format(to_be_searched_n))
script_log.info('There are about {} citations...'.format(int(with_cit_authid_eid["citedby-count"].mean()*len(with_cit_authid_eid.index))))

# create a dataframe representing relations between eid from keywords search and authors:
# first apply a function to the subset of keywords_df made only by rows where citedby-count > 0 (note: column previously converted to int)
# a = with_cit_df.apply(lambda row: build_authors_eid_df(row), axis=1)
with_cit_authid_eid = with_cit_authid_eid.apply(lambda row: build_authors_eid_series(row), axis=1)
# a.to_csv('debug/a.csv', sep=',', encoding='utf-8')
# create a Series of all the authors unpacking the lists inside the author columns
authid_series = with_cit_authid_eid['author'].apply(pd.Series).stack().reset_index(level=1, drop=True)
# give a name to the Series to be used as a column
authid_series.name = 'authid'

with_cit_authid_eid = with_cit_authid_eid.drop('author', axis=1).join(authid_series)
#with_cit_authid_eid.to_csv('debug/with_cit_authid_eid.csv', sep=',', encoding='utf-8')

citations_search_dict = {}

# itertuples faster than iterrows, but int indexes: column order matters
start = time.time()


for row in with_cit_authid_eid.drop_duplicates('eid').itertuples():
    to_be_searched_n -= 1
    # authid = row[0]
    # title = row[1]
    eid = row[2]

    citations_search_dict[eid] = ScopusSearch(query='REFEID({})'.format(eid),
                                              items_per_query=100,
                                              view='COMPLETE', # ONLY STANDARD AT HOME
                                              no_log=True
                                              ).valid_results_list
    script_log.info("Ok, still {} articles with citations to be searched".format(to_be_searched_n))

script_log.info("Itertuples completed in %.3fs" % (time.time() - start))



start = time.time()
ddf = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in citations_search_dict.iteritems() ]))
ddf = ddf.transpose().stack().reset_index(level=1, drop=True)
new_df = ddf.to_frame()
new_df.reset_index(level=0, inplace=True)
script_log.info("Filled new_df in %.3fs" % (time.time() - start))
# add columns unpacking dict inside the '0' column of new_df
new_df = pd.concat([new_df.drop([0], axis=1).rename(columns={'index': 'external_eid'}), new_df[0].apply(pd.Series)], axis=1)
new_df.to_csv('debug/new_df0.csv', sep=',', encoding='utf-8')

cols =  [
    "external_eid",
    #"authkeywords",
    "author",
    #"citedby-count",
    "dc:title",
    "eid",
    #"prism:coverDate",
    #"prism:publicationName",
    #"subtype",
    #"subtypeDescription"
]

new_df = new_df[cols]



s = new_df.apply(lambda x: pd.Series(x['author']),axis=1).stack().reset_index(level=1, drop=True)
s.name = 'author'
new_df = new_df.drop('author', axis=1).join(s)
new_df = pd.concat([new_df.drop(['author'], axis=1), new_df['author'].apply(pd.Series)], axis=1)
new_df = new_df.drop_duplicates(subset=['eid','authid'])
new_df.to_csv('debug/new_df1.csv', sep=',', encoding='utf-8')
new_df = new_df[['external_eid','eid','authid','dc:title']]
#new_df = new_df.sort(['authid'])
new_df.to_csv('debug/new_df2.csv', sep=',', encoding='utf-8')

new_df1 = new_df[new_df.authid.isin(nodes_df.authid)]
new_df1.to_csv('debug/new_df1ok.csv', sep=',', encoding='utf-8')

merged_inner = pd.merge(left=with_cit_authid_eid,right=new_df1, left_on='eid', right_on='external_eid').drop(['external_eid'],axis=1)
merged_inner = merged_inner[['authid_y','authid_x','eid_y','dc:title_y','eid_x','dc:title_x']].rename(columns={
                                                                                                                'authid_y':'Source',
                                                                                                                'authid_x':'Target',
                                                                                                                'eid_y':'source_eid',
                                                                                                                'eid_x':'target_eid',
                                                                                                                'dc:title_y':'source_title',
                                                                                                                'dc:title_x':'target_title'
                                                                                                                })
merged_inner['directed'] = pd.Series('true', index=merged_inner.index)
merged_inner.to_csv(EDGES_CSV, sep=',', encoding='utf-8')

script_log.info('ALL DONE. OK!\n')




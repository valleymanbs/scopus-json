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
script_log = logging.getLogger('\tCitationsGraphBuilder\t')


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
OUTPUT_DIR = os.path.join('output', 'joined_{}'.format(args.folder.replace('/', '_slash_')))
NODES_CSV = os.path.join(OUTPUT_DIR, 'nodes_{}.csv'.format(time.strftime("%d%m%Y_%H%M%S")))
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# keyword_results_list = []
keyword_eid_author_dict = {}
cited_eid_list = []

with open(DATA_FILE) as f:
    keyword_results_list = json.load(f)
    f.close()

# fill pandas dataframe
keywords_df = json_normalize(keyword_results_list)
# drop rows with null authors
keywords_df = keywords_df.dropna(subset=['author', 'eid'])


# keywords_df.to_csv('debug/keyword_results_list.csv', sep=',', encoding='utf-8') # save csv to file (debug)
script_log.info('Keyword search: found {} valid results'.format(len(keywords_df.index)))

# create a new pandas.Series with affiliations data before clening keywords_df from useless columns
# one row per each list element inside column affiliation -> Series of dicts
affiliations_df = keywords_df.apply(lambda x: pd.Series(x['affiliation']), axis=1).stack().reset_index(level=1, drop=True)
# make pandas.DataFrame from eries of dicts, drop rows without afid and rows with non-unique afid
affiliations_df = pd.DataFrame(list(affiliations_df)).dropna(subset=['afid']).drop_duplicates(subset=['afid'])
# clean affiliations dataframe from useless columns
cols = ['afid', 'affilname', 'affiliation-city', 'affiliation-country', 'affiliation-url']
affiliations_df = affiliations_df[cols]
# affiliations_df.to_csv('debug/affiliations.csv', sep=',', encoding='utf-8')

# clean keywords dataframe from useless columns to free some memory
cols = [
        "eid", "dc:title", "dc:creator", "dc:description", "authkeywords", "author", "citedby-count", "dc:identifier",
        "pii", "prism:aggregationType", "prism:coverDate", "prism:doi", "prism:eIssn", "prism:isbn", "prism:issn",
        "prism:publicationName", "pubmed-id", "source-id", "subtype", "subtypeDescription", "link", "prism:url"
        ]
try:
    keywords_df = keywords_df[cols]
except KeyError as e:
    script_log.warn("keywords_df KeyError: {}".format(e))
    

# convert the citedby-count column from whatever type it is to int
keywords_df['citedby-count'] = keywords_df['citedby-count'].apply(int)
# keywords_df.to_csv('debug/keyword_clean.csv', sep=',', encoding='utf-8') # save csv to file (debug)

# create a new pandas.Series with authors data, will be joined with affiliations to make graph nodes
# one row per each list element inside column author -> Series of dicts
authors = keywords_df.apply(lambda x: pd.Series(x['author']), axis=1).stack().reset_index(level=1, drop=True)
# Series of dicts to list of dicts and then to a dataframe; also drop duplicates in the end
authors = pd.DataFrame(list(authors)).drop_duplicates(subset=['authid'])
# authors['afid'] is a list, we need to keep just the last element of this list (current affiliation)
authors['afid'] = authors.apply(lambda row: get_current_afid(row['afid']),axis=1)
# drop the first two columns, useless. [authors = authors.drop(authors.columns[[0, 1]], axis=1)]
# do this while reordering columns: leave out columns [0:1], make authid [3] first column, afid last
cols = ['authid', 'authname', 'surname', 'given-name', 'initials', 'author-url', 'afid']
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
nodes_df.rename(columns={'authid': 'Id', 'authname': 'Label'}).to_csv(NODES_CSV, sep=',', encoding='utf-8')

script_log.info('Saved nodes.csv file: there are {} authors'.format(len(nodes_df.index)))

# with_cit_df = keywords_df[keywords_df['citedby-count'] > 0]
# with_cit_df.to_csv('debug/with_cit.csv', sep=',', encoding='utf-8')
###script_log.info('Searching citations for {} articles'.format(len(with_cit_df.index)))
###script_log.info('There are about {} citations...'.format(int(with_cit_df["citedby-count"].mean()*len(with_cit_df.index))))

# create a dataframe representing relations between eid from keywords search and authors:
# first apply a function to the subset of keywords_df made only by rows where citedby-count > 0 (note: column previously converted to int)
# a = with_cit_df.apply(lambda row: build_authors_eid_df(row), axis=1)
with_cit_authid_eid = keywords_df[keywords_df['citedby-count'] > 0].apply(lambda row: build_authors_eid_series(row), axis=1)
# a.to_csv('debug/a.csv', sep=',', encoding='utf-8')
# create a Series of all the authors unpacking the lists inside the author columns
authid_series = with_cit_authid_eid['author'].apply(pd.Series).stack().reset_index(level=1, drop=True)
# give a name to the Series to be used as a column
authid_series.name = 'authid'

with_cit_authid_eid = with_cit_authid_eid.drop('author', axis=1).join(authid_series)
with_cit_authid_eid.to_csv('debug/with_cit_authid_eid.csv', sep=',', encoding='utf-8')

citations_search_dict = {}
# test_list = []

# itertuples faster than iterrows, but int indexes: column order matters
start = time.time()
#ddf = pd.DataFrame

for row in with_cit_authid_eid.drop_duplicates('eid').itertuples():
    # authid = row[0]
    eid = row[1]

    citations_search_dict[eid] = ScopusSearch(query='REFEID({})'.format(eid),
                                              items_per_query=100,
                                              view='COMPLETE' # ONLY STANDARD AT HOME
                                              ).valid_results_list

script_log.info("Itertuples completed in %.3fs" % (time.time() - start))


start = time.time()
ddf = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in citations_search_dict.iteritems() ]))
ddf = ddf.transpose().stack().reset_index(level=1, drop=True)
new_df = ddf.to_frame()
new_df.reset_index(level=0, inplace=True)
script_log.info("Filled ddf in %.3fs" % (time.time() - start))
# add columns unpacking dict inside the '0' column of new_df
new_df = pd.concat([new_df.drop([0], axis=1).rename(columns={'index': 'external_eid'}), new_df[0].apply(pd.Series)], axis=1)

cols =  [
    "external_eid",
    "authkeywords",
    "author",
    "citedby-count",
    "dc:title",
    "eid",
    "prism:coverDate",
    "prism:publicationName",
    "subtype",
    "subtypeDescription"
]

new_df = new_df[cols]


new_df.to_csv('debug/new_df.csv', sep=',', encoding='utf-8')


# for eid in cited_eid_list:
#     print eid
#     citations_search_dict[eid] = json.loads(
#                                             ScopusSearch(
#                                                 query='REFEID({})'.format(eid),
#                                                 items_per_query=100,
#                                                 view='COMPLETE' # ONLY STANDARD AT HOME
#                                                         ).valid_results_json
#                                             )
#     if len(citations_search_dict[eid]) == 0:
#         dropped += [eid]
#         print 'Dropped citation to article {}. No valid results from REFEID search'.format(eid)
#
#     n_cited -= 1
#     print 'Still {} citations to search'.format(n_cited)
#
# print 'Citation search done. {} citations dropped due to invalid results'.format(len(dropped))
#
# for eid in dropped:
#     cited_eid_list.remove(eid)
#
# citations_eid_author_dict = {}
#
# missing_n = 0
#
# for key in citations_search_dict:
#     for item in citations_search_dict[key]:
#         citations_eid_author_dict[key] = []
#         for a in item['author']:
#             if 'afid' not in a:
#                 missing_n += 1
#                 a['afid'] = [{'$': ''}]
#             citations_eid_author_dict[key] += [
#                                 {'authid': unicode(a['authid']).encode('utf-8'),
#                                  'afid': unicode(a['afid'][-1]['$']).encode('utf-8'),
#                                  'surname': unicode(a['surname']).encode('utf-8'),
#                                  'given-name': unicode(a['given-name']).encode('utf-8')
#                                  }
#                               ]
# if missing_n > 0:
#     print('WARNING: found {} authors with null affiliation'.format(missing_n))
#
# # with open('keyword_eid_author_dict', 'w') as f:
# #     f.write(json.dumps(keyword_eid_author_dict))
# #     f.close()
# #
# #
# # with open('cited_eid_list', 'w') as f:
# #     f.write(json.dumps(cited_eid_list))
# #     f.close()
# #
# #
# # with open('citations_eid_author_dict', 'w') as f:
# #     f.write(json.dumps(citations_eid_author_dict))
# #     f.close()
#
# nodes = []
#
# k_eid_auth_dict = {}
# c_eid_auth_dict = {}
#
#
# for key in keyword_eid_author_dict:
#     for a in keyword_eid_author_dict[key]:
#         nodes += ['{},{} {},{}'.format(a['authid'], a['given-name'], a['surname'],a['afid'])]
# for key in citations_eid_author_dict:
#     for auth in citations_eid_author_dict[key]:
#         nodes += ['{},{} {},{}'.format(auth['authid'], auth['given-name'], auth['surname'],auth['afid'])]
#
# print 'Added {} nodes to list'.format(len(nodes))
#
# nodes = list(set(nodes))
# print 'Cleanup nodes list: {} unique nodes now'.format(len(nodes))
#
# nodes1 = []
# for k,v,y in (x.split(',') for x in nodes):
#     nodes1 += [{'id':k,'name':v,'afid':y}]
#
# # NODES_CSV = os.path.join(OUTPUT_DIR, 'nodes_{}.csv'.format(time.strftime("%d%m%Y_%H%M%S")))
# # df_nodes = json_normalize(nodes1)
# # df_nodes.to_csv('nodes.csv', sep=',', encoding='utf-8')
#
#
# for key in keyword_eid_author_dict:
#     k_eid_auth_dict[str(key)] = list(set(str(a['authid']) for a in keyword_eid_author_dict[key]))
# for key in citations_eid_author_dict:
#     c_eid_auth_dict[str(key)] = list(set(str(a['authid']) for a in citations_eid_author_dict[key]))
#
#
#
# # #carica pandas dataframe
# # df = json_normalize(json.loads(keyword_search.json_final_str))
# # df.to_csv('prima.csv', sep='\t', encoding='utf-8')
# #
# # cols = df.columns.tolist()
# # # losing a col, but its a duplicate. should be  cols[:14] not  cols[:13]
# # cols = [cols[14]] + cols[:13] + cols[15:]
# # df = df[cols]
# # df.to_csv('dopo.csv', sep='\t', encoding='utf-8')
#
#
#
# edges = {}
# for eid in cited_eid_list:
#     edges[str(eid)] = list(itertools.product(c_eid_auth_dict[eid], k_eid_auth_dict[eid]))
#
# edges_n = 0
# for k in edges:
#     edges_n += len(edges[k])
#
# print 'Generated {} edges for {} articles with citations'.format(edges_n,len(edges))
#
# if not os.path.exists(OUTPUT_DIR):
#     os.makedirs(OUTPUT_DIR)
#     print ('New query: creating a new directory to store .gdf results \n\t{}\n'.format(OUTPUT_DIR))
#
# GDF_OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'citations_{}.gdf'.format(time.strftime("%d%m%Y_%H%M%S")))
#
#
# with open(GDF_OUTPUT_FILE, 'w') as f:
#     # nodes header
#     if sys.version_info[0] == 3:
#         f.write('nodedef>name VARCHAR,label VARCHAR,affiliation VARCHAR\n')
#     else:
#         f.write('nodedef>name VARCHAR,label VARCHAR,affiliation VARCHAR\n'.encode('utf-8'))
#     # nodes list
#     for a in nodes:
#         if sys.version_info[0] == 3:
#             f.write('{}\n'.format(a))
#         else:
#             f.write('{}\n'.format(a))
#     # edges header
#     if sys.version_info[0] == 3:
#         f.write('edgedef>node1 VARCHAR,node2 VARCHAR,label VARCHAR\n')
#     else:
#         f.write('edgedef>node1 VARCHAR,node2 VARCHAR,label VARCHAR\n'.encode('utf-8'))
#     # edges list
#     for e in edges:
#         for pair in edges[e]:
#             temp = str(pair).replace('\'', '').replace(' ', '').replace('(','').replace(')','').encode('utf-8')
#             if sys.version_info[0] == 3:
#                 f.write('{},{}\n'.format(temp, str(e)))
#             else:
#                 f.write('{},{}\n'.format(temp.encode('utf-8'), str(e).encode('utf-8')))
#     f.close()
#
script_log.info('ALL DONE. OK!\n')




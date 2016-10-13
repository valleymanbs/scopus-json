import argparse
from api.scopus_search import ScopusSearch
import itertools
import os
import sys
import time
import pandas as pd
from pandas.io.json import json_normalize
import json



parser = argparse.ArgumentParser()
parser.add_argument("keys", help='search keys surrounded by quotation marks, e.g. \'neural network\'')
parser.add_argument("year", help='Starting year, e.g. 2016')
parser.add_argument("num", help='Number of items to get in a single query, 25, 100 or 200')
args = parser.parse_args()

q = 'TITLE-ABS-KEY({args.keys}) AND PUBYEAR > {args.year}'.format(args=args)

keyword_results_list = ScopusSearch(query=q,
                              items_per_query=int(args.num),
                              #fields='eid,author,dc:description,dc:title'
                              view='COMPLETE', # ONLY STANDARD AT HOME
                              ).valid_results_list

keyword_eid_author_dict = {}
cited_eid_list = []

for e in keyword_results_list:
    if 'eid' not in e:
        print('WARNING: skipped an element, no eid. JSON data was: \n{}\n'.format(e))
    else:
        if 'eid' and 'author' in e:
            keyword_eid_author_dict[str(e['eid'])] = []
            for a in e['author']:
                if 'afid' not in a:
                    a['afid'] = [{'$': ''}]

                keyword_eid_author_dict[str(e['eid'])] += [
                    {'authid': unicode(a['authid']).encode('utf-8'),
                     'afid': unicode(a['afid'][-1]['$']).encode('utf-8'),
                     'surname': unicode(a['surname']).encode('utf-8'),
                     'given-name': unicode(a['given-name']).encode('utf-8')
                     }
                ]


        if 'citedby-count' in e:
            if int(e['citedby-count']) is not 0:
                cited_eid_list += [str(e['eid'])]
            else:
                print('EID {} has no citations:'.format(str(e['eid'])))

n_cited = len(cited_eid_list)
print 'Found {} articles with citations'.format(n_cited)

citations_search_dict = {}
dropped = []

for eid in cited_eid_list:
    print eid
    citations_search_dict[eid] = json.loads(
                                            ScopusSearch(
                                                query='REFEID({})'.format(eid),
                                                items_per_query=int(args.num),
                                                view='COMPLETE' # ONLY STANDARD AT HOME
                                                        ).valid_results_json
                                            )
    if len(citations_search_dict[eid]) == 0:
        dropped += [eid]
        print 'Dropped citation to article {}. No valid results from REFEID search'.format(eid)

    n_cited -= 1
    print 'Still {} citations to search'.format(n_cited)

print 'Citation search done. {} citations dropped due to invalid results'.format(len(dropped))

for eid in dropped:
    cited_eid_list.remove(eid)

citations_eid_author_dict = {}

for key in citations_search_dict:
    for item in citations_search_dict[key]:
        citations_eid_author_dict[key] = []
        for a in item['author']:
            if 'afid' not in a:
                print('Warn: missing afid for authid {} in eid {} '.format(a['authid'].encode('utf-8'),
                                                                           str(key).encode('utf-8'))
                      )
                print a
                a['afid'] = [{'$': ''}]
            citations_eid_author_dict[key] += [
                                {'authid': unicode(a['authid']).encode('utf-8'),
                                 'afid': unicode(a['afid'][-1]['$']).encode('utf-8'),
                                 'surname': unicode(a['surname']).encode('utf-8'),
                                 'given-name': unicode(a['given-name']).encode('utf-8')
                                 }
                              ]


with open('keyword_eid_author_dict', 'w') as f:
    f.write(json.dumps(keyword_eid_author_dict))
    f.close()


with open('cited_eid_list', 'w') as f:
    f.write(json.dumps(cited_eid_list))
    f.close()


with open('citations_eid_author_dict', 'w') as f:
    f.write(json.dumps(citations_eid_author_dict))
    f.close()

nodes = []

k_eid_auth_dict = {}
c_eid_auth_dict = {}


for key in keyword_eid_author_dict:
    for a in keyword_eid_author_dict[key]:
        nodes += ['{},{} {},{}'.format(a['authid'], a['given-name'], a['surname'],a['afid'])]
for key in citations_eid_author_dict:
    for auth in citations_eid_author_dict[key]:
        nodes += ['{},{} {},{}'.format(auth['authid'], auth['given-name'], auth['surname'],auth['afid'])]

print 'Added {} nodes to list'.format(len(nodes))
with open('nodes', 'w') as f:
    f.write(json.dumps(nodes))
    f.close()

nodes = list(set(nodes))
print 'Cleanup nodes list: {} unique nodes now'.format(len(nodes))
with open('set(nodes)', 'w') as f:
    f.write(json.dumps(nodes))
    f.close()

nodes = list(set(nodes))
print '2nd Cleanup nodes list: {} unique nodes now'.format(len(nodes))
with open('set(set(nodes))', 'w') as f:
    f.write(json.dumps(nodes))
    f.close()


for key in keyword_eid_author_dict:
    k_eid_auth_dict[str(key)] = list(set(str(a['authid']) for a in keyword_eid_author_dict[key]))
for key in citations_eid_author_dict:
    c_eid_auth_dict[str(key)] = list(set(str(a['authid']) for a in citations_eid_author_dict[key]))

#print '\nk_eid_auth_dict'
#print k_eid_auth_dict
#print '\nc_eid_auth_dict'
#print c_eid_auth_dict

#
# all_articles = []
# all_articles += keyword_results_list
# #print len(all_articles)
# for key in citations_search_dict:
#     all_articles += citations_search_dict[key]
# print len(all_articles)
# with open('{}.txt'.format(time.strftime("%d%m%Y_%H%M%S")), 'w') as f:
#     json.dump(all_articles, f, indent=4)
#     f.close()

# #carica pandas dataframe
# df = json_normalize(json.loads(keyword_search.json_final_str))
# df.to_csv('prima.csv', sep='\t', encoding='utf-8')
#
# cols = df.columns.tolist()
# # losing a col, but its a duplicate. should be  cols[:14] not  cols[:13]
# cols = [cols[14]] + cols[:13] + cols[15:]
# df = df[cols]
# df.to_csv('dopo.csv', sep='\t', encoding='utf-8')



# nodes = []
edges = {}
for eid in cited_eid_list:
    edges[str(eid)] = list(itertools.product(c_eid_auth_dict[eid], k_eid_auth_dict[eid]))

edges_n = 0
for k in edges:
    edges_n += len(edges[k])

print 'Generated {} edges for {} articles with citations'.format(edges_n,len(edges))

#
# for t in s.eid_authors_dict:
#     nodes += s.eid_authors_dict[t]
#     edges[str(t)] = itertools.combinations(set(s.eid_authors_dict[t]), 2)
#
# nodes = sorted(set(nodes))
# # for p in pairs:
# #     print p
# print ('{} nodes'.format(len(nodes)))
# print ('{} edges'.format(len(edges)))
# # create .gdf
# # dump the list to a file, one EID per line
QUERY_DIR = os.path.join(q.replace('/', '_slash_').replace(' ', '_'))

if not os.path.exists(QUERY_DIR):
    os.makedirs(QUERY_DIR)
    print ('New query: creating a new directory to store .gdf results \n\t{}\n'.format(QUERY_DIR))

GDF_OUTPUT_FILE = os.path.join(QUERY_DIR, 'citations_{}.gdf'.format(time.strftime("%d%m%Y_%H%M%S")))


with open(GDF_OUTPUT_FILE, 'w') as f:
    # nodes header
    if sys.version_info[0] == 3:
        f.write('nodedef>name VARCHAR,label VARCHAR,affiliation VARCHAR\n')
    else:
        f.write('nodedef>name VARCHAR,label VARCHAR,affiliation VARCHAR\n'.encode('utf-8'))
    # nodes list
    for a in nodes:
        if sys.version_info[0] == 3:
            f.write('{}\n'.format(a))
        else:
            f.write('{}\n'.format(a))
    # edges header
    if sys.version_info[0] == 3:
        f.write('edgedef>node1 VARCHAR,node2 VARCHAR,label VARCHAR\n')
    else:
        f.write('edgedef>node1 VARCHAR,node2 VARCHAR,label VARCHAR\n'.encode('utf-8'))
    # edges list
    for e in edges:
        for pair in edges[e]:
            temp = str(pair).replace('\'', '').replace(' ', '').replace('(','').replace(')','').encode('utf-8')
            if sys.version_info[0] == 3:
                f.write('{},{}\n'.format(temp, str(e)))
            else:
                f.write('{},{}\n'.format(temp.encode('utf-8'), str(e).encode('utf-8')))
    f.close()

print 'All done! Graph saved into file\n\t{}\n'.format(GDF_OUTPUT_FILE)




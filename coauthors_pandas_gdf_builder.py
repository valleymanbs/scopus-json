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

s = ScopusSearch(query=q,
                 items_per_query=int(args.num),
                 #fields='eid,author,dc:description,dc:title'
                 view='COMPLETE'
                 )

df = json_normalize(s.json_response)
df.to_csv('normal.csv', sep='\t', encoding='utf-8')

def splitDataFrameList(df,target_column,separator):
    ''' df = dataframe to split,
    target_column = the column containing the values to split
    separator = the symbol used to perform the split
    returns: a dataframe with each entry for the target column separated, with each element moved into a new row.
    The values in the other columns are duplicated across the newly divided rows.
    '''
    def splitListToRows(row,row_accumulator,target_column,separator):
        split_row = str(row[target_column]).split(separator)
        for s in split_row:
            new_row = row.to_dict()
            new_row[target_column] = s
            row_accumulator.append(new_row)
    new_rows = []
    df.apply(splitListToRows,axis=1,args = (new_rows,target_column,separator))
    new_df = pd.DataFrame(new_rows)
    return new_df

new = splitDataFrameList(df,'author','\t')
new.to_csv('new.csv', sep='\t', encoding='utf-8')



# def flatten(l):
#     out = []
#     if isinstance(l, (list, tuple)):
#         for item in l:
#             out.extend(flatten(item))
#     elif isinstance(l, (dict)):
#         for dictkey in l.keys():
#             out.extend(flatten(l[dictkey]))
#     elif isinstance(l, (str, int, unicode)):
#         out.append(l)
#     return out
#
# json_2 = flatten(s.json_response)
#
# with open('flat.json', 'w') as f:
#     json.dump(json_2, f, indent=4)
#     f.close()

nodes = []
edges = {}

for t in s.eid_authors_dict:
    nodes += s.eid_authors_dict[t]
    edges[str(t)] = itertools.combinations(set(s.eid_authors_dict[t]), 2)

nodes = sorted(set(nodes))
# for p in pairs:
#     print p
print ('{} nodes'.format(len(nodes)))
print ('{} edges'.format(len(edges)))
# create .gdf
# dump the list to a file, one EID per line
QUERY_DIR = os.path.join(q.replace('/', '_slash_').replace(' ', '_'))

if not os.path.exists(QUERY_DIR):
    os.makedirs(QUERY_DIR)
    print ('New query: creating a new directory to store .gdf results \n\t{}\n'.format(QUERY_DIR))

out_file = os.path.join(QUERY_DIR, '{}.gdf'.format(time.strftime("%d%m%Y_%H%M%S")))


with open(out_file, 'w') as f:
    # nodes header
    if sys.version_info[0] == 3:
        f.write('nodedef>name VARCHAR,label VARCHAR\n')
    else:
        f.write('nodedef>name VARCHAR,label VARCHAR\n'.encode('utf-8'))
    # nodes list
    for a in nodes:
        if sys.version_info[0] == 3:
            f.write('{},{}\n'.format(a, s.authors_dict[a]))
        else:
            f.write('{},{}\n'.format(a.encode('utf-8'), s.authors_dict[a]))
    # edges header
    if sys.version_info[0] == 3:
        f.write('edgedef>node1 VARCHAR,node2 VARCHAR,label VARCHAR\n')
    else:
        f.write('edgedef>node1 VARCHAR,node2 VARCHAR,label VARCHAR\n'.encode('utf-8'))
    # edges list
    for e in edges:
        for pair in edges[e]:
            temp = str(pair).replace('\'', '').replace(' ', '').replace('(','').replace(')','')
            if sys.version_info[0] == 3:
                f.write('{},{}\n'.format(temp, str(e)))
            else:
                f.write('{},{}\n'.format(temp.encode('utf-8'), str(e).encode('utf-8')))
    f.close()




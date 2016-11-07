import os
import json
import time
import sys

SCOPUS_SEARCH_DIR = os.path.abspath('data/search')

combined_results_list = []

folders_list = os.listdir(SCOPUS_SEARCH_DIR)
queries_list = [x.replace('_',' ') for x in folders_list if "REFEID" not in x and '.' not in x]

for i in range(len(queries_list)):
    print('{}\t-->\t{}\t'.format(i, queries_list[i]))

join_str = input('Please type the list of queries to join (indexes, comma-separated: i.e. \'1,2,4,12\'):')

for i in join_str.split(','):
    if int(i) not in range(len(queries_list)):
        raise IndexError('Please, be sure to input the correct query list indexes.')

join_list = [queries_list[int(i)] for i in join_str.split(',') if int(i) in range(len(queries_list))]

for i in join_list:
    print('Join query {}'.format(i))
    JSON_DATA_FILE = os.path.join(SCOPUS_SEARCH_DIR, i.replace(' ', '_'), 'clean.json')
    # load results list from a previously saved JSON data file
    with open(JSON_DATA_FILE) as data:
        combined_results_list += json.load(data)
        data.close()

OUTPUT_DIR = os.path.abspath('data/joined_searches/{}'.format(time.strftime("%d%m%Y_%H%M%S")))
os.makedirs(OUTPUT_DIR)

OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'clean.json')

with open(OUTPUT_FILE, 'w') as f:
    json.dump(combined_results_list, f, indent=4)
    f.close()

INFO_FILE = os.path.join(OUTPUT_DIR, 'README')
with open(INFO_FILE,'w') as f:
    f.write('Queries joined in clean.json:\n\n')
    for i in join_list:
        if sys.version_info[0] == 3:
            f.write('{}\n'.format(i))
        else:
            f.write('{}\n'.format(i).encode('utf-8'))
    f.close()

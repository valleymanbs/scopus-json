import argparse
from api.scopus_search import ScopusSearch
import itertools
import os
import sys
import time
from pandas.io.json import json_normalize
import json

parser = argparse.ArgumentParser()
parser.add_argument("keys", help='search keys surrounded by quotation marks, e.g. \'neural network\'')
parser.add_argument("start_year", help='Starting year, e.g. 2000')
parser.add_argument("end_year", help='Starting year, e.g. 2016')
parser.add_argument("num", help='Number of items to get in a single query, 25, 100 or 200')
args = parser.parse_args()

q = 'TITLE-ABS-KEY({args.keys}) AND PUBYEAR > {args.start_year} AND PUBYEAR < {args.end_year}'.format(args=args)
QUERY_DIR = os.path.join(q.replace('/', '_slash_').replace(' ', '_'))

keyword_results_list = ScopusSearch(query=q,
                                    items_per_query=int(args.num),
                                    view='COMPLETE',  # ONLY STANDARD AT HOME; need complete to get authors data!
                                    ).valid_results_list
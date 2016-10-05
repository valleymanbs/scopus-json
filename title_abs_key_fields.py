import argparse
from api.scopus_search import ScopusSearch

parser = argparse.ArgumentParser()
parser.add_argument("keys", help='search keys surrounded by quotation marks, e.g. \'neural network\'')
parser.add_argument("year", help='Starting year, e.g. 2016')
parser.add_argument("num", help='Number of items to get in a single query, 25, 100 or 200')
args = parser.parse_args()

s = ScopusSearch(query='TITLE-ABS-KEY({args.keys}) AND PUBYEAR > {args.year}'.format(args=args),
                 items_per_query=int(args.num)
                 )

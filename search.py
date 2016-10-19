import argparse
from api.scopus_search import ScopusSearch

parser = argparse.ArgumentParser()
parser.add_argument("query", help='The search query, e.g.:\'TITLE-ABS-KEY(sentiment analysis) '
                                  'AND PUBYEAR > 2000 AND PUBYEAR < 2010 '
                                  'AND SUBJAREA(COMP) OR SUBJAREA(MATH) OR SUBJAREA(DECI) OR SUBJAREA(SOCI)\'')
args = parser.parse_args()

#q = 'TITLE-ABS-KEY({args.keys}) AND PUBYEAR > {args.start_year} AND PUBYEAR < {args.end_year}'.format(args=args)
q = args.query

ScopusSearch(query=q,
             items_per_query=100,
             view='COMPLETE',  # ONLY STANDARD AT HOME; need complete to get authors data!
             )
import requests
import sys
import os
import json

from api_key import MY_API_KEY

SCOPUS_SEARCH_DIR = os.path.abspath('data/search')


class ScopusSearchViews(object):
    """
    Searches a given query using the ScopusSearch API and saves a list of EIDs to a file under data/search .

    API specs at: http://api.elsevier.com/documentation/SCOPUSSearchAPI.wadl

    :param query: the Scopus Search query
    :type query: str or unicode
    :param view: 'STANDARD' or 'COMPLETE', default='COMPLETE'
    :type view: str or unicode
    :param items_per_query: items to fetch in a single query (25 if not subscriber , 100 if COMPLETE, 200 if STANDARD), default=25
    :type items_per_query: int
    :param max_items: max items to fetch from the in an execution (many queries), default=2000
    :type max_items: int

    """
    def __init__(self, query, view='COMPLETE', items_per_query=25, max_items=2000):
        """
        ScopusSearchApi class initialization
        """

        if not os.path.exists(SCOPUS_SEARCH_DIR):
            os.makedirs(SCOPUS_SEARCH_DIR)
            print ('Search data directory not found, created a new one at \n\t{}\n'.format(SCOPUS_SEARCH_DIR))

        print ('Directory found, EIDs list file will be saved at \n\t{}\n'.format(SCOPUS_SEARCH_DIR))

        self.url = 'http://api.elsevier.com/content/search/scopus'
        self.query = query
        self.view = view
        self.items_per_query = items_per_query
        self.max_items = max_items

        if items_per_query > 100 and self.view == 'COMPLETE':
            self.items_per_query = 100
        if items_per_query > 200:
            self.items_per_query = 200

        self.start_item = 0
        self.found_items_num = 1
        self._EIDS = []
        self._authors = []

        print ('GET data from Search API...')

        while self.found_items_num > 0:
            resp = requests.get(self.url,
                                headers={'Accept': 'application/json', 'X-ELS-APIKey': MY_API_KEY},
                                params={'query': self.query, 'view': self.view, 'count': self.items_per_query,
                                        'start': self.start_item})

            print ('Current query url:\n\t{}\n'.format(resp.url))

            if resp.status_code != 200:
                # error
                raise Exception('ScopusSearchApi status {0}, JSON dump:\n{1}\n'.format(resp.status_code, resp.json()))
            # init case
            if self.found_items_num == 1:
                self.found_items_num = int(resp.json().get('search-results').get('opensearch:totalResults'))
                print ('GET returned {} articles.'.format(self.found_items_num))

            if self.found_items_num > self.max_items:
                print('WARNING: too many results, truncating to {}'.format(self.max_items))
                self.found_items_num = self.max_items

            if 'entry' in resp.json().get('search-results', []):
                self._EIDS += [str(r['eid']) for r in resp.json()['search-results']['entry']]
                #self._authors += {str(r['eid']): str(r['author']) for r in resp.json()['search-results']['entry']}
            print ('Stored EIDs, current number is {}'.format(len(self._EIDS)))

            # write fetched JSON file to disk
            out_file = os.path.join(SCOPUS_SEARCH_DIR,
                                    # remove any / in query to use it as a filename
                                    query.replace('/', '_slash_').replace(' ', '_') +'.'+str(self.start_item)+'.json')

            with open(out_file, 'w') as f:
                json.dump(resp.json(), f, indent=4)
                f.close()

            self.found_items_num -= self.items_per_query
            self.start_item += self.items_per_query
            print ('Still {} EIDs to download'.format(self.found_items_num if self.found_items_num > 0 else 0))

        # finished populating _EIDS, dump the list to a file, one EID per line

        out_file = os.path.join(SCOPUS_SEARCH_DIR,
                                # remove any / in query to use it as a filename
                                query.replace('/', '_slash_').replace(' ', '_'))

        with open(out_file, 'w') as f:
            for eid in self._EIDS:
                if sys.version_info[0] == 3:
                    f.write('{}\n'.format(eid))
                else:
                    f.write('{}\n'.format(eid.encode('utf-8')))

        print ('{} EIDs stored in file \n\t{}\n'.format(len(self._EIDS), out_file))






    @property
    def eid_list(self):
        """Return list of EIDs retrieved."""
        return self._EIDS

    @property
    def authors_list(self):
        """Return list of Authors retrieved."""
        return self._authors








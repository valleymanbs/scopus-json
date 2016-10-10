import requests
# import sys
import os
import json


from api_key import MY_API_KEY

SCOPUS_SEARCH_DIR = os.path.abspath('data/search')


class ScopusSearch(object):
    """
    Class implementation to GET data from the ScopusSearch API.

    When initialized, retrieves an abstracts list for the given query and saves JSON data at folder {cwd}/data/search

    Also fills a list of EIDs and a dictionary of {'EID' : [Authors list]}

    API specs at: http://api.elsevier.com/documentation/SCOPUSSearchAPI.wadl

    IMPORTANT
    To get some fields you need COMPLETE view. You need a subscriber APIKey to get a complete view.
    Non-paying users only get a STANDARD view, so you can get errors on requesting some fields when not a subscriber.

    Check the fields you can get at: http://api.elsevier.com/documentation/search/SCOPUSSearchViews.htm


    :param query: the Scopus Search query
    :type query: str or unicode
    :param fields: comma-separated list of fields to be loaded from the API, IMPORTANT: overrides the view parameter, default=None
    :type fields: str or unicode
    :param view: 'STANDARD' or 'COMPLETE', default=None
    :type view: str or unicode
    :param items_per_query: items to fetch in a single query (25 if not subscriber , 100(using COMPLETE view) or 200(using fields or STANDARD view) if subscriber), default=25
    :type items_per_query: int
    :param max_items: max items to fetch from the in an execution (many queries), default=2000
    :type max_items: int

    """
    def __init__(self, query, fields=None, view=None, items_per_query=100, max_items=2000):
        """
        ScopusSearch class initialization
        IMPORTANT: default parameters only work with a subscriber APIKey
        Not paying users can get only 25 items per query and only STANDARD view or selected fields from a STANDARD view
        """
        print 'ScopusSearch class initialization'

        if fields is None and view is None:
            print ('ERROR: You must pass the fields parameter XOR the view parameter to select the result data.\n'
                   'Check ScopusSearch class documentation for more info.'
                   )
            quit()
        if fields is not None and view is not None:
            print ('WARN: You passed both the fields parameter and the view parameter. Fields search will be used.\n'
                   'Check ScopusSearch class documentation for more info.'
                   )

        if not os.path.exists(SCOPUS_SEARCH_DIR):
            os.makedirs(SCOPUS_SEARCH_DIR)
            print ('Search data directory not found, created a new one at \n\t{}\n'.format(SCOPUS_SEARCH_DIR))

        print ('Search data directory found at \n\t{}\n'.format(SCOPUS_SEARCH_DIR))

        QUERY_DIR = os.path.join(SCOPUS_SEARCH_DIR,
                                 # remove any / in query to use it as a filename
                                 query.replace('/', '_slash_').replace(' ', '_'))
        JSON_DATA_FILE = os.path.join(QUERY_DIR, 'articles.json')

        self._JSON = []
        self._json_loaded = False

        if not os.path.exists(QUERY_DIR):
            os.makedirs(QUERY_DIR)
            print ('New query: creating a new directory to store JSON files at \n\t{}\n'.format(QUERY_DIR))
        else:
            print ('This query has already been cached in the data directory. Loading json from file \n\t{}\n'.format(JSON_DATA_FILE))
            with open(JSON_DATA_FILE) as d:
                self._JSON = json.load(d)
            self._json_loaded = True

        # check if items_per_query is ok for the current request:
        # complete view max 100 items per query
        if items_per_query > 100 and view == 'COMPLETE':
            items_per_query = 100
        # fields, standard view max 200 items per query
        if items_per_query > 200:
            items_per_query = 200

        self._url = 'http://api.elsevier.com/content/search/scopus'
        self._query = query
        self._fields = fields
        self._view = view
        self._items_per_query = items_per_query
        self._max_items = max_items
        self._start_item = 0
        self._found_items_num = 1
        self._eid_list = []
        self._eid_authors_dict = {}
        self._affil_dict = {}
        self._author_dict = {}

        if not self._json_loaded:
            while self._found_items_num > 0:

                # view or fields search selection
                if fields is not None:
                    resp = requests.get(self._url,
                                        headers={'Accept': 'application/json', 'X-ELS-APIKey': MY_API_KEY},
                                        params={'query': self._query, 'field': self._fields, 'count': self._items_per_query,
                                                'start': self._start_item})
                else:
                    resp = requests.get(self._url,
                                        headers={'Accept': 'application/json', 'X-ELS-APIKey': MY_API_KEY},
                                        params={'query': self._query, 'view': view, 'count': self._items_per_query,
                                                'start': self._start_item})

                print ('Current query url:\n\t{}\n'.format(resp.url))

                if resp.status_code != 200:
                    # error
                    raise Exception('ScopusSearchApi status {0}, JSON dump:\n{1}\n'.format(resp.status_code, resp.json()))

                # we set found_items_num=1 at initialization, on the first call it has to be set to the actual value
                if self._found_items_num == 1:
                    self._found_items_num = int(resp.json().get('search-results').get('opensearch:totalResults'))
                    print ('GET returned {} articles.'.format(self._found_items_num))

                if self._found_items_num == 0:
                    pass
                else:
                    # write fetched JSON to a file
                    out_file = os.path.join(QUERY_DIR, str(self._start_item) + '.json')

                    with open(out_file, 'w') as f:
                        json.dump(resp.json(), f, indent=4)
                        f.close()
                    print ('Stored JSON file for this response.')

                    # check if results number exceed the given limit
                    if self._found_items_num > self._max_items:
                        print('WARNING: too many results, truncating to {}'.format(self._max_items))
                        self._found_items_num = self._max_items

                    # check if returned some result
                    if 'entry' in resp.json().get('search-results', []):
                        # add it to this json file - combination of all "entry" fields from the json payloads
                        self._JSON += resp.json()['search-results']['entry']


                # set counters for the next cycle
                self._found_items_num -= self._items_per_query
                self._start_item += self._items_per_query
                print ('Still {} results to be downloaded'.format(self._found_items_num if self._found_items_num > 0 else 0))

            # end while - finished downloading JSON data

            # write abstracts JSON to a file - combination of all "entry" fields from the json payloads got from API
            with open(JSON_DATA_FILE, 'w') as f:
                json.dump(self._JSON, f, indent=4)
                f.close()

        for e in self._JSON:
            if 'eid' in e:
                self._eid_list += [str(e['eid'])]
            else:
                print('WARNING: skipped an element, no eid. JSON data was: \n{}\n'.format(e))
            if 'author' in e and 'eid' in e:
                self._eid_authors_dict[str(e['eid'])] = set([str(i['authid']) for i in e['author']])
            else:
                print('WARNING: skipped an element, no eid or author. JSON data was: \n{}\n'.format(e))
            if 'affiliation' in e :
                for i in e['affiliation']:
                    if i['affilname'] is None:
                        i['affilname'] = 'Null'
                    if i['affiliation-city'] is None:
                        i['affiliation-city'] = 'Null'
                    else:
                        self._affil_dict[str(i['afid'])] = [ i['affilname'], i['affiliation-city'] ]
            if 'author' in e:
                for i in e['author']:
                    if 'afid' in i :
                        # tenere solo ultimo della lista? current affiliation
                        self._author_dict[i['authid']] = set([z['$'] for z in i['afid']])
                    else:
                        self._author_dict[i['authid']] = ['Null']


        # # dump the list to a file, one EID per line
        # out_file = os.path.join(QUERY_DIR,'_EIDS.txt')
        #
        # with open(out_file, 'w') as f:
        #     for eid in self._eid_list:
        #         if sys.version_info[0] == 3:
        #             f.write('{}\n'.format(eid))
        #         else:
        #             f.write('{}\n'.format(eid.encode('utf-8')))
        #
        # print ('{} EIDs stored in file \n\t{}\n'.format(len(self._eid_list), out_file))

    # end __init__

    @property
    def eid_list(self):
        """Return list of EIDs retrieved."""
        return self._eid_list

    @property
    def eid_authors_dict(self):
        """Return list of Authors retrieved."""
        return self._eid_authors_dict

    @property
    def json_response(self):
        """Return JSON data."""
        return self._JSON



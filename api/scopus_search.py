import requests
# import sys
import os
import json
import logging
import time

from api_key import MY_API_KEY

# logging utility configuration
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger(" requests.packages.urllib3 ")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True



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
    def __init__(self, query, fields=None, view=None, items_per_query=100, max_items=5000, no_log=False):
        """
        ScopusSearch class initialization
        IMPORTANT: default parameters only work with a subscriber APIKey
        IMPORTANT: ScopusSearch max results limit is 5000 :( you get HTTP 404 for more results
        Not paying users can get only 25 items per query and only STANDARD view or selected fields from a STANDARD view
        """
        search_log = logging.getLogger(' ScopusSearch.{} '.format(query))
        
        if no_log:
            search_log.setLevel(logging.WARNING)
            requests_log.setLevel(logging.WARNING)
        
        search_log.info('ScopusSearch class initialization with query {}'.format(query))

        if fields is None and view is None:
            search_log.error('You must pass the fields parameter XOR the view parameter to select the result data.\n'
                   'Check ScopusSearch class documentation for more info.'
                   )
            quit()
        if fields is not None and view is not None:
            search_log.warn('You passed both the fields parameter and the view parameter. Fields search will be used.\n'
                   'Check ScopusSearch class documentation for more info.'
                   )

        if not os.path.exists(SCOPUS_SEARCH_DIR):
            os.makedirs(SCOPUS_SEARCH_DIR)
            search_log.info('Search data directory not found, created a new one at \n\t{}\n'.format(SCOPUS_SEARCH_DIR))

        # check if items_per_query is ok for the current request:
        # complete view max 100 items per query
        if items_per_query > 100 and view == 'COMPLETE':
            items_per_query = 100
        # fields, standard view max 200 items per query
        if items_per_query > 200:
            items_per_query = 200

        # print ('Search data directory found at \n\t{}\n'.format(SCOPUS_SEARCH_DIR))

        # set data file path strings
        _QUERY_DIR = os.path.join(SCOPUS_SEARCH_DIR, query.replace('/', '_slash_').replace(' ', '_'))
        _JSON_RAW_DATA_FILE = os.path.join(_QUERY_DIR, 'raw.json')
        _JSON_CLEAN_DATA_FILE = os.path.join(_QUERY_DIR, 'clean.json')
        _JSON_FINAL_DATA_FILE = os.path.join(_QUERY_DIR, 'FINAL.json')

        # data from the single queries will be combined here
        self._combined_results_list = []



        self._url = 'http://api.elsevier.com/content/search/scopus'
        # self._query = query
        # self._fields = fields
        # self._view = view
        # self._items_per_query = items_per_query
        self._max_items = max_items
        self._start_item = 0
        self._still_to_download_n = 1
        self._eid_list = []
        self._eid_authors_dict = {}
        self._affil_dict = {}
        self._author_dict = {}

        self._first_run = True
        self._json_loaded = False
        self._results_n = 0

        if not os.path.exists(_QUERY_DIR):
            os.makedirs(_QUERY_DIR)
        else:
            if not os.path.exists(_JSON_RAW_DATA_FILE):
                pass
            else:
                search_log.info(
                        'This query has already been cached in the data directory. '
                        'Loading json from file \n\t{}\n'.format(_JSON_RAW_DATA_FILE)
                      )
                # load results list from a previously saved JSON data file
                with open(_JSON_RAW_DATA_FILE) as data:
                    self._combined_results_list = json.load(data)
                    data.close()
                self._json_loaded = True

        if not self._json_loaded:
            while self._still_to_download_n > 0:
                search_log.info("GET from remote...")
                start = time.time()
                # view or fields search selection
                if fields is not None:
                    resp = requests.get(self._url,
                                        headers={'Accept': 'application/json', 'X-ELS-APIKey': MY_API_KEY},
                                        params={'query': query, 'field': fields, 'count': items_per_query,
                                                'start': self._start_item})
                else:
                    resp = requests.get(self._url,
                                        headers={'Accept': 'application/json', 'X-ELS-APIKey': MY_API_KEY},
                                        params={'query': query, 'view': view, 'count': items_per_query,
                                                'start': self._start_item})

                # print ('Current query url:\n\t{}\n'.format(resp.url))
                search_log.info("Request completed in %.3fs" % (time.time() - start))

                if resp.status_code != 200:
                    # error
                    raise Exception('ScopusSearchApi status {0}, JSON dump:\n{1}\n'.format(resp.status_code, resp.json()))

                # we set found_items_num=1 at initialization, on the first call it has to be set to the actual value
                if self._still_to_download_n == 1 and self._first_run:
                    self._still_to_download_n = int(resp.json().get('search-results').get('opensearch:totalResults'))
                    self._first_run = False
                    self._results_n = self._still_to_download_n


                    search_log.info("Returned {} articles".format(self._results_n))
                    #print ('GET returned {} articles.'.format(self._found_items_num))

                if self._still_to_download_n == 0:
                    pass
                else:
                    # write fetched JSON to a file
                    out_file = os.path.join(_QUERY_DIR, str(self._start_item) + '.json')

                    with open(out_file, 'w') as f:
                        json.dump(resp.json(), f, indent=4)
                        f.close()
                    search_log.info('Stored JSON file for this partial response.')

                    # check if results number exceed the given limit
                    if self._still_to_download_n > self._max_items:
                        search_log.warn('Too many results, truncating to {}'.format(self._max_items))
                        self._still_to_download_n = self._max_items

                    # check if returned some result
                    if 'entry' in resp.json().get('search-results', []):
                        # add it to this json file - combination of all "entry" fields from the json payloads
                        self._combined_results_list += resp.json()['search-results']['entry']

                # set counters for the next cycle
                self._still_to_download_n -= items_per_query
                self._start_item += items_per_query
                search_log.info('Still {} results to be downloaded'.format(self._still_to_download_n if self._still_to_download_n > 0 else 0))

            # end while - finished downloading JSON data

            # write abstracts JSON to a file - combination of all "entry" fields from the json payloads got from API
            with open(_JSON_RAW_DATA_FILE, 'w') as f:
                json.dump(self._combined_results_list, f, indent=4)
                f.close()
        # end if
        search_log.info("Cleaning results list from invalid entries...")
        start = time.time()

        # cleanup JSON, keep only consistent values with both eid and author
        i = 0
        dropped_n = 0
        while i < len(self._combined_results_list):
            if 'author' not in self._combined_results_list[i] or 'eid' not in self._combined_results_list[i]:
                dropped_n += 1
                self._combined_results_list.pop(i)
            else:
                i += 1

        if dropped_n > 0:
            search_log.warn('No eid or author, dropped {} results'.format(dropped_n))

        # save a clean.json with the valid entries
        with open(_JSON_CLEAN_DATA_FILE, 'w') as f:
            json.dump(self._combined_results_list, f, indent=4)
            f.close()

        search_log.info("Results cleaned and written to file in %.3fs" % (time.time() - start))

        # TODO: MOVE OUTSIDE CLASS

#        search_log.info("2nd Cleanup...")
#        start = time.time()
#
#        # begin JSON manipulation, entries will be saved in a new list
#        self._JSON_FINAL = []
#
#        for e in self._combined_results_list:
#            temp = dict(e)
#            for a in e['author']:
#                temp['author'] = dict(a)
#                self._JSON_FINAL.append(temp)
#
#        JSON_DATA_FILE1 = os.path.join(_QUERY_DIR, 'step1.json')
#        with open(JSON_DATA_FILE1, 'w') as f:
#            json.dump(self._JSON_FINAL, f, indent=4)
#            f.close()
#
#        i = 0
#        while i < len(self._JSON_FINAL):
#            if self._JSON_FINAL[i] == self._JSON_FINAL[(i+1) % len(self._JSON_FINAL)]:
#                self._JSON_FINAL.pop(i)
#            else:
#                i += 1
#
#
#        JSON_DATA_FILE3 = os.path.join(_QUERY_DIR, 'step2.json')
#
#        with open(JSON_DATA_FILE3, 'w') as f:
#            json.dump(self._JSON_FINAL, f, indent=4)
#            f.close()
#
#        missing_n = 0
#
#        for e in self._JSON_FINAL:
#            if 'afid' in e['author']:
#                e['author']['afid'] = e['author']['afid'][-1]['$']
#                for z in e['affiliation']:
#
#                    if z['afid'] != e['author']['afid']:
#                        e['affiliation'].remove(z)
#
#                e['affiliation'] = e['affiliation'][-1]
#            else:
#                missing_n += 1
#                e['author']['afid'] = ''
#                e['affiliation'] = {}
#
#        if missing_n > 0:
#            search_log.warn('Found {} authors with missing affiliation, filling with null'.format(missing_n))
#
#        with open(_JSON_FINAL_DATA_FILE, 'w') as f:
#            json.dump(self._JSON_FINAL, f, indent=4)
#            f.close()
#
#        search_log.info("2nd cleanup done in %.3fs" % (time.time() - start))

        # END TODO MOVE OUTSIDE CLASS


        # for e in self._JSON:
        #     if 'eid' in e:
        #         self._eid_list += [str(e['eid'])]
        #     else:
        #         print('WARNING: skipped an element, no eid. JSON data was: \n{}\n'.format(e))
        #     if 'author' in e and 'eid' in e:
        #         #self._eid_authors_dict[str(e['eid'])] = set([str(i['authid']) for i in e['author']])
        #         pass
        #     else:
        #         print('WARNING: skipped an element, no eid or author. JSON data was: \n{}\n'.format(e))
        #     if 'affiliation' in e :
        #         for i in e['affiliation']:
        #             if i['affilname'] is None:
        #                 i['affilname'] = 'Null'
        #             if i['affiliation-city'] is None:
        #                 i['affiliation-city'] = 'Null'
        #             else:
        #                 self._affil_dict[str(i['afid'])] = [ i['affilname'], i['affiliation-city'] ]
        #     if 'author' in e:
        #         for i in e['author']:
        #             if 'afid' in i :
        #                 # tenere solo ultimo della lista? current affiliation
        #                 self._author_dict[i['authid']] = set([z['$'] for z in i['afid']])
        #             else:
        #                 self._author_dict[i['authid']] = ['Null']


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

        # useless
        # if self._results_n-len(self._combined_results_list) > 0:
        #   print 'dropped {} elements'.format(self._results_n - len(self._combined_results_list))

        search_log.info('\n@@@@@@ ScopusSearch completed. {} valid elements found @@@@@@\n'.format(len(self._combined_results_list)))

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
    def authors_dict(self):
        """Return list of Authors retrieved."""
        return self._author_dict

    @property
    def valid_results_list(self):
        """
        Return a list containing the cleaned response
        only valid entries (eid AND author) are listed
        """
        return self._combined_results_list

    @property
    def valid_results_json(self):
        """
        Return JSON string containing the cleaned response
        only valid entries (eid AND author) are listed
        """
        return json.dumps(self._combined_results_list)

#    @property
#    def json_final_str(self):
#        """Return JSON string of the final output"""
#        return json.dumps(self._JSON_FINAL)

    @property
    def results_n(self):
        """Return JSON string of the final output"""
        return self._results_n

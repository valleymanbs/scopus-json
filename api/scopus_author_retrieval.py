import requests
import os
import json

from api.api_key import MY_API_KEY

SCOPUS_AUTHOR_DIR = os.path.abspath('data/author')


class ScopusAuthorRetrieval(object):
    """
    Class implementation to GET data from the Scopus AbstractRetrieval API.

    When initialized, retrieves the abstract having the given EID and saves JSON data at folder {cwd}/data/abstract

    Also initializes a few basic attributes inside the object, like title, website link, date, etc.

    API specs at: http://api.elsevier.com/documentation/AbstractRetrievalAPI.wadl

    IMPORTANT
    To get some fields you need FULL view. You need a subscriber APIKey to get a full view.
    Non-paying users only get a META view, so you can get errors on requesting some fields when not a subscriber.

    Check the fields you can get at: http://api.elsevier.com/documentation/retrieval/AbstractRetrievalViews.htm

    :param eid: abstract's Scopus internal identification code (EID)
    :type eid: str or unicode
    :param fields: comma-separated list of fields to be loaded from the API, IMPORTANT: overrides the view parameter, default=None
    :type fields: str or unicode
    :param view: 'BASIC','META','META_ABS', 'REF' or 'FULL', default=None
    :type view: str or unicode
    """

    def __init__(self, authid, fields=None, view=None):

        print ('ScopusAuthorRetrieval class initialization')

        if fields is None and view is None:
            print ('ERROR: You must pass the fields parameter XOR the view parameter to select the result data.\n'
                   'Check ScopusAbstractRetrieval class documentation for more info.'
                   )
            quit()
        if fields is not None and view is not None:
            print ('WARN: You passed both the fields parameter and the view parameter. Fields search will be used.\n'
                   'Check ScopusAbstractRetrieval class documentation for more info.'
                   )

        if not os.path.exists(SCOPUS_AUTHOR_DIR):
            os.makedirs(SCOPUS_AUTHOR_DIR)
            print ('Abstract data directory not found, created a new one at \n\t{}\n'.format(SCOPUS_AUTHOR_DIR))

        print ('Abstract data directory found at \n\t{}\n'.format(SCOPUS_AUTHOR_DIR))

        # attributes declaration
        self._url = ("http://api.elsevier.com/content/author/author_id/" + authid)
        self._EID = authid
        self._JSON = []
        self._json_loaded = False

        self._date = None
        self._publication = None
        self._title = None
        self._description = None
        self._authors = []

        JSON_DATA_FILE = os.path.join(SCOPUS_AUTHOR_DIR,
                                      self._EID.replace('/', '_slash_').replace(' ', '_') + '.json')

        # check if the query has already been cached and load stored JSON file
        if not os.path.exists(JSON_DATA_FILE):
            print ('New query: results will be saved into this JSON file: \n\t{}\n'.format(JSON_DATA_FILE))
        else:
            print ('This query has already been cached in the data directory. Loading json from file \n\t{}\n'.format(JSON_DATA_FILE))
            with open(JSON_DATA_FILE) as d:
                self._JSON = json.load(d)
            self._json_loaded = True

        # if not, send query to server, fill JSON from response and dump it to a file
        if not self._json_loaded:

            print ('GET data from AuthorRetrieval API...')

            # view or fields search selection
            if fields is not None:
                resp = requests.get(self._url,
                                    headers={'Accept': 'application/json', 'X-ELS-APIKey': MY_API_KEY},
                                    params={'field': fields}
                                    )
            else:
                resp = requests.get(self._url,
                                    headers={'Accept': 'application/json', 'X-ELS-APIKey': MY_API_KEY},
                                    params={'view': view}
                                    )

            print ('Current query url:\n\t{}\n'.format(resp.url))

            #print(resp)

            if resp.status_code != 200:
                # error
                raise Exception('AuthorRetrievalApi status {0}, JSON dump:\n{1}\n'.format(resp.status_code, resp.json()))
            self._JSON = resp.json()

            # write fetched JSON file to disk
            out_file = os.path.join(SCOPUS_AUTHOR_DIR,
                                    # remove any / in query to use it as a filename
                                    self._EID.replace('/', '_slash_').replace(' ', '_') +'.json')

            with open(out_file, 'w') as f:
                json.dump(self._JSON, f, indent=4)
                f.close()

        # endif
        # json loaded, fill the attributes ['afid', 'affilname', 'affiliation-city', 'affiliation-country']

        self._afid = ''
        self._affilname = ''
        self._affilcountry = ''
        self._affilcity = ''

        if 'author-profile' in self._JSON.get('author-retrieval-response', [])[0]:
            profile = self._JSON.get('author-retrieval-response', [])[0]['author-profile']
            if 'affiliation-current' in profile:
                affiliation = profile['affiliation-current']['affiliation']['ip-doc']
                if '@id' in affiliation:
                    self._afid = affiliation['@id']
                if 'afdispname' in affiliation:
                    self._affilname = affiliation['afdispname']
                if 'address' in affiliation:
                    address = affiliation['address']
                    if 'country' in address:
                        self._affilcountry = address['country']
                    if 'city' in address:
                        self._affilcity = address['city']



    @property
    def query_url(self):
        """Query url"""
        return self._url


    @property
    def afid(self):
        """Abstract title."""
        return self._afid.encode('utf-8')

    @property
    def affilname(self):
        """Abstract title."""
        return self._affilname.encode('utf-8')

    @property
    def affilcountry(self):
        """Name of source the abstract is published in."""
        return self._affilcountry.encode('utf-8')

    @property
    def affilcity(self):
        """The date of the cover the abstract is in."""
        return self._affilcity.encode('utf-8')

    @property
    def json_response(self):
        """Return JSON data."""
        return self._JSON

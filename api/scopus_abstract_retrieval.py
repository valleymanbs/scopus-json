import requests
import sys
import os
import json

from api_key import MY_API_KEY

SCOPUS_ABSTRACT_DIR = os.path.abspath('data/abstract')


class ScopusAbstractRetrieval(object):
    """
    Class to represent a response from ScopusAbstractIRetrieval API.

    The results are retrieved by the EID from a query. The results
    are cached in a folder ~/.scopus/xml/{eid}.
    """

    def __init__(self, eid):
        """
        ScopusAbstractRetrievalApi class initialization
        """

        if not os.path.exists(SCOPUS_ABSTRACT_DIR):
            os.makedirs(SCOPUS_ABSTRACT_DIR)
            print ('Abstract data directory not found, created a new one at \n\t{}\n'.format(SCOPUS_ABSTRACT_DIR))

        print ('Directory found, abstracts list file will be saved at \n\t{}\n'.format(SCOPUS_ABSTRACT_DIR))

        self._url = ("http://api.elsevier.com/content/abstract/eid/" + eid)
        self._EID = eid

        print ('GET data from AbstractRetrieval API...')

        resp = requests.get(self.url,
                            headers={'Accept': 'application/json', 'X-ELS-APIKey': MY_API_KEY}
                            )

        print ('Current query url:\n\t{}\n'.format(resp.url))

        if resp.status_code != 200:
            # error
            raise Exception('AbstractRetrievalApi status {0}, JSON dump:\n{1}\n'.format(resp.status_code, resp.json()))

        # write fetched JSON file to disk
        out_file = os.path.join(SCOPUS_ABSTRACT_DIR,
            # remove any / in query to use it as a filename
            self._EID.replace('/', '_slash_').replace(' ', '_') +'.json')

        with open(out_file, 'w') as f:
            json.dump(resp.json(), f, indent=4)
            f.close()


        # if 'coredata' in resp.json().get('abstracts-retrieval-response', []):
        #     self._EID += [str(r['eid']) for r in resp.json()['search-results']['entry']]
        # print ('Stored EIDs, current number is {}'.format(len(self._EIDS)))





        # finished populating _EIDS, dump the list to a file, one EID per line

        # out_file = os.path.join(SCOPUS_ABSTRACT_DIR,
        #                         # remove any / in query to use it as a filename
        #                         query.replace('/', '_slash_').replace(' ', '_'))
        #
        # with open(out_file, 'w') as f:
        #     for eid in self._EIDS:
        #         if sys.version_info[0] == 3:
        #             f.write('{}\n'.format(eid))
        #         else:
        #             f.write('{}\n'.format(eid.encode('utf-8')))
        #
        # print ('{} EIDs stored in file \n\t{}\n'.format(len(self._EIDS), out_file))

    @property
    def query_url(self):
        """Query url"""
        return self._url

    @property
    def website_url(self):
        """Url to the abstract on the Scopus website"""
        return self._web

    @property
    def title(self):
        """Abstract title."""
        return self._title

    @property
    def publication_name(self):
        """Name of source the abstract is published in."""
        return self._publication

    @property
    def cover_date(self):
        """The date of the cover the abstract is in."""
        return self._date

    @property
    def authors_list(self):
        """A list of authors id"""
        return self._authors


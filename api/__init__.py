import os

SCOPUS_API_FILE = os.path.abspath('api/api_key.py')
if os.path.exists(SCOPUS_API_FILE):
    with open(SCOPUS_API_FILE) as f:
        exec(f.read())
else:
    raise Exception('{} not found. Please create it and put your API key in it.'.format(SCOPUS_API_FILE))
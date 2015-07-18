from distutils.core import setup
import py2exe, sys, os

CLIENT_SECRETS = open('client_secrets.json').read()

'''
setup(
    console=['YoutubeCollector.py'],
    data_files=['cacert.pem', 'cacerts.txt', 'client_secrets.json']
)

'''
my_data_files = []

includes = ['apiclient','httplib2','oauth2client', 'uritemplate', 'googleapiclient', 'BaseHTTPServer', 'SimpleHTTPServer']

setup(console=[{'script': "YoutubeCollector.py",
            'other_resources': [
                 (u'CLIENT_SECRETS', 1, CLIENT_SECRETS)
            ]
           }],
            options = {'py2exe': {'bundle_files': 1, 'compressed': True,
                                  "includes" : includes}
           },

           zipfile = None,

     data_files=['cacert.pem', 'cacerts.txt', 'client_secrets.json', 'CHANGES.TXT', ('conf', ['conf/videos_2_follow_config_file.cfg'])]
 )

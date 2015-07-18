#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'antoniofsanjuan'
__file__ = 'YoutubeCollector'

import argparse
import datetime
import commands
import os
import sys
import time
import codecs
import HTMLParser
import getopt
import platform
import math

import httplib2

#from apiclient.errors import HttpError
from YoutubeSearch import YoutubeSearch
from YoutubeChannel import YoutubeChannel
from SocialWebSites import SocialWebSites
from dao.DAOYoutubeCollector import DAOYoutubeCollector
import dao.DAOYoutubeCollector
from S3manager import S3manager

from gdata.youtube import service
from plus import GooglePlusService
from YoutubeComments_v3 import GoogleCommentsService

from apiclient import discovery

from oauth2client.file import Storage
from oauth2client.client import flow_from_clientsecrets
from oauth2client import tools
from oauth2client.client import AccessTokenRefreshError

from oauth2client.client import OAuth2WebServerFlow

from apiclient.discovery import build

import httplib2
import os
import sys

from apiclient.discovery import build_from_document
from apiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow

import progressbar
from threading import Thread, Event
from Queue import Queue

USERNAME = 'uahytcollector@gmail.com'
PASSWORD = 'x3jkW5.a'
#VIDEO_ID = 'eM8OCWmTqTY'  # '4_X6EyqXa2s'

date_time_format = "%Y%m%d_%H%M"
date_time_sec_format = "%Y%m%d %H:%M:%S"

date_time_log = time.strftime(date_time_format)

_mode = "auto"
_from = None
_to = None
_b_avoid_ddbb = True
_load_files_to_s3 = False
_get_files_from_s3 = False
_timeStamp = None

_DIR_LOG = "LOGS"
_DIR_DATA = "DATA"
_DIR_CONFIG = "conf"
_file_log = None

_progress_bar = None
_proccessing_video = None

_query_bulk_load_yt_comments = None
_query_bulk_load_gp_comments = None
_query_insert_yt_video_info = None
_query_insert_yt_channel_info = None
_query_insert_yt_social_shares = None

_query_bulk_load_yt_comments_regex = """LOAD DATA LOCAL INFILE '%s' IGNORE
                                     INTO TABLE YT_COMMENTS CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t'
                                     OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\\n' """

_query_bulk_load_gp_comments_regex = """LOAD DATA LOCAL INFILE '%s' IGNORE
                                     INTO TABLE GP_COMMENTS CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t'
                                     OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\\n' """

_query_insert_yt_video_info_regex = """LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE YT_VIDEOS
                                    FIELDS TERMINATED BY '\\t'
                                    OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\\n' """

_query_insert_yt_channel_info_regex = """LOAD DATA LOCAL INFILE '%s' IGNORE
                                      INTO TABLE YT_CHANNELS FIELDS TERMINATED BY '\\t'
                                      OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\\n' """

_query_insert_yt_social_shares_regex = """LOAD DATA LOCAL INFILE '%s' IGNORE
                                       INTO TABLE YT_SOCIAL_SHARES FIELDS TERMINATED BY '\\t'
                                       OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\\n' """

#_yt_csv_file_path = "g:\\\TFC\\\DATA\\\yt_comments_%s.csv"
#_gp_csv_file_path = "g:\\\TFC\\\DATA\\\gp_comments_%s.csv"
#_yt_videos_csv_file_path = "g:\\\TFC\\\DATA\\\yt_videos_%s.csv"
#_yt_channels_csv_file_path = "g:\\\TFC\\\DATA\\\yt_channel_%s.csv"
#_yt_social_csv_file_path = "g:\\\TFC\\\DATA\\\yt_social_%s.csv"

_yt_csv_file_path = None
_gp_csv_file_path = None
_yt_videos_csv_file_path = None
_yt_channels_csv_file_path = None
_yt_social_csv_file_path = None
_videos_2_follow_config_file_path = None


if (platform.system().lower() == 'windows'):

    _file_log_name = '%s\YoutubeGatherer_%s.log' % (_DIR_LOG, date_time_log)

    _yt_csv_file_path_regex = "DATA\\\yt_comments_%s.csv"
    _gp_csv_file_path_regex = "DATA\\\gp_comments_%s.csv"
    _yt_videos_csv_file_path_regex = "DATA\\\yt_videos_%s.csv"
    _yt_channels_csv_file_path_regex = "DATA\\\yt_channel_%s.csv"
    _yt_social_csv_file_path_regex = "DATA\\\yt_social_%s.csv"
    _videos_2_follow_config_file_path = '%s\\%s'% (_DIR_CONFIG, 'videos_2_follow_config_file.cfg')

else:

    _file_log_name = '%s/YoutubeGatherer_%s.log' % (_DIR_LOG, date_time_log)

    _yt_csv_file_path_regex = "DATA/yt_comments_%s.csv"
    _gp_csv_file_path_regex = "DATA/gp_comments_%s.csv"
    _yt_videos_csv_file_path_regex = "DATA/yt_videos_%s.csv"
    _yt_channels_csv_file_path_regex = "DATA/yt_channel_%s.csv"
    _yt_social_csv_file_path_regex = "DATA/yt_social_%s.csv"
    _videos_2_follow_config_file_path = '%s/%s'% (_DIR_CONFIG, 'videos_2_follow_config_file.cfg')


_gp_csv_file = None
_yt_csv_file = None
_yt_videos_csv_file = None
_yt_channels_csv_file = None
_yt_social_csv_file = None
_videos_2_follow_config_file = None

def printLogTime(msg, b_time):

    global _file_log
    date_sec = time.strftime(date_time_sec_format)

    b_time = True if b_time is None else False

    #msg = msg.replace('\xef\xbb\xbf', '')
    if isinstance(msg, str):
        msg = msg.decode("utf-8")
    else:
        msg = unicode(msg)

    #msg = msg.decode('utf-8')

    try:
        if(b_time):
            _file_log.write('[%s]: %s\n' % (date_sec, msg))
        else:
            _file_log.write('%s\n' % msg)
    except UnicodeDecodeError:
        #print("--------------> Caracter no reconocido. Seguimos...")
        _file_log.write('%s\n' % msg.decode('utf-8'))
        sys.exc_clear()

def printCommentsFile(msg):

    global _yt_csv_file

    if isinstance(msg, str):
        msg = msg.decode("utf-8")
    else:
        msg = unicode(msg)

    try:
        _yt_csv_file.write('%s\n' % msg)
    except UnicodeDecodeError:
        #print("--------------> Caracter no reconocido. Seguimos...")
        _yt_csv_file.write('%s\n' % msg.decode('utf-8'))
        sys.exc_clear()


def printLog(msg):

    printLogTime(msg, True)

def deleteVideoInfo(yt_service, video_id):
    query_delete_gp_video_comments = "DELETE FROM GP_COMMENTS WHERE VIDEO_ID = '%s'" % video_id
    query_delete_yt_video_comments = "DELETE FROM YT_COMMENTS WHERE ID_VIDEO = '%s'" % video_id
    query_delete_yt_video_info = "DELETE FROM YT_VIDEOS WHERE ID_VIDEO = '%s'" % video_id

    yt_service.executeLoadInBD(query_delete_gp_video_comments)
    yt_service.executeLoadInBD(query_delete_yt_video_comments)
    yt_service.executeLoadInBD(query_delete_yt_video_info)


def openDataFiles(mode):
    global _gp_csv_file
    global _yt_csv_file
    global _yt_videos_csv_file
    global _yt_channels_csv_file
    global _yt_social_csv_file

    try:
        _gp_csv_file = codecs.open(_gp_csv_file_path, mode, 'utf-8')
        _yt_csv_file = codecs.open(_yt_csv_file_path, mode, 'utf-8')
        _yt_videos_csv_file = codecs.open(_yt_videos_csv_file_path, mode, 'utf-8')
        _yt_channels_csv_file = codecs.open(_yt_channels_csv_file_path, mode, 'utf-8')
        _yt_social_csv_file = codecs.open(_yt_social_csv_file_path, mode, 'utf-8')
    except IOError as ioe:
        print "ERROR: Error opening file: %s [%s]" % (ioe.filename, ioe.strerror)
        sys.exit(3)

def closeDataFiles():
    global _gp_csv_file
    global _yt_csv_file
    global _yt_videos_csv_file
    global _yt_channels_csv_file
    global _yt_social_csv_file

    try:
        _gp_csv_file.close()
        _yt_csv_file.close()
        _yt_videos_csv_file.close()
        _yt_channels_csv_file.close()
        _yt_social_csv_file.close()

    except:
        pass


def loadDataFilesInBD(yt_service):

    printLog("Loading data videos file into database... ")
    yt_service.executeLoadInBD(_query_insert_yt_video_info)
    #raw_input('Press any key')

    printLog("Loading youtube data comments into database...\n")
    yt_service.executeLoadInBD(_query_bulk_load_yt_comments)
    #raw_input('Press any key')

    printLog("Loading google+ data comments into database...\n")
    yt_service.executeLoadInBD(_query_bulk_load_gp_comments)
    #raw_input('Press any key')

    printLog("Loading channel info into database...\n")
    yt_service.executeLoadInBD(_query_insert_yt_channel_info)
    #raw_input('Press any key')

    printLog("Loading social web-sites shares data into database...\n")
    yt_service.executeLoadInBD(_query_insert_yt_social_shares)
    #raw_input('Press any key')

def loadVideos2Follow():

    daoYoutubeCollector = DAOYoutubeCollector()

    return daoYoutubeCollector.getYoutubeVideosToFollow()

def loadVideos2FollowFromConfigFile(yt_search_service):

    global _videos_2_follow_config_file_path

    arr_videos = []
    arr_id_videos = None

    if os.path.exists(_videos_2_follow_config_file_path):

        _videos_2_follow_config_file = codecs.open(_videos_2_follow_config_file_path, 'r', 'utf-8')
        arr_videos_info = [line.rstrip(os.linesep) for line in _videos_2_follow_config_file]
        _videos_2_follow_config_file.close()

        for row_video_info in arr_videos_info:

            #print "DEBUG: row_video_info=%s" % row_video_info
            row_video = row_video_info

            video = yt_search_service.getVideo(row_video)
            #print "DEBUG: channelId=%s" % video["snippet"]["channelId"]

            arr_videos.append(video)

    else:
        print 'ERROR. The file %s does not exists.' % _videos_2_follow_config_file_path

    return arr_videos


def get_oauth2_authenticated_service(args):

        print "get_oauth2_authenticated_service: *** INIT ***"

        CLIENT_SECRETS_FILE = "client_secrets.json"

        # A limited OAuth 2 access scope that allows for uploading files, but not other
        # types of account access.
        YOUTUBE_READONLY_SCOPE = "https://www.googleapis.com/auth/youtube.readonly"
        YOUTUBE_READ_WRITE_SSL_SCOPE = "https://www.googleapis.com/auth/youtube.force-ssl"
        YOUTUBE_API_SERVICE_NAME = "youtube"
        YOUTUBE_API_VERSION = "v3"

        REDIRECT_URI = 'http://localhost:8080/'

        # Helpful message to display if the CLIENT_SECRETS_FILE is missing.
        MISSING_CLIENT_SECRETS_MESSAGE = """
        WARNING: Please configure OAuth 2.0

        To make this sample run you will need to populate the client_secrets.json file
        found at:

           %s

        with information from the APIs Console
        https://code.google.com/apis/console#access

        For more information about the client_secrets.json file format, please visit:
        https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
        """ % os.path.abspath(os.path.join(os.path.dirname(__file__),
                                           CLIENT_SECRETS_FILE))

        credentials = None
        num_retries = 3
        b_error = True

        while num_retries > 0 and b_error:

            if 0 < num_retries < 3:
                print "Retrying connection..."

            try:

                flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE,
                  message=MISSING_CLIENT_SECRETS_MESSAGE,
                  scope=YOUTUBE_READ_WRITE_SSL_SCOPE)

                #parser = argparse.ArgumentParser(parents=[tools.argparser])
                #flags = parser.parse_args()

                #if credentials is None or credentials.invalid:
                #    credentials = run_flow(flow, storage, flags)


                storage = Storage("%s-oauth2.dat" % sys.argv[0])
                credentials = storage.get()

                # Building default args to pass to run_flow function
                # This avoid conflicts with the custom params of the application
                flags = tools.argparser.parse_args(args=[])

                if credentials is None or credentials.invalid:

                    print "\t Credential is not longer valid"
                    credentials = run_flow(flow, storage, flags)


                youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, http=credentials.authorize(httplib2.Http()))

                #results = youtube.commentThreads().list(
                #    part="snippet",
                #    videoId='PAHRBOYG8sg',
                #    maxResults=5,
                #    key='usKbvXnvKS9XA377cncN0oZJ'
                #).execute()
                #
                #for item in results["items"]:
                #    comment = item["snippet"]["topLevelComment"]
                #    author = comment["snippet"]["authorDisplayName"]
                #    text = comment["snippet"]["textDisplay"]
                #    print "Comment by %s: %s" % (author, text)


                return youtube


                    #auth_uri = flow.step1_get_authorize_url()
                    #
                    #opener = httplib2.Http();
                    #opener.follow_all_redirects = True;
                    #opener.follow_redirects = True;
                    #
                    #(response, body) = opener.request(auth_uri)
                    #
                    #print "response %s" % response
                    #
                    ## Manual refresh entering the url by console
                    #auth_code = raw_input('Enter authorization code (parameter of URL): ')
                    #credentials = flow.step2_exchange(auth_code)
                    #
                    #storage.put(credentials)

                #http_auth = credentials.authorize(httplib2.Http())
                #
                #print "get_oauth2_authenticated_service: *** END ***"
                #
                #return discovery.build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, http=http_auth)

                b_error = False

            except AccessTokenRefreshError:
                print "Warming: Credential is expired. Retrying connection..."
                b_error = True
                num_retries -= 1
                time.sleep(5)
                continue

        print "get_oauth2_authenticated_service: *** END NONE***"
        return None



def usage():
    print 'Usage: '+sys.argv[0]+' -m <auto|search|loader[-t yyyy-mm-dd]>'
    print '\t-m: or --mode, set the working mode (search, auto collector and loader)'
    print '\t\t--from: load data files since the specified date wirh format \'yyyy-mm-dd\''
    print '\t\t--db: load data files into MySql database. MySql database must be created and activated.'
    #print '\t\t--s3: upload data files to S3 Amazon service.'


def print_progress(q, e):

    while True:
        if e.is_set():
            # if our event is set, break out of the infinite loop and
            # prepare to terminate this thread

            sys.stdout.write('\r\t[%s] %s%%' % (('#' * int(p / 2)).ljust(50, ' '), 100))
            break

        while not q.full():
            # wait for more progress to be made
            time.sleep(0.1)

        # get the current progress value
        p = q.get()

        sys.stdout.write('\r\t[%s] %s%%' % (('#' * int(p / 2)).ljust(50, ' '), int(p)))
        #sys.stdout.write(' --> t = %s' % time.strftime(date_time_sec_format))
        sys.stdout.flush()


############################################
###############     MAIN    ################
############################################

## TODO: Pensar como tratar los comentarios repetidos que aparecen en el fichero yt_comments
##      Si se utiliza la opcion IGNORE en el LOAD no hay problema en la carga, solo muestra esas lineas como warning

def main(argv):
    reload(sys)
    sys.setdefaultencoding("utf-8")


def main(argv):

    #print os.path.abspath(os.path.join(os.path.dirname(__file__), 'cacerts.txt'))

    #exit(0)

    try:
        opts, args = getopt.getopt(argv[1:], 'hmf:', ['help', 'mode=', 'from', 'db', 's3', 's3-get'])
        if not opts:
            print 'No options supplied'
            #usage()
    except getopt.GetoptError,e:
        print e
        usage()
        sys.exit(2)

    for opt, arg in opts:
        #print 'DEBUG: opt=%s' % opt
        if opt in ('-h', '--help'):
            usage()
            sys.exit(2)
        elif opt in ('-m', '--mode'):
            print 'opt es -m; arg es: %s' % arg
            global _mode
            _mode = arg
            #print 'DEBUG: _mode = %s' % _mode
        elif opt in ('-f', '--from'):
            if _mode == 'loader':
                global _from
                _from = arg
            else:
                print "Error. '--mode loader' is required to set --from parameter."
                sys.exit(2)
        elif opt in ('-t'): # If parameter set, load data into database
            global _timeStamp
            _timeStamp = arg
        elif opt in ('--db'): # If parameter set, load data into database
            global _b_avoid_ddbb
            _b_avoid_ddbb = False
        elif opt in ('--s3'): # If parameter set, upload data files to Amazon S3
            global _load_files_to_s3
            _load_files_to_s3 = True
        elif opt in ('--s3-get'): # If parameter set, daownload data files from Amazon S3 to local
            global _get_files_from_s3
            _get_files_from_s3 = True


    argv = []

    global _query_bulk_load_yt_comments
    global _query_bulk_load_gp_comments
    global _query_insert_yt_video_info
    global _query_insert_yt_channel_info
    global _query_insert_yt_social_shares

    global _yt_csv_file_path
    global _gp_csv_file_path
    global _yt_videos_csv_file_path
    global _yt_channels_csv_file_path
    global _yt_social_csv_file_path

    global _gp_csv_file
    global _yt_csv_file
    global _yt_videos_csv_file
    global _yt_channels_csv_file
    global _yt_social_csv_file

    global _progress_bar
    global _proccessing_video

    _proccessing_video = None
    # ----- SOCIAL WEBS -------

    video_url = "https://www.youtube.com/watch?v=%s"
    social_web_service = SocialWebSites()

    #social_web_service.getFacebookLinkSharedCount(video_url)
    #social_web_service.getTwitterLinkSharedCount(video_url)
    #social_web_service.getLinkedlnLinkSharedCount(video_url)

    yt_search_service = YoutubeSearch()

    # ----- USER --------

    #yt_service = service.YouTubeService()
    #user_entry = yt_service.GetYouTubeUserEntry(username='vegetta777')
    #yt_search_service.PrintUserEntry(user_entry)

    # --- CHANNELS ----

    yt_channel_service = YoutubeChannel()

    yt_comments_service = GoogleCommentsService(argv)
    gp_service = GooglePlusService(argv)

    # Verify if diectory log exists, if not, create it
    if not os.path.exists(_DIR_LOG):
        os.makedirs(_DIR_LOG)

    if not os.path.exists(_DIR_DATA):
        os.makedirs(_DIR_DATA)

    if not os.path.exists(_DIR_CONFIG):
        os.makedirs(_DIR_CONFIG)

    #TODO: Cargar todos los parametros desde un fichero de configuracion: rutas, constantes, etc.
    #TODO: Cambiar el load por otra forma en la que se puedan cargar en remoto de forma rapida

    ts = time.time()
    today = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

    _yt_csv_file_path = _yt_csv_file_path_regex % today
    _gp_csv_file_path = _gp_csv_file_path_regex % today
    _yt_videos_csv_file_path = _yt_videos_csv_file_path_regex % today
    _yt_channels_csv_file_path = _yt_channels_csv_file_path_regex % today
    _yt_social_csv_file_path = _yt_social_csv_file_path_regex % today

    _query_bulk_load_yt_comments = _query_bulk_load_yt_comments_regex % _yt_csv_file_path
    _query_bulk_load_gp_comments = _query_bulk_load_gp_comments_regex % _gp_csv_file_path
    _query_insert_yt_video_info = _query_insert_yt_video_info_regex  % _yt_videos_csv_file_path
    _query_insert_yt_channel_info = _query_insert_yt_channel_info_regex % _yt_channels_csv_file_path
    _query_insert_yt_social_shares = _query_insert_yt_social_shares_regex %_yt_social_csv_file_path


    #client = service.YouTubeService()
    #client.ClientLogin(USERNAME, PASSWORD)


    #args = argparser.parse_args()
    yt_service = get_oauth2_authenticated_service(args)


    #http_auth = credentials.authorize(httplib2.Http())
    #yt_service = build('youtube', 'v3', http=http_auth)

    global _file_log
    global _file_log_name
    _file_log = codecs.open(_file_log_name, mode = 'wb', encoding = 'utf-8')

    '''
    s = '(\xef\xbd\xa1\xef\xbd\xa5\xcf\x89\xef\xbd\xa5\xef\xbd\xa1)\xef\xbe\x89'
    s1 = s.decode('utf-8')
    #aux = aux.replace('\xef\xbb\xbf', '')

    _file_log.write(s1)
    exit(0)
    '''

    ''' Uncomment to test files without delete content ('wb' mode) previusly'''
    '''
    openDataFiles('r')
    '''



    #GooglePlusService(argv)


    '''
    printLog("Loading youtube data file comments %s into database...\n" % _yt_csv_file_path)
    yt_comments_service.executeLoadInBD(_query_bulk_load_yt_comments)
    exit(0)
    '''

    '''
    printLog("Loading data videos file into database... ")
    yt_comments_service.executeLoadInBD(query_insert_yt_video_info)
    exit(0)
    '''


    '''
    printLog("Loading google+ data comments into database...\n")
    yt_comments_service.executeLoadInBD(query_bulk_load_gp_comments)

    printLog("Loading channel info into database...\n")
    yt_comments_service.executeLoadInBD(query_insert_yt_channel_info)
    '''

    '''
    printLog("Loading social web-sites shares data into database...\n")
    yt_comments_service.executeLoadInBD(_query_insert_yt_social_shares)

    exit(0)
    '''

    try:
        print "MODE: %s" % _mode
        printLog("MODE: %s" % _mode)

        arr_videos = []

        if(_mode == 'search'):

            openDataFiles('wb')

            print "* Write the subject you are looking for. The first result will be used to retrive the data.\n"
            subject_input = raw_input('What are you looking for? ')
            if subject_input == "":
                print "The subject cannot be empty."
                exit(0)
            #    subject_input = "T_km8oZ2g98"

            order_input = raw_input(
                'What order do you wish (date, rating, relevance, title, videoCount, viewCount)? (Default: relevance)')
            #max_results_input = raw_input('Max results? (Default: 1)')
            max_results_input = "1"
            _b_avoid_ddbb = raw_input('Avoid Database (Y/N)? (Default: Y) ')

            order_input = "relevance" if (order_input == "") else order_input
            max_results_input = "1" if (max_results_input == "") else max_results_input
            _b_avoid_ddbb = True if ((_b_avoid_ddbb == "") | (_b_avoid_ddbb == "Y")) else False

            argparser.add_argument("--q", help="Search term", default=subject_input)
            argparser.add_argument("--max-results", help="Max results", default=max_results_input)
            argparser.add_argument("--order", help="Order of results", default=order_input)

            args = argparser.parse_args()

            try:

                arr_videos = yt_search_service.youtube_search(args)

                yt_search_service.printYoutubeInfo2CSVFile(arr_videos, _yt_videos_csv_file)

            except HttpError, e:
                print "An HTTP error %d occurred:\n%s" % (e.resp.status, e.content)

            printLog("Looking for:")
            printLog("\tSubject: %s" % subject_input)
            printLog("\tMax-results: %s" % max_results_input)
            printLog("\tOrder by: %s" % order_input)
            printLog("\n")

        elif _mode == 'auto':

            print "Processing videos in automatic mode.\n"
            printLog("Processing videos in automatic mode.\n")

            if not _b_avoid_ddbb:
                print "\t* Load files into database activated.\n"
                printLog("\t* Load files into database activated.\n")

            if _load_files_to_s3:
                print "\t* Upload files to Amazon S3 service is activated."
                printLog("\t* Upload files to Amazon S3 service is activated.")

            if _get_files_from_s3:
                print "\t* Downloading data files from Amazon S3..."
                printLog("\t* Downloading data files from Amazon S3...")

                s3_service = S3manager()

                s3_service.get_files_to_local('G:\\tmp')

                exit(0)


            ''' In automatic mode, we have to APPEND multiple videos data to files'''
            openDataFiles('a')
            arr_videos = loadVideos2FollowFromConfigFile(yt_search_service)

            yt_search_service.printYoutubeInfo2CSVFile(arr_videos, _yt_videos_csv_file)


        elif _mode == 'loader':

            printLog("Loading data files into database...")

            # Concatenate the timeStamp config in arg
            time_stamp_file = ""
            time_stamp_file = _timeStamp if _timeStamp is not None else today;

            _yt_csv_file_path = _yt_csv_file_path_regex % time_stamp_file
            _gp_csv_file_path = _gp_csv_file_path_regex % time_stamp_file
            _yt_videos_csv_file_path = _yt_videos_csv_file_path_regex  % time_stamp_file
            _yt_channels_csv_file_path = _yt_channels_csv_file_path_regex % time_stamp_file
            _yt_social_csv_file_path = _yt_social_csv_file_path_regex % time_stamp_file

            _query_bulk_load_yt_comments = _query_bulk_load_yt_comments_regex % _yt_csv_file_path
            _query_bulk_load_gp_comments = _query_bulk_load_gp_comments_regex % _gp_csv_file_path
            _query_insert_yt_video_info = _query_insert_yt_video_info_regex  % _yt_videos_csv_file_path
            _query_insert_yt_channel_info = _query_insert_yt_channel_info_regex % _yt_channels_csv_file_path
            _query_insert_yt_social_shares = _query_insert_yt_social_shares_regex %_yt_social_csv_file_path

            openDataFiles('r')
            loadDataFilesInBD(yt_comments_service)

            closeDataFiles()

            printLog("\tData files have been loaded correctly!\n")




        yt_count = 0
        gp_count = 0
        for video in arr_videos:
            video_id = video['id']
            printLog("Processing video with id: '%s' and total comments: %s\n" % ( video_id, video['statistics']['commentCount'] ))
            print "Processing video with id: '%s' and total comments: %s\n" % ( video_id, video['statistics']['commentCount'] )

            channel_id = video['snippet']['channelId']

            if channel_id is not None and channel_id != "":
                printLog("Printing channel ['%s'] info into file... " % channel_id)
                yt_channel_service.printChannelInfo2CSVFile(channel_id, yt_channel_service.get_channel_info(channel_id),
                                                        _yt_channels_csv_file)

                channel_id = video['snippet']['channelId']
                printLog("\nRetrieving video info %s and channel detail %s" % (video['id'], video['snippet']['channelId']))
                yt_channel_service.get_channel_info(channel_id)

            printLog("\nRetrieving \"Shares\" from social websites...")
            n_fb_shares = social_web_service.getFacebookLinkSharedCount(video_url % video['id'])
            n_tw_shares = social_web_service.getTwitterLinkSharedCount(video_url % video['id'])
            n_lk_shares = social_web_service.getLinkedlnLinkSharedCount(video_url % video['id'])

            try:

                social_web_service.printSocialSharesInfo2CSVFile(video['id'], _yt_social_csv_file)

            except:
                printLogTime("ERROR: Error writing Social Shares to file.\nException: %s" % sys.exc_info()[0])

            printLog("\n")

            printLog("\nRetrieving \"Comments\" from Youtube...\n")
            print "Retrieving comments from Youtube..."

            #yt_comments_service.comments_generator(yt_service, video_id)
            ###bar = Bar('Loading', fill='@', suffix='%(percent)d%%')

            queue = Queue(1)   # used to communicate progress to the thread
            event = Event()    # used to tell the thread when to finish
            progress = 0
            _progress_bar = Thread(target=print_progress, args=(queue, event))

            total_comments = int(video['statistics']['commentCount'])
            _progress_bar.start()

            for item in yt_comments_service.comments_generator(yt_service, video_id):

                yt_count += 1

                comment = item["snippet"]["topLevelComment"]
                author = comment["snippet"]["authorDisplayName"]
                text = comment["snippet"]["textDisplay"]
                likes = comment["snippet"]["likeCount"]

                comment_id = comment['id']

                reply_count = int( item["snippet"]['totalReplyCount'] )
                #print "\tDEBUG: Author = %s" % author
                #print "\tDEBUG: Comment = %s" % text
                #print "\tDEBUG: reply_count = %s" % reply_count

                try:

                    _yt_csv_file.write(yt_comments_service.printCSVYoutubeComment(item, video_id, likes))

                except:
                    printLogTime("ERROR: Error writing Youtube comment to file.\nException: %s" % sys.exc_info()[0], True)
                    printLogTime(sys.exc_info(), True);
                    pass

                if reply_count > 0:
                    # Retriving Likes for the global activity
                    gp_comment_activity_likes = gp_service.getActivityById(comment_id)
                    printLog("\n\tRetrieving \"G+ Comments\" from G+ Social Network...")

                    # Retriving all comments and fields, then write them down to the G+ file
                    for gp_comment in gp_service.googlePlusActitivyInfoGenerator(comment_id):

                        gp_count+=1
                        # TODO: Los comentarios aqui impresos no todos son G+, los que no contienen codigo HTML pueden ser de Youtube--> comprobar
                        printLogTime("\t\tG+_Author:%s" % gp_comment['actor']['displayName'], False)
                        #print("Comment: %s" % gp_comment['object']['content'])
                        printLogTime("\t\t\t%s" % gp_comment['object']['content'], False)

                        try:
                            _gp_csv_file.write(yt_comments_service.printCSVGooglePlusComment(gp_service, gp_comment, comment_id,
                                                                                        reply_count, video_id))
                        except:
                            printLogTime("ERROR: Error writing G+ comment to file.\nException: %s" % sys.exc_info()[0], True)
                            pass

                        # End For gp_comments Loop
                else:

                    try:
                        _yt_csv_file.write(yt_comments_service.printCSVYoutubeComment(comment, video_id, 0))
                    except:
                        printLogTime("ERROR: Error writing Youtube comment to file.\nException: %s" % sys.exc_info()[0], True)
                        pass

                printLogTime("", False)
                # End For comments Loop

                progress = math.ceil( ((yt_count + gp_count) * 100) / total_comments )
                queue.put( min(progress, 100) )

            # reached 100%; kill the thread and exit
            event.set()
            _progress_bar.join()
            print ""


        _COMMENNTS_COUNT = yt_count + gp_count
        printLog("\n*****************************************")
        printLog("Total comments: %s" % _COMMENNTS_COUNT)
        printLog("\tYT comments: %s" % yt_count)
        printLog("\tGP comments: %s" % gp_count)
        printLog("*****************************************\n")

        # Close data files after writting info
        closeDataFiles()

        printLog("\n")
        if not _b_avoid_ddbb:

            if _mode != 'auto':
                b_cargar = raw_input("*** Do you want to load comments data files into database? (Y/N): ")
            else:
                b_cargar = "Y";

            if b_cargar == "Y":
                printLog("Loading data files into database...")
                openDataFiles('r')

                loadDataFilesInBD(yt_comments_service)

                closeDataFiles()

                printLog("\tData files have been loaded correctly!\n")
            else:
                printLog("*** WARM ***: \tData files have not been loaded!")


        if _load_files_to_s3:

            ## Uncomment to list files in working directory at HEROKU
            #print "List of files:\n"
            #for file in os.listdir("."):
            #    print file


            s3_service = S3manager()

            time_folder = time.strftime("%Y%m%d")
            data_folder = time_folder + s3_service._OS_REMOTE_PATH_SEPARATOR + 'DATA'

            s3_service.createFolder(data_folder)

            openDataFiles('r')

            arr_files_to_s3 = []
            arr_files_to_s3.append( _gp_csv_file_path )
            arr_files_to_s3.append( _yt_csv_file_path )
            arr_files_to_s3.append( _yt_videos_csv_file_path )
            arr_files_to_s3.append( _yt_channels_csv_file_path )
            arr_files_to_s3.append( _yt_social_csv_file_path )

            s3_result = s3_service.upload_files(arr_files_to_s3, time_folder)

            if s3_result:
                print "Files upload to S3 completed successfully!"
            else:
                print "Error: Files couldn't be upload to S3 storage!"


            if not s3_service.check_space_available():
                print "Warning: Check space available in S3 service. Delete files if needed."


    except (KeyboardInterrupt, SystemExit):
        print "\n\nShutting down youtube-gatherer..."
        pass
    finally:
        closeDataFiles()

    _file_log.close()


if __name__ == '__main__':
    main(sys.argv)
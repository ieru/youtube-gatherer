#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'antoniofsanjuan'
__file__ = 'YoutubeCollector'

import datetime
import commands
import os
import sys
import time
import codecs
import HTMLParser
import getopt
import dao.DAOYoutubeCollector

from apiclient.errors import HttpError
from YoutubeSearch import YoutubeSearch
from YoutubeChannel import YoutubeChannel
from SocialWebSites import SocialWebSites
from dao.DAOYoutubeCollector import DAOYoutubeCollector

from gdata.youtube import service
from plus import GooglePlusService
from YoutubeComments import GoogleCommentsService
from oauth2client.tools import argparser


USERNAME = 'uahytcollector@gmail.com'
PASSWORD = 'x3jkW5.a'
#VIDEO_ID = 'eM8OCWmTqTY'  # '4_X6EyqXa2s'

date_time_format = "%Y%m%d_%H%M"
date_time_sec_format = "%Y%m%d %H:%M:%S"

date_time_log = time.strftime(date_time_format)

_mode = "search"
_from = None
_to = None

_DIR_LOG = "LOGS"
_DIR_DATA = "DATA"
_file_log = None
_file_log_name = '%s\YoutubeGatherer_%s.log' % (_DIR_LOG, date_time_log)

_query_bulk_load_yt_comments = """LOAD DATA LOCAL INFILE '%s' IGNORE
                                 INTO TABLE YT_COMMENTS CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t'
                                 OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\\n' """

_query_bulk_load_gp_comments = """LOAD DATA LOCAL INFILE '%s' IGNORE
                                 INTO TABLE GP_COMMENTS CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t'
                                 OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\\n' """

_query_insert_yt_video_info = """LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE YT_VIDEOS
                                FIELDS TERMINATED BY '\\t'
                                OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\\n' """

_query_insert_yt_channel_info = """LOAD DATA LOCAL INFILE '%s' IGNORE
                                  INTO TABLE YT_CHANNELS FIELDS TERMINATED BY '\\t'
                                  OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\\n' """

_query_insert_yt_social_shares = """LOAD DATA LOCAL INFILE '%s' IGNORE
                                   INTO TABLE YT_SOCIAL_SHARES FIELDS TERMINATED BY '\\t'
                                   OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\\n' """

#_yt_csv_file_path = "g:\\\TFC\\\DATA\\\yt_comments_%s.csv"
#_gp_csv_file_path = "g:\\\TFC\\\DATA\\\gp_comments_%s.csv"
#_yt_videos_csv_file_path = "g:\\\TFC\\\DATA\\\yt_videos_%s.csv"
#_yt_channels_csv_file_path = "g:\\\TFC\\\DATA\\\yt_channel_%s.csv"
#_yt_social_csv_file_path = "g:\\\TFC\\\DATA\\\yt_social_%s.csv"

_yt_csv_file_path = "DATA\\\yt_comments_%s.csv"
_gp_csv_file_path = "DATA\\\gp_comments_%s.csv"
_yt_videos_csv_file_path = "DATA\\\yt_videos_%s.csv"
_yt_channels_csv_file_path = "DATA\\\yt_channel_%s.csv"
_yt_social_csv_file_path = "DATA\\\yt_social_%s.csv"


_gp_csv_file = None
_yt_csv_file = None
_yt_videos_csv_file = None
_yt_channels_csv_file = None
_yt_social_csv_file = None

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
        print("--------------> Caracter no reconocido. Seguimos...")
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
        print("--------------> Caracter no reconocido. Seguimos...")
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

    _gp_csv_file = codecs.open(_gp_csv_file_path, mode, 'utf-8')
    _yt_csv_file = codecs.open(_yt_csv_file_path, mode, 'utf-8')
    _yt_videos_csv_file = codecs.open(_yt_videos_csv_file_path, mode, 'utf-8')
    _yt_channels_csv_file = codecs.open(_yt_channels_csv_file_path, mode, 'utf-8')
    _yt_social_csv_file = codecs.open(_yt_social_csv_file_path, mode, 'utf-8')

def closeDataFiles():
    global _gp_csv_file
    global _yt_csv_file
    global _yt_videos_csv_file
    global _yt_channels_csv_file
    global _yt_social_csv_file

    _gp_csv_file.close()
    _yt_csv_file.close()
    _yt_videos_csv_file.close()
    _yt_channels_csv_file.close()
    _yt_social_csv_file.close()



def loadDataFilesInBD(yt_service):

    printLog("Loading data videos file into database... ")
    yt_service.executeLoadInBD(_query_insert_yt_video_info)

    printLog("Loading youtube data comments into database...\n")
    yt_service.executeLoadInBD(_query_bulk_load_yt_comments)

    printLog("Loading google+ data comments into database...\n")
    yt_service.executeLoadInBD(_query_bulk_load_gp_comments)

    printLog("Loading channel info into database...\n")
    yt_service.executeLoadInBD(_query_insert_yt_channel_info)

    printLog("Loading social web-sites shares data into database...\n")
    yt_service.executeLoadInBD(_query_insert_yt_social_shares)

def loadVideos2Follow():

    daoYoutubeCollector = DAOYoutubeCollector()

    return daoYoutubeCollector.getYoutubeVideosToFollow()




def usage():
    print 'Usage: '+sys.argv[0]+' -m <auto|search|loader[-t yyyy-mm-dd]>'
    print '\t-m: or --mode, set the working mode (search, auto collector and loader)'
    print '\t\t--from: load data files since the specified date wirh format \'yyyy-mm-dd\''
    print '\t\t--to: load data files until the specified date wirh format \'yyyy-mm-dd\' or today if none is set.'

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
        opts, args = getopt.getopt(argv[1:], 'hmf:', ['help', 'mode='])
        if not opts:
            print 'No options supplied'
            #usage()
    except getopt.GetoptError,e:
        print e
        usage()
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage()
            sys.exit(2)
        elif opt in ('-m', '--mode'):
            global _mode
            _mode = arg
        elif opt in ('-f', '--from'):
            if _mode == 'loader':
                global _from
                _from = arg
            else:
                print "Error. '--mode loader' is required to set --from parameter."
                sys.exit(2)


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

    if _mode == 'loader' and _from is None:
        print "Error. '--from' parameter is required to set mode 'loader'."
        sys.exit(2)
    elif _from is not None and (_mode is None or _mode != 'loader'):
        print "Error. '--mode' parameter is required to set --from parameter."
        sys.exit(2)


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

    #TODO: Cargar todos los parametros desde un fichero de configuracion: rutas, constantes, etc.
    #TODO: Cambiar el load por otra forma en la que se puedan cargar en remoto de forma rapida

    ts = time.time()
    today = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

    _yt_csv_file_path %= today
    _gp_csv_file_path %= today
    _yt_videos_csv_file_path %= today
    _yt_channels_csv_file_path %= today
    _yt_social_csv_file_path %= today

    _query_bulk_load_yt_comments %= _yt_csv_file_path
    _query_bulk_load_gp_comments %= _gp_csv_file_path
    _query_insert_yt_video_info %= _yt_videos_csv_file_path
    _query_insert_yt_channel_info %= _yt_channels_csv_file_path
    _query_insert_yt_social_shares %= _yt_social_csv_file_path


    client = service.YouTubeService()
    client.ClientLogin(USERNAME, PASSWORD)

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
        printLog("MODE: %s" % _mode)

        arr_videos = []
        b_avoid_ddbb = True

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
            b_avoid_ddbb = raw_input('Avoid Database (Y/N)? (Default: Y) ')

            order_input = "relevance" if (order_input == "") else order_input
            max_results_input = "1" if (max_results_input == "") else max_results_input
            b_avoid_ddbb = True if ((b_avoid_ddbb == "") | (b_avoid_ddbb == "Y")) else False

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

            printLog("Processing videos in automatic mode...")
            ''' In automatic mode, we have to APPEND multiple videos data to files'''
            openDataFiles('a')
            arr_videos = loadVideos2Follow()
            b_avoid_ddbb = False

        elif _mode == 'loader':

            printLog("Loading data files into database...")

            openDataFiles('r')
            loadDataFilesInBD(yt_comments_service)

            closeDataFiles()

            printLog("\tData files have been loaded correctly!\n")


        yt_count = 0
        gp_count = 0
        for video in arr_videos:
            video_id = video['id']
            printLog("Processing video with id: %s\n" % video_id)

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
            for comment in yt_comments_service.comments_generator(client, video_id):

                #print "-----> DEBUG: Tenemos un nuevo comentario..."
                yt_count += 1
                author_name = comment.author[0].name.text
                text = comment.content.text
                last_pos = comment.id.text.rfind('/')

                comment_id = comment.id.text[last_pos + 1::]
                printLogTime("YT_Comment:%s" % comment.id.text, False)
                printLogTime("Author:%s" % comment.author[0].name.text, False)

                printLogTime(comment.content.text, False)

                comment_string = comment.ToString()
                replycount_first = comment_string.find("replyCount")
                replycount_last = comment_string.find("</ns1:replyCount>")
                printLog("*** G+ Comments? ----> %s" % comment_string[replycount_first + 11:replycount_last])
                reply_count = int(comment_string[replycount_first + 11:replycount_last])

                if reply_count > 0:
                    # Retriving Likes for the global activity
                    gp_comment_activity_likes = gp_service.getActivityById(comment_id)
                    printLog("\n\tRetrieving \"G+ Comments\" from G+ Social Network...")

                    try:

                        _yt_csv_file.write(yt_comments_service.printCSVYoutubeComment(comment, video_id,
                                                                                 gp_comment_activity_likes))
                    except:
                        printLogTime("ERROR: Error writing Youtube comment to file.\nException: %s" % sys.exc_info()[0], True)
                        pass

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

        _COMMENNTS_COUNT = yt_count + gp_count
        printLog("\n*****************************************")
        printLog("Total comments: %s" % _COMMENNTS_COUNT)
        printLog("\tYT comments: %s" % yt_count)
        printLog("\tGP comments: %s" % gp_count)
        printLog("*****************************************\n")

        # Close data files after writting info
        closeDataFiles()

        printLog("\n")
        if not b_avoid_ddbb:

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

        _file_log.close()

    except (KeyboardInterrupt, SystemExit):
        print "\n\nShutting down youtube-gatherer..."
        pass

if __name__ == '__main__':
    main(sys.argv)
__author__ = 'antoniofsanjuan'
# -*- coding: utf-8 -*-

import os
import sys
import time
import codecs
import HTMLParser

from apiclient.errors import HttpError
from YoutubeSearch import YoutubeSearch
from YoutubeChannel import YoutubeChannel
from SocialWebSites import SocialWebSites

from gdata.youtube import service
from plus import GooglePlusService
from YoutubeComments import GoogleCommentsService
from oauth2client.tools import argparser



USERNAME = 'uahytcollector@gmail.com'
PASSWORD = 'u4hytc0ll3ct0r'
VIDEO_ID = 'eM8OCWmTqTY'  # '4_X6EyqXa2s'

date_time_format = "%Y%m%d_%H%M"
date_time_sec_format = "%Y%m%d %H:%M:%S"

date_time_log = time.strftime(date_time_format)

_DIR_LOG = "G:\TFC\LOGS"
_file_log = None
_file_log_name = '%s\ytCollector_%s.log' % (_DIR_LOG, date_time_log)


def printLog(msg):
    date_sec = time.strftime(date_time_sec_format)
    global _file_log
    _file_log.write('[%s]: %s\n' % (date_sec, msg))


def deleteVideoInfo(yt_service, video_id):
    query_delete_gp_video_comments = "DELETE FROM GP_COMMENTS WHERE VIDEO_ID = '%s'" % video_id
    query_delete_yt_video_comments = "DELETE FROM YT_COMMENTS WHERE ID_VIDEO = '%s'" % video_id
    query_delete_yt_video_info = "DELETE FROM YT_VIDEOS WHERE ID_VIDEO = '%s'" % video_id

    yt_service.executeLoadInBD(query_delete_gp_video_comments)
    yt_service.executeLoadInBD(query_delete_yt_video_comments)
    yt_service.executeLoadInBD(query_delete_yt_video_info)


############################################
###############     MAIN    ################
############################################

## TODO: Pensar como tratar los comentarios repetidos que aparecen en el fichero yt_comments
##      Si se utiliza la opcion IGNORE en el LOAD no hay problema en la carga, solo muestra esas lineas como warning

def main(argv):

    reload(sys)
    sys.setdefaultencoding("utf-8")

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
    yt_channel_service.get_channel_info("UCam8T03EOFBsNdR0thrFHdQ")


    yt_comments_service = GoogleCommentsService(argv)
    gp_service = GooglePlusService(argv)

    # Verify if diectory log exists, if not, create it
    if not os.path.exists(_DIR_LOG):
        os.makedirs(_DIR_LOG)

    #TODO: Cargar todos los parametros desde un fichero de configuracion: rutas, constantes, etc.
    #TODO: Cambiar el load por otra forma en la que se puedan cargar en remoto de forma rapida

    query_bulk_load_yt_comments = "SET NAMES utf8mb4; LOAD DATA INFILE 'G:/TFC/yt_comments.csv' " \
                                  "INTO TABLE YT_COMMENTS CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' " \
                                  "OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' "

    query_bulk_load_gp_comments = "SET NAMES utf8mb4; LOAD DATA INFILE 'G:/TFC/gp_comments.csv' " \
                                  "INTO TABLE GP_COMMENTS CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' " \
                                  "OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' "

    query_insert_yt_video_info = "LOAD DATA INFILE 'G:/TFC/yt_videos.csv' INTO TABLE YT_VIDEOS " \
                                 "FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' "

    #query_insert_yt_video_info = "INSERT INTO YT_VIDEO_SHARES (VIDEO_ID)"

    gp_csv_file = codecs.open("g:\TFC\gp_comments.csv", 'wb', 'utf-8')
    yt_csv_file = codecs.open("g:\TFC\yt_comments.csv", 'wb', 'utf-8')

    #GooglePlusService(argv)

    client = service.YouTubeService()
    client.ClientLogin(USERNAME, PASSWORD)

    global _file_log
    global _file_log_name
    _file_log = codecs.open(_file_log_name, 'wb', 'utf-8')

    #############################
    ### Youtube Videos Search ###
    #############################
    yt_videos_csv_file = codecs.open("g:\TFC\yt_videos.csv", 'wb', 'utf-8')
    yt_channels_csv_file = codecs.open("g:\TFC\yt_channel.csv", 'wb', 'utf-8')
    yt_social_csv_file = codecs.open("g:\TFC\yt_social.csv", 'wb', 'utf-8')

    subject_input = raw_input('What are you looking for? ')
    order_input = raw_input('What order do you wish (date, rating, relevance, title, videoCount, viewCount)? ')
    max_results_input = raw_input('Max results? ')
    b_avoid_ddbb = raw_input('Avoid Database (Y/N)? ')

    argparser.add_argument("--q", help="Search term", default=subject_input)
    argparser.add_argument("--max-results", help="Max results", default=max_results_input)
    argparser.add_argument("--order", help="Order of results", default=order_input)

    args = argparser.parse_args()

    try:

        arr_videos = yt_search_service.youtube_search(args)

        yt_search_service.printYoutubeInfo2CSVFile(arr_videos, yt_videos_csv_file)

    except HttpError, e:
        print "An HTTP error %d occurred:\n%s" % (e.resp.status, e.content)

    printLog("Looking for:")
    printLog("\tSubject: %s" % subject_input)
    printLog("\tMax-results: %s" % max_results_input)
    printLog("\tOrder by: %s" % order_input)

    for video in arr_videos:
        video_id = video['id']
        printLog("Processing video with id: %s" % video_id)

        channel_id = video['snippet']['channelId']
        printLog("Printing channel info into file... ")
        yt_channel_service.printChannelInfo2CSVFile(channel_id, yt_channel_service.get_channel_info(channel_id),
                                                    yt_channels_csv_file)

        channel_id = video['snippet']['channelId']
        print "Retrieving video info %s and channel detail %s" % (video['id'], video['snippet']['channelId'])
        yt_channel_service.get_channel_info(channel_id)
        print "Retrieving \"Shares\" from social websites..."
        n_fb_shares = social_web_service.getFacebookLinkSharedCount(video_url % video['id'])
        n_tw_shares = social_web_service.getTwitterLinkSharedCount(video_url % video['id'])
        n_lk_shares = social_web_service.getLinkedlnLinkSharedCount(video_url % video['id'])

        social_web_service.printSocialSharesInfo2CSVFile(video['id'], yt_social_csv_file)

        print "\n"

        exit(0)

        if not b_avoid_ddbb:
            printLog("Deleting database comments and info for video...")
            deleteVideoInfo(yt_comments_service, video_id)

        count = 0
        #for comment in comments_generator2(client, VIDEO_ID, 0):
        for comment in yt_comments_service.comments_generator(client, video_id):
            count += 1
            author_name = comment.author[0].name.text
            text = comment.content.text
            last_pos = comment.id.text.rfind('/')
            printLog(comment.content.text)
            comment_id = comment.id.text[last_pos + 1::]
            printLog("\t%s" % comment.id.text)
            printLog("\t%s" % comment_id)
            #print "\treplyCount: %s" % comment.replyCount
            #print "\tcomment: %s" % comment
            comment_string = str(comment)
            replycount_first = comment_string.find("replyCount")
            replycount_last = comment_string.find("</ns1:replyCount>")
            printLog("\tyt:replyCount: ----> %s" % comment_string[replycount_first + 11:replycount_last])
            reply_count = int(comment_string[replycount_first + 11:replycount_last])

            if reply_count > 0:
                # Obtenemos los Likes de la actividad global
                gp_comment_activity_likes = gp_service.getActivityById(comment_id)
                #print >> yt_csv_file, \
                #    printCSVYoutubeComment(comment, gd_comment_activity_likes)
                yt_csv_file.write(yt_comments_service.printCSVYoutubeComment(comment, video_id,
                                                                             gp_comment_activity_likes))

                # Obtenemos todos los comentarios y sus campos y los imprimimos a fichero CSV de Google Plus
                for comment in gp_service.googlePlusActitivyInfoGenerator(comment_id):
                    #gp_service.printGooglePlusComment(comment)
                    #print >> gp_csv_file, gp_service.printCSVGooglePlusComment(comment, comment_id,
                    # reply_count).encode('utf-8')
                    gp_csv_file.write(yt_comments_service.printCSVGooglePlusComment(gp_service, comment, comment_id,
                                                                                    reply_count, video_id))

                    #gp_csv_writer.writerow(gp_service.printCSVGooglePlusComment(comment, replyCount).encode('utf-8'))
                    #print gp_service.printCSVGooglePlusComment(comment, replyCount).encode('utf-8')

                    #### 2014-02-16: Version antigua que imprime la info de los comentarios de G+ de un comentario de YT
                    ####gp_service.getGooglePlusActitivyInfo(comment_id)
            else:
                #print >> yt_csv_file, printCSVYoutubeComment(comment, 0)
                yt_csv_file.write(yt_comments_service.printCSVYoutubeComment(comment, video_id, 0))

    _COMMENNTS_COUNT = count

    yt_csv_file.close()
    gp_csv_file.close()
    yt_videos_csv_file.close()

    if not b_avoid_ddbb:
        b_cargar = raw_input("*** Do you want to load comments data files into database? (Y/N): ")

        if b_cargar == "Y":
            printLog("Loading data videos file into database... ")
            yt_comments_service.executeLoadInBD(query_insert_yt_video_info)
            printLog("Loading youtube data comments into database...\n")
            yt_comments_service.executeLoadInBD(query_bulk_load_yt_comments)
            printLog("Loading google+ data comments into database...\n")
            yt_comments_service.executeLoadInBD(query_bulk_load_gp_comments)

            printLog("Data files have been loaded correctly!\n")
        else:
            printLog("*** ERROR ***: \tData files have not been loaded!")

    _file_log.close()


if __name__ == '__main__':
    main(sys.argv)
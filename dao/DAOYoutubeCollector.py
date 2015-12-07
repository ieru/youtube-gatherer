#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'antoniofsanjuan'

from mysql.connector import errorcode

import mysql.connector
import MySQLdb
from warnings import filterwarnings
import MySQLdb as Database
filterwarnings('ignore', category = Database.Warning)

class DAOYoutubeCollector():

    def __init__(self):
        print ""

    def formatDateToYoutubeDate(self, str_datetime):
        """ Formato Youtube: 2014-05-28T07:48:00.000Z """

        date = str(str_datetime)[:10]
        time = str(str_datetime)[11:]

        formated_time = date + 'T' + time + '.000Z'

        return formated_time

    def getYoutubeVideosToFollow(self):

        query = "SELECT ID_VIDEO, ID_CHANNEL " \
                        "FROM YT_VIDEOS_TO_FOLLOW " \
                        "WHERE DT_STARTED <= CURDATE()"

        arr_videos = []

        try:

            yt_conn = MySQLdb.connect('127.0.0.1', 'root', 'cc1251', 'youtube')

            yt_cursor = yt_conn.cursor()

            yt_cursor.execute(query)

            for row_video in yt_cursor:

                video = {'id': '', 'snippet': {'channelId': row_video[1], 'title': '', 'description': '', 'publishedAt': ''},
                         'contentDetails': {'duration': -1},
                         'statistics': {'viewCount': '', 'likeCount': '', 'dislikeCount': ''}, "id": row_video[0]}

                video["snippet"]["channelId"] = row_video[1]

                arr_videos.append(video)

            yt_cursor.close()
            yt_conn.close()

            return arr_videos

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("*** ERROR ***: Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("*** ERROR ***: Database does not exists")
            elif err.errno == errorcode.CR_CONN_HOST_ERROR:
                print("*** ERROR ***: Database is not running")
            else:
                print(err)
        else:
            if yt_conn is not None:
                print "Cerrando conexion con mysql"
                #yt_conn.close()


    def executeLoadInBD(self, query):
        try:

            yt_conn = MySQLdb.connect('127.0.0.1', 'root', 'cc1251', 'youtube')
            yt_cursor = yt_conn.cursor()

            yt_cursor.execute(query)

            yt_conn.commit()

            # Uncomment to Debug
            total = long(str(vars(yt_cursor)['_info']).split(':')[1].strip(' ').split(' ')[0])
            skipped = long(str(vars(yt_cursor)['_info']).split(':')[3].strip(' ').split(' ')[0])

            print "********************************************************"
            #print vars(yt_cursor)
            print "Query: %s" % vars(yt_cursor)['_executed']
            print "Result: %s" % vars(yt_cursor)['_info']
            print "Loaded: %s" % str(total - skipped)
            print "********************************************************"


            yt_cursor.close()
            yt_conn.close()

        except (KeyboardInterrupt, SystemExit):
            yt_conn.rollback()
            yt_cursor.close()
            yt_conn.close()
            print "\nRolling back database changes..."
            print "\n\nShutting down youtube-gatherer..."
            raise

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("*** ERROR ***: Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("*** ERROR ***: Database does not exists")
            elif err.errno == errorcode.CR_CONN_HOST_ERROR:
                print("*** ERROR ***: Database is not running")
            else:
                print(err)

            if yt_conn is not None:
                yt_conn.close()


    def loadDataFilesInDB(self, query):

        self.executeLoadInBD(query)
        #raw_input('Press any key')

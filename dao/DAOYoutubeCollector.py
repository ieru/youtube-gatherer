#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'antoniofsanjuan'

from mysql.connector import errorcode

import mysql.connector
import MySQLdb

class DAOYoutubeCollector():

    def __init__(self):
        print ""

    def durationInSecondsToISO8601Format(self, duration):
        """
        duration - ISO 8601 time format
        examples :
            'P1W2DT6H21M32S' - 1 week, 2 days, 6 hours, 21 mins, 32 secs,
            'PT7M15S' - 7 mins, 15 secs
        """

        weeks = duration / (7 * 3600 * 24)
        days = (duration - (weeks * (7 * 3600 * 24))) / 3600 * 24
        hours = (duration - (weeks * (7 * 3600 * 24)) - (days * (3600 * 24))) / 3600
        minutes = (duration - (weeks * (7 * 3600 * 24)) - (days * (3600 * 24)) - (hours * 3600)) / 60
        seconds = duration - (weeks * (7 * 3600 * 24)) - (days * (3600 * 24)) - (hours * 3600) - (minutes * 60)

        result = 'P'
        result += str(weeks) + 'W' if (weeks > 0) else ''
        result += str(days) + 'D' if (days > 0) else ''
        result += 'T'
        result += str(hours) + 'H' if (hours > 0) else ''
        result += str(minutes) + 'M' if (minutes > 0) else ''
        result += str(seconds) + 'S' if (seconds > 0) else '0S'

        return result

    def formatDateToYoutubeDate(self, str_datetime):
        """ Formato Youtube: 2014-05-28T07:48:00.000Z """

        #print "Formateando fecha: %s " % str_datetime
        date = str(str_datetime)[:10]
        time = str(str_datetime)[11:]

        formated_time = date + 'T' + time + '.000Z'

        return formated_time

    def getYoutubeVideosToFollow(self):

        query = "SELECT ID_VIDEO, ID_CHANNEL, DS_TITLE, DS_DESCRIPTION, NUM_DURATION, DT_PUBLISH, NUM_VISITS, " \
                "NUM_LIKES, NUM_DISLIKES FROM YT_VIDEOS"

        arr_videos = []

        try:

            yt_conn = MySQLdb.connect('127.0.0.1', 'root', 'cc1251', 'youtube')
            print "\nConexion con mysql establecida"

            yt_cursor = yt_conn.cursor()

            yt_cursor.execute(query)

            for row_video in yt_cursor:

                video = {'id': '',
                         'snippet': {'channelId': '', 'title': '', 'description': ''},
                         'contentDetails': {'duration': -1},
                         'statistics': {'viewCount': '', 'likeCount': '', 'dislikeCount': ''}}

                video["id"] = row_video[0]
                video["snippet"]["channelId"] = row_video[1]
                video["snippet"]["title"] = row_video[2]
                video["snippet"]["description"] = row_video[3]
                video["contentDetails"]["duration"] = self.durationInSecondsToISO8601Format(row_video[4])
                video["snippet"]["publishedAt"] = self.formatDateToYoutubeDate(row_video[5])
                video["statistics"]["viewCount"] = row_video[6]
                video["statistics"]["likeCount"] = row_video[7]
                video["statistics"]["dislikeCount"] = row_video[8]
                '''
                print("id = %s" % row_video[0])
                print("channelId = %s" % row_video[1])
                print("channelId = %s" % video["snippet"]["channelId"])
                print("title = %s" % row_video[2])
                print("description = %s" % row_video[3])
                print("duration = %s" % row_video[4])
                print("duration = %s" % self.durationInSecondsToISO8601Format(row_video[4]))
                print("publishedAt = %s" % self.formatDateToYoutubeDate(row_video[5]))
                print("viewCount = %s" % row_video[6])
                print("likeCount = %s" % row_video[7])
                print("dislikeCount = %s" % row_video[8])
                '''
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


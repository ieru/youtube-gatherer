#!/usr/bin/python

__author__ = 'antoniofsanjuan'

#import httplib2
import time
import datetime
import mysql.connector
from mysql.connector import errorcode

from apiclient.discovery import build


# Explicitly tell the underlying HTTP transport library not to retry, since
# we are handling retry logic ourselves.
#httplib2.RETRIES = 1

#CLIENT_SECRETS_FILE = "client_secrets.json"


class YoutubeChannel():
    # An OAuth 2 access scope that allows for full read/write access.
    DEVELOPER_KEY = "AIzaSyBQ2PoALuuf4HivoLnLhthRdgBo-NvRMRE"
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"

    _youtube = None

    def __init__(self):
        self._youtube = build(self.YOUTUBE_API_SERVICE_NAME, self.YOUTUBE_API_VERSION,
                              developerKey=self.DEVELOPER_KEY)

    def printCSVYoutubeChannelInfo(self, channel_id, record):
        csv_format_string = "%s\t%s\t%s\t%s\t%s\t%s\t%s\n"

        str_title = record["snippet"]["title"]
        str_viewCount = record["statistics"]["viewCount"]
        str_commentCount = record["statistics"]["commentCount"]
        str_subscriberCount = record["statistics"]["subscriberCount"]
        str_hiddenSubscriberCount = 0 if str(record["statistics"]["hiddenSubscriberCount"]).upper() == "false".upper() else 1
        str_videoCount = record["statistics"]["videoCount"]
        
        ''' # Uncomment to Debug
        print "Channel Title: %s" % record["snippet"]["title"]
        print "Channel viewCount: %s" % record["statistics"]["viewCount"]
        print "Channel commentCount: %s" % record["statistics"]["commentCount"]
        print "Channel subscriberCount: %s" % record["statistics"]["subscriberCount"]
        print "Channel hiddenSubscriberCount: %s" % record["statistics"]["hiddenSubscriberCount"]
        print "Channel videoCount: %s" % record["statistics"]["videoCount"]
        '''

        return csv_format_string % (channel_id, str_title, str_viewCount, str_commentCount,
                                    str_subscriberCount, str_hiddenSubscriberCount, str_videoCount)

    def printChannelInfo2CSVFile(self, channel_id, record, csv_file):

        csv_format_string = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"
        ts = time.time()

        str_title = record["snippet"]["title"]
        str_title = str(str_title).replace('"', '\\"')
        str_title = '"%s"' % str_title

        str_viewCount = record["statistics"]["viewCount"]
        str_commentCount = record["statistics"]["commentCount"]
        str_subscriberCount = record["statistics"]["subscriberCount"]
        str_hiddenSubscriberCount = 0 if str(record["statistics"]["hiddenSubscriberCount"]).upper() == "false".upper() else 1
        str_videoCount = record["statistics"]["videoCount"]
        str_timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        csv_file.write(csv_format_string % (channel_id, str_timestamp, str_title, str_viewCount, str_commentCount,
                                            str_subscriberCount, str_hiddenSubscriberCount, str_videoCount))

    def get_channel_info(self, channel_id):
        channels_response = self._youtube.channels().list(
            id=channel_id,
            part="id,snippet,statistics"
        ).execute()

        # Searching by channel_id only returns one record
        #for search_result in channels_response.get("items", []):
        #if channels_response is not None:
        #    print self.printCSVYoutubeChannelInfo(channel_id, channels_response.get("items", [])[0])

        return channels_response.get("items", [])[0]

    def executeUpdate(self, query):
        try:

            yt_conn = mysql.connector.connect(user='root', password='cc1251',
                                              host='127.0.0.1',
                                              database='youtube')
            #print "\nConexion con mysql establecida"
            yt_cursor = yt_conn.cursor()
            #data_comment = (00001, 'Antonio', 'Este es el cuerpo de un comentario', datetime.now(), 1, 1, 1)
            #
            #print "Ejecutando query: %s" % query
            if ";" in str(query):
                yt_cursor.execute(query, multi=True)
            else:
                yt_cursor.execute(query)


            #print "Resultado: %s" % yt_conn.info()

            #comment_no = yt_cursor.lastrowid
            #print "Ult. comentario insertado: %d" % comment_no
            #
            #yt_conn.commit()

            #for (result) in yt_cursor:
            #    print "Resultado: %s" % result

            yt_conn.commit()
            yt_cursor.close()
            yt_conn.close()

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
                #print "Cerrando conexion con mysql"
                yt_conn.close()

    def insert_channel_info(self, channel_id, record):

        str_query = """INSERT INTO YT_CHANNELS (ID_CHANNEL, SYSDATE, DT_STORED, DS_TITLE, ID_VIDEO, NUM_VIEWS, " \
                    "NUM_COMMENTS, NUM_SUBSCRIBERS, NUM_HIDDEN_SUBSCRIBERS, NUM_VIDEOS) " \
                    "VALUES (%s, %s, %s, %s, %s, %s)"""

        str_title = record["snippet"]["title"]
        str_viewCount = record["statistics"]["viewCount"]
        str_commentCount = record["statistics"]["commentCount"]
        str_subscriberCount = record["statistics"]["subscriberCount"]
        str_hiddenSubscriberCount = record["statistics"]["hiddenSubscriberCount"]
        str_videoCount = record["statistics"]["videoCount"]

        str_query = str_query % (channel_id, str_viewCount, str_commentCount, str_subscriberCount,
                                 str_hiddenSubscriberCount, str_videoCount)

        self.executeUpdate(str_query)



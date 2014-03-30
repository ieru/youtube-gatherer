#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'antoniofsanjuan'

from mysql.connector import errorcode

import gdata.service

import mysql.connector
import time
import datetime


class GoogleCommentsService(object):

    MAX_RESULTS = 40
    _COMMENNTS_COUNT = 0
    _TEXT_CHARSET = 'utf-8'

    def __init__(self, argv):
        print ""

    def comments_generator2(self, client, video_id, next_page):
        print "----------> comments_generator2()"
        global _COMMENNTS_COUNT

        start_index = (next_page * self.MAX_RESULTS) + 1
        print "Start-Index:%s" % start_index
        url = "https://gdata.youtube.com/feeds/api/videos/%s/comments?orderby=published&start-index=%s&max-results=%s" % (
            video_id, start_index, self.MAX_RESULTS)
        print "URL:'%s'" % url
        comment_feed = client.GetYouTubeVideoCommentFeed(uri=url)
        while comment_feed is not None:
            for comment in comment_feed.entry:
                _COMMENNTS_COUNT += 1
                yield comment

            if len(comment_feed.entry) == 0:
                comment_feed = None
            else:
                next_page += 1
                start_index = (next_page * self.MAX_RESULTS) + 1
                url = "https://gdata.youtube.com/feeds/api/videos/%s/comments?start-index=%s&max-results=%s" % (
                    video_id, start_index, self.MAX_RESULTS)
                print "URL:'%s'" % url
                comment_feed = client.GetYouTubeVideoCommentFeed(uri=url)

    def formatYoutubeDate(self, yt_datetime):
        _tmp = time.strptime(yt_datetime[:-5], '%Y-%m-%dT%H:%M:%S')
        formated_time = datetime.datetime(*_tmp[:6])

        return formated_time

    def printCSVYoutubeComment(self, yt_comment, video_id, gp_likes_activity):
        csv_format_string = "%s\t%s\t%s\t%s\t%s\t%s\t%s\n"

        yt_author_name = yt_comment.author[0].name.text
        yt_content = str(yt_comment.content.text)
        # Remove BOM character
        yt_content = yt_content.replace('\xef\xbb\xbf', '')

        # Escapamos las dobles comillas ya que se utilizaran para separar englobar campos en el fichero csv
        yt_content = yt_content.replace('"', '\\"')
        yt_content = '"%s"' % yt_content

        last_pos = yt_comment.id.text.rfind('/')

        yt_comment_id = yt_comment.id.text[last_pos + 1::]
        yt_published = yt_comment.published.text

        comment_string = str(yt_comment)

        replycount_first = comment_string.find("replyCount")
        replycount_last = comment_string.find("</ns1:replyCount>")

        yt_reply_count = int(comment_string[replycount_first + 11:replycount_last])

        #print (csv_format_string % (yt_comment_id, yt_author_name, yt_content,
        #                            yt_published, yt_reply_count, gp_likes_activity))

        return csv_format_string % (yt_comment_id, yt_author_name, yt_content,
                                    self.formatYoutubeDate(yt_published), yt_reply_count, gp_likes_activity, video_id)

    def printCSVGooglePlusComment(self, gp_service, gp_comment, yt_comment_id, num_replies, video_id):
        csv_format_string = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"
        arr_gp_comment_fields = gp_service.getArrayGooglePlusCommentFields(gp_comment)

        print arr_gp_comment_fields[4]
        formated_published_time = self.formatYoutubeDate(arr_gp_comment_fields[3])

        print "DEBUG: content: '%s'" % arr_gp_comment_fields[2]
        return csv_format_string % (arr_gp_comment_fields[0], arr_gp_comment_fields[1],
                                    arr_gp_comment_fields[2], formated_published_time, arr_gp_comment_fields[4],
                                    yt_comment_id, num_replies, video_id)

    def executeLoadInBD(self, query):
        try:

            yt_conn = mysql.connector.connect(user='root', password='cc1251',
                                              host='127.0.0.1',
                                              database='youtube')
            print "\nConexion con mysql establecida"
            yt_cursor = yt_conn.cursor()
            #data_comment = (00001, 'Antonio', 'Este es el cuerpo de un comentario', datetime.now(), 1, 1, 1)
            #
            print "Ejecutando query: %s" % query
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

    def comments_generator(self, client, video_id):
        print "----------> comments_generator()"
        retries_counter = 3
        #start_index = (next_page * MAX_RESULTS) + 1
        #print "Start-Index:%s" % start_index
        #url = "https://gdata.youtube.com/feeds/api/videos/%s/comments?start-index=%s&max-results=%s" % (video_id, start_index, MAX_RESULTS)
        #print "URL:'%s'" % url
        #comment_feed = client.GetYouTubeVideoCommentFeed(url)

        comments_count = 0
        url = "https://gdata.youtube.com/feeds/api/videos/%s/comments?orderby=published" % video_id
        #comment_feed = client.GetYouTubeVideoCommentFeed(video_id=video_id)

        comment_feed = client.GetYouTubeVideoCommentFeed(uri=url)
        while comment_feed is not None and (retries_counter > 0):
            try:
                for comment in comment_feed.entry:
                    comments_count += 1
                    yield comment
                next_link = comment_feed.GetNextLink()
                if next_link is None:
                    comment_feed = None
                else:
                    print "Comments count=%s" % comments_count
                    print "Next link: %s" % next_link.href
                    comment_feed = client.GetYouTubeVideoCommentFeed(next_link.href)
                    #next_page += 1
                    #start_index = (next_page * MAX_RESULTS) + 1
                    #url = "https://gdata.youtube.com/feeds/api/videos/%s/comments?start-index=%s&max-results=%s" % (video_id, start_index, MAX_RESULTS)
                    #print "URL:'%s'" % url
                    #comment_feed = client.GetYouTubeVideoCommentFeed(url)
            except gdata.service.RequestError, request_error:
                    print "\n****************************************************"
                    print "********** STATUS ERROR: %s **********" % request_error[0]['status']
                    print "****************************************************\n"
                    raw_input("Press any key to continue...")
                    if request_error[0]['status'] == '500':
                        retries_counter -= 1
                        print "Reintentando desde indice de comentarios [%s]..." % comments_count
                        url += "&start-index=%s" % comments_count
                        print "URL: %s" % url
                        raw_input("Press any key to continue...")
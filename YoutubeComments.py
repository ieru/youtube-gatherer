#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'antoniofsanjuan'

from mysql.connector import errorcode

import gdata.service

import mysql.connector
import time
import datetime
import HTMLParser
import MySQLdb

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

        # Remove double quotes because couse problems with comment delimiters
        yt_content = yt_content.replace('"', '')
        yt_content = '"%s"' % yt_content

        print "printCSVYoutubeComment: Hay chars UTF-8? %s" % isinstance(yt_content, str)

        # Prueba Utf8 unicodeerror
        try:
            yt_content = yt_content.decode("utf-8") if isinstance(yt_content, str) else unicode(yt_content)
        except UnicodeDecodeError:
            print "-------> printCSVYoutubeComment: Error en conversion unicode. Se intenta decode utf-8"
            yt_content = yt_content.decode("utf-8")

        last_pos = yt_comment.id.text.rfind('/')

        yt_comment_id = yt_comment.id.text[last_pos + 1::]
        yt_published = yt_comment.published.text

        comment_string = str(yt_comment)

        replycount_first = comment_string.find("replyCount")
        replycount_last = comment_string.find("</ns1:replyCount>")

        yt_reply_count = int(comment_string[replycount_first + 11:replycount_last])

        #Prueba: Verificamos uno a uno el tipo de codificacion de cada string y lo pasamos en caso de ser necesario
        yt_comment_id = yt_comment_id.decode("utf-8") if isinstance(yt_comment_id, str) else unicode(yt_comment_id)
        yt_author_name = yt_author_name.decode("utf-8") if isinstance(yt_author_name, str) else unicode(yt_author_name)
        yt_content = yt_content.decode("utf-8") if isinstance(yt_content, str) else unicode(yt_content)
        yt_published = yt_published.decode("utf-8") if isinstance(yt_published, str) else unicode(yt_published)
        yt_reply_count = yt_reply_count.decode("utf-8") if isinstance(yt_reply_count, str) else unicode(yt_reply_count)
        gp_likes_activity = gp_likes_activity.decode("utf-8") if isinstance(gp_likes_activity, str) else unicode(gp_likes_activity)
        video_id = video_id.decode("utf-8") if isinstance(video_id, str) else unicode(video_id)

        #print (csv_format_string % (yt_comment_id, yt_author_name, yt_content,
        #                            yt_published, yt_reply_count, gp_likes_activity))
        print "yt_comment_id: %s" % yt_comment_id
        print "yt_author_name: %s" % yt_author_name
        print "yt_content: %s" % yt_content
        print "yt_published: %s" % self.formatYoutubeDate(yt_published)
        print "yt_reply_count: %s" % yt_reply_count
        print "gp_likes_activity: %s" % gp_likes_activity
        print "video_id: %s" % video_id

        '''
        try:
            print "%s\t%s\t%s\t%s\t%s\t%s\n" % (yt_comment_id, yt_author_name,
                                       self.formatYoutubeDate(yt_published), yt_reply_count, gp_likes_activity, video_id)
        except UnicodeDecodeError:
            print "-----> Error devolviendo el string concatenado"
            print "-----> comentario: %s" % yt_content
            try:
                msg = "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (yt_comment_id, yt_author_name, yt_content.decode('utf-8'),
                                       self.formatYoutubeDate(yt_published), yt_reply_count, gp_likes_activity, video_id)
            except UnicodeDecodeError:
                print "-----> Error intentando decodificar a utf-8"
                try:
                    msg = "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (yt_comment_id, yt_author_name, yt_content[:-3],
                                           self.formatYoutubeDate(yt_published), yt_reply_count, gp_likes_activity, video_id)
                except UnicodeDecodeError:
                    print "-----> Error intentando substring -3"
                    print "-----> comentario: %s" % yt_content[:-3]
                    try:
                        msg = "%s\t%s\t%s\n" % (yt_comment_id, yt_author_name, yt_content)
                    except UnicodeDecodeError:
                        print "-----> Error intentando eliminar ultimos 3 caracteres"
                        yt_content = yt_content.encode('utf-8')
                        try:
                            msg = "%s\t%s\t%s\n" % (yt_comment_id, yt_author_name, yt_content)
                            print "-----> comentario: %s" % msg
                        except UnicodeDecodeError:
                            print "-----> Error intentando codificar a utf8"

        '''
        return csv_format_string % (yt_comment_id, yt_author_name, yt_content,
                                    self.formatYoutubeDate(yt_published), yt_reply_count, gp_likes_activity, video_id)

    def printCSVGooglePlusComment(self, gp_service, gp_comment, yt_comment_id, num_replies, video_id):
        csv_format_string = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"
        arr_gp_comment_fields = gp_service.getArrayGooglePlusCommentFields(gp_comment)

        htmlParser = HTMLParser.HTMLParser()
        #print arr_gp_comment_fields[4]
        formatted_published_time = self.formatYoutubeDate(arr_gp_comment_fields[3])
        parsed_comment_body = htmlParser.unescape(arr_gp_comment_fields[2])

        print "DEBUG: content: '%s'" % arr_gp_comment_fields[2]
        return csv_format_string % (arr_gp_comment_fields[0], arr_gp_comment_fields[1],
                                    parsed_comment_body, formatted_published_time, arr_gp_comment_fields[4],
                                    yt_comment_id, num_replies, video_id)

    def executeLoadInBD_OLD(self, query):
        try:

            yt_conn = mysql.connector.connect(user='root', password='cc1251',
                                              host='127.0.0.1',
                                              database='youtube')
            print "\nConexion con mysql establecida"
            yt_cursor = yt_conn.cursor()

            print "Ejecutando query: %s" % query
            if ";" in str(query):
                yt_cursor.execute(query, multi=True)
            else:
                yt_cursor.execute(query)

            print "********************************************************"
            print vars(yt_cursor)
            print "********************************************************"

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

    def executeLoadInBD(self, query):
        try:

            yt_conn = MySQLdb.connect('127.0.0.1', 'root', 'cc1251', 'youtube')
            print "\nConexion con mysql establecida"
            yt_cursor = yt_conn.cursor()

            yt_cursor.execute(query)

            yt_conn.commit()

            total = long(str(vars(yt_cursor)['_info']).split(':')[1].strip(' ').split(' ')[0])
            skipped = long(str(vars(yt_cursor)['_info']).split(':')[3].strip(' ').split(' ')[0])

            print "********************************************************"
            '''print vars(yt_cursor)'''
            print "Query: %s" % vars(yt_cursor)['_executed']
            print "Result: %s" % vars(yt_cursor)['_info']
            print "Loaded: %s" % str(total - skipped)
            print "********************************************************"

            #comment_no = yt_cursor.lastrowid
            #print "Ult. comentario insertado: %d" % comment_no
            #
            #yt_conn.commit()

            #for (result) in yt_cursor:
            #    print "Resultado: %s" % result

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
                print "Cerrando conexion con mysql"
                #yt_conn.close()

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

                    if retries_counter > 0:
                        if request_error[0]['status'] == '500':
                            retries_counter -= 1
                            print "Reintentando desde indice de comentarios [%s]..." % comments_count
                            url += "&start-index=%s" % comments_count
                            print "URL: %s" % url
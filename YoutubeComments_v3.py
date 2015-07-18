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
    _COMMENTS_COUNT = 0
    _TEXT_CHARSET = 'utf-8'

    _FS = ';' # Field Separator

    def __init__(self, argv):
        print ""


    def formatYoutubeDate(self, yt_datetime):
        _tmp = time.strptime(yt_datetime[:-5], '%Y-%m-%dT%H:%M:%S')
        formated_time = datetime.datetime(*_tmp[:6])

        return formated_time

    def printCSVYoutubeComment(self, json_comment, video_id, gp_likes_activity):

        csv_format_string = "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s\n"

        #print "\nprintCSVYoutubeComment() - INIT"

        yt_comment = json_comment["snippet"]["topLevelComment"]

        author = yt_comment["snippet"]["authorDisplayName"]
        #print "\tauthor: %s" % author
        text = yt_comment["snippet"]["textDisplay"]
        #print "\ttext: %s" % text
        likes = yt_comment["snippet"]["likeCount"]
        #print "\tlikes: %s" % likes
        yt_comment_id = yt_comment["id"]
        #print "\tyt_comment_id: %s" % yt_comment_id
        yt_published = yt_comment["snippet"]["publishedAt"]
        #print "\tyt_published: %s" % yt_published

        yt_reply_count = int( json_comment["snippet"]['totalReplyCount'] )
        #print "\tyt_reply_count: %s" % yt_reply_count

        yt_author_name = str(author)
        #print "\tyt_author_name: %s" % yt_author_name

        #print "Text: %s " % text
        #print "\ttext isInstance of STR? %s; type? %s" % ( isinstance(text, str), type(text) )
        yt_content = text.encode('utf-8')
        #print "2"
        #print "\tyt_content: %s" % yt_content

        # Remove BOM character
        yt_content = yt_content.replace('\xef\xbb\xbf', '')
        #print "3"
        #print "\tyt_content without BOM: %s" % yt_content

        # Remove double quotes because couse problems with comment delimiters
        yt_content = yt_content.replace('"', '')
        #print "4"
        yt_content = '"%s"' % yt_content

        #print "yt_content: %s" % yt_content

        ### KEY POINT: Check 1 per 1 field if string codification is Unicode or String. Convert them if necessary
        ### Python is not able to concatenate string in different codifications
        yt_content = yt_content.decode("utf-8") if isinstance(yt_content, str) else unicode(yt_content)
        yt_comment_id = yt_comment_id.decode("utf-8") if isinstance(yt_comment_id, str) else unicode(yt_comment_id)
        yt_author_name = yt_author_name.decode("utf-8") if isinstance(yt_author_name, str) else unicode(yt_author_name)
        yt_content = yt_content.decode("utf-8") if isinstance(yt_content, str) else unicode(yt_content)
        yt_published = yt_published.decode("utf-8") if isinstance(yt_published, str) else unicode(yt_published)
        yt_reply_count = yt_reply_count.decode("utf-8") if isinstance(yt_reply_count, str) else unicode(yt_reply_count)
        gp_likes_activity = gp_likes_activity.decode("utf-8") if isinstance(gp_likes_activity, str) else unicode(gp_likes_activity)
        video_id = video_id.decode("utf-8") if isinstance(video_id, str) else unicode(video_id)

        ''' # Uncomento to Debug
        print "yt_comment_id: %s" % yt_comment_id
        print "yt_author_name: %s" % yt_author_name
        print "yt_content: %s" % yt_content
        print "yt_published: %s" % self.formatYoutubeDate(yt_published)
        print "yt_reply_count: %s" % yt_reply_count
        print "gp_likes_activity: %s" % gp_likes_activity
        print "video_id: %s" % video_id
        '''

        return csv_format_string % (yt_comment_id, yt_author_name, yt_content,
                                    self.formatYoutubeDate(yt_published), yt_reply_count, gp_likes_activity, video_id)

    def printCSVGooglePlusComment(self, gp_service, gp_comment, yt_comment_id, num_replies, video_id):

        #print 'DEBUG: printCSVGooglePlusComment() - INIT'

        csv_format_string = "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s\n"
        arr_gp_comment_fields = gp_service.getArrayGooglePlusCommentFields(gp_comment)

        htmlParser = HTMLParser.HTMLParser()

        formatted_published_time = self.formatYoutubeDate(arr_gp_comment_fields[3])
        parsed_comment_body = htmlParser.unescape(arr_gp_comment_fields[2])

        gp_author = arr_gp_comment_fields[1].decode("utf-8") if isinstance(arr_gp_comment_fields[1], str) else unicode(arr_gp_comment_fields[1])

        return csv_format_string % (arr_gp_comment_fields[0], gp_author,
                                    parsed_comment_body, formatted_published_time, arr_gp_comment_fields[4],
                                    yt_comment_id, num_replies, video_id)

    def executeLoadInBD(self, query):
        try:

            yt_conn = MySQLdb.connect('127.0.0.1', 'root', 'cc1251', 'youtube')
            #print "\nConexion con mysql establecida"
            yt_cursor = yt_conn.cursor()

            yt_cursor.execute(query)

            yt_conn.commit()

            # Uncomment to Debug
            total = long(str(vars(yt_cursor)['_info']).split(':')[1].strip(' ').split(' ')[0])
            skipped = long(str(vars(yt_cursor)['_info']).split(':')[3].strip(' ').split(' ')[0])

            print "********************************************************"
            # print vars(yt_cursor)
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


    # Call the API's commentThreads.list method to list the existing comment threads.
    def get_comment_threads(self, yt_service, video_id, next_page):

        #print "get_comment_threads(): video_id = %s\n" % video_id

        if next_page is None:
            #print "get_comment_threads(): \tnext_page IS NONE"
            results = yt_service.commentThreads().list(
                part='id,snippet',
                videoId=video_id,
                maxResults=100,
                order='time',
                textFormat='plainText',
                key='usKbvXnvKS9XA377cncN0oZJ'
            ).execute()
        else:
            #print "get_comment_threads(): \tnext_page IS:%s" % next_page
            results = yt_service.commentThreads().list(
                part='id,snippet',
                videoId=video_id,
                maxResults=100,
                order='time',
                textFormat='plainText',
                pageToken=next_page,
                key='usKbvXnvKS9XA377cncN0oZJ'
            ).execute()

        ## Uncomment to DEBUG
        #print "TOTAL: %s" % len(results['items'])

        #for item in results["items"]:
        #  comment = item["snippet"]["topLevelComment"]
        #  author = comment["snippet"]["authorDisplayName"]
        #  text = comment["snippet"]["textDisplay"]
        #  print "Comment by %s: %s" % (author, text)

        return results


    # Call the API's comments.list method to list the existing comment replies.
    def get_comments(self, yt_service, video_id):

        results = youtube.comments().list(
            part="id,snippet",
            videoId=parent_id,
            textFormat="plainText",
            key='usKbvXnvKS9XA377cncN0oZJ'
        ).execute()

        #for item in results["items"]:
        #    author = item["snippet"]["authorDisplayName"]
        #    text = item["snippet"]["textDisplay"]
        #    print "Comment by %s: %s" % (author, text)

        return results


    # Retrieves comments from a specific video comment thread
    def comments_generator(self, yt_service, video_id):

        #print "comments_generator: *** INIT ***"

        retries_counter = 3

        video_comment_threads = self.get_comment_threads(yt_service, video_id, None)


        comments_count = 0

        while video_comment_threads is not None and (retries_counter > 0):
            try:

                for item in video_comment_threads["items"]:

                    comments_count += 1
                    #print "\tComments #%s\tID: %s\tPublished At: %s" % (comments_count, item['id'], item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                    yield item

                next_page = video_comment_threads['nextPageToken']

                if next_page is None or next_page == "":
                    video_comment_threads = None
                else:
                    #print "Comments count=%s" % comments_count
                    video_comment_threads = self.get_comment_threads(yt_service, video_id, next_page)

            except gdata.service.RequestError, request_error:
        #            print "\n****************************************************"
        #            print "********** STATUS ERROR: %s **********" % request_error[0]['status']
        #            print "****************************************************\n"
        #
        #            if retries_counter > 0:
        #                retries_counter -= 1
        #                ''' Uncomment to debug
        #                print "Retrying extraction from index of comments [%s]..." % comments_count
        #                '''
                        next_page = video_comment_threads['nextPageToken']
        #                ''' Uncomment to debug
        #                print "URL: %s" % url
        #                '''

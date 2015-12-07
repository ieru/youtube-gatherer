#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'antoniofsanjuan'

import gdata.service

import time
import datetime
import HTMLParser


class YoutubeCommentsService(object):

    _FS = ';' # Field Separator

    def __init__(self, argv):
        print ""

    def force_decode(self, string):

        codecs=['utf_8', 'ascii', 'latin_1', 'iso8859_2', 'utf_16', 'cp1250', 'cp1251', 'cp1252', 'iso8859_3', 'iso8859_4', 'iso8859_5', 'big5',
                                            'cp857', 'cp860', 'cp861', 'cp862', 'cp863', 'cp864', 'cp865', 'cp866', 'cp869', 'cp874', 'cp875', 'cp932', 'cp949', 'cp950',
                                            'cp1006', 'cp1026', 'cp1140', 'cp1253', 'cp1254', 'cp1255', 'cp1256', 'cp1257', 'cp1258', 'euc_jp', 'big5hkscs', 'cp037', 'cp424', 'cp437',
                                            'euc_jis_2004', 'euc_jisx0213', 'euc_kr', 'gb2312', 'gbk', 'gb18030', 'hz', 'iso2022_jp', 'iso2022_jp_1', 'iso2022_jp_2',
                                            'iso2022_jp_2004', 'iso2022_jp_3', 'iso2022_jp_ext', 'iso2022_kr', 'cp500', 'cp737', 'cp775', 'cp850', 'cp852', 'cp855', 'cp856',
                                            'iso8859_6', 'iso8859_7', 'iso8859_8', 'iso8859_9', 'iso8859_10', 'iso8859_13', 'iso8859_14', 'iso8859_15', 'johab', 'koi8_r',
                                            'koi8_u', 'mac_cyrillic', 'mac_greek', 'mac_iceland', 'mac_latin2', 'mac_roman', 'mac_turkish', 'ptcp154', 'shift_jis',
                                            'shift_jis_2004', 'shift_jisx0213', 'utf_16_be', 'utf_16_le', 'utf_7']
        for i in codecs:
            try:
                return string.decode(i)
            except:
                pass

        return string

    def detect_encoding(self, string):

        codecs=['utf_8', 'ascii', 'latin_1', 'iso8859_2', 'utf_16', 'cp1250', 'cp1251', 'cp1252', 'iso8859_3', 'iso8859_4', 'iso8859_5', 'big5',
                                            'cp857', 'cp860', 'cp861', 'cp862', 'cp863', 'cp864', 'cp865', 'cp866', 'cp869', 'cp874', 'cp875', 'cp932', 'cp949', 'cp950',
                                            'cp1006', 'cp1026', 'cp1140', 'cp1253', 'cp1254', 'cp1255', 'cp1256', 'cp1257', 'cp1258', 'euc_jp', 'big5hkscs', 'cp037', 'cp424', 'cp437',
                                            'euc_jis_2004', 'euc_jisx0213', 'euc_kr', 'gb2312', 'gbk', 'gb18030', 'hz', 'iso2022_jp', 'iso2022_jp_1', 'iso2022_jp_2',
                                            'iso2022_jp_2004', 'iso2022_jp_3', 'iso2022_jp_ext', 'iso2022_kr', 'cp500', 'cp737', 'cp775', 'cp850', 'cp852', 'cp855', 'cp856',
                                            'iso8859_6', 'iso8859_7', 'iso8859_8', 'iso8859_9', 'iso8859_10', 'iso8859_13', 'iso8859_14', 'iso8859_15', 'johab', 'koi8_r',
                                            'koi8_u', 'mac_cyrillic', 'mac_greek', 'mac_iceland', 'mac_latin2', 'mac_roman', 'mac_turkish', 'ptcp154', 'shift_jis',
                                            'shift_jis_2004', 'shift_jisx0213', 'utf_16_be', 'utf_16_le', 'utf_7']
        for i in codecs:
            try:
                string.decode(i)
                return i
            except:
                pass

        return "unicode"

    def formatYoutubeDate(self, yt_datetime):
        _tmp = time.strptime(yt_datetime[:-5], '%Y-%m-%dT%H:%M:%S')
        formated_time = datetime.datetime(*_tmp[:6])

        return formated_time

    def printCSVYoutubeComment(self, json_comment, video_id, gp_likes_activity):

        csv_format_string = "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s" + self._FS + "%s" + self._FS + "%s" + self._FS + "%s\n"

        yt_comment = json_comment["snippet"]["topLevelComment"]

        author = yt_comment["snippet"]["authorDisplayName"]
        text = yt_comment["snippet"]["textDisplay"]
        likes = yt_comment["snippet"]["likeCount"]
        yt_comment_id = yt_comment["id"]
        yt_published = yt_comment["snippet"]["publishedAt"]

        yt_reply_count = int( json_comment["snippet"]['totalReplyCount'] )

        yt_author_name = author

        yt_content = yt_comment["snippet"]["textDisplay"]

        # Escaping double-quotes and double-quoting the text
        yt_content = yt_content.replace('"', '\\"')
        yt_content = '"%s"' % yt_content

        yt_author_name = yt_author_name.replace('"', '\\"')
        yt_author_name = '"%s"' % yt_author_name


        ### KEY POINT: Check 1 per 1 field if string codification is Unicode or String. Convert them if necessary
        ### Python is not able to concatenate string in different codifications
        yt_content = yt_content.decode("utf-8") if isinstance(yt_content, str) else unicode(yt_content)
        yt_comment_id = yt_comment_id.decode("utf-8") if isinstance(yt_comment_id, str) else unicode(yt_comment_id)

        try:

            yt_author_name_decoded = self.force_decode(yt_author_name)
            yt_author_name = yt_author_name_decoded

        except Exception as e:
            print "No se puede codificar en 'utf8' el nombre de autor: %s\n" % yt_author_name
            print "Excepcion:\n%s" % e

        try:

            # Detect encoding and convert to unicode or leave in unicode format
            if isinstance(yt_content, str):
                yt_content_decoded = self.force_decode(yt_content)
            else:
                yt_content_decoded = yt_content

            yt_content = yt_content_decoded.encode('utf-8-sig')
            yt_content = yt_content.decode('utf-8-sig')

        except Exception as e:
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            print "No se puede codificar en 'utf8' el contenido: %s\n" % yt_content
            print "Excepcion:\n%s" % e
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
            exit(1)

        yt_published = yt_published.decode("utf-8") if isinstance(yt_published, str) else unicode(yt_published)
        yt_reply_count = yt_reply_count.decode("utf-8") if isinstance(yt_reply_count, str) else unicode(yt_reply_count)
        gp_likes_activity = gp_likes_activity.decode("utf-8") if isinstance(gp_likes_activity, str) else unicode(gp_likes_activity)
        video_id = video_id.decode("utf-8") if isinstance(video_id, str) else unicode(video_id)

        ts = time.time()
        str_stored_timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        return csv_format_string % (yt_comment_id, yt_author_name, yt_content,
                                    self.formatYoutubeDate(yt_published), str_stored_timestamp, yt_reply_count, gp_likes_activity, video_id)



    # Call the API's commentThreads.list method to list the existing comment threads.
    def get_comment_threads(self, yt_service, video_id, next_page):

        if next_page is None:

            results = yt_service.commentThreads().list(
                part='id,snippet',
                videoId=video_id,
                maxResults=100,
                order='time',
                textFormat='plainText',
                key='usKbvXnvKS9XA377cncN0oZJ'
            ).execute()

        else:

            results = yt_service.commentThreads().list(
                part='id,snippet',
                videoId=video_id,
                maxResults=100,
                order='time',
                textFormat='plainText',
                pageToken=next_page,
                key='usKbvXnvKS9XA377cncN0oZJ'
            ).execute()

        return results


    # Call the API's comments.list method to list the existing comment replies.
    def get_comments(self, yt_service, video_id):

        results = youtube.comments().list(
            part="id,snippet",
            videoId=parent_id,
            textFormat="plainText",
            key='usKbvXnvKS9XA377cncN0oZJ'
        ).execute()

        return results


    # Retrieves comments from a specific video comment thread
    def comments_generator(self, yt_service, video_id):

        retries_counter = 3

        video_comment_threads = self.get_comment_threads(yt_service, video_id, None)

        comments_count = 0
        next_page = None
        while video_comment_threads is not None and (retries_counter > 0):
            try:
                num_retries_nextPageToken = 3

                for item in video_comment_threads["items"]:

                    comments_count += 1
                    yield item

                if 'nextPageToken' in video_comment_threads:
                    next_page = video_comment_threads['nextPageToken']
                else:
                    # Intentamos recuperar otras 3 veces el nextPageToken por si se hubiera recuperado mal
                    num_retries_nextPageToken = 3
                    video_comment_thread_tmp = ""
                    while num_retries_nextPageToken > 0 and 'nextPageToken' not in video_comment_thread_tmp:
                        video_comment_thread_tmp = self.get_comment_threads(yt_service, video_id, next_page)

                        if 'nextPageToken' in video_comment_thread_tmp:
                            next_page = video_comment_thread_tmp['nextPageToken']
                            video_comment_threads = video_comment_thread_tmp
                        else:
                            num_retries_nextPageToken -= 1
                            time.sleep(3)


                if next_page is None or next_page == "" or num_retries_nextPageToken == 0:
                    video_comment_threads = None
                else:
                    video_comment_threads = self.get_comment_threads(yt_service, video_id, next_page)

            except gdata.service.RequestError, request_error:
                    # DEBUG print "\n****************************************************"
                    # DEBUG print "********** STATUS ERROR: %s **********" % request_error[0]['status']
                    # DEBUG print "****************************************************\n"

                    if retries_counter > 0:
                        retries_counter -= 1
        #                ''' Uncomment to debug

                        #print "Retrying extraction from index of comments [%s]..." % comments_count
        #                '''
                    next_page = video_comment_threads['nextPageToken']


#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Simple command-line sample for the Google+ API.

Command-line application that retrieves the list of the user's posts."""

import datetime
import oauth2client
import time
import httplib2
import urllib2
import json

from oauth2client import client
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.tools import argparser, run_flow
from oauth2client.file import Storage

__author__ = 'antoniofsanjuan'
__file__ = 'client_secrets.json'

from apiclient import sample_tools


class GooglePlusService(object):

    _FS = ';' # Field Separator


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


    def getGooglePlusActitivyInfo(self, activity_id):

        num_retries = 3
        b_error = True

        while num_retries > 0 and b_error:

            if 0 < num_retries < 3:
                print "Retrying connection..."

            try:

                if self._gp_service is None:
                    self._gp_service, flags = sample_tools.init(
                        self._argv, 'plus', 'v1', __doc__, __file__,
                        scope='https://www.googleapis.com/auth/plus.me')

                b_error = False

            except AccessTokenRefreshError:
                print "Warming: Credential is expired. Retrying connection..."
                b_error = True
                num_retries -= 1
                self._gp_service = None
                time.sleep(5)
                continue

        comments_resource = self._gp_service.comments()
        comments_document = comments_resource.list(maxResults=500, activityId=activity_id).execute()

        if 'items' in comments_document:
          print '\t\tNumber of G+ comments: %d' % len( comments_document['items'])
          for comment in comments_document['items']:
              print '\t\tG+ Comment ID: %s' % comment['id']
              print '\t\t%s: %s' % (comment['actor']['displayName'],comment['object']['content'])
              print '\t\tG+ Likes: %d' % comment['plusoners']['totalItems']


    def printGooglePlusComment(self, comment):
        if comment is not None:
          print '\t\tG+ Comment ID: %s' % comment['id']
          print '\t\t%s: %s' % (comment['actor']['displayName'],comment['object']['content'])
          print '\t\tG+ Likes: %d' % comment['plusoners']['totalItems']


    def printCSVGooglePlusComment(self, gp_service, gp_comment, yt_comment_id, num_replies, video_id):

        csv_format_string = "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s" + self._FS + "%s" + self._FS + "%s" + self._FS + "%s \n"
        arr_gp_comment_fields = gp_service.getArrayGooglePlusCommentFields(gp_comment)

        formatted_published_time = self.formatYoutubeDate(arr_gp_comment_fields[3])

        parsed_comment_body = self.force_decode(arr_gp_comment_fields[2])

        parsed_comment_body = parsed_comment_body[1:]
        parsed_comment_body = parsed_comment_body[:-1]
        parsed_comment_body = parsed_comment_body.replace('\"', '\\"')
        parsed_comment_body = '"' + parsed_comment_body + '"'

        gp_author = self.force_decode(arr_gp_comment_fields[1])
        gp_author = gp_author.replace('\"', '\\"')

        ts = time.time()
        str_stored_timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        return csv_format_string % (arr_gp_comment_fields[0], gp_author, parsed_comment_body,
                                    formatted_published_time, str_stored_timestamp, arr_gp_comment_fields[4],
                                    yt_comment_id, num_replies, video_id)


    def getArrayGooglePlusCommentFields(self, comment):

        if comment is not None:

            id_comment = comment['id']
            author = comment['actor']['displayName']
            content = comment['object']['content']

            # Replace tabs with blanks couse problems with delimiters
            content = content.replace('\t', '   ')
            # Remove double quotes because couse problems with comment delimiters
            content = content.replace('"', '')
            content = '"%s"' % content

            dt_published = comment['published']
            num_replies = comment['plusoners']['totalItems']

            return [id_comment, author, content, dt_published, num_replies]

    def googlePlusActitivyInfoGenerator(self, activity_id):

        comments_resource = None
        comments_document = None

        retries_counter = 3
        retries_counter_first = 3
        nextPageToken = None
        b_finish = False

        while retries_counter_first > 0 and comments_document is None:
            try:
                comments_resource = self._gp_service.comments()
                comments_document = comments_resource.list(maxResults=500, activityId=activity_id).execute()

            except Exception as e:

                retries_counter_first -= 1
                continue


        while retries_counter > 0 and not b_finish:

            try:
                # If None, we had an exception. We need to retry from last page +1 of comments
                if comments_document is None:

                    # Getting the last page processed
                    comments_document = comments_resource.list(maxResults=500,
                                                               activityId=activity_id,
                                                               pageToken=nextPageToken
                    ).execute()

                    # Getting the NEXT page to continue the process
                    if 'nextPageToken' in comments_document:
                        nextPageToken = comments_document['nextPageToken']

                        comments_document = comments_resource.list(maxResults=500,
                                                                   activityId=activity_id,
                                                                   pageToken=nextPageToken
                        ).execute()


                if 'items' in comments_document:
                    for comment in comments_document['items']:
                        yield comment

                    if 'nextPageToken' in comments_document:
                        nextPageToken = comments_document['nextPageToken']

                    if nextPageToken is None or nextPageToken == "":
                        comments_document = None
                        b_finish = True
                    else:
                        comments_document = comments_resource.list(maxResults=500,
                                                                   activityId=activity_id,
                                                                   pageToken=nextPageToken
                        ).execute()

                    # Reset counter on each succesfull retrieve
                    retries_counter = 3

            except Exception as e:

                #print "\n****************************************************"
                #print "********** STATUS ERROR: %s **********" % request_error[0]['status']
                #print "****************************************************\n"
                #print "\nException: Trying to get next page of G+ comments."

                retries_counter -= 1

                # We only can continue if we have the nextPageToken value
                if nextPageToken is not None and nextPageToken != "":
                    continue

    def getActivityById(self, comment_id):

        num_retries = 3
        b_error = True

        while num_retries > 0 and b_error:

            try:
                if self._gp_service is None:
                    self._gp_service, flags = sample_tools.init(
                        self._argv, 'plus', 'v1', __doc__, __file__,
                        scope='https://www.googleapis.com/auth/plus.me')

                b_error = False

                activity_resource = self._gp_service.activities()
                activity = activity_resource.get(activityId=comment_id).execute()

            except HttpError, err:
                if err.resp.get('content-type', '').startswith('application/json'):
                    data = simplejson.loads(self.content)
                    reason = data['error']['message']

                    print "Warming: Error retrieving G+ comment. Reason: %s" % reason

                # If the error is a rate limit or connection error, wait and
                # try again.
                if err.resp.status in [403, 404, 500, 503]:
                    time.sleep(5)
                else:
                    raise
            except AccessTokenRefreshError:
                print "Warming: Credential is expired. Retrying connection..."
                b_error = True
                num_retries -= 1
                self._gp_service = None
                time.sleep(5)
                continue

        return activity['object']['plusoners']['totalItems']

    def search(self, query):
        activities_resource = self._gp_service.activities()
        activities_document = activities_resource.search(maxResults=5,orderBy='best', query=query).execute()

        if 'items' in activities_document:

            for activity in activities_document['items']:
                self.getGooglePlusActitivyInfo(self._gp_service, activity['id'])


    def get_oauth2_authenticated_service(self):

        num_retries = 3
        b_error = True

        while num_retries > 0 and b_error:

            if 0 < num_retries < 3:
                print "Retrying connection..."

            try:

                # List the scopes your app requires:
                SCOPES = ['https://www.googleapis.com/auth/plus.me',
                          'https://www.googleapis.com/auth/plus.stream.write']

                # The following redirect URI causes Google to return a code to the user's
                # browser that they then manually provide to your app to complete the
                # OAuth flow.
                REDIRECT_URI = 'http://localhost:8080/'

                # For a breakdown of OAuth for Python, see
                # https://developers.google.com/api-client-library/python/guide/aaa_oauth
                # CLIENT_ID and CLIENT_SECRET come from your Developers Console project
                flow = OAuth2WebServerFlow(client_id='458527613508-8r8huecc0ab00i7ca7kngj445nt06df1.apps.googleusercontent.com',
                           client_secret='dJiglItLVeorg1ic8qd5Bk6y',
                           scope=SCOPES,
                           redirect_uri=REDIRECT_URI)


                #flow = client.flow_from_clientsecrets(
                #    'client_secrets.json',
                #    scope='https://www.googleapis.com/auth/drive.metadata.readonly',
                #    redirect_uri=REDIRECT_URI)


                storage = Storage('plus.dat')
                credentials = storage.get()

                if credentials is None or credentials.invalid:

                    print "Credential has expired. Trying to refresh..."

                    #*****
                    #flags = argparser.parse_args()
                    #credentials = run_flow(flow, storage, flags)
                    #*****

                    auth_uri = flow.step1_get_authorize_url()

                    print "G+: auth_uri = '%s'" % auth_uri

                    opener = httplib2.Http();
                    opener.follow_all_redirects = True;
                    opener.follow_redirects = True;

                    (response, body) = opener.request(auth_uri)

                    print "response %s" % response

                    # Manual refresh entering the url by console
                    auth_code = raw_input('Enter authorization code (parameter of URL): ')
                    credentials = flow.step2_exchange(auth_code)


                http_auth = credentials.authorize(httplib2.Http())

                self._gp_service = build('plus', 'v1', http=http_auth)

                b_error = False

            except AccessTokenRefreshError:
                print "Warming: Credential is expired. Retrying connection..."
                b_error = True
                num_retries -= 1
                self._gp_service = None
                time.sleep(5)
                continue



    def __init__(self, argv):

        # Authenticate and construct service.
        self._gp_service = None
        self._flags = None
        self._argv = argv


        class MyRedirectHandler(urllib2.HTTPRedirectHandler):
            def http_error_302(self, req, fp, code, msg, hdrs):

                if fp.geturl().startswith('http://localhost:8080/?code'):
                    # This will raise an exception similar to this:
                    # urllib2.HTTPError: HTTP Error 302: FOUND
                    return None
                else:
                    # Let the default handling occur
                    return super(MyRedirectHandler, self).http_error_302(req, fp, code, msg, hdrs)


        self.get_oauth2_authenticated_service()

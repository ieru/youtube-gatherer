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
from apiclient.errors import HttpError
import oauth2client
from oauth2client.client import AccessTokenRefreshError
import time
from oauth2client.anyjson import simplejson


__author__ = 'antoniofsanjuan@gmail.com'
__file__ = 'client_secrets.json'

from apiclient import sample_tools


class GooglePlusService(object):

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

    def printCSVGooglePlusComment(self, comment):

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

            return "%s\t%s\t%s\t%s\t%s\n" % (id_comment, author, content, dt_published, num_replies)

    def getArrayGooglePlusCommentFields(self, comment):

        #print 'DEBUG: getArrayGooglePlusCommentFields() - INIT'

        if comment is not None:

            id_comment = comment['id']
            author = comment['actor']['displayName']
            content = comment['object']['content']
            #print 'DEBUG: getArrayGooglePlusCommentFields() - content: %s' % content
            # Replace tabs with blanks couse problems with delimiters
            content = content.replace('\t', '   ')
            # Remove double quotes because couse problems with comment delimiters
            content = content.replace('"', '')
            content = '"%s"' % content
            #print 'DEBUG: getArrayGooglePlusCommentFields() - content: %s' % content

            dt_published = comment['published']
            num_replies = comment['plusoners']['totalItems']

            return [id_comment, author, content, dt_published, num_replies]

    def googlePlusActitivyInfoGenerator(self, activity_id):

        #print 'DEBUG: googlePlusActitivyInfoGenerator() - INIT'

        num_retries = 3
        b_error = True

        while num_retries > 0 and b_error:

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
            for comment in comments_document['items']:
                yield comment

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

        #print "\tLikes: %s" % activity['object']['plusoners']['totalItems']

        #print '\t%s' % comment['id'], comment['object']['content']
        #print '\tG+ Likes: %d' % comments_document['plusoners']['totalItems']

        return activity['object']['plusoners']['totalItems']

    def search(self, query):
        activities_resource = self._gp_service.activities()
        activities_document = activities_resource.search(maxResults=5,orderBy='best', query=query).execute()

        if 'items' in activities_document:
            ###print 'Number of Activities: %d' % len(activities_document['items'])
            for activity in activities_document['items']:
                #print activity['id'], activity['object']['content']
                self.getGooglePlusActitivyInfo(self._gp_service, activity['id'])

    def __init__(self, argv):
        # Authenticate and construct service.
        self._gp_service = None
        self._flags = None
        self._argv = argv

        #print "Se ha llamado a  ____init____"
        num_retries = 3
        b_error = True

        while num_retries > 0 and b_error:

            if 0 < num_retries < 3:
                print "Retrying connection..."

            try:
                self._gp_service, flags = sample_tools.init(
                    argv, 'plus', 'v1', __doc__, __file__,
                    scope='https://www.googleapis.com/auth/plus.me')

                b_error = False

            except AccessTokenRefreshError:
                print "Warming: Credential is expired. Retrying connection..."
                b_error = True
                num_retries -= 1
                self._gp_service = None
                time.sleep(5)
                continue


    #    print ('The credentials have been revoked or expired, please re-run'
    #      'the application to re-authorize.')
    #  try:
    #     getCommentById('z125exihcvf1fzty504ci5nailr4h5g4bs00k')
         #search(_gp_service, 'samsung S3 opinion')

    #    person = _gp_service.people().get(userId='+JimGomes').execute()
    #
    #    print 'Got your ID: %s' % person['displayName']
    #    print
    #    print '%-040s -> %s' % ('[Activitity ID]', '[Content]')
    #
    #    # Don't execute the request until we reach the paging loop below.
    #    request = _gp_service.activities().list(
    #        userId=person['id'], collection='public')
    #
    #    comments_resource = _gp_service.comments()
    #
    #    # Loop over every activity and print the ID and a short snippet of content.
    #    while request is not None:
    #      activities_doc = request.execute()
    #
    #
    #      for item in activities_doc.get('items', []):
    #        print '%-040s -> %s' % (item['id'], item['object']['content'][:30])
    #        print '%-040s -> %s' % (item['id'], item['title'][:30])
    #
    #
    #      activity_id = activities_doc['items'][0]

          #sys.stdout.write('activity_id = %s\n' % activity_id['id'])
          #print '%-040s -> %s' % (activity_id['id'], activity_id['title'][:30])

          #comments_document = comments_resource.list( \
          #  maxResults=10,activityId=activity_id).execute()

          #if 'items' in comments_document:
          #  print 'got page with %d' % len( comments_document['items'] )
          #  for comment in comments_document['items']:
          #    print comment['id'], comment['object']['content']

    #    request = _gp_service.activities().list_next(request, activities_doc)

    #  except client.AccessTokenRefreshError:

    #    print ('The credentials have been revoked or expired, please re-run'
    #      'the application to re-authorize.')

#if __name__ == '__main__':
#  main(sys.argv)

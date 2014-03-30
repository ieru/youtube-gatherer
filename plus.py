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

__author__ = 'antoniofsanjuan'

import sys

from oauth2client import client
from apiclient import sample_tools


class GooglePlusService(object):

    def getGooglePlusActitivyInfo(self, activity_id):

        if self._gp_service is None:
              self._gp_service, flags = sample_tools.init(
                argv, 'plus', 'v1', __doc__, __file__,
                scope='https://www.googleapis.com/auth/plus.me')

        comments_resource = self._gp_service.comments()
        comments_document = comments_resource.list( \
          maxResults=500,activityId=activity_id).execute()

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
            return "%s\t%s\t%s\t%s\t%s\n" % (comment['id'], comment['actor']['displayName'],
                                                 comment['object']['content'], comment['published'],
                                                 comment['plusoners']['totalItems'])

    def getArrayGooglePlusCommentFields(self, comment):
        if comment is not None:
            return [comment['id'], comment['actor']['displayName'], comment['object']['content'], comment['published'],
                     comment['plusoners']['totalItems']]

    def googlePlusActitivyInfoGenerator(self, activity_id):

        if self._gp_service is None:
              self._gp_service, flags = sample_tools.init(
                argv, 'plus', 'v1', __doc__, __file__,
                scope='https://www.googleapis.com/auth/plus.me')

        comments_resource = self._gp_service.comments()
        comments_document = comments_resource.list( \
          maxResults=500,activityId=activity_id).execute()

        if 'items' in comments_document:
          for comment in comments_document['items']:
              yield comment


    def getActivityById(self, comment_id):

        if self._gp_service is None:
              self._gp_service, flags = sample_tools.init(
                argv, 'plus', 'v1', __doc__, __file__,
                scope='https://www.googleapis.com/auth/plus.me')

        activity_resource = self._gp_service.activities()
        activity = activity_resource.get( \
            activityId=comment_id).execute()

        print "\tLikes: %s" % activity['object']['plusoners']['totalItems']

        #print '\t%s' % comment['id'], comment['object']['content']
        #print '\tG+ Likes: %d' % comments_document['plusoners']['totalItems']

        return activity['object']['plusoners']['totalItems']



    def search(self, query):
        activities_resource = self._gp_service.activities()
        activities_document = activities_resource.search( \
        maxResults=5,orderBy='best',query=query).execute()

        if 'items' in activities_document:
          print 'Number of Activities: %d' % len( activities_document['items'])
          for activity in activities_document['items']:
            #print activity['id'], activity['object']['content']
            getGooglePlusActitivyInfo(self._gp_service, activity['id'])


    def __init__(self, argv):
        # Authenticate and construct service.
        self._gp_service = None
        self._flags = None

        print "Se ha llamado a  ____init____"
        self._gp_service, flags = sample_tools.init(
          argv, 'plus', 'v1', __doc__, __file__,
          scope='https://www.googleapis.com/auth/plus.me')

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

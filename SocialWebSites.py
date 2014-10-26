#!/usr/bin/python

__author__ = 'antoniofsanjuan'

import json
import urllib2
import time, datetime

class SocialWebSites():

    def getFacebookLinkSharedCount(self, video_url):

        url = "http://graph.facebook.com/?id=%s" % video_url

        result = 0
        # this takes a python object and dumps it to a string which is a JSON
        # representation of that object
        data = json.load(urllib2.urlopen(url))

        # print the result
        #print data
        #print "Id: %s" % data['id']

        try:
            result = data['shares']
        except KeyError, key_error:
            result = 0

        #print "Facebook: %s" % result
        return result

    def getTwitterLinkSharedCount(self, video_url):

        url = "http://cdn.api.twitter.com/1/urls/count.json?url=%s" % video_url
        result = 0

        # this takes a python object and dumps it to a string which is a JSON
        # representation of that object
        data = json.load(urllib2.urlopen(url))

        # print the result
        #print data
        #print "Twitter: %s" % data['count']
        try:
            result = data['count']
        except KeyError, key_error:
            result = 0

        return result

    def getLinkedlnLinkSharedCount(self, video_url):

        url = "http://www.linkedin.com/countserv/count/share?url=%s&format=json" % video_url
        result = 0

        # this takes a python object and dumps it to a string which is a JSON
        # representation of that object
        data = json.load(urllib2.urlopen(url))

        # print the result
        #print data
        #print "LinkedLn: %s" % data['count']
        try:
            result = data['count']
        except KeyError, key_error:
            result = 0

        return result

    def printSocialSharesInfo2CSVFile(self, video_id, csv_file):

        video_url = "https://www.youtube.com/watch?v=%s"
        csv_format_string = "%s\t%s\t%s\t%s\n"
        ts = time.time()

        id_facebook = 1
        id_twitter = 2
        id_linkedln = 3

        n_fb_shares = self.getFacebookLinkSharedCount(video_url % video_id)
        n_tw_shares = self.getTwitterLinkSharedCount(video_url % video_id)
        n_lk_shares = self.getLinkedlnLinkSharedCount(video_url % video_id)

        str_timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        csv_file.write(csv_format_string % (video_id, n_fb_shares, str_timestamp, id_facebook))

        csv_file.write(csv_format_string % (video_id, n_tw_shares, str_timestamp, id_twitter))

        csv_file.write(csv_format_string % (video_id, n_lk_shares, str_timestamp, id_linkedln))

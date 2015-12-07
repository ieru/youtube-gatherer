#!/usr/bin/python

__author__ = 'antoniofsanjuan'

import json
import urllib2
import time, datetime


class SocialWebSitesService():

    _FS = ';' # Field Separator

    def getFacebookLinkSharedCount(self, video_url):

        url = "http://graph.facebook.com/?id=%s" % video_url

        result = -1
        retries_count = 3
        while retries_count > 0:
            try:

                # this takes a python object and dumps it to a string which is a JSON
                # representation of that object
                data = json.load(urllib2.urlopen(url, timeout=10))

                try:
                    result = data['shares']
                    break
                except KeyError, key_error:
                    result = -1

            except Exception as e:
                print "Facebook: Something wrong with the remote server [retry:%s]" % (4 - retries_count)
                retries_count -= 1
                continue

        return result

    def getTwitterLinkSharedCount(self, video_url):

        url = "https://cdn.api.twitter.com/1/urls/count.json?url=%s" % video_url

        result = -1
        retries_count = 3
        while retries_count > 0:
            try:

                # this takes a python object and dumps it to a string which is a JSON
                # representation of that object
                data = json.load(urllib2.urlopen(url, timeout=10))

                try:
                    result = data['count']
                    break
                except KeyError, key_error:
                    result = -1

            except Exception as e:
                print "Twitter: Something wrong with the remote server [retry:%s]" % (4 - retries_count)
                retries_count -= 1
                time.sleep(3)
                continue

        return result

    def getLinkedlnLinkSharedCount(self, video_url):

        url = "http://www.linkedin.com/countserv/count/share?url=%s&format=json" % video_url
        result = -1

        retries_count = 3
        while retries_count > 0:
            try:

                # this takes a python object and dumps it to a string which is a JSON
                # representation of that object
                data = json.load(urllib2.urlopen(url, timeout=10))

                try:
                    result = data['count']
                    break
                except KeyError, key_error:
                    result = -1

            except Exception as e:
                print "Linkedin: Something wrong with the remote server [retry:%s]" % (4 - retries_count)
                retries_count -= 1
                time.sleep(3)
                continue

        return result

    def printSocialSharesInfo2CSVFile(self, video_id, csv_file):

        video_url = "https://www.youtube.com/watch?v=%s"
        csv_format_string = "%s" + self._FS + "%s" + self._FS + "%s" + self._FS + "%s\n"
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

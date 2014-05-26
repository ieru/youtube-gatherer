#!/usr/bin/python

__author__ = 'antoniofsanjuan'

import httplib
import httplib2
import time
import datetime

from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow

# Explicitly tell the underlying HTTP transport library not to retry, since
# we are handling retry logic ourselves.
httplib2.RETRIES = 1

CLIENT_SECRETS_FILE = "G:\\TFC\\API\\v3\\client_secrets.json"


class YoutubeChannel():
    # An OAuth 2 access scope that allows for full read/write access.
    DEVELOPER_KEY = "AIzaSyAj3BdWypes8R3hkQ3x8TeQLMkBNygw6tU"
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
        str_hiddenSubscriberCount = record["statistics"]["hiddenSubscriberCount"]
        str_videoCount = record["statistics"]["videoCount"]
        print "Channel Title: %s" % record["snippet"]["title"]
        print "Channel viewCount: %s" % record["statistics"]["viewCount"]
        print "Channel commentCount: %s" % record["statistics"]["commentCount"]
        print "Channel subscriberCount: %s" % record["statistics"]["subscriberCount"]
        print "Channel hiddenSubscriberCount: %s" % record["statistics"]["hiddenSubscriberCount"]
        print "Channel videoCount: %s" % record["statistics"]["videoCount"]

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
        str_hiddenSubscriberCount = record["statistics"]["hiddenSubscriberCount"]
        str_videoCount = record["statistics"]["videoCount"]
        str_timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        csv_file.write(csv_format_string % (channel_id, str_title, str_viewCount, str_commentCount,
                                            str_subscriberCount, str_hiddenSubscriberCount, str_videoCount,
                                            str_timestamp))

    def get_channel_info(self, channel_id):
        channels_response = self._youtube.channels().list(
            id=channel_id,
            part="id,snippet,statistics"
        ).execute()

        # Searching by channel_id only returns one record
        #for search_result in channels_response.get("items", []):
        if channels_response is not None:
            print self.printCSVYoutubeChannelInfo(channel_id, channels_response.get("items", [])[0])

        return channels_response.get("items", [])[0]


#!/usr/bin/python

__author__ = 'antoniofsanjuan'

from apiclient.discovery import build

import time
import datetime

class YoutubeSearch():
    # Set DEVELOPER_KEY to the API key value from the APIs & auth > Registered apps
    # tab of
    #   https://cloud.google.com/console
    # Please ensure that you have enabled the YouTube Data API for your project.
    DEVELOPER_KEY = ""
    YOUTUBE_API_SERVICE_NAME = ""
    YOUTUBE_API_VERSION = ""

    _youtube = None

    def __init__(self):
        #self.DEVELOPER_KEY = "AIzaSyCGdK6aLpY5o-f_rvk1NOQJUcmD-Cu8C5o"
        self.DEVELOPER_KEY = "AIzaSyAj3BdWypes8R3hkQ3x8TeQLMkBNygw6tU"
        self.YOUTUBE_API_SERVICE_NAME = "youtube"
        self.YOUTUBE_API_VERSION = "v3"

        self._youtube = build(self.YOUTUBE_API_SERVICE_NAME, self.YOUTUBE_API_VERSION,
                              developerKey=self.DEVELOPER_KEY)

    def formatYoutubeDate(self, yt_datetime):
        print "Formateando fecha: %s " % yt_datetime
        _tmp = time.strptime(yt_datetime[:-5], '%Y-%m-%dT%H:%M:%S')
        formated_time = datetime.datetime(*_tmp[:6])

        return formated_time

    def getVideo(self, video_id):
        search_response = self._youtube.videos().list(
            id=video_id,
            part="id,snippet,statistics",
            maxResults=1
        ).execute()

        # Uncomment to DEBUG
        #for search_result in search_response.get("items", []):
        #    print "\tVideo Id: %s" % search_result["id"]
        #    print "\tVideo Title: %s" % search_result["snippet"]["title"]
        #    print "\tviewCount: %s" % search_result["statistics"]["viewCount"]
        #    print "\tlikeCount: %s" % search_result["statistics"]["likeCount"]
        #    print "\tdislikeCount: %s" % search_result["statistics"]["dislikeCount"]
        #    print "\tfavoriteCount: %s" % search_result["statistics"]["favoriteCount"]
        #    print "\tcommentCount: %s" % search_result["statistics"]["commentCount"]

        return search_response['items'][0]

    def printYoutubeInfo2CSVFile(self, arr_yt_videos_info, csv_file):

        csv_format_string = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"

        for yt_video_info in arr_yt_videos_info:
            #print "Video Id: %s" % yt_video_info["id"]

            yt_text_title = yt_video_info["snippet"]["title"]
            yt_text_description = yt_video_info["snippet"]["description"]

            # Escapamos las dobles comillas ya que se utilizaran para separar englobar campos en el fichero csv
            yt_text_title = str(yt_text_title).replace('"', '\\"')
            yt_text_title = '"%s"' % yt_text_title

            yt_text_description = str(yt_text_description).replace('"', '\\"')
            yt_text_description = '"%s"' % yt_text_description

            csv_line = csv_format_string % (yt_video_info["id"],
                                            yt_text_title,
                                            yt_text_description,
                                            "None",
                                            self.formatYoutubeDate(yt_video_info["snippet"]["publishedAt"]),
                                            yt_video_info["statistics"]["viewCount"],
                                            yt_video_info["statistics"]["likeCount"],
                                            yt_video_info["statistics"]["dislikeCount"])

            csv_file.write(csv_line)

    def youtube_search(self, options):
        # Call the search.list method to retrieve results matching the specified
        # query term.
        search_response = self._youtube.search().list(
            q=options.q,
            part="id,snippet",
            type="video",
            maxResults=options.max_results,
            order=options.order
        ).execute()

        videos = []
        #channels = []
        #playlists = []

        # Add each result to the appropriate list, and then display the lists of
        # matching videos, channels, and playlists.
        for search_result in search_response.get("items", []):
            if search_result["id"]["kind"] == "youtube#video":
                videos.append(self.getVideo(search_result["id"]["videoId"]))

                #print "\n\n"
                #videos.append("%s (%s)" % (search_result["snippet"]["title"],
                #                           search_result["id"]["videoId"]))
                #elif search_result["id"]["kind"] == "youtube#channel":
                #    channels.append("%s (%s)" % (search_result["snippet"]["title"],
                #                                 search_result["id"]["channelId"]))
                #elif search_result["id"]["kind"] == "youtube#playlist":
                #    playlists.append("%s (%s)" % (search_result["snippet"]["title"],
                #                                  search_result["id"]["playlistId"]))

        #print "Videos:\n", "\n".join(videos), "\n"
        #print "Channels:\n", "\n".join(channels), "\n"
        #print "Playlists:\n", "\n".join(playlists), "\n"

        return videos

#!/usr/bin/python

__author__ = 'antoniofsanjuan'

from apiclient.discovery import build

import time
import datetime

class YoutubeVideosService():
    # Set DEVELOPER_KEY to the API key value from the APIs & auth > Registered apps
    # tab of
    #   https://cloud.google.com/console
    # Please ensure that you have enabled the YouTube Data API for your project.
    DEVELOPER_KEY = ""
    YOUTUBE_API_SERVICE_NAME = ""
    YOUTUBE_API_VERSION = ""

    _FS = ';' # Field Separator

    _youtube = None

    def __init__(self):
        self.DEVELOPER_KEY = "AIzaSyBQ2PoALuuf4HivoLnLhthRdgBo-NvRMRE"
        self.YOUTUBE_API_SERVICE_NAME = "youtube"
        self.YOUTUBE_API_VERSION = "v3"

        self._youtube = build(self.YOUTUBE_API_SERVICE_NAME, self.YOUTUBE_API_VERSION,
                              developerKey=self.DEVELOPER_KEY)

    def durationToSeconds(self, duration):
        """
        duration - ISO 8601 time format
        examples :
            'P1W2DT6H21M32S' - 1 week, 2 days, 6 hours, 21 mins, 32 secs,
            'PT7M15S' - 7 mins, 15 secs
        """
        split   = duration.split('T')
        period  = split[0]
        time    = split[1]
        timeD   = {}

        # days & weeks
        if len(period) > 1:
            timeD['days']  = int(period[-2:-1])
        if len(period) > 3:
            timeD['weeks'] = int(period[:-3].replace('P', ''))

        # hours, minutes & seconds
        if len(time.split('H')) > 1:
            timeD['hours'] = int(time.split('H')[0])
            time = time.split('H')[1]
        if len(time.split('M')) > 1:
            timeD['minutes'] = int(time.split('M')[0])
            time = time.split('M')[1]
        if len(time.split('S')) > 1:
            timeD['seconds'] = int(time.split('S')[0])

        # convert to seconds
        timeS = timeD.get('weeks', 0)   * (7*24*60*60) + \
                timeD.get('days', 0)    * (24*60*60) + \
                timeD.get('hours', 0)   * (60*60) + \
                timeD.get('minutes', 0) * (60) + \
                timeD.get('seconds', 0)

        return timeS

    def formatYoutubeDate(self, yt_datetime):
        """ Formato Youtube: 2014-05-28T07:48:00.000Z """

        _tmp = time.strptime(yt_datetime[:-5], '%Y-%m-%dT%H:%M:%S')
        formated_time = datetime.datetime(*_tmp[:6])

        return formated_time

    def getVideo(self, video_id):
        search_response = self._youtube.videos().list(
            id=video_id,
            part="id,snippet,statistics,contentDetails",
            maxResults=1
        ).execute()

        return search_response['items'][0]

    def printYoutubeInfo2CSVFile(self, arr_yt_videos_info, csv_file):

        csv_format_string = "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s\n"

        for yt_video_info in arr_yt_videos_info:

            yt_text_title = yt_video_info["snippet"]["title"]
            yt_text_description = yt_video_info["snippet"]["description"]

            yt_text_title = yt_text_title.decode("utf-8") if isinstance(yt_text_title, str) else unicode(yt_text_title)
            yt_text_description = yt_text_description.decode("utf-8") if isinstance(yt_text_description, str) else unicode(yt_text_description)

            # Escapamos las dobles comillas ya que se utilizaran para separar englobar campos en el fichero csv
            yt_text_title = yt_text_title.replace('"', '\\"')
            yt_text_title = '"%s"' % yt_text_title

            yt_text_description = yt_text_description.replace('"', '\\"')
            yt_text_description = '"%s"' % yt_text_description

            yt_text_channelId = yt_video_info["snippet"]["channelId"]

            ts = time.time()
            str_stored_timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

            csv_line = csv_format_string % (yt_video_info["id"],
                                            yt_text_channelId,
                                            yt_text_title,
                                            yt_text_description,
                                            self.durationToSeconds(yt_video_info["contentDetails"]["duration"]),
                                            self.formatYoutubeDate(yt_video_info["snippet"]["publishedAt"]),
                                            yt_video_info["statistics"]["viewCount"],
                                            yt_video_info["statistics"]["likeCount"],
                                            yt_video_info["statistics"]["dislikeCount"],
                                            str_stored_timestamp)

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

        # Add each result to the appropriate list, and then display the lists of
        # matching videos, channels, and playlists.
        for search_result in search_response.get("items", []):
            if search_result["id"]["kind"] == "youtube#video":
                videos.append(self.getVideo(search_result["id"]["videoId"]))

        return videos

    def get_total_video_comments(self, options):

        # Call the search.list method to retrieve results matching the specified
        # query term.
        search_response = self._youtube.search().list(
            q=options.q,
            part="statistics",
            type="video",
            maxResults=1,
            order=options.order
        ).execute()

        result = -1
        for search_result in search_response.get("items", []):
            result = search_result["statistics"]["commentCount"]

        return result

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

        #print "Formateando fecha: %s " % yt_datetime
        _tmp = time.strptime(yt_datetime[:-5], '%Y-%m-%dT%H:%M:%S')
        formated_time = datetime.datetime(*_tmp[:6])

        return formated_time

    def getVideo(self, video_id):
        search_response = self._youtube.videos().list(
            id=video_id,
            part="id,snippet,statistics,contentDetails",
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
        #    print "\tduration: %s" % search_result["contentDetails"]["duration"]
        #    print "\tduration formatted: %s" % self.durationToSeconds(search_result["contentDetails"]["duration"])

        return search_response['items'][0]

    def printYoutubeInfo2CSVFile(self, arr_yt_videos_info, csv_file):

        csv_format_string = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"

        for yt_video_info in arr_yt_videos_info:
            #print "Video Id: %s" % yt_video_info["id"]

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

            csv_line = csv_format_string % (yt_video_info["id"],
                                            yt_text_channelId,
                                            yt_text_title,
                                            yt_text_description,
                                            self.durationToSeconds(yt_video_info["contentDetails"]["duration"]),
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

    def PrintUserEntry(self, entry):
      # print required fields where we know there will be information
      print 'URI: %s\n' % entry.id.text
      print 'Age: %s\n' % (entry.age, '')[entry.age is not None]
      print 'Gender: %s\n' % ('', entry.gender)[entry.gender is not None]
      print 'Location: %s\n' % ('', entry.location.text)[entry.location is not None]

      # check if there is information in the other fields and if so print it
      if entry.first_name:
        print 'First Name: %s\n' % entry.first_name.text
      if entry.last_name:
        print 'Last Name: %s\n' % entry.last_name.text
      if entry.relationship:
        print 'Relationship: %s\n' % entry.relationship.text
      if entry.description:
        print 'About me: %s\n' % entry.description.text
      for link in entry.link:
        if link.rel == 'related':
          print 'Website: %s\n' % link.href
      if entry.company:
        print 'Company: %s\n' % entry.company.text
      if entry.occupation:
        print 'Occupation: %s\n' % entry.occupation.text
      if entry.school:
        print 'School: %s\n' % entry.school.text
      if entry.hobbies:
        print 'Hobbies: %s\n' % entry.hobbies.text
      if entry.movies:
        print 'Movies: %s\n' % entry.movies.text
      if entry.music:
        print 'Music: %s\n' % entry.music.text
      if entry.books:
        print 'Books: %s\n' % entry.books.text
      if entry.hometown:
        print 'Hometown: %s\n' % entry.hometown.text
      if entry.statistics is not None:
        print entry.statistics
        #print 'Subscribers: %s\n' % entry.statistics['subscriberCount']
        #print 'videoWatchCount: %s\n' % entry.statistics.videoWatchCount.text
        #print 'viewCount: %s\n' % entry.statistics.viewCount.text
        #print 'totalUploadViews: %s\n' % entry.statistics.totalUploadViews.text
1. Uncompress all files in one folder. Ex: C:\YoutubeCollector

2. Once you execute the exe file, the application open your default Internet browser to ask for permissions within Google Account. The account related with this compilation is:

User: uahytcollector@gmail.com
Passwd: x3jkW5.a

This is a required step to retrieve Google+ comments using the appropiate API. Only the first time you execute you have to log in the account. The following times, the application
use retrieved data from the generated file "plus.dat"

2. The app prompts for the subject to look for. Here, you can insert a word or sentence and the app looks for like you did in the Youtube browser.
NOTE: The app use the first result attempting to the subject and sort way you insert.

3. The app prompts for the kind of sort to use in the search. Availabled options are showed, "relevance" type by default.

4. The app prompts If you want to avoid to insert the results of searching to database. By default this action is avoid because database is only installed in my computer. So, leave the
default option selected.

5. Once you avoid database, the app starts the extraction data. No progress state is showed, so you have to wait until console prompt shows again or you can open the running log file located
in the created folder: LOGS (Ex. LOGS\YoutubeGatherer_20141201_2049.log)
Each execution created 1 new log file.

6. The results of extraction are 5 data files (Tab separated) in the sub-folder "DATA":
    gp_comments_<YYYY>-<MM>-<DD>.csv     -> Google+ comments
    yt_comments_<YYYY>-<MM>-<DD>.csv     -> Youtube comments
    yt_channel_<YYYY>-<MM>-<DD>.csv      -> Channel info related that contains the video
    yt_videos_<YYYY>-<MM>-<DD>.csv       -> Video info
    yt_social_<YYYY>-<MM>-<DD>.csv       -> Statistics from social networks where the video link appears (Facebook, Twitter and LinkedId )

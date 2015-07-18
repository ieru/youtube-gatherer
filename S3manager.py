#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'antoniofsanjuan'

import platform
import os
import sys
import math
import boto.s3
from boto.s3.key import Key


class S3manager():

    _bucket = None
    _bucket_name = None
    _conn = None

    _OS_HOST_PATH_SEPARATOR = '\\' if (platform.system().lower() == 'windows') else '/'
    _OS_REMOTE_PATH_SEPARATOR = '/'

    def __init__(self):

        #self._bucket_name = self.AWS_ACCESS_KEY_ID.lower() + '-' + 'heroku-ytg'
        self._bucket_name = 'heroku-ytg-v2'
        #self._conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        self._conn = boto.connect_s3()

        # self._conn = boto.connect_s3() This way get the ID and pass from default user folder "~/.aws/credentials" file

        try:
            if (self._conn is not None):
                self._bucket = self._conn.get_bucket(self._bucket_name)
            else:
                print "Connection is None"
        except boto.exception.S3ResponseError as e:
            if (self._bucket is not None):
                print "Bucket is NOT None."
            else:
                print "Bucket IS None.  Except: %s" % e.message
                exit(1)

    def uploadFile(self, path_orig, path_dest):

        print 'Uploading %s to Amazon S3 bucket %s' % \
               (path_orig, self._bucket_name)

        try:
            path, filename = os.path.split(path_orig)

            print "\tPath orig: %s; filename orig: %s" % (path, filename)

            k = self._bucket.new_key(path_dest)
            k.set_contents_from_filename(path_orig, cb=self.percent_cb, num_cb=10)

        except Exception as e:
            print "Exception uploading file '%s' to S3" % path_filename
            print "Message: %s" % e.message
            return 1

        return True

    def upload_files(self, arr_files_path, path_remote):

        try:

          for file_path in arr_files_path:

              print '\nUploading %s to Amazon S3 bucket %s ' % \
                  (file_path, self._bucket_name),

              path, filename = os.path.split(file_path)

              path_file_dest = (path_remote + '/' + path + '/' + filename)

              k = self._bucket.new_key(path_file_dest)

              k.set_contents_from_filename(file_path, cb=self.percent_cb, num_cb=10)
              sys.stdout.write(' [DONE]')
              sys.stdout.flush()

        except Exception as e:
            print "Exception uploading file '%s' to S3" % file_path
            print "Message: %s" % e.message
            return False

        return True


    def createFolder(self, folder_name):

        try:

            k = self._bucket.new_key(folder_name + '/')
            k.set_contents_from_string('')

        except Exception as e:
            print "Exception uploading file '%s' to S3" % path_filename
            return 1

        return True

    def percent_cb(self, complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()


    def list_files(self):

        try:

            keys = self._bucket.list()
            for key in keys:
                #print key.name
                print key.name

        except Exception as e:
            print "Exception listing files from bucket"
            print e.message
            return 1

        return True

    def get_files(self):

        try:

            keys = self._bucket.list()

            for key in keys:
                if not key.name.endswith('/'): yield key

        except Exception as e:
            print "Exception listing files from bucket"
            print e.message

    def get_folders(self):

        try:

            keys = self._bucket.list()

            for key in keys:
                if key.name.endswith('/'): yield key

        except Exception as e:
            print "Exception listing folders from bucket"
            print e.message


    def get_files_to_local(self, local_path):

        try:

            for key in self.get_folders():

                if (platform.system().lower() == 'windows'):
                    local_folder_path = local_path + '\\' + key.name.replace('/', '\\')
                else:
                    local_folder_path = local_path + '/' + key.name.replace('\\', '/')

                if not os.path.exists(local_folder_path):
                    os.makedirs(local_folder_path)

            for key in self.get_files():

                if (platform.system().lower() == 'windows'):
                    local_file_path = local_path + '\\' + key.name.replace('/', '\\')
                else:
                    local_file_path = local_path + '/' + key.name.replace('\\', '/')

                if not os.path.exists(local_file_path):
                    print "\tDownloading file: %s" % key.name
                    key.get_contents_to_filename(local_file_path)

        except Exception as e:
            print 'Exception: %s' % e


    def get_space_used(self):

        try:

            total_bytes = 0

            for key in self.get_files():
                total_bytes += key.size

        except Exception as e:
            print 'Exception: %s' % e


    def check_space_available(self):

        total_available_space = 500 * 1000000;

        try:

            total_bytes = 0

            for key in self.get_files():
                total_bytes += key.size

            percent_free = 100 - math.ceil( ( total_bytes * 100 ) / total_available_space )

            print "Total space [bytes]: %s" % total_available_space
            print "Total space used [bytes]: %s" % total_bytes
            print "Free space [%%]: %s" % percent_free
            if percent_free < 10:
                print "\tWarming: Free space under 10% [<50MB]"
                return False

        except Exception as e:
            print 'Exception: %s' % e

        return True

def main(argv):
    print ""
    s3_service = S3manager()

    # Uncomment to test
    #s3_service.uploadFile('DATA/xxx.csv')

    # Uncomment to test
    #s3_service.list_files()

    #for key in s3_service.get_files():
    #    print key.name

    #s3_service.get_files_to_local('G:\\tmp')

    s3_service.check_space_available()

if __name__ == '__main__':
    main(sys.argv)

#!/usr/bin/env python3
import argparse
from datetime import datetime
import errno
import mimetypes
import os
import stat
import sys
import logging

from fuse import FUSE, FuseOSError, Operations
from google.cloud import storage
import redis
import redis.exceptions


logger = logging.getLogger(__name__)


BLOCK_SIZE = 256
DIR_METADATA = '__dir_metadata__'


class Festivus(Operations):
    """ An open source version of Festivus, Descartes Labs Google Cloud Storage backed
    FUSE implementation that uses REDIS to cache metadata.

    Arguments:
            bucket_name (required str): Bucket name on Google Cloud Storage
            service_account (path): Path to Google Cloud Service Account JSON file to use for
                connection
            redis_url (str): URI for Redis host and database.
            base_key (str): Use a base key if several Festivus instances are sharing the same Redis
                Database. You probably shouldn't do that though.
            init (bool): Set init to True to build the initial Redis metadata cache.
        """

    def __init__(self, bucket_name, service_account=None, redis_url='redis://localhost:6379/0', base_key='', init=False):  # NOQA: E501
        if service_account:
            self.client = storage.Client.from_service_account_json(service_account)
        else:
            self.client = storage.Client()
        self.bucket_name = bucket_name.split('gs://')[-1]
        self.bucket = self.client.get_bucket(self.bucket_name)
        try:
            self.redis = redis.StrictRedis.from_url(redis_url)
        except redis.exceptions.ConnectionError:
            logger.critical('Cannot connect to Redis server')
        self.base_key = base_key
        self.temp_files = {}
        # self.st_mode = set based on permissions to write to the bucket

        self.uid = os.getuid()

        if init:
            for blob in self.bucket.list_blobs():
                # One pipeline for blob. Might be more efficient to enumerate blobs
                # and only make a new pipeline every X number of blobs.
                pipe = self.redis.pipeline()

                # build our path structure for fast file listing
                path, name = os.path.split(blob.name)
                parts = (part + '/' for part in path.split('/') if part != '')
                base = base_key + '/'

                for part in parts:
                    base_metadata = base + DIR_METADATA
                    dir_stat = self.redis.hgetall(base_metadata)
                    try:
                        st_mtime = datetime.strptime(dir_stat['st_mtime'], '%s')
                        st_ctime = datetime.strftime(dir_stat['st_ctime'], '%s')
                    except KeyError:  # directory metadata doesn't yet exist
                        self.redis.hset(base_metadata, 'st_mtime',
                                        blob.updated.strftime('%s'))
                        self.redis.hset(base_metadata, 'st_atime',
                                        blob.updated.strftime('%s'))
                        self.redis.hset(base_metadata, 'st_ctime',
                                        blob.time_created.strftime('%s'))
                    else:
                        if blob.updated > st_mtime:
                            self.redis.hset(base_metadata, 'st_mtime',
                                            blob.updated.strftime('%s'))
                            self.redis.hset(base_metadata, 'st_atime',
                                            blob.updated.strftime('%s'))
                        if blob.time_created < st_ctime:
                            self.redis.hset(base_metadata, 'st_ctime',
                                            blob.time_created.strftime('%s'))
                    pipe.sadd(base, part)
                    base += part

                pipe.sadd(base, name)

                # set time and size metadata for files
                # times are seconds since epoch
                name = base + name
                pipe.hset(name, 'st_mtime', blob.updated.strftime('%s'))
                pipe.hset(name, 'st_atime', blob.updated.strftime('%s'))
                pipe.hset(name, 'st_ctime', blob.time_created.strftime('%s'))
                pipe.hset(name, 'st_size', blob.size)
                try:
                    pipe.execute()
                except redis.exceptions.ConnectionError:
                    logger.critical('Redis is not avaliable. Exiting')
                    sys.exit(0)

    # Filesystem methods. Uses Redis to cache for faster access
    # def access(self, path, mode):
    #     raise NotImplementedError

    def chmod(self, path, mode):
        raise NotImplementedError

    def chown(self, path, uid, gid):
        raise NotImplementedError

    def getattr(self, path, fh=None):
        '''This method is called before all operations at least once,
        to check if the file object at *path* exists at all'''
        try:
            attrs = self.redis.hgetall(self.base_key + path)
        except redis.exceptions.ResponseError:
            # throws error when trying to access directory which is a set,
            # need to retrieve data from directory metadata
            attrs = self.redis.hgetall(self.base_key + path + DIR_METADATA)
            if len(attrs) > 0:
                attrs = {key.decode(): attrs[key].decode() for key in attrs}
                attrs['st_mode'] = (stat.S_IFDIR | 0o644)
                attrs['st_size'] = BLOCK_SIZE * 4
                attrs['st_nlink'] = 2
            else:
                raise FuseOSError(errno.ENOENT)  # No such directory
        else:
            if len(attrs) > 0:
                attrs = {key.decode(): attrs[key].decode() for key in attrs}
                attrs['st_mode'] = (stat.S_IFREG | 0o644)
                attrs['st_nlink'] = 1
            else:
                # hgetall will return an empty dict for unknown keys,
                # only throws exceptions for other data types
                attrs = self.redis.hgetall(self.base_key + path + '/' + DIR_METADATA)
                if len(attrs) > 0:
                    attrs = {key.decode(): attrs[key].decode() for key in attrs}
                    attrs['st_mode'] = (stat.S_IFDIR | 0o644)
                    attrs['st_size'] = BLOCK_SIZE * 4
                    attrs['st_nlink'] = 2
                else:
                    raise FuseOSError(errno.ENOENT)  # No such file or directory
        attrs['st_uid'] = self.uid
        attrs['st_gid'] = self.uid
        attrs['st_dev'] = 0
        attrs['st_ino'] = 0
        attrs['st_blocks'] = int((int(attrs['st_size']) + BLOCK_SIZE - 1) / BLOCK_SIZE)
        for t in ('st_atime', 'st_mtime', 'st_ctime'):
            attrs[t] = float(attrs[t])
        attrs['st_size'] = int(attrs['st_size'])
        return attrs

    def readdir(self, path, fh):
        '''This method is called when you list a directory.
        I.e. by calling ls /my_filesystem/
        The method getattr is called first, to see if path exists and is a directory.
        Returns a list of strings with the file or directory names in the directory *path*.'''
        dir = list(self.redis.smembers(self.base_key + path))
        return [item.decode() for item in dir]

    def readlink(self, path):
        raise NotImplementedError

    def mknod(self, path, mode, dev):
        raise NotImplementedError

    def rmdir(self, path):
        raise NotImplementedError

    def mkdir(self, path):
        raise NotImplementedError

    def statsfs(self, path):
        raise NotImplementedError

    def unlink(self, path):
        raise NotImplementedError

    def symlink(self, name, target):
        raise NotImplementedError

    def rename(self, old, new):
        raise NotImplementedError

    def link(self, target, name):
        raise NotImplementedError

    def utimens(self, path, times=None):
        raise NotImplementedError

    # File methods
    # will use Blob.upload_from_string and Blob.download_from_string

    def open(self, path, flags):
        '''This method is called before the first call to the read or write methods,
        to open a file. May open a file at path, i.e. setting flags for permission or mode.
        The return value can be a file object, which will be passed as the fh parameter
        to subsequent calls to the methods read or write.
        This method does not need to be implemented; you can also open the file in the
        read/write methods.'''
        blob = self.bucket.get_blob(path[1:])
        print(path, blob)
        self.temp_files[path] = blob.download_as_string()
        return 0

    def create(self, path, mode, fi=None):
        raise NotImplementedError

    def read(self, path, length, offset, fh):
        '''This method is called when you read a file. I.e. by calling cat /my_filesystem/my_file.
        It may be called multiple times, to read data sequentially or at random.
        But first the method getattr will be called once and then the method open.
        Returns data as a string.
        Should return *size* bytes of data associated with the file *path* at position *offset*.'''
        if not self.temp_files[path]:
            blob = self.bucket.get_blob(path[1:])
            self.temp_files[path] = blob.download_as_string()
        self.temp_files[path][offset:offset+length]
        return length

    def write(self, path, buf, offset, fh):
        '''This method is called when you write to a file.
        I.e. by calling echo "hello world" > /my_filesystem/my_file
        It may be called multiple times, to write data sequentially or at random.
        But first the method getattr will be called once and then the method open.
        Returns data as a string.
        Should write the strin in buf to the file *path* at position *offset*.'''
        raise NotImplementedError

    def truncate(self, path, length, fh=None):
        raise NotImplementedError

    def flush(self, path, fh):
        blob = self.bucket.get_blob(path[1:])
        content_type = mimetypes.guess_type(path)
        blob.upload_from_string(self.temp_files[path], content_type)

    def release(self, path, fh):
        del self.temp_files[path]

    def fsync(self, path, fdatasync, fh):
        raise NotImplementedError


if __name__ == '__main__':
    """
    Arguments:
        bucket_name (required str): Bucket name on Google Cloud Storage
        service_account (path): Path to Google Cloud Service Account JSON file to use for connection
        redis_url (str): URI for Redis host and database.
        base_key (str): Use a base key if several Festivus instances are sharing the same Redis
            Database. You probably shouldn't do that though.
        init (bool): Set init to True to build the initial Redis metadata cache.
    """
    parser = argparse.ArgumentParser(
            description='FUSE filesystem for Google Cloud Storage with a Redis metadata cache')
    parser.add_argument('mount_point',
                        help='Where should the Google Cloud Storage bucket be mounted?')
    parser.add_argument('bucket_name', help='Name of Google storage bucket to use')
    parser.add_argument('-r', '--redis',
                        dest='redis_uri',
                        help='URI for Redis instance for metadata',
                        default='redis://localhost:6379/0')
    parser.add_argument('-b', '--base',
                        dest='base_key',
                        help='Base path for redis keys if cluster is shared for different uses',
                        default='')
    parser.add_argument('-s', '--service-account',
                        dest='service_account',
                        help='Path to service account credentials for GCS bucket')
    parser.add_argument('-i', '--init',
                        dest='init',
                        action='store_true',
                        help='Should this instance initialize the Redis metadata instance')
    parser.add_argument('-d', '--debug',
                        dest='debug',
                        action='store_true',
                        help='Debug mode')
    args = parser.parse_args()
    print(args)

    FUSE(Festivus(args.bucket_name,
                  args.service_account,
                  args.redis_uri,
                  args.base_key,
                  args.init),
         args.mount_point,
         foreground=True,
         nothreads=True,
         debug=args.debug)

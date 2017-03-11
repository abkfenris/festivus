import os

from fuse import FUSE, FuseOSError, Operations
from google.cloud import storage
import redis



class Festivus(Operations):
    """ An open source version of Festivus, Descartes Labs Google Cloud Storage backed
    FUSE implementation that uses REDIS to cache metadata.
    
    Arguments:
            bucket_name (required str): Bucket name on Google Cloud Storage
            service_account (path): Path to Google Cloud Service Account JSON file to use for connection
            redis_url (str): URI for Redis host and database.
            base_key (str): Use a base key if several Festivus instances are sharing the same Redis Database. You probably shouldn't do that though.
            init (bool): Set init to True to build the initial Redis metadata cache. 
        """

    def __init__(self, bucket_name, service_account=None, redis_url='redis://localhost:6379/0', base_key='/', init=False):
        """
        
        """
        if service_account:
            self.client = storage.Client.from_service_account_json(service_account)
        else:
            self.client = storage.Client()
        self.bucket_name = bucket_name.split('gs://')[-1]
        self.bucket = self.client.get_bucket(self.bucket_name)
        self.redis = redis.StrictRedis.from_url(redis_url)
        self.base_key = base_key
        #self.st_mode = set based on permissions to write to the bucket

        if init:
            for blob in self.bucket.list_blobs():
                # One pipeline for blob. Might be more efficient to enumerate blobs and only make a new
                # pipeline every X number of blobs.
                pipe = self.redis.pipeline()

                # build our path structure for fast file listing
                path, name = os.path.split(blob.name)
                parts = (part + '/' for part in path.split('/') if part != '')
                base = base_key

                for part in parts:
                    pipe.sadd(base, part)
                    base += part
                
                pipe.sadd(base, name)

                # set time and size metadata for files, times are seconds since epoch
                name = base + name
                pipe.hset(name, 'st_mtime', blob.updated.strftime('%s'))
                pipe.hset(name, 'st_atime', blob.updated.strftime('%s'))
                pipe.hset(name, 'st_ctime', blob.time_created.strftime('%s'))

                pipe.execute()

    # Filesystem methods. Uses Redis to cache for faster access

    def access(self, path, mode):
        raise NotImplementedError
    
    def chmod(self, path, mode):
        raise NotImplementedError
    
    def chown(self, path, uid, gid):
        raise NotImplementedError
    
    def getattr(self, path, fh=None):
        raise NotImplementedError
    
    def readdir(self, path, fh):
        raise NotImplementedError
    
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
        raise NotImplementedError
    
    def create(self, path, mode, fi=None):
        raise NotImplementedError
    
    def read(self, path, length, offset, fh):
        raise NotImplementedError
    
    def write(self, path, buf, offset, fh):
        raise NotImplementedError
    
    def truncate(self, path, length, fh=None):
        raise NotImplementedError
    
    def flush(self, path, fh):
        raise NotImplementedError
    
    def release(self, path, fh):
        raise NotImplementedError
    
    def fsync(self, path, fdatasync, fh):
        raise NotImplementedError
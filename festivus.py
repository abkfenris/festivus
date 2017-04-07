import mimetypes
import os

from fuse import FUSE, FuseOSError, Operations, Stat
from google.cloud import storage
import redis

BLOCK_SIZE = 256


class FestivusStat(Stat):
    #st_mode (protection bits)
    #st_ino (inode number)
    #st_dev (device)
    #st_nlink (number of hard links)
    #st_uid (user ID of owner)
    #st_gid (group ID of owner)
    #st_size (size of file, in bytes)
    #st_atime (time of most recent access)
    #st_mtime (time of most recent content modification)
    #st_ctime (time of most recent content modification or metadata change).
    pass


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

    def __init__(self, bucket_name, service_account=None, redis_url='redis://localhost:6379/0', base_key='', init=False):
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
        self.temp_files = {}
        #self.st_mode = set based on permissions to write to the bucket

        if init:
            for blob in self.bucket.list_blobs():
                # One pipeline for blob. Might be more efficient to enumerate blobs and only make a new
                # pipeline every X number of blobs.
                pipe = self.redis.pipeline()

                # build our path structure for fast file listing
                path, name = os.path.split(blob.name)
                parts = (part + '/' for part in path.split('/') if part != '')
                base = base_key + '/'

                for part in parts:
                    pipe.sadd(base, part)
                    base += part
                
                pipe.sadd(base, name)

                # set time and size metadata for files, times are seconds since epoch
                name = base + name
                pipe.hset(name, 'st_mtime', blob.updated.strftime('%s'))
                pipe.hset(name, 'st_atime', blob.updated.strftime('%s'))
                pipe.hset(name, 'st_ctime', blob.time_created.strftime('%s'))
                pipe.hset(name, 'st_blocks', int((blob.size + BLOCK_SIZE - 1) / BLOCK_SIZE))

                pipe.execute()


    # Filesystem methods. Uses Redis to cache for faster access
    def access(self, path, mode):
        raise NotImplementedError
    
    def chmod(self, path, mode):
        raise NotImplementedError
    
    def chown(self, path, uid, gid):
        raise NotImplementedError
    
    def getattr(self, path, fh=None):
        '''This method is called before all operations at least once, 
        to check if the file object at *path* exists at all'''
        st = FestivusStat()
        attrs = self.redis.hgetall(self.base_key + path)
    
    def readdir(self, path, fh):
        '''This method is called when you list a directory.
        I.e. by calling ls /my_filesystem/
        The method getattr is called first, to see if path exists and is a directory.
        Returns a list of strings with the file or directory names in the directory *path*.'''
        return list(self.redis.smembers(self.base_key + path))
    
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
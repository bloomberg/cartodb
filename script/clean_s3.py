import argparse
import boto
from boto.s3.key import Key
from boto.s3.connection import S3Connection
from boto.s3.connection import OrdinaryCallingFormat
import datetime
import sys

class UserDefinedOptions:
    """Responsible for parsing commandline options"""
    def __init__(self, argv):
        self.bucket = 'dev'
        self.expiryInSeconds = None
        self.host = None
        self.port = None
        self.accessKey = None
        self.secretAccessKey = None
        parser = argparse.ArgumentParser(description='Clean objects in S3 older than a specified time period')
        parser.add_argument('-b', '--bucket',
                            required=True,
                            help='The bucket to clean')
        parser.add_argument('-e', '--expire-in-seconds',
                            default=86400,
                            help='The expiration time in seconds for an object')
        parser.add_argument('-n', '--host',
                            required=True,
                            help='S3 FDQN hostname')
        parser.add_argument('-p', '--port',
                            required=True,
                            help='S3 port')
        parser.add_argument('-k', '--access-key',
                            required=True,
                            help='S3 access key id')
        parser.add_argument('-s', '--secret-access-key',
                            required=True,
                            help='S3 access key id')
        args = parser.parse_args(argv)
        if(args.bucket is not None):
            self.bucket = args.bucket
        if(args.expire_in_seconds is not None):
            self.expiryInSeconds = int(args.expire_in_seconds)
        if(args.host is not None):
            self.host = args.host
        if(args.port is not None):
            self.port = int(args.port)
        if(args.access_key is not None):
            self.accessKey = args.access_key
        if(args.secret_access_key is not None):
            self.secretAccessKey = args.secret_access_key

def cleanBucket(opt):
    """Clean the s3 bucket"""
    expiration = datetime.timedelta(seconds=opt.expiryInSeconds)
    expirationDate = datetime.datetime.now()
    conn = S3Connection(
            aws_access_key_id=opt.accessKey,
            aws_secret_access_key=opt.secretAccessKey,
            is_secure=False,
            host=opt.host,
            port=opt.port,
            calling_format=OrdinaryCallingFormat())
    bucket = conn.get_bucket(opt.bucket)
    for obj in  bucket.list():
        lastModified = boto.utils.parse_ts(obj.last_modified)
        if( expirationDate - lastModified > expiration):
            print '{} was last modified on {} which is {} old. This object will be cleaned up'.format(
                    obj.name,
                    lastModified,
                    expirationDate - lastModified)
            bucket.delete_key(obj)

    
    print conn.get_all_buckets()

def main(argv):
    """Main function"""
    try:
        opt = UserDefinedOptions(argv)
        cleanBucket(opt)
    except Exception, error:
        print 'Exception - {}'.format(error)

if __name__ == "__main__":
    main(sys.argv[1:]) 

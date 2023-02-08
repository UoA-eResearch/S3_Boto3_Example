#!/usr/bin/env python3
import boto3
from boto3.session import Session
import botocore
import hashlib
import re
import json
import os

def bucket_ls(s3, bucket, prefix="", suffix="", delimiter = "/"):
  '''
  Generate objects in an S3 bucket. Derived from AlexWLChan 2019

  :param s3: authenticated client session.
  :param bucket: Name of the S3 bucket.
  :param prefix: Only fetch objects whose key starts with this prefix (optional).
  :param suffix: Only fetch objects whose keys end with this suffix (optional).
  :param delimiter: Fake directories with '/' delimiter in object names.
  '''
  paginator = s3.get_paginator("list_objects") # should be ("list_objects_v2"), but only getting first page with this

  kwargs = {'Bucket': bucket}

  # We can pass the prefix directly to the S3 API.  If the user has passed
  # a tuple or list of prefixes, we go through them one by one.
  if isinstance(prefix, str):
    prefixes = (prefix, )
  else:
    prefixes = prefix

  for key_prefix in prefixes:
    kwargs["Prefix"] = key_prefix

    for page in paginator.paginate(**kwargs):
      try:
        contents = page["Contents"]
      except KeyError:
        break

      for obj in contents:
        key = obj["Key"]
        if key.endswith(suffix):
          yield obj

def icos_ls(conf, prefix = ""):
  '''
  Connect to the University 'ICOS' S3 Store and list the bucket named in the 'conf' file

  :param conf: The S3 keys and s3 URL in a dictionary
  :param prefix: Only fetch objects whose key starts with this prefix (optional).
  '''

  icos_session = boto3.Session(
       aws_access_key_id=conf['access_key'],
       aws_secret_access_key=conf['secret_key']
  )

  icos_connection = icos_session.client(
      's3',
      aws_session_token=None,
      region_name='us-east-1',
      use_ssl=True,
      endpoint_url=conf['endpoint'],
      config=None
  )

  total = 0
  count = 0
  for r in bucket_ls(s3=icos_connection, bucket=conf['bucket'], prefix=prefix):
    print( r['Key'], ' ', r['ETag'], r['Size'])
    total = total + int(r['Size'])
    count = count + 1

  print( "Count: ", count, " Total: ", total )

def load_keys(filename):
  '''
  Load the keys and bucket details from the json configuration file
  This is kept separate, so the keys can be exclude from the github repository
  .gitignore file line
  conf/keys.json

  :param filename: the configuration filename
  '''
  # Our configuration files are in the current directory's conf/ directory
  if filename.startswith('/'):
    # filename is absolute
    conf_file = filename
  else:
    # Filename is relative to the script directory
    conf_file = os.path.abspath( os.path.join( os.path.dirname(__file__), filename ) )
  try:
    with open( conf_file ) as f:
      conf = json.load(f)
  except Exception as e:
    print( f"load_keys({filename}): ", e )
    raise SystemExit(1)
  return conf


# Keep the keys separate, so they don't end up in a github repository for everyone to see
conf = load_keys('conf/keys.json')

#
icos_ls(conf, prefix='diphot/GB0705/R')

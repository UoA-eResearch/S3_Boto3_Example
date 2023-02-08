#! /usr/bin/env python3
import boto3
from boto3.session import Session
import botocore
import hashlib
import re
import json
import os

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

def print_metadata(conf, filename):
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

  try:
    metadata = icos_connection.head_object(Bucket=conf['bucket'], Key=filename)
    print(metadata['Metadata'])
  except:
    print(f"Failed to read {filename}")
    raise SystemExit(1)

# Keep the keys separate, so they don't end up in a github repository for everyone to see
conf = load_keys('conf/keys.json')
print_metadata(conf, 'GB/fit/GB0101/R/B3313-gb1-R-1.fit')

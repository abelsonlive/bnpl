"""
self contained utlities
should not reference other modules here
"""
import gevent.monkey
gevent.monkey.patch_all()
import gevent
from gevent.pool import Pool

import os
import re
import sys
import uuid
import time
import hashlib
import json
import logging
import collections
import subprocess
import argparse 
from datetime import datetime, date
from inspect import isgenerator
from functools import wraps
from traceback import format_exc
from StringIO import StringIO

import requests
import yaml
import pytz 
from slugify import slugify 
from unidecode import unidecode
from flask import request
from flask import make_response
from flask import Response
from flask import send_file
from werkzeug.utils import secure_filename

import iso8601
from unidecode import unidecode
from tzlocal import get_localzone

##########################################
# CONSTANTS
##########################################

CLI_FILE_FORMATS = [
    'yml', 'yaml', 'json'
]

##########################################
# DEFAULTS
##########################################

LIST_DELIMITER = ','
STRING_SLUG_DELIMITER = '-'

# boolean parsing.
BOOLEAN_TRUE_VALUES = [
  'y', 'yes', '1', 't', 'true', 'on', 'ok'
]

BOOLEAN_FALSE_VALUES = [
  'n', 'no', '0', 'f', 'false', 'off', ''
]

NULL_STRING = ''

NULL_VALUES = [
  'null', 'na', 'n/a', 'nan', 'none'
]

MIMETYPE_DEFAULT = 'application/octet-stream'

##########################################
# REGEXES
##########################################
RE_TYPE = type(re.compile(r''))
RE_CLI_ARG = re.compile(r'^[\'\"]?(.*)[\'\"]?$')
RE_NUMERIC = re.compile(r'[0-9\.\,]+')

##########################################
# JSON UTILITES
##########################################

def json_serializer(o):

  """
  obj > json
  """

  class _encoder(json.JSONEncoder):

    """ This encoder will serialize all entities that have a to_dict
    method by calling that method and serializing the result.
    Taken from: https://github.com/pudo/apikit
    """

    def __init__(self, refs=False):
      self.refs = refs
      super(_encoder, self).__init__()

    def default(self, o):
      """
      """
      if isinstance(o, (date, datetime)):
        return date_to_iso(o)
      if isinstance(o, set):
        return list(o)
      if isgenerator(o):
        return list(o)
      if isinstance(o, Counter):
        return dict(o)
      if isinstance(o, RE_TYPE):
        return o.pattern
      if hasattr(o, 'to_dict'):
        return o.to_dict()
      if hasattr(o, 'to_json'):
        return o.to_json()
      return json.JSONEncoder.default(self, o)

  data = _encoder().encode(o)
  return data

def json_deserializer(s):
  """
  json > obj
  """
  return json.loads(s)

##########################################
# YML UTILITES
##########################################

def yml_serializer(o):
  """
  obj > yml
  """
  return yaml.safe_dump(o)

def yml_deserializer(s):
  """
  Load a yaml file in order.
  """
  return yaml.safe_load(s)

##########################################
# NULL UTILITIES
##########################################

def null_prepare(s):
  """
  prepare a string
  """
  if check_string(s) and s.lower() in NULL_VALUES: 
    return None
  elif not s: 
    return None 
  raise ValueError('Invalid null type: {0}'.format(s))

def null_check(s):
  """
  check if an item is a string.
  """
  if not s or (check_string(s) and s.lower() in NULL_VALUES): 
    return True
  return False

def null_to_string(n):
  """
  format a null string; placeholder
  """
  return NULL_STRING 

def null_from_string(s):
  """
  placeholder
  """
  return None 

##########################################
# STRING UTILITIES
##########################################

def string_prepare(s):
  """
  prepare a string
  """
  try:
    if not s:
      return s
    return unidecode(unicode(s))
  except Exception as s:
    raise ValueError('Invalid string type: {0}:\nTraceback:{1}'.format(s, error_tb()))

def string_check(s):
  """
  check if an item is a string.
  """
  return isinstance(s, basestring)

def string_to_uid(fingerprint=None, salt='', length=10):
  """
  format a uid.
  """
  if not fingerprint:
    fingerprint = unicode(uuid.uuid4())
  fingerprint += salt
  return hashlib.sha1(fingerprint.encode("UTF-8")).hexdigest()[:length+1]

def string_camel_case_to_slug(string, delim=STRING_SLUG_DELIMITER):
  """
  covert camel to slug case
  """
  replace = '\1' + delim + '\2'
  return re.sub('([a-z0-9])([A-Z])', replace, re.sub('(.)([A-Z][a-z]+)', replace, string)).lower()

def string_to_slug(string, delim=STRING_SLUG_DELIMITER, convert_camel=True):
  """
  slugify a string, handling camelcasing
  """
  if convert_camel:
    string = string_camel_case_to_slug(string, delim=delim)
  string = slugify(string_prepare(string))
  if delim is not '-':
    string = string.replace('-', delim)
  return string

##########################################
# INTEGER UTILITIES
##########################################

def integer_prepare(s):
  """

  """
  try:
    return int(s)
  except:
    raise ValueError("Invalid int type: {0}".format(s))

def integer_check(s):
  """
  """
  try:
    s = int(s)
    return True
  except Exception as e: 
    return False

##########################################
# FLOAT UTILITIES
##########################################

def float_prepare(s):
  """
  prepare a float type.
  """
  try:
    return float(s)
  except:
    raise ValueError("Invalid float type: {0}".format(s))

def float_check(s):
  """
  """
  try:
    s = float(s)
    return True
  except Exception as e: 
    return False

def float_to_string(s):
  """
  placeholder
  """
  return unicode(str(s))

def float_from_string(s):
  """
  placeholder
  """
  return float(s)

##########################################
# BOOLEAN UTILITIES
##########################################

def boolean_prepare(b):
  """
  prepare a boolean type
  """
  if string_check(b):
    if b in BOOLEAN_TRUE_VALUES:
      return True 
    if b in BOOLEAN_FALSE_VALUES:
      return False 

  if integer_check(b) or float_check(b):
    if b == 0:
      return False 
    if b == 1:
      return True 

  if boolean_check(b):
    return b 

  raise ValueError('Invalid boolean type: {0}'.format(b))

def boolean_check(b):
  """
  check if an object is boolean
  """
  if b in (True, False):
    return True 
  return False 

##########################################
# DATE UTILITIES
##########################################

def date_prepare(ds):
  """
  prepare a date type
  """
  dt = date_from_any(ds)
  if not dt:
    raise ValueError('Invalid date: {0}'.format(ds))
  return dt

def date_check(ds):
  """
  check if an obeject is a date.
  """
  if isinstance(ds, datetime):
    return True

  if date_from_any(ds):
    return True

  return False

def date_now(format='datetime', local=False):
  """
  Current time.
  """
  dt = datetime.utcnow()
  if local:
    dt = date_utc_to_local(dt)
  return {
    'string': date_to_iso,
    'datetime': date_to_datetime,
  }.get(format)(dt)

def date_from_iso(ds, force_tz=True):
  """
  parse an isodate/datetime string with or without
  a datestring.  Convert timzeone aware datestrings
  to UTC.
  """
  try:
   return date_to_datetime(iso8601.parse_date(ds))
  except:
    return None

def date_from_parse(ds):
  """
  dateutil parser
  """
  if not ds or not ds.strip():
    return None
  try:
    return parser.parse(ds)
  except ValueError:
    return None

def date_from_any(ds, **kw):
  """
  Check for isoformat, timestamp, fallback to dateutil.
  """

  if isinstance(ds, datetime):
    return date_to_datetime(ds)

  # check for valid input
  if not ds or not str(ds).strip():
    return

  # run parsers in decreasing order of sensitivity
  if isinstance(ds, datetime):
    return 
  for fn in (date_from_iso, date_from_parse):
    dt = fn(ds, **kw)
    if dt: break

  return date_to_datetime(dt)

def date_to_datetime(dt, tz=None):
  """
  Force a datetime.date into a datetime obj
  """
  return datetime(
    year=dt.year,
    month=dt.month,
    day=dt.day,
    hour=getattr(dt, 'hour', 0),
    minute=getattr(dt, 'minute',  0,),
    second=getattr(dt, 'second', 0),
    tzinfo=getattr(dt, 'tzinfo', pytz.utc)
  )

def date_to_iso(dt):
  """
  date to isoformat string
  """
  return dt.isoformat()

def date_to_ts(dt):
  """
  date to timestamp
  """
  return ts_from_date(dt)

def date_utc_to_local(dt):
  """ 
  conver utc to local
  """
  return dt.replace(tzinfo=pytz.utc).astimezone(get_localzone())

def date_local_to_utc(dt):
  """
  convert local to utc
  """
  return dt.replace(tzinfo=get_localzone()).astimezone(pytz.utc)

##########################################
# TIMESTAMP UTILITIES
##########################################

def ts_prepare(o):
  """

  """
  if ts_check:
    return integer_prepare(o)
  raise Exception('Invalid ts type: {0}'.format(o))

def ts_check(o):
  """

  """
  return (len(str(o)) >= 10 and str(o).startswith('1'))

def ts_now(local=False):
  """

  """
  return ts_from_date(date_now(format='datetime', local=local))

def ts_from_date(dt):
  """

  """
  return int(dt.strftime('%s'))

def ts_to_date(ts):
  """
  Get a datetime object from a utctimestamp.
  """
  return datetime.utcfromtimestamp(float(ts))


##########################################
# DICT UTILITIES 
##########################################

def dict_prepare(d):
  """
  prepare a dict
  """
  try:
    if dict_check(d):
      return d  

    if string_check(d):
      return dict_from_yml(d) # works for yml + json 

    if list_check(d):
      return dict_from_list(d)

  except Exception as e:
    raise ValueError("Invalid dict type: {0}\nTraceback:{1}".format(d, error_tb()))

def dict_check(d):
  """
  check if an object is a dict
  """
  return isinstance(d, collections.MappingType)

def dict_from_json(s):
  """
  
  """
  return json_deserializer(s)

def dict_from_yml(s):
  """
  yml string -> dict
  """
  return yml_deserializer(s)

def dict_from_json_file(p):
  """
  json file to dict
  """
  return dict_from_json(open(p).read())

def dict_from_yml_file(p):
  """
  yml file -> dict
  """
  return yml_deserializer(open(p).read())

def dict_from_string(s):
  """ 
  alias
  """
  return dict_from_yml(s)

def dict_from_list(d):
  """
  must be a list of lists with to items
  """
  if len(d[0]) == 2:
    return dict(d)
  raise ValueError('Invalid list type for dict converstion: {0}'.format(d))

def dict_to_json(d):
  """
  dict > json string
  """
  return json_serializer(d)

def dict_to_yml(d):
  """
  dict -> yml string
  """
  return yml_serializer(d)

def dict_to_string(d):
  """ 
  alias for dict_to_json
  """
  return dict_to_json(d)

def dict_to_list(d):
  """
  convert a dict to a list of key value pairs
  """
  return [[k,v] for k,v in d.iteritems()]

def dict_to_json_file(d, p):
  """
  dict to json file
  """
  with open(p, 'wb') as f:
    f.write(dict_to_json(d))

def dict_to_yml_file(d, p):
  """
  dict > yml file 
  """
  with open(p, 'wb') as f:
    f.write(dict_to_yml(d))

def dict_flatten(d, parent_key='', sep='_'):
  """
  Recursively flatten a dictiory.
  """
  items = []
  for k, v in d.items():
    new_key = parent_key + sep + k if parent_key else k
    if isinstance(v, collections.MutableMapping):
      items.extend(dict_flatten(v, new_key, sep=sep).items())
    else:
      items.append((new_key, v))
  return dict(items)

def dict_update(d, u, overwrite=False):
  """
  Recursively update a nested dictionary.
  From: http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
  """
  for k, v in u.iteritems():
    if isinstance(v, collections.Mapping):
      r = dict_update(d.get(k, {}), v, overwrite)
      d[k] = r
    else:
      # only add it if it doesn't already exist
      if not overwrite:
        if not d.get(k):
          d[k] = u[k]
      else:
        d[k] = u[k]
  return d

##########################################
# LIST UTILITIES
##########################################

def list_prepare(lst, delim=LIST_DELIMITER):
  """
  Prepare a lst.
  """
  if list_check(lst):
    return lst 

  if set_check(lst):
    return set_to_list(lst)

  if string_check(lst):
    return list_to_string(lst, delim=delim)

  raise ValueError('Invalid list type: {0}'.format(lst))

def list_check(lst):
  """
  check if an object is a list
  """
  return isinstance(lst, collections.Iterable)

def _list_determine_delim(str, default=LIST_DELIMITER):
  """
  intelligently determine list delimiter
  """
  pipes = str.count('|')
  commas = str.count(',')
  if pipes > 0 and pipes > comma:
    return '|'
  if commas > 0 and comma > pipes:
    return ','
  return default

def list_from_string(string, delim=LIST_DELIMITER, items=string_prepare):
  """
  list from delimited string
  """
  if not delim:
    delim = _list_determine_delim(string, default=delim)
  return [items(i.strip()) for i in string.split(delim) if i.strip()]

def list_from_json(string):
  """
  list from json
  """
  lst =  dict_from_json(string)
  if list_check(lst):
    return lst 
  raise ValueError('Invalid json list type: {0}'.format(string))

def list_to_string(lst, delim=','):
  """
  list to delimited string
  """
  return delim.join([unicode(i) for i in lst])

def list_to_json(lst):
  """
  list to json
  """
  return dict_to_json(lst)

def list_to_uniq(seq, idfun=lambda x: x):
    """
    Order-preserving unique function.
    """
    # order preserving
    seen = {}
    result = []
    for item in seq:
        marker = idfun(item)
        if marker in seen:
            continue
        seen[marker] = 1
        result.append(item)
    return result

def list_to_chunks(l, n=100):
    """
    Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def list_flatten(lst):
  """
  flatten a list.
  """
  for item in lst:
    if isisntance(item, list):
      for lst2 in list_flatten(item):
        for item2 in lst2:
          yield item2
    else:
      yield item

##########################################
# SET UTILITIES
##########################################

def set_prepare(st):
  """
  Prepare a set type
  """
  if list_check(st):
    return set_from_list(st)

  if string_check(st):
    return set_from_string(st)

  if set_check(st):
    return st 

  raise ValueError('Invalid set type: {0}'.format(st))

def set_check(st):
  """
  check if an object is a set.
  """
  return isinstance(st, (set, frozenset))

def set_from_list(lst):
  """
  set from list
  """
  return set(lst)

def set_from_string(string):
  """
  set from delimted string
  """
  return set_from_list(list_from_string(string))

def set_from_json(st):
  """
  set from json string
  """
  return set_from_list(list_from_json(st))

def set_to_list(st):
  """
  set to list
  """
  return list(st)

def set_to_string(string):
  """
  set to delimited string
  """
  return list_to_string(set_to_list(string))

def set_to_json(st):
  """
  set to json string
  """
  return list_to_json(set_to_list(st))


##########################################
# PATH UTILITIES 
##########################################

def path_prepare(p):
  """
  Prepare a path type.
  """
  p = path_make_abs(p)
  if path_check(p):
    return p 
  raise ValueError('Invalid path type: {0}'.format(p))

def path_check(p):
  """
  Check if a path exists.
  """
  return os.path.exists(path_make_abs(p))

def path_remove(p):
  """
  Check if a path exists.
  """
  return os.remove(p)

def path_make_abs(p):
  """
  make a path absolute
  """
  return os.path.abspath(os.path.expanduser(p))

def path_here(f, *args):
  """
  Get the current directory and absolute path of a file.
  """
  return os.path.abspath(os.path.join(os.path.dirname(f), *args))

def path_list(p):
  """
  Recursively list files under a directory.
  """
  return (os.path.join(dp, f) for dp, dn, fn in
          os.walk(os.path.expanduser(p)) for f in fn)

def path_get_filename(path):
  """
  get the filename from a path.
  """
  return path.split('/')[-1]

def path_get_ext(path):
  """
  get a filepath's extension.
  """
  _, ext = os.path.splitext(path)
  if not ext: return None
  if ext.startswith('.'):
    ext = ext[1:]
  return ext.lower()

def path_get_mimetype(path, lookup={}, default=MIMETYPE_DEFAULT):
  """
  get a filepath's mimetype.
  """
  mime = path_get_mimetype_from_ext(path, lookup, default=None)
  if not mime:
    mime = path_guess_mimetype(path, default=default)
  return mime 

def path_get_mimetype_from_ext(path, lookup={}, default=MIMETYPE_DEFAULT):
  """
  get a filepath's mimetype.
  """
  ext = path_get_ext(path_make_abs(path))
  return lookup.get(ext.lower(), default)

def path_guess_mimetype(path, default=MIMETYPE_DEFAULT):
  """
  guess mimetype from filepath
  """
  parts = mimetypes.guess_type(path_prepare(path))
  if not len(parts): return default 
  return parts[0]

##########################################
# REGEX UTILITIES 
##########################################

def regex_prepare(s):
  """
  
  """
  return re.compile(s)

def regex_check(s):
  """

  """
  if isinstance(s, RE_TYPE):
    return True 
  try:
    re.compile(s)
    return True 
  except:
    return False

##########################################
# SYSTEM UTILITIES # 
##########################################

def sys_get_platform():
  """
  Get the current platform
  """
  if 'linux' in sys.platform.lower():
    return 'linux'
  return 'osx'

def sys_get_env(package='bnpl'):
  """

  """
  d = {}
  prefix = '%s_' % package
  
  # override with env variables
  for key, val in os.environ.iteritems():
    key = key.lower()

    # filter this package's env vars.
    if key.startswith(prefix) or package == 'all':

      # parse key
      key = key.replace(prefix)
      top = key.split('_')[0]
      sub = "_".join(key.split('_')[1:])
      
      # json
      if val.startswith('{'):
        d[top][sub] = json_to_obj(val)
      
      # lists
      elif ',' in val:
        d[top][sub] = [v.strip() for v in val.split(',') if v.strip()]

  return d

def sys_get_config(d=None):
  """
  Load configurations.
  """
  if not d:
    d = path_here(__file__, "config")
  
  # from yaml.
  conf = {}
  for f in (f for f in path_list(d) if f.endswith('yml')):
    conf.update(dict_from_yml_file(f))
  conf.update(sys_get_env())
  conf['platform'] = sys_get_platform()
  return conf

def sys_read_yml():
  """
  Get yml from stdin
  """
  return dict_from_yml(sys.stdin.read())

def sys_read_json():
  """
  """
  return dict_from_json(sys.stdin.read())

def sys_read_jsonl():
  """
  """
  for line in sys.stdin.readlines():
    if not line.strip(): continue
    yield dict_from_json(line)

def sys_write_yml(o):
  """
  put yml to stdout
  """
  sys.stdout.write(dict_to_yml(o))
  return

def sys_write_json(o):
  """
  put json to sdout 
  """
  sys.stdout.write(dict_to_json(o))
  return

def sys_write_jsonl(o):
  """
  put json to stdout
  """
  if list_check(o):
    for oo in o:
      sys.stdout.write(dict_to_json(o) + "\n")
    return
  sys.stdout.write(dict_to_json(o) + "\n")
  return

def sys_exec(cmd):
  """
  Run a shell command.
  """
  class _proc(object):

    def __init__(self, command):
      self.command = command
      self._stdin = None
      self._stdout = None
      self._stdout_text = None
      self._returncode = None

    def set_stdin(self, stdin):
      self._stdin = stdin

    def set_stdout(self, stdout):
      self._stdout = stdout

    @property
    def stdin(self):
      return 'stdin'

    @property
    def stdout(self):
      if self._stdout_text is not None:
        return self._stdout_text

    @property
    def returncode(self):
      if self._returncode is not None:
        return self._returncode

    @property
    def ok(self):
      if self._returncode is not None:
        return self.returncode is 0

    @property
    def subprocess(self):
      if self._subprocess is not None:
        return self._subprocess

    def start(self):
      self._subprocess = subprocess.Popen(
        args=self.command,
        shell=True,
        stdin=self._stdin if self._stdin else subprocess.PIPE,
        stdout=subprocess.PIPE
      )

    def wait(self, unread=False):
      self._returncode = self._subprocess.wait()
      if self._subprocess.stdout is not None and not unread:
        self._stdout_text = self._subprocess.stdout.read().decode()

    def run(self):
      self.start()
      self.wait()

    def __repr__(self):
      return '<Process: {0}>'.format(self.command)
  
  p = _proc(cmd)
  p.run()
  return p

##########################################
# COMMAND LINE UTILITIES
##########################################

def cli_read_options(prog='bnpl'):
  """
  Parses arbitrary command-line args of the format:
  --option1=value1 -option2="value" -option3 value3
  Into a dictionary of:
  {'option1':'value1', 'option2':'value2', 'option3': 'value3'}
  """
  
  # get raw args
  parser = argparse.ArgumentParser(prog=prog)
  opts, arg_strings = parser.parse_known_args()
  opts = {"help": getattr(opts, 'help', False)}
  
  for arg_string in arg_strings:

    # remove '--'
    while True:
      if arg_string.startswith('-'):  arg_string = arg_string[1:]
      else: break
    
    # parse arg string
    parts = arg_string.split("=")
    key = string_to_slug(parts[0])
    value = "=".join(parts[1:])

    # assume this means a boolean flag
    if len(parts) == 1:
      opts[key] = True
      break ;

    # basic parsing
    else:
      m = RE_CLI_ARG.search(value)
      if not m:
        raise ValueError('Invalid command line arg: {0}'.format(arg_string))

      # get value
      value = m.group(0).strip()
      
      # check nulls
      if null_check(value):
        opts[key] = None

      # load data file args
      elif _cli_arg_is_data(value):
        opts[key] = _cli_arg_load_data(value)

      elif value.startswith("{") and value.endswith('}'):
        try:
          opts[key] = dict_from_json(value)
        except:
          pass 

      elif value.startswith("[") and value.endswith(']'):
        try:
          opts[key] = list_from_json(value)
        except:
          pass 
 
  return opts

def cli_read_data():
  """
  alias
  """
  return sys_read_jsonl()


def cli_write_data(output):
  """
  alias
  """
  return sys_write_jsonl(output)

def _cli_arg_is_data(path_or_string):
  """
  determine if we can load a file from a cli arg.
  """
  if not path_or_string: return False
  for fmt in CLI_FILE_FORMATS:
    ext = path_get_ext(path_or_string)
    if path_or_string.lower().endswith(ext):
      return True
  return False

def _cli_arg_load_data(path_or_string):
  """
  Load data in from a filepath or a string
  """
  if not path_or_string:
    return {}
  try:
    return dict_from_yml_file(path_or_string)
  except Exception as e:
    raise IOError(
          "Could not read cli arg file '{}' \n{}"
          .format(fp, e.message))

##########################################
# API UTILITIES
##########################################

def api_read_options():
  """
  Get options from request.
  """
  return dict(request.args.to_dict().items())

def api_read_data():
  """
  Fetch request data from json / form / raw json string.
  """
  data = request.get_json(silent=True)
  if data is None:
    try:
      data = dict_from_json(request.data)
    except:
      data = None

  if data is None:
    data = dict(request.form.items())

  return data

def api_write_data(obj, status=200, headers={}):
  """
  Write data as json resposne
  """
  data = dict_to_json(obj)

  # accept callback
  if 'callback' in request.args:
      cb = request.args.get('callback')
      data = '%s && %s(%s)' % (cb, cb, data)

  return Response(data, headers=headers,
                  status=status,
                  mimetype='application/json')

def api_read_file():
  """
  Download file from request.
  """
  if 'file' not in request.files:
    return None 

  file = request.files['file'] 
  # if user does not select file, browser also
  # submit a empty part without filename
  if not file or file.filename == '':
    raise ValueError('No selected file')
  path = os.path.join('/tmp', secure_filename(file.filename))
  format = path_get_ext(file.filename)
  file.save(path)
  return path 

def api_write_file(path_or_byes, **kwargs):
  """
  Write a file as an api response.
  """
  kwargs.setdefault('as_attachment', True)
  kwargs.setdefault('mimetype', MIMETYPE_DEFAULT)
    
  # existing files
  if path_check(path_or_bytes):
    kwargs['attachment_filename'] = path_get_filename(path_or_bytes)
    kwargs['mimetype'] = path_get_mimetype(path_or_byes)
    return send_file(path_or_bytes, **kwargs)

  file = StringIO()
  file.write(path_or_bytes)
  response = make_response(file.getvalue())
  file.close()
  response.headers['Content-Type'] = kwargs.get('mimetype')
  response.headers['Content-Disposition'] = 'attachment; filename={filename}'.format(**kwargs)
  return response

##########################################
# ERROR UTILITIES
##########################################

def error_tb():
  """

  """
  return format_exc()

##########################################
# EXECUTION UTILITIES
##########################################

def exec_pooled(fn, itr, size=10):
  """
  Pooled execution
  """
  p = Pool(size)
  for resp in p.imap_unordered(fn, itr):
    yield resp

def exec_async(*funcs, **kwargs):
  """
  Execute a list of functions in parallel.
  """ 
  def _exec(f):
    return f()
  return pooled(_exec, funcs, **kwargs)

def exec_retry(*dargs, **dkwargs):
  """
  A decorator for performing http requests and catching all concievable errors.
  Useful for including in scrapers for unreliable webservers.
  @retry(attempts=3)
  def buggy_request():
    return requests.get('http://www.gooooooooooooogle.com')
  buggy_request()
  >>> None
  """
  # set defaults
  attempts = dkwargs.get('attempts',3)
  wait = dkwargs.get('wait', 1)
  backoff = dkwargs.get('backoff', 2)
  verbose = dkwargs.get('verbose', True)
  raise_uncaught_errors = dkwargs.get('raise_uncaught_errors', True)
  null_value = dkwargs.get('null_value', None)

  # wrapper
  def wrapper(f):

    # logger
    log = logging.getLogger(f.__name__)

    @wraps(f)
    def wrapped_func(*args, **kw):

      # defaults
      r = null_value
      tries = 0
      err = True

      # for ref problems
      bckof = backoff * 1
      wait_time = wait * 1

      while 1:

        # if we've exceeded the maximum number of tries,
        # return
        if tries == attempts:
          if verbose:
            log.error('Request to {} Failed after {} tries.'.format(args, tries))
          return r

        # increment tries
        tries += 1

        # calc wait time for this step
        wait_time *= bckof

        # try the function
        try:
          r = f(*args, **kw)
          err = False
          if isinstance(r, requests.Response): 
            r.raise_for_status()
          break

        # catch all exceptions
        except Exception as e:
          err = True
          if verbose:
            logging.warning('Exception - {} on try {} for {}'.format(error_tb(), tries, args))
          if raise_uncaught_errors:
            raise e
          else:
            time.sleep(wait_time)

      # return whatever we have
      return r

    return wrapped_func

  return wrapper

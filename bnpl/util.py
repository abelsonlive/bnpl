"""
self contained utlities
should not reference other modules here
"""
import gevent.monkey
gevent.monkey.patch_socket()
import gevent
from gevent.pool import Pool

import os
import re
import sys
import uuid
import hashlib
import json
import platform
import sha
import collections
import subprocess
from datetime import datetime 

import yaml


def here(f, *args):
  """
  Get the current directory and absolute path of a file.
  """
  return os.path.abspath(os.path.join(os.path.dirname(f), *args))


def platform():
  """
  Get the current platform
  """
  if 'linux' in sys.platform.lower():
    return 'linux'
  return 'osx'

def now(format='string'):
  """
  Current time.
  """
  d = datetime.utcnow()
  if format == 'string': return d.isoformat()
  if format == 'epoch': return int(d.strftime('%s'))
  if format == 'datetime': return d
  raise ValueError("Invalid format: {0}".format(format))

def uid(fingerprint=None, length=10, **kwargs):
  """
  format a uid.
  """
  if not fingerprint:
    fingerprint = unicode(uuid.uuid4())
  return hashlib.sha1(fingerprint.encode("UTF-8")).hexdigest()[:length+1]


def listdir(directory):
  """
  Recursively list files under a directory.
  """
  return (os.path.join(dp, f) for dp, dn, fn in
          os.walk(os.path.expanduser(directory)) for f in fn)


def flatten(d, parent_key='', sep='_'):
  """
  Recursively flatten a dictiory.
  """
  items = []
  for k, v in d.items():
    new_key = parent_key + sep + k if parent_key else k
    if isinstance(v, collections.MutableMapping):
      items.extend(flatten(v, new_key, sep=sep).items())
    else:
      items.append((new_key, v))
  return dict(items)


def pipe_delim(lst):
  """
  pipe-delimited list
  """
  return "|".join([str(i) for i in lst])


def yml_to_obj(s):
  """

  """
  return yaml.safe_load(s)


def yml_file_to_obj(p):
  """
  
  """
  return yml_to_obj(open(p))


def obj_to_yml(o):
  """

  """
  return yaml.safe_dump(o)


def obj_to_yml_file(o, p):
  """

  """
  with open(p, 'wb') as f:
    f.write(obj_to_yml(o))


def json_file_to_obj(p):
  """

  """
  return json_to_obj(open(p))


def json_to_obj(s):
  """
  
  """
  return json.load(s)


def obj_to_json(o):
  """
  
  """
  return json.dumps(o)


def obj_to_json_file(o, p):
  """

  """
  with open(p, 'wb') as f:
    f.write(obj_to_json(o))


def camel_case_to_underscore(name):
  """

  """
  s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
  return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def get_env(package='bnpl'):
  """

  """
  
  d = {}
  prefix = '%s_' % package
  
  # override with env variables
  for key, val in os.environ.iteritems():
    key = key.lower()

    # filter this package's env vars.
    if key.startswith(prefix):

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


def get_config(d=here(__file__, "config")):
  """
  Load configurations.
  """

  # from yaml.
  conf = {}
  for f in (f for f in listdir(d) if f.endswith('yml')):
    conf.update(yml_file_to_obj(f))
  conf.update(get_env())
  conf['platform'] = platform()
  return conf


def pooled(fn, itr, size=10):
  """
  Pooled execution
  """
  p = Pool(size)
  for resp in p.imap_unordered(fn, itr):
    yield resp


def async(*funcs):
  """
  Execute a list of functions in parallel.
  """ 
  greenlets = [gevent.spawn(f) for f in funcs]
  return gevent.joinall(greenlets)


def shell(cmd):
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

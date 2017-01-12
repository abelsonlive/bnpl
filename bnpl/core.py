from datetime import datetime
import json
import csv
import os
from StringIO import StringIO
import tempfile
import copy

import s3plz
from unidecode import unidecode
from elasticsearch import Elasticsearch

from bnpl import util

config = util.sys_get_config(os.getenv('BNPL_CONFIG', util.path_here(__file__, 'config/')))

# configurations mixin.
class Config(object):
  config = config 


# core storage object
class Store(Config):


  def put(self, sound):
    """
    """
    raise NotImplemented

  def get(self, sound):
    """
    """
    raise NotImplemented

  def rm(self, sound):
    """
    """
    raise NotImplemented 

  def bulk(self, sounds):
    """
    """
    raise NotImplemented


class S3FileStore(Store):
  """
  Blob Store works with s3 / local directory
  """
  s3 = s3plz.connect(config['aws']['s3_bucket'], 
                     key=config['aws']['key'], 
                     secret=config['aws']['secret'],
                     serializer=config['bnpl']['file_compression'])

  @util.exec_retry(attempts=3)
  def get(self, sound):
    """
    get sound byes from the blob store.
    """
    return self.s3.get(sound.url)

  @util.exec_retry(attempts=3)
  def put(self, sound):
    """
    put a sound into the blob store
    """
    if not self.exists(sound):
      sound.is_local = False
      with open(sound.path, 'rb') as f:
        self.s3.put(f.read(), sound.url)
      return sound

  @util.exec_retry(attempts=3)
  def rm(self, sound):
    """
    Remove a sound from the blob store
    """
    self.s3.delete(sound.url)


  def exists(self, sound):
    """
    check if a sound exists in the blob store
    """
    return self.s3.exists(sound.url)

  @util.exec_retry(attempts=3)
  def bulk(self, sounds, size=10):

    for sound in sounds:
      self.put(sound)
    # for snd in util.exec_pooled(self.put, sounds):
    #   pass


class ElasticRecordStore(Store):
  """
  
  """
  es = Elasticsearch(config['elastic']['urls'])
  index = config['elastic']['index']
  doc_type = config['elastic']['doc_type']

  def get(self, sound):
    """
    Get a sound from ElasticSearch
    """
    hit = self.es.get(index=self.index, 
                      doc_type=self.doc_type, id=sound.uid)
    return self._sound_from_hit(hit)

  def mget(self, sounds):
    """
    """
    res = self.es.mget(index=self.index, 
                       doc_type=self.doc_type,
                       ids=[sound.uid for sound in sounds])
    return self._sounds_from_res(res)

  def query(self, query):
    """

    """
    res = self.es.search(index=self.index, 
                         doc_type=self.doc_type,
                         body=query)
    return self._sounds_from_res(res)

  @util.exec_retry(attempts=3)
  def put(self, sound):
    """
    Upsert a record
    """
    self.es.index(index=self.index, 
                  doc_type=self.doc_type, 
                  id=sound.uid,
                  body=sound.to_dict())
    return sound

  @util.exec_retry(attempts=3)
  def rm(self, sound):
    """
    Delete a record
    """
    return self.es.delete(index=self.index, 
                          doc_type=self.doc_type, 
                          id=sound.uid)

  @util.exec_retry(attempts=3)
  def exists(self, sound):
    """
    Delete a record
    """
    return self.es.exists(index=self.index, 
                          doc_type=self.doc_type, 
                          id=sound.uid)
  @util.exec_retry(attempts=3)
  def bulk(self, sounds):
    """
    Bulk load sounds
    """
    return self.es.bulk(body="\n".join([self._format_bulk(sound) for sound in sounds]))

  @util.exec_retry(attempts=3)
  def refresh(self):
    """
    Refresh the infex.
    """
    return self.es.indices.refresh(index=self.index)


  def _format_bulk(self, sound):
    """
    Bulk request format
    """
    return '{"update": {"_index":"{0}", "_type": "{1}", "_id": "{2}"}}\n{"doc":{3}}'\
              .format(self.config['elastic']['index'], 
                      self.doc_type,
                      sound.uid,
                      sound.to_json())

  def _sound_from_hit(self, hit):
    """
    Helper
    """ 
    return Sound(**hit.get("_source",{}))

  def _sounds_from_res(self, res):
    """
    Helper
    """
    for hit in res.get('hits', []).get('hits', []):
      yield self._sound_from_hit(hit)


class Sound(Config):
  """
  A sound is initialized by recieving a path and a list of arbitrary parameters.
  on update it can either overwrite existing parameters or only add those 
  which don't yet exist. 

  A sound's unique ID is calculated via fpcalc. This determine whether the sound 
  currently exists. 

  A sound is saved to s3 and postgres.


  """
  # TODO: make these configurable
  file_store = S3FileStore()
  record_store = ElasticRecordStore() 

  def  __init__(self, **properties):
    self.path = properties.pop('path','')
    self.is_local = util.path_check(self.path)
    self.uid = properties.pop('uid', util.string_to_uid(self.path))
    self.created_at = util.date_from_any(properties.pop('created_at', None))
    self.updated_at = util.date_from_any(properties.pop('updated_at', None))
    self.format = properties.pop('format', util.path_get_ext(self.path))
    self.mimetype = properties.pop('mimetype', util.path_get_mimetype_from_ext(self.path))
    self._set_properties(properties)

  def _set_properties(self, properties):
    """
    set / update properties
    """
    _properties = properties.pop('properties', {})
    self.properties = properties
    self.properties.update(_properties)

  @property
  def slug(self):
    try:
      assert('format' in self.properties)
    except:
      ValueError('A sound needs a format to generate a filename.')

    # create format string
    frmt = ""
    for k in self.config['bnpl']['file_path_keys']:
      v = getattr(self, k, self.properties.get(k, None))
      if v:
        frmt += str(v) + self.config['bnpl']['file_path_delim']
    f = frmt[:-1].strip()
    if not f:
      path = ".".join(util.path_get_filename(self.path).split('.')[:-1])
      f = util.string_to_slug(unidecode(path).lower())
    return f

  @property 
  def filename(self):
    """
    generate a filename for a sound
    """
    # blah

    fn = self.slug + "." + self.format + '.' +  self.config['bnpl'].get('file_compression', '')

    # handle no compression
    if fn.endswith('.'):
      fn = fn[:-1]

    return fn

  @property
  def tempfilename(self):
    """

    """
    d = tempfile.mkdtemp(suffix='bnpl')
    fn = os.path.join(d, self.filename)\
                .replace("." + self.config['bnpl']['file_compression'], '')
    return fn 

  @property
  def url(self):

    """
    ugly:
    this is designed to 
    break if certain things arent present
    """
    if self.uid is None:
      raise ValueError('You must include a uid when saving a sound')
    return "{0}/{1}/sound.{2}".format(self.config['bnpl']['file_dir'], self.uid, self.format)

  def to_dict(self):
    """

    """
    return {
      "uid": self.uid,
      "path": self.path,
      "format": self.format,
      "mimetype": self.mimetype,
      "slug": self.slug,
      "filename": self.filename,
      "url": self.url,
      "is_local": self.is_local,
      "created_at": self.created_at, 
      "updated_at": self.updated_at, 
      "properties": self.properties
    }

  def to_flat_dict(self):
    """

    """
    d = self.to_dict()
    p = d.pop('properties', {})
    d.update(p)
    return util.dict_flatten(d)

  def to_json(self):
    """
    Sound as json.
    """
    return util.dict_to_json(self.to_dict())

  def to_yml(self):
    """
    Sound as yml.
    """
    return util.dict_to_yml(self.to_dict())

  # local file storage

  def path_get(self):
    """
  
    """
    if self.path_exists():
      return open(self.path).read()

  def path_rm(self):
    """
    remove local file
    """
    if self.path_exists():
      return os.path.remove(self.path)

  def path_exists(self):
    """
    local file exists?
    """
    return os.path.exists(self.path)

  def file_get(self):
    """
    Get a file.
    """
    return self.file_store.get(self)

  def file_put(self):
    """
    Create/Replace a file 
    """
    self.created_at = util.date_now()
    self.updated_at = util.date_now()
    self.file_store.put(self)
    return self

  def file_mv(self):
    """
    Create/Replace a file in the file store 
    """
    if self.path_exists():
      sound = self.file_put()
      sound.is_local = False
      self.path_rm()
      return sound

  def file_rm(self):
    """
    delete a file.
    """
    self.file_store.rm(self)
    return self

  def file_dl(self):
    """
    Download a file 
    """
    fp = copy.copy(self.tempfilename)
    with open(fp, 'wb') as f:
      f.write(self.file_store.get(self))
    self.path = fp
    return self

  def file_exists(self):
    """
    Check if a file exists.
    """
    return self.file_store.exists(self)

  # record storage

  def record_get(self):
    """
    Get a record.
    """
    return self.record_store.get(self)

  def record_put(self):
    """
    Create/Replace a record 
    """
    return self.record_store.put(self)

  def record_rm(self):
    """
    Delete a record.
    """
    return self.record_store.rm(self)

  def record_exists(self):
    """
    Check if a record exists.
    """
    return self.file_store.exists(self)

  # sound storage 

  def get(self):
    """
    Shortcut for record_get
    """
    return self.record_get()

  def exists(self):
    """
    Shortcut for record_exists 
    """
    return self.record_exists()

  def put(self):
    """
    Save file + record. remove path.
    """
    if not self.exists():
      self.created_at = util.date_now()
    self.updated_at = util.date_now()
    self.file_put()
    self.record_put()
    return self

  def rm(self):
    """
    Remove file + record
    """
    self.file_rm()
    self.record_rm()


# core storage object
class Store(Config):


  def put(self, sound):
    """

    """
    raise NotImplemented

  def get(self, sound):
    """

    """
    raise NotImplemented

  def rm(self, sound):
    """

    """
    raise NotImplemented 

  def bulk(self, sounds):
    """

    """
    raise NotImplemented



class Option(Config): 
  """
  Options for Plugins
  """
  parsers = {
    'null': util.null_prepare,
    'string': util.string_prepare,
    'boolean': util.boolean_prepare,
    'integer': util.integer_prepare,
    'float': util.integer_prepare,
    'date': util.date_prepare,
    'ts': util.ts_prepare,
    'dict': util.dict_prepare,
    'list': util.list_prepare,
    'set': util.set_prepare,
    'path': util.path_prepare,
    'regex': util.regex_prepare
  }
  def __init__(self, name, **kwargs):
    self.name = name
    self.type = kwargs.get('type', 'string')
    self.default = kwargs.get('default', None)
    self.required = kwargs.get('required', False)
    self.help = kwargs.get('help', '')
    self.value = None

  def prepare(self, val=None):
    """
    Prepare an option.
    """
    if not val and self.default:
      self.value = copy.copy(self.default)
    else:
      self.value = val 
    self.value = self.parsers.get(self.type)(self.value)
    if not self.value and self.required:
      raise ValueError('Missing required option: {0}'.format(self.name))
    return self.value

  def to_dict(self):
    """
    Render an option as a dictionary.
    """
    return {
      'name': self.name,
      'type': self.type,
      'default': self.default,
      'required': self.required
    }

  def to_json(self):
    """
    Render an option as json.
    """
    return util.dict_to_tml(self.to_dict())

  def to_yml(self):
    """
    Render an option as yml.
    """
    return util.dict_to_yml(self.to_dict())


class OptionSet(Config):
  """

  """
  defaults = [
    Option("help", type="boolean", default=False)
  ]

  def __init__(self, *opts, **kwargs):
    self._parsers = {}
    self._options = {}
    self._required = []
    opts = [o for o in opts]
    opts.extend(self.defaults)

    for opt in opts:
      self._parsers[opt.name] = opt
      if opt.required and not opt.default:
        self._required.append(opt.name)

  def prepare(self, **raw):
    """
    Prepare a list of key/value options
    """
    for name, opt in self._parsers.iteritems():
      value = opt.prepare(raw.get(name, opt.default))
      self._options[name] = value
      setattr(self, name, value)

    # check required 
    for name in self._required:
      if name not in self._options:
        raise ValueError("Missing required option: {0}".format(name))

  def to_dict(self):
    """
    """
    return self._options

  def to_json(self):
    """

    """
    return util.dict_to_json(self.to_dict())

  def to_yml(self):
    """

    """
    return util.dict_to_yml(self.to_dict())

  def __getitem__(self, item):
    """
    """
    return self.to_dict().get(item)


class Plugin(Config):
  
  options = OptionSet(Option("help", type="boolean", default=False))

  def __init__(self, **options):
    """

    """
    self.load_options(**options)

  def run(self, *args, **kwargs):
    """
    """

  def load_options(self, **options):
    """

    """
    self.options.prepare(**options)

  def load_cli_options(self):
    """
    load options from cli
    """
    self.load_options(**util.cli_read_options())

  def load_api_options(self):
    """
    load options from api request 
    """
    self.load_options(**util.api_read_options())


class Exporter(Plugin):

  """
  accepts parameters and one or more sounds 
  and returns a link to a file. 
  """

  def export(self, sounds):
    raise NotImplemented


class Extractor(Plugin):
  """
  accepts parameters and 
  returns one or sounds
  """
  def extract(self):
    raise NotImplemented


class Transformer(Plugin):
  """
  accepts a sound + parameters and 
  returns a modified sound or one or more new sounds
  """


  def transform(self, sound):
    raise NotImplemented






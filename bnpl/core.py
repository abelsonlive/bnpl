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
class ConfigMixin(object):
  """
  """
  config = config 


# core storage object
class Store(ConfigMixin):

  def list(self, **kwargs):
    """

    """
    raise NotImplemented

  def put(self, sound, **kwargs):
    """

    """
    raise NotImplemented

  def get(self, sound, **kwargs):
    """

    """
    raise NotImplemented

  def rm(self, sound, **kwargs):
    """

    """
    raise NotImplemented 

  def bulk(self, sounds, **kwargs):
    """

    """
    raise NotImplemented

  def exists(self, **kwargs):
    """

    """
    raise NotImplemented


class S3Store(Store):
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
  def put(self, sound, _ret=True):
    """
    put a sound into the blob store
    """
    if not self.exists(sound):
      with open(sound.path, 'rb') as f:
        self.s3.put(f.read(), sound.url)
      if _ret: return sound

  @util.exec_retry(attempts=3, wait=0.25, backoff=1.1)
  def rm(self, sound):
    """
    Remove a sound from the blob store
    """
    self.s3.delete(sound.url)
    return sound

  @util.exec_retry(attempts=3, wait=0.25, backoff=1.1)
  def exists(self, sound):
    """
    check if a sound exists in the blob store
    """
    return self.s3.exists(sound.url)

  @util.exec_retry(attempts=2, wait=5, backoff=2)
  def bulk(self, sounds, size=10):
    """
    Bulk uploader via gevent.
    """
    return list(util.exec_pooled(self.run, sounds, _ret=False))


class ElasticStore(Store):
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


class Sound(ConfigMixin):
  """
  A sound is initialized by recieving a path and a list of arbitrary parameters.
  on update it can either overwrite existing parameters or only add those 
  which don't yet exist. 

  A sound's unique ID is calculated via fpcalc. This determine whether the sound 
  currently exists. 

  A sound is saved to s3 and elasticsearch 
  """

  # TODO: make these configurable
  fs = S3Store()
  db = ElasticStore() 

  def  __init__(self, **properties):
    self.path = properties.pop('path','')
    self.uid = properties.pop('uid', util.string_to_uid(self.path))
    self.created_at = util.date_from_any(properties.pop('created_at', None))
    self.updated_at = util.date_from_any(properties.pop('updated_at', None))
    self.ext = properties.pop('format', util.path_get_ext(self.path))
    self.mime = properties.pop('mimetype', self._get_mimetype(self.path))
    self._set_properties(properties)

  def _get_mimetype(self, path):
    """
    """
    return util.path_get_mimetype(path, 
                                  lookup=self.config['mimetypes']['lookup'])

  def _set_properties(self, properties):
    """
    set / update properties
    """
    _properties = properties.pop('properties', {})
    self.properties = properties
    self.properties.update(_properties)

  def _format_slug_key(self, k):
    """
    """
    v = str(getattr(self, k, self.properties.get(k, None)))
    if not v:
      return ""
    return "{0}{slug_delim}".format(v, **self.config['bnpl'])

  @property
  def slug(self):
    """
    """
    # create format string
    slug = "".join(map(self._format_slug_key, self.config['bnpl']['slug_keys'])).strip()
    if not slug:
      return util.string_to_slug(util.path_get_filename(self.path, ext=False))
    return slug[:-1]

  @property 
  def filename(self):
    """
    generate a filename for a sound
    """
    fn = "{}.{}.{}".format(self.slug, self.ext, self.config['bnpl'].get('file_compression', ''))

    # handle no compression
    if fn.endswith('.'):
      fn = fn[:-1]

    return fn

  @property
  def tempfilename(self):
    """

    """
    return "{tmp_dir}/{0}-{1}".format(util.ts_now(), self.filename, **self.config['bnpl'])

  @property
  def url(self):

    """
    ugly:
    this is designed to 
    break if certain things arent present
    """
    if self.uid is None:
      raise ValueError('You must include a uid when saving a sound')
    return "{0}/{1}/sound.{2}".format(self.config['bnpl']['file_dir'], self.uid, self.ext)

  @property
  def is_local(self):
    """
    """
    return util.path_check(self.path)

  def has_property(self, k):
    """
    """
    return hasattr(self, k) or k in self.properties

  def to_dict(self):
    """

    """
    return {
      "uid": self.uid,
      "path": self.path,
      "ext": self.ext,
      "mimetype": self.mimetype,
      "slug": self.slug,
      "filename": self.filename,
      "url": self.url,
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
    return self.is_local

  def fs_get(self):
    """
    Get a file.
    """
    return self.fs.get(self)

  def fs_rm(self):
    """
    delete a file.
    """
    self.fs.rm(self)
    return self

  def fs_exists(self):
    """
    Check if a file exists.
    """
    return self.fs.exists(self)

  def fs_put(self, ):
    """
    Create/Replace a file 
    """
    self.fs.put(self)
    return self

  def fs_mv(self):
    """
    Create/Replace a file in the file store 
    """
    if os.path.exists(self.path):
      self.fs_put()
      self.is_local = False
      os.path.remove(self.path)
      return self

  def fs_dl(self, to=self.tempfilename):
    """
    Download a file 
    """
    with open(to, 'wb') as f:
      f.write(self.fs.get(self))
    self.path = to
    return self

  # record storage

  def db_get(self):
    """
    Get a record.
    """
    return self.db.get(self)

  def db_put(self):
    """
    Create/Replace a record 
    """
    self.db.put(self)
    return self

  def db_rm(self):
    """
    Delete a record.
    """
    self.db.rm(self)
    return self

  def db_exists(self):
    """
    Check if a record exists.
    """
    return self.fs.exists(self)

  # sound storage 

  def read(self):
    """
    shortcut of fs_get / pa
    """
    if self.is_local:
      return self.path_read()
    return self.fs_read()

  def get(self):
    """
    Shortcut for db_get
    """
    return self.db_get()

  def rm(self):
    """
    Remove file + record
    """
    self.file_rm()
    self.db_rm()
    return self

  def exists(self):
    """
    Shortcut for db_exists 
    """
    return self.db_exists()

  def put(self):
    """
    Save file + record. remove path.
    """
    if not self.exists():
      self.created_at = util.date_now()
    self.updated_at = util.date_now()
    util.exec_async([self.file_put, self.db_put])
    return self

#########################################
# type utilies 
########################################
class Type(object):

  _types = OrdererdDict(
    'null': [util.null_prepare, util.null_check],
    'boolean': [util.boolean_prepare, util.boolean_check],
    'date': [util.date_prepare, util.date_check],
    'ts': [util.ts_prepare, util.ts_check],
    'integer': [util.integer_prepare, util.integer_check],
    'float': [util.float_prepare, util.float_check],
    'dict': [util.dict_prepare, util.dict_check],
    'list': [util.list_prepare, util.list_check],
    'set': [util.set_prepare, util.set_check],
    'path': [util.path_prepare, util.path_check],
    'regex': [util.regex_prepare, util.regex_check],
    'string': [util.string_prepare, util.string_check]
  )

  def __init__(self, type="string"):
    self.type = type
    self.types = self._types.keys()

  @classmethod
  def prepare(cls, value, type=None):
    """
    """
    func = cls._types[type or cls.type][0]
    if isinstance(value, types.I terable):
      return map(func, value)
    return func(value)  

  @classmethod
  def check(cls, value, type=None):
    """
    """ 
    func = cls._types[type or cls.type][1]
    return func(value)

  @classmethod
  def sniff(cls, value, types=[]):
    """
    """
    if not len(types):
      types = []

    for t in types:
      if cls.check(value, t):
        try:
          return cls.prepare(value, t)
        except:
          pass 
    return cls._prepare(value, "string")


class Option(ConfigMixin): 
  """
  Options for Plugins
  """

  def __init__(self, name, **kwargs):
    """
    """
    self.name = util.string_to_slug(name, delim="_") # fight me.
    self.alias = kwargs.get('alias', None)
    self.type = kwargs.get('type', 'string')
    self.items = kwargs.get('items', None)
    self.default = kwargs.get('default', None)
    self.required = kwargs.get('required', False)
    self.description = kwargs.get('description', kwargs.get('help', ''))
    self.value = None
    self.parser = Type(self.type)

  def prepare(self, val=None):
    """
    Prepare an option.
    """
    if not val and self.default:
      self.value = copy.copy(self.default)
    else:
      self.value = val 
    try:
      self.value = self.parser.prepare(self.value)
    except:
      raise ValueError('Invalid {0} type: {1}'.format(self.type, val))
    if not self.value and self.required:
      raise ValueError('Missing required option: {0}'.format(self.name))
    if self.type == "list" and self.items:
      self.value = map(self.value, Type(self.items).prepare)
    return self.value

  def to_dict(self):
    """
    Render an option as a dictionary.
    """
    d = {
      'name': self.name,
      'description': self.description,
      'type': self.type,
      'default': self.default,
      'required': self.required
    }
    if self.value:
      d['value'] = self.value 
    if self.alias:
      d['alias'] = self.alias
    if self.items:
      d['items'] = self.items
    return d

  def describe(self):
    """
    """
    return self.to_dict()

  def to_json(self):
    """
    Render an option as json.
    """
    return util.dict_to_json(self.to_dict())

  def to_yml(self):
    """
    Render an option as yml.
    """
    return util.dict_to_yml(self.to_dict())


class OptionSet(ConfigMixin):
  """

  """
  defaults = [
    Option("help", type="boolean", default=False)
  ]

  def __init__(self, *opts, **kwargs):
    self._options = opts
    self._parsers = {}
    self._parsed = {}
    self._aliases = {}
    self._required = []
    self._errors = []
    self._setup()

  def prepare(self, **raw):
    """
    Prepare a list of key/value options
    """
    for name, value in self._map_aliases(**raw).iteritems():
     
      else:
        try:
          self._set_option(name, value)
        except Exception as e:
          self._errors.append("Invalid option '{0}': {1}.".format(e.message))
    self._check_errors()
    
  def describe(self):
    """
    describe an OptionSet
    """
    return [o.describe() for o in self._options]

  def to_dict(self):
    """
    Format options as a dictionary
    """
    d = {}
    for o in self._opts:
      v = o.describe()
      v.pop('slug', None)
      d[o.slug] = v
    return d

  def to_yml(self):
    """
    Format options as json.
    """
    return util.dict_to_yml(self.to_dict())

  def to_json(self):
    """
    Render an option as json.
    """
    return util.dict_to_json(self.to_dict())

  def __getitem__(self, item):
    """
    """
    return self.to_dict().get(item)

  def _setup(self):
    """
    setup parsers + required options
    """
    self._options.extend(self.defaults)
    map(self._setup_option, self._options)

  def _setup_option(self, opt):
    """
    """
    # required
    if opt.required and not opt.default:
      self._required.append(opt.name)

    # aliases 
    if opt.alias:
      self._aliases[opt.alias] = opt.name

  def _map_aliases(self, **raw):
    """
    map inputted aliases to full names
    """
    for k,v in raw.iteritems():
      if k in self._aliases:
        raw[self._aliases[k]] = v 
        del raw[k]
    return raw

  def _set_option(self, name, value):
    """
    """
    value = self._parsers.get(name, Type.sniff)(value)
    self._parsed[name] = value
    setattr(self, name, value)

  def _check_required(self):
    """
    check required options
    """
    missing = [name if name not in self._parsed for name in self._required]
    n_missing = len(missing)
    if n_missing > 0:
      return "Missing required option{0}: {1}"\
                .format("s" if n_missing > 1 else "".  ", ".join(missing))

  def _check_errors(self):
    """
    check for errors.
    """
    # check required
    err = self._check_required()
    if err: 
      self._errors.append(err)

    if len(self._errors):
      message = "Invalid Option Set!\nErrors:\n{0}".format("\n".join(self._errors))
      raise ValueError(message)


class Plugin(ConfigMixin):
  """
  Plugin description placeholder.
  """
  type = "core"
  options = OptionSet(
    Option("help", type="boolean", default=False, help="placeholder")
  )

  def __init__(self, data=[], _context="python", **options):
    """

    """
    self._load_context(_context)
    self._load_options(**options)
    self._load_data(data)
    self._load_file()

  def _load_context(self, ctxt):
    """
    """
    if ctxt not in ['python', 'cli', 'api']:
      raise ValueError('Invalid plugin context: {}'.format(ctxt))
    self._context = ctxt

  def _load_options(self, **options):
    """

    """
    self.options.prepare(**options)
    if self._context == 'api':
      self.options.update(util.api_read_options())
    elif self._context == 'cli':
      self.options.update(util.cli_read_options())

  def _load_data(self, data):
    """

    """
    self.data = data
    if self._context == 'api':
      self.data = util.api_read_data()
    elif self._context == 'cli':
      self.data = util.cli_read_data()

  def _load_file(self):
    """
    
    """
    self.file = None 
    if self._context = 'api':
      self.file = util.api_read_file()

  @property
  def name(self):
    """

    """
    return self.__class__.__name__

  @property
  def slug(self):
    """
    """
    return util.string_camel_case_to_slug(self.name)

  @property
  def to_dict(self):
    """

    """
   return {
      'name': self.name,
      'slug': self.slug,
      'description': self.__doc__,
      'type': self.type,
      'options': self.options.describe()
    }

  def run(self, *args, **kwargs):
    """
    """
    raise NotImplemented

  def exec(self, *args, **kwargs):
    """
    For internal usage.
    """
    return self.run() 


class Extractor(Plugin):
  """
  accepts parameters and 
  returns one or sounds
  """
  type = 'extractor'


class Transformer(Plugin):
  """
  accepts a sound + parameters and 
  returns a modified sound or one or more new sounds
  """
  type = 'transformer'

class Exporter(Plugin):

  """
  accepts parameters and one or more sounds 
  and returns a link to a file. 
  """
  type = 'exporter'

class Importer(Plugin):

  """
  stores one or more sounds.
  default behavior can be overridden by subclassing 
  this object.
  exists for consistency + coherence of plugin types 
  within the pipeline context. 
  """

  type = 'importer'
  options = OptionSet(
    Option('pool_size', type="integer", default=10)
  )

  def run(self):
    """
    """
    if not util.check_list(self.data, strict=True):
      self.data = [self.data]
    return util.exec_pooled(self._put_sound, self.data, size=self.options['pool_size'])
  
  def _put_sound(self, sound):
    """
    """
    if not isinstance(sound, Sound):
      sound = Sound(**sound)
    return sound.put()


class Pipeline(Plugin):

  type = 'pipeline'

  options = OptionSet(
    Option('extractors', type="dict", default={'extractor':{}})
    Option('transformers', type="dict", default={})
    Option('importers', type="dict", default={})
    Option('exporters', type="dict", default={})
  )

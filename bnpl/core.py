from datetime import datetime
import json
import csv
import os
from StringIO import StringIO
import tempfile
import copy

import s3plz
from unidecode import unidecode
from slugify import slugify
from elasticsearch import Elasticsearch


from bnpl.util import here, get_config, flatten, async, pooled, uid, obj_to_json, now

config = get_config(os.getenv('BNPL_CONFIG', here(__file__, 'config/')))

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
  s3 = s3plz.connect(config['s3']['bucket'], 
                     key=config['s3']['key'], 
                     secret=config['s3']['secret'],
                     serializer=config['bnpl']['file_compression'])

  def get(self, sound):
    """
    get sound byes from the blob store.
    """
    return self.s3.get(sound.url)

  def put(self, sound):
    """
    put a sound into the blob store
    """
    if not self.exists(sound):
      with open(sound.path, 'rb') as f:
        return self.s3.put(f.read(), sound.url)

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

  def bulk(self, sounds, size=10):

    for sound in sounds:
      self.put(sound)
    # for snd in pooled(self.put, sounds):
    #   pass


class ElasticRecordStore(Store):
  """
  
  """
  es = Elasticsearch()
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

  def put(self, sound):
    """
    Upsert a record
    """
    if not self.exists(sound):
      sound.created_at = now()
      sound.updated_at = now()
    self.es.index(index=self.index, 
                  doc_type=self.doc_type, 
                  id=sound.uid,
                  body=sound.to_dict())

  def rm(self, sound):
    """
    Delete a record
    """
    return self.es.delete(index=self.index, 
                          doc_type=self.doc_type, 
                          id=sound.uid)

  def exists(self, sound):
    """
    Delete a record
    """
    return self.es.exists(index=self.index, 
                          doc_type=self.doc_type, 
                          id=sound.uid)

  def bulk(self, sounds):
    """
    Bulk load sounds
    """
    return self.es.bulk(body="\n".join([self._format_bulk(sound) for sound in sounds]))

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
    self.uid = properties.pop('uid', uid(self.path))
    self.is_local = not self.path.startswith('s3://')
    self.created_at = properties.pop('created_at', None) 
    self.updated_at = properties.pop('updated_at', None) 
    self._set_properties(properties)

  def _set_properties(self, properties):
    """
    set / update properties
    """
    _properties = properties.pop('properties', {})
    self.properties = properties
    self.properties.update(_properties)

  @property 
  def filename(self):
    """
    generate a filename for a sound
    """
    # blah
    try:
      assert('format' in self.properties)
    except:
      ValueError('A sound needs a format to generate a filename.')

    # create format string
    frmt = ""
    for k in self.config['bnpl']['file_path_keys']:
      v = getattr(self, k, self.properties.get(k, None))
      if v:
        frmt += "{0}{1}".format(slugify(str(v).lower()), self.config['bnpl']['file_path_delim'])
    f = frmt[:-1].strip()
    if not f:
      f = slugify(".".join(self.path.split('/')[-1].split('.')[:-1])).lower()
    fn = f + "." + self.properties['format'] + '.' +  self.config['bnpl']['file_compression']

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
    return "{0}/{1}/{2}".format(self.config['bnpl']['file_dir'], self.uid, self.filename)

  def to_dict(self):
    """

    """
    return {
      "uid": self.uid,
      "path": self.path,
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
    return flatten(d)

  def to_json(self):
    """
    Sound as json.
    """
    return obj_to_json(self.to_dict())


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
    if self.path_exists():
      return self.file_store.put(self)

  def file_mv(self):
    """
    Create/Replace a file in the file store 
    """
    if self.path_exists():
      self.file_put()
      self.path_rm()

  def file_rm(self):
    """
    delete a file.
    """
    return self.file_store.rm(self)

  def file_dl(self):
    """
    Download a file 
    """

    with open(self.tempfilename, 'wb') as f:
      f.write(self.file_store.get(self))
    self.path = copy.copy(self.tempfilename)
    return self.path

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
    self.file_put()
    return self.record_put()
    # return parallel([self.file_put, self.record_put]) 

  def rm(self):
    """
    Remove file + record
    """
    self.file_rm()
    return self.record_rm()
    # return parallel([self.file_rm, self.record_rm])


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
  def __init__(self, name, **kwargs):
    self.name = name
    self.type = kwargs.get('type', 'string')
    self.default = kwargs.get('default', None)


class Plugin(Config):
  
  # TODO:
  def __init__(self, **params):
    self.params = params 


class Exporter(Plugin):

  """
  accepts parameters and one or more sounds 
  and returns a link to a file. 
  """

  def export(self, sounds):
    raise NotImplementede


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






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

Config = util.sys_get_config(os.getenv('BNPL_CONFIG', util.path_here(__file__, 'config/')))

# configurations mixin.
class ConfigMixin(object):
  """
  """
  config = Config


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
  s3 = s3plz.connect(Config['aws']['s3_bucket'], 
                     key=Config['aws']['key'], 
                     secret=Config['aws']['secret'],
                     serializer=Config['bnpl']['file_compression'])

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
  es = Elasticsearch(Config['elastic']['urls'])
  index = Config['elastic']['index']
  doc_type = Config['elastic']['doc_type']

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
    self.created_at = util.date_from_any(properties.pop('created_at', None))
    self.updated_at = util.date_from_any(properties.pop('updated_at', None))
    self.ext = properties.pop('ext', util.path_get_ext(self.path))
    self.uid = properties.pop('uid', util.string_to_uid(self.path)) + "." + self.ext
    self.mimetype = properties.pop('mimetype', self._get_mimetype(self.path))
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
    v = getattr(self, k, self.properties.get(k, None))
    if not v: return ""
    return "{0}{slug_delim}".format(util.string_to_slug(str(v)), **self.config['bnpl'])

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
    return "{0}/uid={1}/ext={2}/{1}.{2}".format(self.config['bnpl']['file_dir'], self.uid, self.ext)

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

  def fs_dl(self, to=None):
    """
    Download a file 
    """
    self.path = to or self.tempfilename
    with open(self.path, 'wb') as f:
      f.write(self.fs.get(self))
    return self 

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
    self.fs_rm()
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
    util.exec_async([self.fs_put, self.db_put])
    return self

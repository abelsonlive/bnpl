import os

from bnpl.util import listdir
from bnpl.core import Extractor
from bnpl.core import Sound

class Directory(Extractor):

  def extract(self):
    
    assert('path' in self.params)
    self.params.setdefault('formats', ['mp3', 'wav', 'aif', 'aiff', 'm4a', 'flac'])

    for f in listdir(self.params['path']):
      for fmt in self.params['formats']:
        if f.endswith(fmt):
          yield Sound(format=fmt, is_local= True, path=f)

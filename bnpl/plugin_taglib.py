import taglib

from bnpl.core import Transformer


class GetTags(Transformer):
  
  EXTRACT = [
    'ARTIST',
    'ALBUM',
    'TITLE',
    'GENRE',
    'TRACKNUMBER',
    'DATE'
  ]
  def transform(self, sound):

    sound.properties.update({
      key.lower():values[0] 
      for key, values in taglib.File(sound.path).tags.iteritems() 
      if key.upper() in self.EXTRACT
    })
    return sound
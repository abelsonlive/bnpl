import taglib

from bnpl.core import Option, OptionSet
from bnpl.core import Transformer


class GetTags(Transformer):
  
  options = OptionSet(
    Option('tags', type='list',
           default=['artist', 'album', 'title', 'genre', 'tracknumber', 'date'])
  )

  def run(self, sound):
    """
    """
    sound.properties.update({
      key.lower():values[0] 
      for key, values in taglib.File(sound.path).tags.iteritems() 
      if key.lower() in self.options['tags']
    })
    return sound
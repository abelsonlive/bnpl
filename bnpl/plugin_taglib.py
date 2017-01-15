import taglib

from bnpl import Option, OptionSet
from bnpl import Transformer


class GetTags(Transformer):
  """
  Set tags on a sound using taglib.
  """
  options = OptionSet(
    Option('tags', type='list', items='string',
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
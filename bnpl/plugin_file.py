import os

from bnpl import util
from bnpl import Option, OptionSet 
from bnpl import Extractor
from bnpl import Sound

class Directory(Extractor):
  """
  Extract sounds from a local directory.
  """
  options = OptionSet(
    Option('path', type='path', required=True),
    Option('formats', alias="f", type='list', items="string", 
           default=['mp3', 'wav', 'aif', 'aiff', 'm4a', 'flac'])
  )

  def run(self):
    """
    """    
    for f in util.path_list(self.options.path):
      ext = util.path_get_ext(f)
      if  ext in self.options.formats:
        yield Sound(path=f, format=ext, is_local=True)
from pyItunes import Library

from bnpl.core import Option, OptionSet
from bnpl.core import Extractor


class ItunesSongs(Extractor):
  """
  """
  options = OptionSet(
    Option('library_xml', type='path', required=True)
  )
  def run(self):
    """
    """
    l = Library(self.options['library_xml'])
    for id, song in l.songs.items():
      yield song
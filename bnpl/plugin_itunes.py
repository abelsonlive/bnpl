from pyItunes import Library

from bnpl import Option, OptionSet
from bnpl import Extractor


class ItunesSongs(Extractor):
  """
  Extract sounds from your Itunes Library.
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
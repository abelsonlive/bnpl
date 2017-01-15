from bnpl import util
from bnpl import Config, Importer
from bnpl import plugin_file as file
from bnpl import plugin_fpcalc as fpcalc 
from bnpl import plugin_taglib as taglib 
from bnpl import plugin_essentia as essentia

# extract sounds from a directory
directory = file.Directory(path=util.path_here(__file__, 'fixtures'), 
                           formats=['mp3', 'wav', 'aif', 'aiff', 'm4a', 'flac'])

# run UID transform
sounds = fpcalc.UID(directory.do()).do()
sounds = taglib.GetTags(sounds).do()

if Config['platform'] == 'linux':
  sounds = essentia.FreeSound(sounds).do()

# store sounds
for sound in Importer(sounds).do():
  print sound.to_json()


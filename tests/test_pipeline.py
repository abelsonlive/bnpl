from bnpl import util
from bnpl import Config, Importer
from bnpl import plugin_file as file
from bnpl import plugin_fpcalc as fpcalc 
from bnpl import plugin_taglib as taglib 
from bnpl import plugin_essentia as essentia

# extract sounds from a directory
directory = file.Directory(path=util.path_here(__file__, 'fixtures'), 
                           formats=['mp3', 'wav', 'aif', 'aiff', 'm4a', 'flac'])
sounds = directory.run()

# run UID transform
fpcalc = fpcalc.UID()
sounds = util.exec_pooled(fpcalc.run, sounds)

# get tags
tag = taglib.GetTags()
sounds = util.exec_pooled(tag.run, sounds)

# get bpm/key
if Config['platform'] == 'linux':
  sounds = util.exec_pooled(essentia.FreeSound().run, sounds)

# store sounds
imp = Importer()
for sound in imp.run(sounds):
  print sound.to_json()


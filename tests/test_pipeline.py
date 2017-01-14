from bnpl import util
from bnpl.core import config, Importer
from bnpl import plugin_file as file
from bnpl import plugin_fpcalc as fpcalc 
from bnpl import plugin_taglib as taglib 
from bnpl import plugin_essentia as essentia

# extract sounds from a directory
directory = file.Directory(path=util.path_here(__file__, 'fixtures'), 
                           formats=['mp3', 'wav', 'aif', 'aiff', 'm4a', 'flac'])
print directory.describe()
sounds = directory.run()

# run UID transform
fpcalc = fpcalc.UID()
print fpcalc.describe()
sounds = util.exec_pooled(fpcalc.run, sounds)

# get tags
tag = taglib.GetTags()
print tag.describe()
sounds = util.exec_pooled(tag.run, sounds)

# get bpm/key
if config['platform'] == 'linux':
  sounds = util.exec_pooled(essentia.FreeSound().run, sounds)

# store sounds
imp = Importer()
print imp.describe()
for sound in imp.run(sounds):
  print sound.url


from bnpl import util
from bnpl.core import config
from bnpl import plugin_file as file
from bnpl import plugin_fpcalc as fpcalc 
from bnpl import plugin_taglib as taglib 
from bnpl import plugin_essentia as essentia

# extract sounds from a directory
sounds = file.Directory(path=util.path_here(__file__, 'fixtures')).run()

# run UID transform
sounds = util.exec_pooled(fpcalc.UID().run, sounds)

# get tags
sounds = util.exec_pooled(taglib.GetTags().run, sounds)

# get bpm/key
if config['platform'] == 'linux':
  sounds = util.exec_pooled(essentia.FreeSound().run, sounds)

# store sounds
sounds = list(util.exec_pooled(lambda x: x.put(), sounds))
for sound in sounds:
	print sound.to_json()


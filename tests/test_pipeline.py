from bnpl import util
from bnpl.core import config
from bnpl import plugin_file as file
from bnpl import plugin_fpcalc as fpcalc 
from bnpl import plugin_taglib as taglib 
from bnpl import plugin_essentia as essentia

# extract sounds from a directory
extract = file.Directory(path=util.path_here(__file__, 'fixtures'))

snds = extract.run()

# run UID transform
snds = util.exec_pooled(fpcalc.UID().run, snds)

# get tags
snds = util.exec_pooled(taglib.GetTags().run, snds)

# get bpm/key
if config['platform'] == 'linux':
  snds = util.exec_pooled(essentia.FreeSound().run, snds)

# store in s3/es
def put(s):
  return s.put()

# print results
for snd in snds:
  snd.put()
  print snd.to_json()
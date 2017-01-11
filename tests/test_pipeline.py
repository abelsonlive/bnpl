from bnpl.util import here, pooled
from bnpl.core import config
from bnpl import plugin_file as file
from bnpl import plugin_fpcalc as fpcalc
from bnpl import plugin_essentia as essentia
from bnpl import plugin_taglib as taglib 

# extract sounds from a directory
snds = file.Directory(path=here(__file__, 'fixtures')).extract()

# run UID transform
snds = pooled(fpcalc.UID().transform, snds)

# get tags
snds = pooled(taglib.GetTags().transform, snds)

# get bpm/key
if config['platform'] == 'linux':
  snds = pooled(essentia.FreeSound().transform, snds)

# store in s3/es
def put(s):
  return s.put()

# print results
for snd in snds:
  snd.put()
  print snd.to_json()
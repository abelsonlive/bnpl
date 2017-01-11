from bnpl.util import here
from bnpl import plugin_file as file
from bnpl import plugin_fpcalc as fpcalc
from bnpl import plugin_essentia as essentia
from bnpl import plugin_taglib as taglib 

FIXTURES = here(__file__, 'fixtures')

directory = file.Directory(path=FIXTURES)
ess = essentia.FreeSound()
fpc = fpcalc.UID()
tags = taglib.GetTags()
snds = list(directory.extract())
snds = map(fpc.transform, snds)
snds = map(tags.transform, snds)
snds = map(ess.transform, snds)
map(lambda x: x.put(), snds)
print snds[-1].to_json()
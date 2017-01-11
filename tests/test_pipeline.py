from bnpl.util import here
from bnpl import plugin_file as file
from bnpl import plugin_fpcalc as fpcalc
from bnpl import plugin_essentia as essentia

FIXTURES = here(__file__, 'fixtures')

directory = file.Directory(path=FIXTURES)
ess = essentia.FreeSound()
fpc = fpcalc.UID()
snds = list(directory.extract())
print snds[0].to_dict()
snd = fpc.transform(snds[0])
snd = ess.transform(snd)
snd.put()
print snd.properties.keys()
from bnpl.util import here
from bnpl import plugin_file as file
from bnpl import plugin_fpcalc as fpcalc
from bnpl import plugin_essentia as essentia

FIXTURES = here(__file__, 'fixtures')

directory = file.Directory(path='fixtures/')
# ess = essentia.FreeSound()
fpc = fpcalc.UID()
snds = list(directory.extract())
snds = map(fpc.transform, snds)
for s in snds:
	s.put()

print snds[0].to_json()
from bnpl.util import shell, uid, here, json_to_obj
from bnpl.core import Transformer
from bnpl.exc import TransformerError

class UID(Transformer):

  def transform(self, sound):
    self.options.setdefault('fpcalc_path', here(__file__, 'ext/{0}/chromaprint-fpcalc'.format(self.config['platform'])))

    cmd = "{fpcalc_path} {0} -json".format(sound.path, **self.options)
    p = shell(cmd)
    if not p.ok:
      raise TransformerError(p.stdout)

    d = json_to_obj(p.stdout)
    
    # update sounds properties
    if 'fingerprint' in d:
      sound.uid = uid(fingerprint=d.get('fingerprint', None))
    sound.properties.update(d)
    return sound

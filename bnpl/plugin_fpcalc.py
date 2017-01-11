import json
import hashlib

from bnpl.util import shell, uid, here
from bnpl.core import Transformer


class UID(Transformer):

  def transform(self, sound):
    self.options.setdefault('fpcalc_path', here(__file__, 'ext/{0}/chromaprint-fpcalc'.format(self.config['platform'])))

    cmd = "{fpcalc_path} {0} -json".format(sound.path, **self.options)
    p = shell(cmd)
    if not p.ok:
      raise Exception(p.stdout)
    d = json.loads(p.stdout)
    
    # update sounds properties
    if 'fingerprint' in d:
      sound.uid = uid(fingerprint=d.get('fingerprint', None))
    sound.properties.update(d)
    return sound

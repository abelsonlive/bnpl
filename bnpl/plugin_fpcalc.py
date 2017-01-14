from bnpl import util
from bnpl.core import Option, OptionSet
from bnpl.core import config
from bnpl.core import Transformer
from bnpl.exc import TransformerError


class UID(Transformer):
  """
  Use chromaprint's fpcalc to assign a sound uid.
  """
  options = OptionSet(
    Option('fpcalc_path', type="path", required=True,
            default=util.path_here(__file__, 'ext/{0}/chromaprint-fpcalc'.format(config['platform'])))
  )

  def run(self, sound):
    """
    Run the UID transformation.
    """

    # run command
    cmd = "{0} '{1}' -json".format(self.options.fpcalc_path, sound.path)
    p = util.sys_exec(cmd)
    if not p.ok:
      raise TransformerError("Error running {0}: {1}".format(cmd, p.stdout))
    d = util.dict_from_json(p.stdout)

    # update sounds properties
    if 'fingerprint' in d:
      sound.uid = util.string_to_uid(fingerprint=d.get('fingerprint', None))
    sound.properties.update(d)
    return sound

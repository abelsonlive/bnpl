import os
from datetime import datetime

from bnpl import util
from bnpl.core import config
from bnpl.core import Option, OptionSet
from bnpl.core import Transformer
from bnpl.exc import TransformerError


class FreeSound(Transformer):
  
  options = OptionSet(
    Option('freesound_path', type='path', required=True, help="local path to freesound binary"
          default=util.path_here(__file__, 'ext/{0}/essentia-streaming-freesound-extractor')\
                                            .format(config['platform'])),
    Option('load_frames', type="boolean", default=False, help="Whether or not to load frame analysis.")
  )

  def run(self, sound):
    """

    """
    # configure
    o = '/tmp/{0}-freesound-output'.format(util.ts_now())
    oframes = o + "_frames.json"
    ostats = o + "_statistics.yaml"

    # cmd 
    cmd = "{freesound_path} '{0}' '{1}'".format(sound.path, o, **self.options.to_dict())
    proc = util.sys_exec(cmd)
    if not proc.ok: 
      raise TransformerError("Error running: {0}\n{1}".format(cmd, proc.stdout))

    # grab output files
    stats = self._parse_output(util.dict_from_yml_file(ostats))
    if self.options['load_frames']:
      frames = util.dict_from_json_file(oframes)

    # remove
    try:
      util.path_remove(ostats)
      util.path_remove(oframes)
    except:
      pass

    # update sound
    sound.properties.update(stats)
    return sound

  def _parse_output(self, output):
    """

    """
    properties = {
        "bpm": round(output.get('rhythm',{}).get('bpm',0), 1),
        "key": output.get('tonal', {}).get('key_key', '') + output.get('tonal', {}).get('key_scale', ''),
        "chord": output.get('tonal', {}).get('chord_key', '') + output.get('tonal', {}).get('chord_scale', '')
    }

    return properties

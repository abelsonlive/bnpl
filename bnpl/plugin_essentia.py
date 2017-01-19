from bnpl import util
from bnpl import Config, Option, OptionSet, Transformer
from bnpl.exc import TransformerError


class FreeSound(Transformer):
  """
  Extract bpm + key via essentia free sound.
  """
  options = OptionSet(
    Option('freesound_path', type='path', required=True, 
            help="Local path to freesound binary",
            default=util.path_here(__file__, 'ext/{0}/essentia-streaming-freesound-extractor')\
                                            .format(Config['platform'])),
    Option('load_frames', type="boolean", default=False, 
           help="Whether or not to load frame analysis.")
  )

  def run(self, sound):
    """

    """
    # configure
    o = '{tmp_dir}/{0}-freesound-output'.format(util.ts_now(), **self.config['bnpl'])
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
        "bpm": round(output.get('rhythm',{}).get('bpm', 0), 1),
        "key": output.get('tonal', {}).get('key_key', '') + output.get('tonal', {}).get('key_scale', ''),
        "chord": output.get('tonal', {}).get('chord_key', '') + output.get('tonal', {}).get('chord_scale', '')
    }

    return properties

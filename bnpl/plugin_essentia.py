import os
import math
from datetime import datetime

from bnpl.core import Option
from bnpl.util import shell
from bnpl.util import here
from bnpl.util import yml_file_to_obj, obj_to_yml, json_to_obj
from bnpl.core import Transformer
from bnpl.exc import TransformerError

class FreeSound(Transformer):

  def transform(self, sound):
    """

    """
    self.params.setdefault('freesound_path', 
                            here(__file__, 'ext/{0}/essentia-streaming-freesound-extractor')
                                            .format(self.config['platform']))
    
    # configure
    o = '/tmp/{0}-freesound-output'.format(datetime.now().isoformat())
    oframes = o + "_frames.json"
    ostats = o + "_statistics.yaml"

    # cmd 
    cmd = "{freesound_path} '{0}' '{1}'".format(sound.path, o, **self.params)
    print cmd
    proc = shell(cmd)
    if not proc.ok: 

        raise TransformerError("Error running: {0}\n{1}".format(cmd, proc.stdout))

    # grab output files
    # frames = json_file_to_obj(oframes)
    stats = self._parse_output(yml_file_to_obj(ostats))
    os.remove(ostats)
    os.remove(oframes)
    
    # update sound
    sound.properties.update(stats)
    return sound

  def _parse_output(self, output):
    """

    """
    properties = {
        "bpm": math.round(output.get('rhythm',{}).get('bpm',0), 1),
        "key": output.get('tonal', {}).get('key_key', '') + output.get('tonal', {}).get('key_scale', ''),
        "chord": output.get('tonal', {}).get('chord_key', '') + output.get('tonal', {}).get('chord_scale', '')
    }

    return properties

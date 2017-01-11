import os
from datetime import datetime

from bnpl.core import Option
from bnpl.util import shell
from bnpl.util import here
from bnpl.util import yml_file_to_obj, obj_to_yml, json_to_obj
from bnpl.core import Transformer

class FreeSound(Transformer):

  def transform(self, sound):
    """

    """
    self.params.setdefault('freesound_path', 
                            here(__file__, 'ext/{0}/essentia-streaming-extractor-freesound')
                                            .format(self.config['platform']))
    
    # configure
    o = here(__file__, '{0}-freesound-output'.format(datetime.now().isoformat()))
    oframes = o + "_frames.json"
    ostats = o + "_statistics.yaml"

    # cmd 
    cmd = "{freesound_path} '{0}' '{1}' > /dev/null".format(sound.file, o, **self.params)
    proc = shell(cmd)
    if not proc.ok: 
        raise Exception("Error running: {0}".format(cmd))

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
        "bpm": output.get('rhythm',{}).get('bpm', None),
        "key": output.get('tonal', {}).get('key_key', '') + output.get('tonal', {}).get('key_scale', ''),
        "chord": output.get('tonal', {}).get('chord_key', '') + output.get('tonal', {}).get('chord_scale', '')
    }

    return properties

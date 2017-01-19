"""
# API SPEC:
# GET /sounds - search for sounds
# POST /sounds - upsert sounds
# DELETE /sounds - delete sounds by query
# GET /sounds/:uid - fetch a sound 
# POST /sounds/:uid - upsert a sound by it's id
# DELETE /sounds/:uid - delete a sound by it's id
# GET/POST /sounds/:uid/transform/:transform - apply a transformation to a sound, yielding one or more sounds.
"""

from flask import Flask 

from bnpl import util
from bnpl import Factory
from bnpl.plugin import Extractor, Importer, Deleter

app = Flask(__name__)
plugins = Factory()


@app.route('/api/sounds', methods=['GET'])
def list_sounds():
  """
  """
  return Extractor(_context="api").do()

@app.route('/api/sounds', methods=['POST', 'PUT'])
def create_sound():
  """
  """
  return Importer(_context="api").do()

@app.route('/api/sounds', methods=['DELETE'])
def delete_sounds():
  pass

@app.route('/api/sounds/<uid>.<ext>', methods=['GET'])
def get_sound(uid, ext):
  pass

@app.route('/api/sounds/<uid>', methods=['POST', 'PUT'])
def update_sound(uid):
  pass

@app.route('/api/sounds/<uid>', methods=['DELETE'])
def delete_sound(uid):
  pass

@app.route('/api/plugins', methods=['GET'])
def list_plugins():
  """
  """
  return plugins.to_api()

@app.route('/api/plugins/<name>', methods=['GET'])
def get_plugin(name):
  """
  """
  p = plugins.to_dict().get(name)
  return util.api_write_data(p)

@app.route('/api/plugins/<name>', methods=['POST'])
def run_plugin(name):
  pass

if __name__ == '__main__':
  app.run()
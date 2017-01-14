from flask import Flask 



# API SPEC:
# GET /sounds - search for sounds
# POST /sounds - upsert sounds
# DELETE /sounds - delete sounds by query
# GET /sounds/:uid - fetch a sound 
# POST /sounds/:uid - upsert a sound by it's id
# DELETE /sounds/:uid - delete a sound by it's id
# GET/POST /sounds/:uid/transform/:transform - apply a transformation to a sound, yielding one or more sounds.


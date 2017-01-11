Bossa Nova Public Library 
=========================
A public directory of music, intellgently catalogued, automatically collected, and easily remixable. 

`bnpl` consists of two objects: sounds and plugins. 

sounds can be any audio file, conceptually we might break them into four key categories:
- mixes 
- tracks
- loops 
- one-shots

rather than dealing with all the vagaries inherent in distinguishing between these categories, we're just going to treat them all the same. 

plugins are bits of code that generate, transform, or combine / aggregate / export sounds. there are currently 3 types of plugins:

* _extractors_ -> accepts arbitrary parameters and return one or more sounds
	
	- specify parameter schema
	- specify properties added / modified
	- i.e. Directory, iTunes Library, Facebook Wall, YouTube Playlist, etc.
	
* _transformers_ -> idempotently modify a sound or create one or more new sounds given arbitrary parameters

	- specify parameter schema
	- specify properties added / modified
	- i.e. FPCalc, Essentia, Discogs Reconciliation, auto-tagging, etc.
	- VST Support?

* _exporters_ -> accept arbitrary parameters are return an arbitrary file (usually an archive, but could )
	- specify parameter schema
	- i.e. Rekordbox, iTunes Library, MPC Program, etc. 


plugins can be combined into disparate pipelines to check sounds in and out of the library an improve the associated metadata, etc.
	
[tests/test_pipeline.py](tests/test_pipeline.py) puts everything i've done so far together.

You should only use the library via the `Dockerfile` - I've had trouble getting some of the external binaries (in `pkg`) working on OSX.



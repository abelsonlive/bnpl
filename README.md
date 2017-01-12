Bossa Nova Public Library 
=========================
A public directory of music, intellgently catalogued, automatically collected, and easily remixable. 

## Installation 

```shell
git clone git@github.com:abelsonlive/bnpl.git
cd bnpl && make install
```

## Configurations

Edit all `.yml.example` files in [bnpl/config](bnpl/config/)

These are accessible in the app via:

```python
from bnpl.core import config
```

## Installation (Docker)

```shell
git clone git@github.com:abelsonlive/bnpl.git
cd bnpl && make build
make ssh # connect to container
python tests/test_pipeline.py
```

## API / Usage

See [tests/test_pipeline.py](tests/test_pipline.py) for now.

The idea is to string pipelines together through three core plugin objects:

`extractors` - takes options and returns one or more sounds (i.e. a directory or a iTunes XML file or an ElasticSearch query)
`transformers` - takes a sound and options returns one or more sounds
`exporters` - takes a list of sounds and options returns a link to a single file or archive of files representing all of these sounds (i.e. a directory or a iTunes XML file).


## TODO 

- [x] Create "Options" object for declaring inputs to Plugins
- [ ] Execute plugins via CLI 
- [ ] Execute plugins via API
- [ ] Fix Mac OS X Essentia Installation 
- [ ] DIY BPM / Pitch Detection? Can we get speed Increases ?
- [ ] ElasticSearch Plugin for searching sounds
- [ ] React App for browsing / listening / uploading sounds.
- [ ] Massive Import
- [ ] Add Users / Customizations to App



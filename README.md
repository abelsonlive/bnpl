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


from bnpl.core import Config, Sound
from bnpl.plugin import (
	Plugin, Option, OptionSet,
	Extractor, Transformer, Importer, Exporter,
	Factory
)

plugins = Factory()
print plugins.describe()

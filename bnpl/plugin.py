import importlib
import inspect
from collections import OrderedDict, defaultdict

from bnpl import util
from bnpl.core import ConfigMixin 
from bnpl.core import Sound


########################################
# type utilies 
########################################

class OptionType(object):

  _types = OrderedDict({
    'null': [util.null_prepare, util.null_check],
    'boolean': [util.boolean_prepare, util.boolean_check],
    'date': [util.date_prepare, util.date_check],
    'ts': [util.ts_prepare, util.ts_check],
    'integer': [util.integer_prepare, util.integer_check],
    'float': [util.float_prepare, util.float_check],
    'dict': [util.dict_prepare, util.dict_check],
    'list': [util.list_prepare, util.list_check],
    'set': [util.set_prepare, util.set_check],
    'path': [util.path_prepare, util.path_check],
    'regex': [util.regex_prepare, util.regex_check],
    'string': [util.string_prepare, util.string_check]
  })

  def __init__(self, type="string"):
    self.type = type
    self.types = self._types.keys()

  def prepare(self, value, type=None):
    """
    """
    func = self._types[type or self.type][0]
    return func(value)  

  def check(self, value, type=None):
    """
    """ 
    func = self._types[type or self.type][1]
    return func(value)

  @classmethod
  def sniff(cls, value, types=[]):
    """
    """
    if not len(types):
      types = cls._types.keys()

    for t in types:
      if cls.check(value, t):
        try:
          return cls.prepare(value, t)
        except:
          pass 
    return cls.prepare(value, "string")

class Option(ConfigMixin): 
  """
  Options for Plugins
  """

  def __init__(self, name, **kwargs):
    """
    """
    self.name = util.string_to_slug(name, delim="_") # fight me.
    self.alias = kwargs.get('alias', None)
    self.type = kwargs.get('type', 'string')
    self.items = kwargs.get('items', None)
    self.default = kwargs.get('default', None)
    self.required = kwargs.get('required', False)
    self.description = kwargs.get('description', kwargs.get('help', ''))
    self.value = None
    self.parser = OptionType(self.type)

  def prepare(self, val=None):
    """
    Prepare an option.
    """
    if not val and self.default:
      self.value = copy.copy(self.default)
    else:
      self.value = val 
    try:
      self.value = self.parser.prepare(self.value)
    except:
      raise ValueError('Invalid {0} type: {1}'.format(self.type, val))
    if not self.value and self.required:
      raise ValueError('Missing required option: {0}'.format(self.name))
    if self.type == "list" and self.items:
      self.value = map(OptionType(self.items).prepare, self.value)
    return self.value

  def to_dict(self):
    """
    Render an option as a dictionary.
    """
    d = {
      'name': self.name,
      'description': self.description,
      'type': self.type,
      'default': self.default,
      'required': self.required
    }
    if self.value:
      d['value'] = self.value 
    if self.alias:
      d['alias'] = self.alias
    if self.items:
      d['items'] = self.items
    return d

  def describe(self):
    """
    """
    return self.to_dict()

  def to_json(self):
    """
    Render an option as json.
    """
    return util.dict_to_json(self.to_dict())

  def to_yml(self):
    """
    Render an option as yml.
    """
    return util.dict_to_yml(self.to_dict())


class OptionSet(ConfigMixin):
  """

  """
  defaults = [
    Option("help", type="boolean", default=False, required=False)
  ]

  def __init__(self, *opts, **kwargs):
    self._options = [o for o in opts]
    self._parsers = {}
    self._parsed = {}
    self._aliases = {}
    self._required = []
    self._errors = []
    self._setup()

  def _setup(self):
    """
    setup parsers + required options
    """
    self._options.extend(self.defaults)
    map(self._setup_option, self._options)

  def _setup_option(self, opt):
    """
    """
    # function map
    self._parsers[opt.name] = opt.prepare

    # required
    if opt.required and not opt.default:
      self._required.append(opt.name)

    # aliases 
    if opt.alias:
      self._aliases[opt.alias] = opt.name

    # defaults
    if opt.default:
      self._parsed[opt.name] = opt.default
      setattr(self, opt.name, opt.default)

  def _set_option(self, name, value):
    """
    """
    fn = self._parsers.get(name)
    if not fn:
      return
    try:
      value = fn(value)
      self._parsed[name] = value
      setattr(self, name, value)
    except ValueError as e:
      self._errors.append("Invalid option: '{0}'\n{1}.".format(name, e.message))

  def _map_aliases(self, **raw):
    """
    map inputted aliases to full names
    """
    for k,v in raw.iteritems():
      if k in self._aliases:
        raw[self._aliases[k]] = v 
        del raw[k]
    return raw

  def _check_required(self):
    """
    check required options
    """
    missing = filter(lambda x: x not in self._parsed, self._required)
    n_missing = len(missing)
    if n_missing > 0:
      suffix = "s"
      if n_missing <2:
        suffix = ""
      return True, "Missing required option{0}: {1}".format(suffix, ", ".join(missing))
    return False, ""

  def _check_errors(self):
    """
    check for errors.
    """
    # check required
    err, msg = self._check_required()
    if err: 
      self._errors.append(msg)

    if len(self._errors):
      message = "Invalid Option Set!\nErrors:\n{0}".format("\n".join(self._errors))
      raise ValueError(message)

  def prepare(self, **raw):
    """
    Prepare a list of key/value options
    """
    for name, value in self._map_aliases(**raw).iteritems():
      try:
        self._set_option(name, value)
      except ValueError as e:
        self._errors.append("Invalid option: '{0}':'{1}' > {2}.".format(name, value, e.message))
    self._check_errors()

  def to_dict(self):
    """
    describe an OptionSet
    """
    return self._parsed

  def describe(self):
    """
    Describe all options.
    """
    return [o.describe() for o in self._options]

  def to_yml(self):
    """
    Format options as yml.
    """
    return util.dict_to_yml(self.to_dict())

  def to_json(self):
    """
    Render an option as json.
    """   
    return util.dict_to_json(self.to_dict())

  def __getitem__(self, item):
    """
    """
    return self._parsed.get(item, None)

class Plugin(ConfigMixin):
  """
  Plugin description placeholder.
  """
  type = "core"

  defaults = [
    Option("help", type="boolean", default=False, help="placeholder", reqired=False)
  ]

  def __init__(self, data=[], _context="python", **options):
    """

    """
    self._load_context(_context)
    self._load_options(**options)
    self._load_data(data)
    self._load_file()
    self._load_sounds()
    self._errors = []
    # overwritten by Factory

  def _load_context(self, ctxt):
    """
    """
    if ctxt not in ['python', 'cli', 'api', 'internal']:
      raise ValueError('Invalid plugin context: {}'.format(ctxt))
    self._context = ctxt

  def _load_options(self, **options):
    """

    """
    if self._context == 'python':
      self.options.prepare(**options)
    if self._context == 'api':
      self.options.prepare(**util.api_read_options())
    elif self._context == 'cli':
      self.options.prepare(**util.cli_read_options())

  def _load_data(self, data):
    """

    """
    self.data = data
    if self._context == 'api':
      self.data = util.api_read_data()
    elif self._context == 'cli':
      self.data = util.cli_read_data()

  def _load_file(self):
    """
    
    """
    self.file = None 
    if self._context == 'api':
      self.file = util.api_read_file()

  def _load_sounds(self):
    """
    """
    self.sounds = []

    if self.has_data:
      def __gen():      
        for sound in self.data:
          if isinstance(sound, Sound):
            yield sound 
          else:
            yield Sound(**sound)
      self.sounds =  __gen()

    elif self.has_file:
      self.sounds =  [Sound(path=self.file.filename)]

  @property
  def has_data(self):
    """
    """
    if inspect.isgenerator(self.data):
      return True 
    return len(self.data) > 0

  @property
  def has_file(self):
    """
    """
    return self.file is not None

  @property
  def name(self):
    """

    """
    return util.string_camel_case_to_slug(self.__class__.__name__)

  @property
  def description(self):
    """

    """
    d = self.__doc__
    if not d:
      d = ""
    return d.replace("\n", " ").strip()

  def to_dict(self):
    """

    """
    return {
      'name': self.name,
      'description': self.description,
      'type': self.type,
      'options': self.options.describe()
    }

  def to_json(self):
    """
    """
    return util.dict_to_json(self.to_dict())

  def to_api(self):
    """
    """
    return util.api_write_data(self.to_dict())

  def describe(self):
    """
    """
    return self.to_dict()

  def run(self, *args, **kwargs):
    """
    """
    raise NotImplemented

  def do(self, *args, **kwargs):
    """
    For internal usage.
    """
    if self.options._parsed.get("help", False):
      output = self.describe()
    else:
      output = self._switch(*args, **kwargs)
    return self._return(output)

  def _switch(self, *args, **kwargs):
    """
    determine execution
    """
    def _run():
      return self.run()
    
    if self.type not in ('extractor', 'pipeline') and not self.has_data:
      def _run():
        return self.run(args[0]) # accepts data as first positional arg.

    elif self.type == "transformer":
      def _run():
        for sound in self.sounds:
          yield self.run(sound)
    
    elif self.type in ["exporter", "importer"]:
      def _run():
        return self.run(self.sounds)

    return _run()

  def _return(self, output):
    """
    handle output
    """
    if self._context == "python":
      return output 

    elif self._context == "api":
      return util.api_write_data(output)

    elif self._context == "cli":
      return util.cli_write_data(output)


class Extractor(Plugin):
  """
  Accepts parameters and returns one or sounds
  """
  type = 'extractor'

class Importer(Plugin):

  """
  Stores one or more sounds. 
  Returns sounds with created / updated_at timestamps.
  """

  type = 'importer'

  options = OptionSet(
    Option('pool_size', type="integer", default=10)
  )

  def run(self, sounds):
    """
    """
    if not util.list_check(sounds, strict=True):
      sounds = [sounds]
    return util.exec_pooled(self._put_sound, sounds, size=self.options.pool_size)
  
  def _put_sound(self, sound):
    """
    """
    if not isinstance(sound, Sound):
      sound = Sound(**sound)
    return sound.put()

class Deleter(object):
  """

  """
  type = 'deleter'


class Transformer(Plugin):
  """
  Accepts a sound + parameters and returns a modified sound or one or more new sounds
  """
  type = 'transformer'

class  Mixer(Plugin):
  """
  Accepts sounds + parameters and returns a single sound.
  """
  type = 'mixer'

class Exporter(Plugin):

  """
  Accepts parameters and one or more sounds and returns a link to a file. 
  """
  type = 'exporter'


class Pipeline(Plugin):
  """
  A plugin that runs plugins.
  """
  type = 'pipeline'

  # dictionaries of plugin_name, **options
  options = OptionSet(
    Option('extractors', type="dict", default={'extractor':{}}),
    Option('transformers', type="dict", default={}),
    Option('importers', type="dict", default={}),
    Option('exporters', type="dict", default={})
  )

class Factory(ConfigMixin):

  INTERNAL = [
    'Transformer', 'Exporter', 'Importer', 
    'Extractor', 'Plugin', 'Pipeline', 'Deleter',
  ]

  def __init__(self):
    self._plugins = defaultdict(dict)
    self._factory = {}
    self._factory['core.importer'] = Importer 
    self._plugins['core.importer']['class'] = Importer 
    self._plugins['core.importer']['module'] = 'core'
    self._plugins['core.importer']['import_path'] = 'bnpl.Importer'
    self._register_plugins()

  def describe(self):
    """
    """
    _l = []
    for nm, items in self._plugins.iteritems():
      d = self._factory[nm](_context="internal").describe()
      d['name'] = nm
      d['import_path'] = items['import_path']
      d['module'] = items['module']
      _l.append(d)
    return _l

  def to_dict(self):
    """
    """
    return {
      '{name}'.format(**d):d for d in self.describe()
    } 

  def to_api(self):
    return util.api_write_data(self.describe())

  def to_json(self):
    """
    """
    return util.dict_to_json(self.describe())

  def to_yml(self):
    """
    """
    return util.dict_to_yml(self.to_dict())

  def __getitem__(self, key):
    """
    """
    return self._factory.get(key)

  def get(self, key, default=None):
    """
    """
    return self._factory.get(key, default)

  def __iter__(self):
    for _, obj in self._factory.iteritems():
      yield obj

  def _register_plugins(self):
    """

    """
    import bnpl

    package_name = bnpl.__name__
    package_path = bnpl.__path__[0]

    for fp in util.path_list(package_path):

      # filter crud
      if not (fp.endswith('.py') and 'plugin_' in fp):
        continue

      name = util.path_get_filename(fp, ext=False)
      module = name.replace("plugin_", "")    
      pkg = '%s.%s' % (package_name, name)
      m = importlib.import_module(pkg)

      for item in dir(m):
        # ignore subclasses
        if item in self.INTERNAL:
          continue

        # ignore internal
        if item.startswith("_"):
          continue

        # load object
        obj = getattr(m, item, None)
        if not obj:
          continue

        # ensure class
        if not inspect.isclass(obj):
          continue

        # check type
        if not issubclass(obj, Plugin):
          continue

        import_path = "{0}.{1}".format(pkg, item)
        if import_path in self._plugins:
          continue
        nm = obj(_context="internal").name
        nm = "{0}.{1}".format(module, nm)
        self._factory[nm] = obj
        self._plugins[nm]['class'] = obj 
        self._plugins[nm]['module'] = module 
        self._plugins[nm]['import_path'] = import_path



import os
import sys
import argparse
import logging
from traceback import format_exc
from argparse import RawTextHelpFormatter, SUPPRESS

from bnpl import Factory

def setup_plugins(subparser, plugins):
  """
  install the subparsers
  """
  subcommands = {}
  for plugin in plugins.describe():
    key = "{module}.{name}".format(**plugin)
    cmd_parser = subparser.add_parser(key, help=plugin["description"])
    for opt in plugin['options']:
      if opt['name'] == "help": continue
      desc = 'Accepts: "{type}" type. '.format(**opt)
      default = opt.get('default', None)
      if default:
        desc += 'Default: "{default}". '.format(**opt)
      cmd_parser.add_argument('--{0}'.format(opt['name']), 
                              help=opt.get('descrption', desc), 
                              default=default,
                              required=opt.get('required', default is not None))
    subcommands[key] = plugins[plugin["name"]]
  return subcommands


def run():
  """
  The main cli function.
  """
  # create an argparse instance
  parser = argparse.ArgumentParser(prog='bnpl', usage=SUPPRESS)

  subparser = parser.add_subparsers(help='Plugins', dest='cmd')

  plugins = Factory()
  subcommands = setup_plugins(subparser, plugins)

  opts, _ = parser.parse_known_args()

  # check for proper subcommands
  if opts.cmd not in subcommands:
    logging.error("No such subcommand.")
    sys.exit(1)

  try:
    subcommands[opts.cmd](_context="cli").do()
    sys.exit(0)

  except KeyboardInterrupt as e:
    logging.warning('Interrupted by user.')
    sys.exit(2)  # interrupt

  except Exception as e:
    logging.error(e.message)
    sys.exit(1)

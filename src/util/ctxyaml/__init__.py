import os
import sys
import yaml

from util import paths
from util.ctxyaml.context import Context, WithContext

__all__ = [ 'HUMAN', 'MACHINE', 'load', 'load_all', 'dump', 'dump_all' ]


class Loader(yaml.SafeLoader, WithContext):
    pass


class Dumper(yaml.SafeDumper, WithContext):
    def __init__(self, *args, **kwargs):
        kwargs['allow_unicode'] = True
        kwargs['width'] = sys.maxsize
        kwargs['indent'] = self.context.indent
        kwargs['default_style'] = self.context.default_style
        kwargs['default_flow_style'] = self.context.default_flow_style
        super(Dumper, self).__init__(*args, **kwargs)

for c in "0123456789":
    Dumper.yaml_implicit_resolvers[c] = [ ( t, r ) for ( t, r ) in Dumper.yaml_implicit_resolvers[c] if t != 'tag:yaml.org,2002:timestamp' ]


HUMAN = Context(
    human=True,
    indent=2,
    default_style=None,
    default_flow_style=False,
    resolvers={ }
)

MACHINE = Context(
    human=False,
    indent=1,
    default_style='"',
    default_flow_style=True
)

def load(source, context=HUMAN, **kwargs):
    ctx = context.apply(current_dir=paths.directory_of(source), **kwargs).defaults(root_dir=os.getcwd())
    return yaml.load(source, Loader.create_class(ctx))

def load_all(source, context=HUMAN, **kwargs):
    ctx = context.apply(current_dir=paths.directory_of(source), **kwargs).defaults(root_dir=os.getcwd())
    return yaml.load_all(source, Loader.create_class(ctx))

def dump(value, target=None, context=HUMAN, **kwargs):
    ctx = context.apply(current_dir=paths.directory_of(target), **kwargs).defaults(root_dir=os.getcwd())
    return yaml.dump(value, target, Dumper.create_class(ctx))

def dump_all(values, target=None, context=HUMAN, **kwargs):
    ctx = context.apply(current_dir=paths.directory_of(target), **kwargs).defaults(root_dir=os.getcwd())
    return yaml.dump_all(values, target, Dumper.create_class(ctx))


import util.ctxyaml.include
import util.ctxyaml.scalars

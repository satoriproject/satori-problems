import collections
import datetime
import re
import yaml

from util.ctxyaml import Loader, Dumper, HUMAN

__all__ = [ 'Scalar', 'Implicit', 'RegexLoad', 'register' ]


def construct_any(loader, node):
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_scalar(node)

def represent_any(dumper, tag, value):
    if isinstance(value, dict) or isinstance(value, collections.OrderedDict):
        return dumper.represent_mapping(tag, value)
    if isinstance(value, list):
        return dumper.represent_sequence(tag, value)
    return dumper.represent_scalar(tag, value)

class Scalar(object):
    def __init__(self, **kwargs):
        self.type = kwargs['type']
        self.tag = kwargs['tag']
        if 'load' in kwargs:
            self.load = kwargs['load']
        if 'dump' in kwargs:
            self.dump = kwargs['dump']
        def construct(loader, node):
            return self.load(construct_any(loader, node), loader.context)
        Loader.add_constructor(self.tag, construct)
        def represent(dumper, value):
            return represent_any(dumper, self.tag, self.dump(value, dumper.context))
        Dumper.add_representer(self.type, represent)
    # override any of the following if needed
    def load(self, value, context):
        return self.type(value)
    def dump(self, value, context):
        return str(value)

class Regex(Scalar):
    def __init__(self, **kwargs):
        super(Regex, self).__init__(**kwargs)
        self.regex = re.compile('^' + kwargs['regex'] + '$')

class Implicit(Regex):
    def __init__(self, **kwargs):
        super(Implicit, self).__init__(**kwargs)
        HUMAN.resolvers[self.tag] = self.regex

class RegexLoad(Regex):
    def __init__(self, **kwargs):
        if 'load' in kwargs:
            raise Exception("Cannot manually override load() for RegexLoad scalar types")
        super(RegexLoad, self).__init__(**kwargs)
        self.regex_load = kwargs['regex_load']
    def load(self, value, context):
        return self.regex_load(**self.regex.match(value).groupdict())


def register(*classes, **kwargs):
    type('+'.join([ c.__name__ for c in classes ]), classes, { })(**kwargs)


def load_datetime(year=0, month=0, day=0, hour=0, minute=0, second=0):
    return datetime.datetime(
        year=int(year),
        month=int(month),
        day=int(day),
        hour=int(hour),
        minute=int(minute),
        second=int(second)
    )

register(
    Implicit, RegexLoad,
    type=datetime.datetime,
    tag='!datetime',
    regex='(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\s+(?P<hour>\d{1,2}):(?P<minute>\d{2}):(?P<second>\d{2})',
    regex_load=load_datetime
)


def load_timespan(days=0, hours=0, minutes=0, seconds=0.0):
    return datetime.timedelta(
        days=int(days),
        hours=int(hours),
        minutes=int(minutes),
        seconds=float(seconds)
    )

def dump_timespan(value, context):
    return re.sub(r'(\.[0-9]*[1-9])0*', r'\1', str(value))

register(
    Implicit, RegexLoad,
    type=datetime.timedelta,
    tag='!timespan',
    regex='(?:(?P<days>\d+)\s*(d|day|days))?,?\s*(?P<hours>\d+):(?P<minutes>\d{2})(?::(?P<seconds>\d{2}(?:\.\d+)?))?',
    regex_load=load_timespan,
    dump=dump_timespan
)

import collections
import glob
import yaml

from util import paths
from util.nsdict import NamespaceDict
from util.ctxyaml import Loader, Dumper, load

__all__ = [ ]


INCLUDE_TAG = u'!include'

def construct_include(loader, node):
    value = loader.construct_scalar(node)
    path = paths.combine(loader.context.root_dir, loader.context.current_dir, value)
    with open(path, 'r') as source:
        return load(source, loader.context)

Loader.add_constructor(INCLUDE_TAG, construct_include)


class MergeItem(object):
    def __init__(self, content):
        self.content = content

def construct_mapping(loader, node):
    result = NamespaceDict()
    for key_node, value_node in node.value:
        key = loader.construct_scalar(key_node)
        if key_node.tag == INCLUDE_TAG:
            if key:
                raise yaml.MarkedYAMLError(None, None, "non-empty include used as mapping key", key_node.start_mark)
            value = construct_include(loader, value_node)
            if isinstance(value, collections.OrderedDict):
                result.update(value)
            elif isinstance(value, list):
                result = MergeItem(value)
        else:
            value = loader.construct_object(value_node)
            result[key] = value
    return result

Loader.add_constructor(Loader.DEFAULT_MAPPING_TAG, construct_mapping)


def represent_mapping(dumper, value):
    return dumper.represent_mapping(Dumper.DEFAULT_MAPPING_TAG, value.items())

Dumper.add_representer(collections.OrderedDict, represent_mapping)
Dumper.add_representer(NamespaceDict, represent_mapping)


def construct_sequence(loader, node):
    result = [ ]
    for child_node in node.value:
        child = loader.construct_object(child_node)
        if isinstance(child, MergeItem):
            result.extend(child.content)
        else:
            result.append(child)
    return result

Loader.add_constructor(Loader.DEFAULT_SEQUENCE_TAG, construct_sequence)


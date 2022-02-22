# vim:ts=4:sts=4:sw=4:et
import base64
import collections
import hashlib
import os.path
import six
import sys
import yaml

from util import blob, ctxyaml
from util.nsdict import NamespaceDict

from satori.client.common import want_import
want_import(globals(), '*')


def copy_file(src, dst):
    BUF_SIZ = 16 * 1024
    while True:
        buf = src.read(BUF_SIZ)
        if not buf:
            break
        dst.write(buf)


def _calculate_blob_hash(blob_path):
    with open(blob_path, 'rb') as blob:
        # TODO: use a buffer to avoid unbounded memory usage
        return base64.urlsafe_b64encode(hashlib.sha384(blob.read()).digest())


def upload_blob(blob_path):
    blob_hash = _calculate_blob_hash(blob_path)
    if not Blob.exists(blob_hash):
        with open(blob_path, 'rb') as local_blob:
            blob_size = os.path.getsize(blob_path)
            remote_blob = Blob.create(blob_size)
            print 'Uploading blob', os.path.basename(blob_path) + ',',
            print 'size =', blob_size, 'bytes' + '...',
            sys.stdout.flush()
            copy_file(local_blob, remote_blob)
            print 'done'
        remote_blob_hash = remote_blob.close()
        assert blob_hash == remote_blob_hash
    blob_name = os.path.basename(blob_path)
    return AnonymousAttribute(is_blob=True, value=blob_hash, filename=blob_name)

simple_types = tuple(list(six.string_types) + list(six.integer_types) + [bool, float,])
list_types = (list, tuple)
dict_types = (dict, collections.OrderedDict, NamespaceDict)

def simplify(value):
    if value is None:
        return None
    if isinstance(value, simple_types):
        return value
    if isinstance(value, list_types):
        return list([ simplify(element) for element in value ])
    if isinstance(value, dict_types):
        return dict([ (simplify(key), simplify(val)) for (key,val) in value.items() ])

def serialize(value):
    if isinstance(value, simple_types):
        return str(value)
    return str(yaml.safe_dump(simplify(value)))

def make_test_data(test_pair, include_name_in_data=False):
    test_name, test_yaml = test_pair
    test_yaml['name'] = test_name
    test_data = {}
    for key, value in test_yaml.items():
        key = str(key)
        if key == 'name' and not include_name_in_data:
            continue
        if type(value) == blob.Blob:
            test_data[key] = upload_blob(value.path)
        else:
            test_data[key] = AnonymousAttribute(is_blob = False, value = serialize(value))
    return test_data

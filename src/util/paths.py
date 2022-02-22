import glob
import os

__all__ = [ 'directory_of', 'combine' ]


def directory_of(file):
    name = getattr(file, 'name', None)
    return name and os.path.dirname(name)


def combine(root_dir, current_dir, path):
    if not path:
        return None
    elif os.path.isabs(path):
        drive, tail = os.path.splitdrive(path)
        path = os.path.relpath(tail, drive + os.sep)
        path = os.path.join(root_dir, path)
    else:
        path = os.path.join(current_dir, path)
    matches = glob.glob(path)
    if len(matches) == 0:
        raise Exception("file %s does not exist" % path)
    if len(matches) > 1:
        raise Exception("path %s is ambiguous" % path)
    path = matches[0]
    return path

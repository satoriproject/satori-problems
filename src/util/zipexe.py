import io
import modulefinder
import os
#import pip
import sys
import zipfile

from util import dpkg

__all__ = [ 'create' ]


def create(path):
    """Pack a script (with dependencies) into an executable zip archive."""
    extension = os.path.splitext(path)[1]
    if extension == '.zip':
        with io.open(path, 'rb') as stream:
            return stream.read()
    buffer = io.BytesIO()
    buffer.write(('#!/usr/bin/env python%s.%s\n' % ( sys.version_info.major, sys.version_info.minor )).encode())
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        if extension == '.py':
            finder = modulefinder.ModuleFinder()
            finder.run_script(path)
            items = [ ]
            packages = set()
            for name, module in finder.modules.items():
                source = module.__file__
                if not source:
                    continue
                package = dpkg.find_package(source)
                if package:
                    packages.add(package)
                    continue
                target = os.path.join(*name.split('.'))
                base, extension = os.path.splitext(os.path.basename(source))
                if base == '__init__':
                    target = os.path.join(target, base)
                target += extension
                items.append(( target, source ))
            for target, source in sorted(items):
                archive.write(source, target)
            archive.writestr('packages.txt', '\n'.join(packages) + '\n')
        else:
            raise Exception("unsupported script type '%s'" % extension)
    return buffer.getvalue()


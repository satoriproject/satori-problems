import locale
import subprocess

options = dict(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
encoding = locale.getdefaultlocale()[1]

package_map = { }

def list_files(*packages):
    child = subprocess.Popen([ 'dpkg', '-L' ] + list(packages), **options)
    ( stdout, stderr ) = child.communicate()
    if child.returncode:
        raise ValueError(stderr.decode(encoding))
    return stdout.decode(encoding).splitlines()


def find_package(path):
    if not path in package_map:
        try:
            child = subprocess.Popen([ 'dpkg', '-S', path ], **options)
        except OSError:
            return None # FIXME: what should we do here?
        ( stdout, stderr ) = child.communicate()
        if not child.returncode:
            package = stdout.decode(encoding).splitlines()[0].split(':')[0]
            try:
                for path in list_files(package):
                    if path in package_map:
                        package_map[path] = None
                    else:
                        package_map[path] = package
            except ValueError:
                pass
    return package_map.get(path, None)


import hashlib
import io
import os
import shutil
import tempfile

from util.ctxyaml import scalars
from util import paths


class BlobState(object):

    MISSING = "a nonexistent BLOB"
    WRITING = "a BLOB while it is being created"
    HASDATA = "a finished BLOB without a backing file"
    HASFILE = "a finished BLOB with a backing file"


class BlobStateException(Exception):

    pass


class Blob(object):

    def __init__(self, path=None, basename=None, content=None, digest=None):
        self.path = path
        self.basename = basename or (path and os.path.basename(path))
        if self.path:
            with io.open(self.path, 'rb') as stream:
                self.digest = hashlib.sha1(stream.read()).hexdigest()
            self.content = None
            self.state = BlobState.HASFILE
        elif content:
            self.content = content
            if isinstance(self.content, bytes):
                self.digest = hashlib.sha1(self.content).hexdigest()
            else:
                self.digest = hashlib.sha1(self.content.encode()).hexdigest()
            if digest:
                assert self.digest == digest
            self.state = BlobState.HASDATA
        else:
            self.digest = digest
            self.state = BlobState.MISSING

    def load(self):
        if (self.state == BlobState.MISSING):
            if self.digest:
                self.acquire()
            elif self.path:
                with io.open(self.path, 'rb') as stream:
                    self.digest = hashlib.sha1(stream.read()).hexdigest()
                self.content = None
                self.state = BlobState.HASFILE

    def acquire(self):
        if self.state == BlobState.MISSING:
            if not self.digest:
                raise BlobStateException("Cannot download a BLOB without a digest")
            # TODO: replace this with proper downloading of BLOBs by digest
            cachepath = os.path.join('/tmp', self.digest)
            self.path = os.path.join('/tmp', self.basename or self.digest)
            self.basename = os.path.basename(self.path)
            if self.path != cachepath:
                shutil.copy(cachepath, self.path)
            self.state = BlobState.HASFILE
        else:
            raise BlobStateException("Cannot download %s" % self.state)

    def release(self):
        self.load()
        # TODO: replace this with proper uploading of BLOBs
        cachepath = os.path.join('/tmp', self.digest)
        if self.state in [ BlobState.MISSING, BlobState.WRITING ]:
            raise BlobStateException("Cannot upload %s" % self.state)
        elif self.state == BlobState.HASDATA:
            if isinstance(self.content, bytes):
                mode = 'wb'
            else:
                mode = 'wt'
            with io.open(cachepath, mode) as stream:
                stream.write(self.content)
        elif self.state == BlobState.HASFILE:
            if cachepath != self.path:
                shutil.copy(self.path, cachepath)

    def open(self, mode='r', **kwargs):
        if self.state == BlobState.MISSING:
            if self.digest:
                self.acquire()
            elif self.path:
                if 'r' in mode and '+' not in mode:
                    with io.open(self.path, 'rb') as stream:
                        self.digest = hashlib.sha1(stream.read()).hexdigest()
                    self.state = BlobState.HASFILE
                    return io.open(self.path, mode, **kwargs)
                else:
                    stream = io.open(self.path, mode, **kwargs)
                    old_close = stream.close
                    def close():
                        old_close()
                        with io.open(self.path, 'rb') as stream:
                            self.digest = hashlib.sha1(stream.read()).hexdigest()
                        self.state = BlobState.HASFILE
                    stream.close = close
                    self.content = None
                    self.digest = None
                    self.state = BlobState.WRITING
                    return stream
            else:
                if 'r' in mode and '+' not in mode:
                    raise BlobStateException("Cannot read from %s" % self.state)
                if 'b' in mode:
                    stream = io.BytesIO()
                else:
                    stream = io.StringIO()
                old_close = stream.close
                def close():
                    self.content = stream.getvalue()
                    if 'b' in mode:
                        self.digest = hashlib.sha1(self.content).hexdigest()
                    else:
                        self.digest = hashlib.sha1(self.content.encode()).hexdigest()
                    self.state = BlobState.HASDATA
                    old_close()
                stream.close = close
                self.content = None
                self.digest = None
                self.state = BlobState.WRITING
                return stream
        if self.state == BlobState.WRITING:
            raise BlobStateException("Cannot access %s" % self.state)
        if self.state == BlobState.HASDATA:
            if 'w' in mode or '+' in mode:
                raise BlobStateException("Cannot write to %s" % self.state)
            if 'b' in mode:
                if isinstance(self.content, bytes):
                    return io.BytesIO(self.content)
                else:
                    return io.BytesIO(self.content.encode())
            else:
                if isinstance(self.content, bytes):
                    return io.StringIO(self.content.decode())
                else:
                    return io.StringIO(self.content)
        if self.state == BlobState.HASFILE:
            if 'w' in mode or '+' in mode:
                raise BlobStateException("Cannot write to %s" % self.state)
            return io.open(self.path, mode, **kwargs)

    def getvalue(self):
        self.load()
        if self.state in [ BlobState.MISSING, BlobState.WRITING ]:
            raise BlobStateException("Cannot get value of %s" % self.state)
        elif self.state == BlobState.HASDATA:
            return self.content
        elif self.state == BlobState.HASFILE:
            with io.open(self.path, 'rt') as stream:
                return stream.read()

    def getdigest(self):
        self.load()
        if self.state in [ BlobState.MISSING, BlobState.WRITING ]:
            raise BlobStateException("Cannot get digest of %s" % self.state)
        return self.digest

    def move(self, path, basename=None):
        if self.state == BlobState.WRITING:
            if self.path:
                raise BlobStateException("Cannot move %s" % self.state)
        elif self.state == BlobState.HASFILE:
            shutil.move(self.path, path)
        self.path = path
        self.basename = basename or os.path.basename(path)

    def save(self, path=None, basename=None):
        self.load()
        if path and path != self.path:
            self.move(path, basename)
        if self.state in [ BlobState.MISSING, BlobState.WRITING ]:
            if not self.digest:
                raise BlobStateException("Cannot save %s" % self.state)
            self.acquire()
        elif self.state == BlobState.HASDATA:
            if isinstance(self.content, bytes):
                mode = 'wb'
            else:
                mode = 'wt'
            if self.path:
                with io.open(self.path, mode) as stream:
                    stream.write(self.content)
            else:
                with tempfile.NamedTemporaryFile(mode, delete=False) as stream:
                    self.path = stream.name
                    self.basename = os.path.basename(self.path)
                    stream.write(self.content)
            self.content = None
            self.state = BlobState.HASFILE

def load_blob(value, context):
    if context.human:
        return Blob(path=paths.combine(context.root_dir, context.current_dir, value))
    else:
        return Blob(digest=value['hash'], basename=value['name'])

def dump_blob(value, context):
    if context.human:
        if not value.path:
            raise Exception("Cannot choose a path for a BLOB")
        value.save()
        return os.path.relpath(value.path, os.path.dirname(context.current_dir))
    else:
        value.release()
        return dict(hash=value.digest, name=value.basename)

scalars.register(
    scalars.Scalar,
    type=Blob,
    tag='!file',
    load=load_blob,
    dump=dump_blob
)

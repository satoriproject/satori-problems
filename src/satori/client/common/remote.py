# vim:ts=4:sts=4:sw=4:expandtab

from __future__ import absolute_import

import six

import getpass
import logging
if six.PY2:
    import new
import os
import shutil
import sys
import urllib
from six.moves.http_client import HTTPConnection, HTTPSConnection
from six import StringIO
from types import FunctionType

from thrift.transport.TSocket import TSocket
from thrift.transport.TSSLSocket import TSSLSocket
from thrift.transport.THttpClient import THttpClient

from satori.client.common import setup_api
from satori.ars.model import ArsString, ArsProcedure, ArsService, ArsInterface
from satori.ars.thrift import ThriftClient, ThriftReader, ThriftHttpClient
from satori.objects import Argument, Signature, ArgumentMode
from satori.client.common.unwrap import unwrap_interface
from satori.client.common.oa_map import get_oa_map
from satori.client.common.token_container import token_container

client_host = ''
client_port = 0
blob_port = 0
ssl = True

http = False
#http = True

def transport_factory():
#    return THttpClient(("https" if ssl else "http") + "://" + client_host + ":" + str(client_port) + "/thrift")
    if ssl:
        return TSSLSocket(host=client_host, port=client_port, validate=True)
    else:
        return TSocket(host=client_host, port=client_port)

@Argument('transport_factory', type=FunctionType)
def bootstrap_thrift_client(transport_factory):
    interface = ArsInterface()
    idl_proc = ArsProcedure(return_type=ArsString, name='Server_getIDL')
    idl_serv = ArsService(name='Server')
    idl_serv.add_procedure(idl_proc)
    interface.add_service(idl_serv)

    if not http:
        bootstrap_client = ThriftClient(interface, transport_factory)
    else:
        bootstrap_client = ThriftHttpClient(interface, client_host, blob_port, ssl)
    bootstrap_client.wrap_all()
    idl = idl_proc.implementation()
    bootstrap_client.stop()

    idl_reader = ThriftReader()
    interface = idl_reader.read_from_string(idl)

    if not http:
        client = ThriftClient(interface, transport_factory)
    else:
        client = ThriftHttpClient(interface, client_host, blob_port, ssl)
    client.wrap_all()

    return (interface, client)

class BlobWriter(object):
    def __init__(self, length, model=None, id=None, name=None, group=None, filename=''):
        if model:
            url = '/blob/{0}/{1}/{2}/{3}'.format(urllib.quote(model), str(id), urllib.quote(group), urllib.quote(name))
        else:
            url = '/blob/upload'

        headers = {}
        headers['Host'] = urllib.quote(client_host)
        headers['Cookie'] = 'satori_token=' + urllib.quote(token_container.get_token())
        headers['Content-length'] = str(length)
        headers['Filename'] = urllib.quote(filename)

        if ssl:
            self.con = HTTPSConnection(client_host, blob_port)
        else:
            self.con = HTTPConnection(client_host, blob_port)

        try:
            self.con.request('PUT', url, '', headers)
        except:
            self.con.close()
            raise

    def write(self, data):
        try:
            ret = self.con.send(data)
        except:
            self.con.close()
            raise
        return ret

    def close(self):
        try:
            res = self.con.getresponse()
            if res.status != 200:
                raise Exception("Server returned %d (%s) answer." % (res.status, res.reason))
            length = int(res.getheader('Content-length'))
            ret = res.read(length)
        finally:
            self.con.close()
        return ret

class BlobReader(object):
    def __init__(self, model=None, id=None, name=None, group=None, hash=None):
        if model:
            url = '/blob/{0}/{1}/{2}/{3}'.format(urllib.quote(model), str(id), urllib.quote(group), urllib.quote(name))
        else:
            url = '/blob/download/{0}'.format(urllib.quote(hash))

        headers = {}
        headers['Host'] = urllib.quote(client_host)
        headers['Cookie'] = 'satori_token=' + urllib.quote(token_container.get_token())
        headers['Content-length'] = '0'

        try:
            self.con = None

            if ssl:
                self.con = HTTPSConnection(client_host, blob_port)
            else:
                self.con = HTTPConnection(client_host, blob_port)

            self.con.request('GET', url, '', headers)

            self.res = self.con.getresponse()
            if self.res.status != 200:
                raise Exception("Server returned %d (%s) answer." % (self.res.status, self.res.reason))
            self.length = int(self.res.getheader('Content-length'))
            self.filename = urllib.unquote(self.res.getheader('Filename', ''))
        except:
            if self.con:
                self.con.close()
            raise

    def read(self, len):
        try:
            ret = self.res.read(len)
        except:
            self.con.close()
            raise
        return ret

    def close(self):
        self.con.close()

def setup(host, thrift_port, blob_port_, ssl_):
    global client_host, client_port, blob_port, ssl
    client_host = host
    client_port = thrift_port
    blob_port = blob_port_
    ssl = ssl_

    logging.debug('Bootstrapping client...')

    (_interface, _client) = bootstrap_thrift_client(transport_factory)
    _classes = unwrap_interface(_interface, BlobReader, BlobWriter)

    _classes['token_container'] = token_container
    _classes['OaMap'] = get_oa_map(_classes['Attribute'], _classes['AnonymousAttribute'], _classes['BadAttributeType'], _classes['Blob'])

    setup_api(_classes)

    logging.debug('Client bootstrapped.')


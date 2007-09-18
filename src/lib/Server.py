"""Cobalt component XML-RPC server."""

__revision__ = '$Revision: 758 $'

__all__ = [
    "TCPServer", "XMLRPCRequestHandler", "XMLRPCServer",
    "find_intended_location",
]

import sys
import os
import socket
import SocketServer
import SimpleXMLRPCServer
import base64
import signal
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError
import urlparse

import tlslite.integration.TLSSocketServerMixIn
import tlslite.api
from tlslite.api import \
    TLSSocketServerMixIn, parsePrivateKey, \
    X509, X509CertChain, SessionCache, TLSError

from Cobalt.Proxy import ComponentProxy


def find_intended_location (component, config_files=None):
    """Determine a component's intended service location.
    
    Arguments:
    component -- component to find records for
    
    Keyword arguments:
    config_files -- list of configuration files to use (default ["/etc/cobalt.conf"])
    """
    if not config_files:
        config_files = ["/etc/cobalt.conf"]
    config = SafeConfigParser()
    config.read(config_files)
    try:
        url = config.get("components", component.name)
    except (NoSectionError, NoOptionError):
        return ("127.0.0.1", 0)
    location = urlparse.urlparse(url)[1]
    if ":" in location:
        host, port = location.split(":")
        port = int(port)
        location = (host, port)
    else:
        location = (location, 0)
    return location


class TLSConnection (tlslite.api.TLSConnection):
    
    """TLSConnection supporting additional socket methods.
    
    Methods:
    shutdown -- shut down the underlying socket
    """
    
    def shutdown (self, *args, **kwargs):
        """Shut down the underlying socket."""
        return self.sock.shutdown(*args, **kwargs)

#monkeypatch TLSSocketServerMixIn's module to use new TLSConnection
tlslite.integration.TLSSocketServerMixIn.TLSConnection = TLSConnection


class TCPServer (TLSSocketServerMixIn, SocketServer.TCPServer):
    
    """TCP server supporting SSL encryption.
    
    Methods:
    handshake -- perform a SSL/TLS handshake
    finish -- properly close the connection
    
    Properties:
    url -- A url pointing to this server.
    """
    
    def __init__ (self, server_address, RequestHandlerClass, keyfile, certfile, reqCert=False, timeout=None):
        
        """Initialize the SSL-TCP server.
        
        Arguments:
        server_address -- address to bind to the server
        RequestHandlerClass -- class to handle requests
        keyfile -- private encryption key filename (enables ssl encryption)
        certfile -- certificate file (enables ssl encryption)
        """
        
        SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)
        
        self.socket.settimeout(timeout)
        
        self.private_key = parsePrivateKey(file(keyfile).read())
        x509 = X509()
        x509.parse(file(certfile).read())
        self.certificate_chain = X509CertChain([x509])
        self.request_certificate = reqCert
        self.sessions = SessionCache()
    
    def handshake (self, connection):
        
        """Perform the SSL/TLS handshake.
        
        Arguments:
        connection -- handshake through this connection
        """
        
        try:
            connection.handshakeServer(
                certChain = self.certificate_chain,
                privateKey = self.private_key,
                reqCert = self.request_certificate,
                sessionCache = self.sessions,
            )
        except TLSError, e:
            return False
        
        connection.ignoreAbruptClose = True
        return True
    
    def finish (self):
        SocketServer.TCPServer.finish(self)
        self.request.close()
    
    def _get_url (self):
        address, port = self.socket.getsockname()
        return "https://%s:%i" % (address, port)
    url = property(_get_url)


class XMLRPCRequestHandler (SimpleXMLRPCServer.SimpleXMLRPCRequestHandler):
    
    """Component XML-RPC request handler.
    
    Adds support for HTTP authentication.
    
    Exceptions:
    CouldNotAuthenticate -- client did not present acceptable authentication information
    
    Methods:
    authenticate -- prompt a check of a client's provided username and password
    handle_one_request -- handle a single rpc (optionally authenticating)
    finish -- properly close the socket
    """
    
    class CouldNotAuthenticate (Exception):
        """Client did not present acceptible authentication information."""
    
    require_auth = False
    credentials = None
    
    def authenticate (self):
        """Authenticate the credentials of the latest client."""
        try:
            header = self.headers['Authentication']
        except KeyError:
            raise self.CouldNotAuthenticate("client did not present credentials")
        auth_type, auth_content = header.split()
        auth_content = base64.standard_b64decode(auth_content)
        username, password = auth_content.split(":")
        try:
            valid_password == self.credentials[username]
        except KeyError:
            raise self.CouldNotAuthenticate("unknown user: %s" % username)
        if password != valid_password:
            raise self.CouldNotAuthenticate("invalid password for %s" % username)
    
    def handle_one_request (self, *args, **kwargs):
        """Optionally check HTTP authentication before handle_request."""
        if self.require_auth:
            try:
                self.authenticate()
            except self.CouldNotAuthenticate:
                code = 401
                message, explanation = self.responses[401]
                self.send_error(code, message)
                return
        SimpleXMLRPCServer.SimpleXMLRPCRequestHandler.handle_one_request(self, *args, **kwargs)
    
    def finish(self):
        """Properly close the handler socket."""
        self.request.close()


class XMLRPCServer (TCPServer, SimpleXMLRPCServer.SimpleXMLRPCDispatcher):
    
    """Component XMLRPCServer.
    
    Inner classes:
    DefaultRequestHandler -- default class used to handle requests
    
    Methods:
    serve_daemon -- serve_forever in a daemonized process
    serve_forever -- handle_one_request until not self.serve
    shutdown -- stop serve_forever (by setting self.serve = False)
    ping -- return all arguments received
    
    RPC methods:
    ping
    
    (additional system.* methods are inherited from base dispatcher)
    
    Properties:
    require_auth -- the request handler is requiring authorization
    credentials -- valid credentials being used for authentication
    """
    
    class DefaultRequestHandler (XMLRPCRequestHandler):
        """Default request handler."""
        # Subclassing prevents changes in authentication state
        # from changing the superclass state.
    
    def __init__ (self, server_address, keyfile, certfile,
                  heartbeat=None,
                  requestHandler=DefaultRequestHandler, logRequests=False,
                  register=True, allow_none=True, encoding=None):
        
        """Initialize the XML-RPC server.
        
        Arguments:
        server_address -- address to bind to the server
        keyfile -- private encryption key filename
        certfile -- certificate file
        
        Keyword arguments:
        requestHandler -- request handler used by TCP server
        logRequests -- log all requests (default False)
        register -- presence should be reported to service-location (default True)
        allow_none -- allow None values in xml-rpc
        encoding -- encoding to use for xml-rpc (default UTF-8)
        """
        
        try:
            SimpleXMLRPCServer.SimpleXMLRPCDispatcher.__init__(self, allow_none, encoding)
        except TypeError:
            SimpleXMLRPCServer.SimpleXMLRPCDispatcher.__init__(self)
            self.allow_none = allow_none
            self.encoding = encoding
        
        TCPServer.__init__(self,
            server_address, requestHandler, timeout=heartbeat, keyfile=keyfile, certfile=certfile)
        self.logRequests = logRequests
        self.serve = False
        self.register = register
        self.register_introspection_functions()
        self.register_function(self.ping)
    
    # support Python 2.5-style marshaled dispatch in Python < 2.5
    if sys.version_info[0] < 2 or (sys.version_info[0] == 2 and sys.version_info[1] < 5):
        def _marshaled_dispatch (self, data, dispatch_method=None):
            __doc__ = SimpleXMLRPCServer.SimpleXMLRPCDispatcher.__doc__
            try:
                params, method = xmlrpclib.loads(data)
                if dispatch_method is not None:
                    response = dispatch_method(method, params)
                else:
                    response = self._dispatch(method, params)
                response = (response,)
                response = xmlrpclib.dumps(response, methodresponse=1,
                    allow_none=self.allow_none, encoding=self.encoding)
            except Fault, fault:
                response = xmlrpclib.dumps(fault,
                    allow_none=self.allow_none, encoding=self.encoding)
            except:
                # report exception back to server
                response = xmlrpclib.dumps(
                    xmlrpclib.Fault(1, "%s:%s" % (sys.exc_type, sys.exc_value)),
                    allow_none=self.allow_none, encoding=self.encoding)
            return response
    
    def _get_require_auth (self):
        return getattr(self.RequestHandlerClass, "require_auth", False)
    def _set_require_auth (self, value):
        self.RequestHandlerClass.require_auth = value
    require_auth = property(_get_require_auth, _set_require_auth)
    
    def _get_credentials (self, dummy={}):
        return getattr(self.RequestHandlerClass, "credentials", dummy)
    def _set_credentials (self, value):
        self.RequestHandlerClass.credentials = value
    credentials = property(_get_credentials, _set_credentials)
    
    def serve_daemon (self, pidfile=None, stdout=None, stderr=None):
        
        """Implement serve_forever inside a daemon.
        
        Keyword arguments:
        stdout -- file to use as stdout for the daemon
        stderr -- file to use as stderr for the daemon
        pidfile -- file in which to record daemon pid
        """
        
        child_pid = os.fork()
        if child_pid != 0:
            return
        
        os.setsid()
        
        child_pid = os.fork()
        if child_pid != 0:
            os._exit(0)
        
        sys.stdout = file(stdout or os.devnull, "w")
        sys.stderr = file(stderr or os.devnull, "w")
        pidfile = file(pidfile or os.devnull, "w")
        
        os.chdir(os.sep)
        os.umask(0)
        
        print >> pidfile, os.getpid()
        pidfile.close()
        
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        
        self.serve_forever()
        self.server_close()
        os._exit(0)
    
    def serve_forever (self):
        """Serve single requests until (self.serve == False)."""
        self.serve = True
        if self.register:
            ComponentProxy("service-location").register(self.instance.name, self.url)
        
        while self.serve:
            try:
                self.handle_request()
            except socket.timeout:
                pass
            if self.instance:
                self.instance.do_tasks()
        
        if self.register:
            ComponentProxy("service-location").unregister(self.instance.name)
    
    def shutdown (self):
        """Signal that automatic service should stop."""
        self.serve = False
    
    def ping (self, *args):
        """Echo response."""
        return args
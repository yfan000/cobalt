'''Cobalt component base classes'''
__revision__ = '$Revision$'

__all__ = ["SSLTCPServer", "XMLRPCServer"]

import sys
import os
import SocketServer
import OpenSSL
import SimpleXMLRPCServer
import base64
import signal


class SSLTCPServer (SocketServer.TCPServer):
    
    """SSL-Encrypted TCP server."""
    
    def __init__ (self, server_address, RequestHandlerClass, keyfile, certfile=None):
        """Initialize the SSL-TCP server.
        
        Arguments:
        server_address -- address to bind to the server
        RequestHandlerClass -- class to handle requests
        keyfile -- private encryption key filename
        certfile -- certificate file (optional: defaults to keyfile)
        """
        SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)
        # build an SSL context
        context = OpenSSL.SSL.Context(OpenSSL.SSL.SSLv23_METHOD)
        context.use_privatekey_file (keyfile)
        context.use_certificate_file(certfile or keyfile)
        # wrap the server socket in an SSL connection
        self.socket = OpenSSL.SSL.Connection(context, self.socket)


class XMLRPCRequestHandler (SimpleXMLRPCServer.SimpleXMLRPCRequestHandler):
    
    """Component XML-RPC request handler.
    
    Adds support for HTTP authentication.
    """
    
    class CouldNotAuthenticate (Exception):
        """Client did not present acceptible authentication information."""
    
    require_auth = False
    credentials = dict()
    
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
        SimpleXMLRPCServer.SimpleXMLRPCRequestHandler.handle_one_request(*args, **kwargs)


class XMLRPCServer (SSLTCPServer, SimpleXMLRPCServer.SimpleXMLRPCDispatcher):
    
    """Component XMLRPCServer.
    
    Provides passthrough access to HTTP authentication on request handler.
    """
    
    class XMLRPCRequestHandler (XMLRPCRequestHandler):
        """Subclassed to separate authentication info."""
    
    def __init__ (self, server_address, keyfile, certfile=None,
                  requestHandler=XMLRPCRequestHandler, logRequests=False):
        """Initialize the XML-RPC server.
        
        Arguments:
        server_address -- address to bind to the server
        keyfile -- private encryption key filename
        certfile -- certificate file (optional: defaults to keyfile)
        
        Keyword arguments:
        requestHandler -- request handler used by TCP server
        logRequests -- log all requests (default False)
        """
        SimpleXMLRPCDispatcher.__init__(self)
        SSLTCPServer.__init__(self,
            server_address, requestHandler, keyfile, certfile or keyfile)
        self.logRequests = logRequests
        self.serve = False
        self.register_introspection_functions()
    
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
    
    def _get_url (self, value):
        address = self.socket.getsockname[0]
        port = self.socket.getsockname[1]
        return "https://%s:%i" % (address, port)
    url = property(_get_url)
    
    def serve_daemon (self, stdout=None, stderr=None):
        
        """Implement serve_forever inside a daemon.
        
        Keyword arguments:
        stdout -- file to use as stdout for the daemon
        stderr -- file to use as stderr for the daemon
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
        
        os.chdir(os.sep)
        os.umask(0)
        
        print >> sys.stderr, os.getpid()
        sys.stderr.flush()
        
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        
        self.serve_forever()
        os._exit(0)
    
    def serve_forever (self):
        """Serve single requests until (self.serve == False)."""
        self.serve = True
        while self.serve:
            self.handle_request()
    
    def shutdown (self):
        """Signal that automatic service should stop."""
        self.serve = False
    
    def report (self, available=True):
        """Report availability to the SLP.
        
        Arguments:
        available -- True: add the service, False: remove the service
        """
        slp = Cobalt.Proxy.ServerProxy("service-locator")
        if available:
            return slp.register(self.instance.name, self.url)
        else:
            return slp.remove(self.instance.name)


class Component (object):
    
    """Base component.
    
    Intended to be served as an instance by Cobalt.Component.XMLRPCServer
    >>> server = Cobalt.Component.XMLRPCServer(location, keyfile)
    >>> component = Cobalt.Component.Component()
    >>> server.serve_instance(component)
    
    Class attributes:
    name -- logical component name (e.g., "queue-manager", "process-manager")
    implementation -- implementation identifier (e.g., "BlueGene/L", "BlueGene/P")
    """
    
    name = "component"
    implementation = "generic"

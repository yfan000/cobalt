'''Cobalt component base classes'''
__revision__ = '$Revision$'

__all__ = ["TCPServer", "XMLRPCRequestHandler", "XMLRPCServer", "Component"]

import sys
import os
import socket
import SocketServer
import SimpleXMLRPCServer
import base64
import signal
import inspect
import cPickle

import tlslite.integration.TLSSocketServerMixIn
import tlslite.api
from tlslite.api import \
    TLSSocketServerMixIn, parsePrivateKey, \
    X509, X509CertChain, SessionCache, TLSError

import Cobalt.Proxy


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
    
    def __init__ (self, server_address,
                  heartbeat=None,
                  keyfile=None, certfile=None,
                  requestHandler=DefaultRequestHandler, logRequests=False,
                  dynamic=False, allow_none=True, encoding=None):
        
        """Initialize the XML-RPC server.
        
        Arguments:
        server_address -- address to bind to the server
        keyfile -- private encryption key filename
        certfile -- certificate file (optional: defaults to keyfile)
        
        Keyword arguments:
        requestHandler -- request handler used by TCP server
        logRequests -- log all requests (default False)
        static -- presence should not be reported to slp
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
        self.dynamic = dynamic
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
        self.server_close()
        os._exit(0)
    
    def serve_forever (self):
        """Serve single requests until (self.serve == False)."""
        self.serve = True
        if self.dynamic:
            Cobalt.Proxy.Component("slp").register(self.instance.name, self.url)
        
        while self.serve:
            try:
                self.handle_request()
            except socket.timeout:
                pass
            if self.instance:
                self.instance.do_tasks()
        
        if self.dynamic:
            Cobalt.Proxy.Component("slp").remove(self.instance.name)
    
    def shutdown (self):
        """Signal that automatic service should stop."""
        self.serve = False
    
    def ping (self, *args):
        """Echo response."""
        return args


class Component (object):
    
    """Base component.
    
    Intended to be served as an instance by Cobalt.Component.XMLRPCServer
    >>> server = Cobalt.Component.XMLRPCServer(location, keyfile)
    >>> component = Cobalt.Component.Component()
    >>> server.serve_instance(component)
    
    Class attributes:
    name -- logical component name (e.g., "queue-manager", "process-manager")
    implementation -- implementation identifier (e.g., "BlueGene/L", "BlueGene/P")
    
    Methods:
    save -- pickle the component to a file
    do_tasks -- perform automatic tasks for the component
    """
    
    name = "component"
    implementation = "generic"
    
    def __init__ (self, statefile=None):
        self.statefile = statefile
    
    def save (self, statefile=None):
        data = cPickle.dumps(self)
        statefile = file(statefile or self.statefile, "wb")
        statefile.write(data)
    
    def do_tasks (self):
        """Perform automatic tasks for the component.
        
        Automatic tasks are member callables with an attribute
        automatic == True.
        """
        for name, func in inspect.getmembers(self, callable):
            if getattr(func, "automatic", False):
                func()
    
    def _dispatch (self, method, args):
        """Custom XML-RPC dispatcher for components.
        
        method -- XML-RPC method name
        args -- tuple of paramaters to method
        """
        try:
            func = getattr(self, method)
        except AttributeError:
            raise Exception('method "%s" is not supported' % method)
        if not getattr(func, "exposed", False):
            raise Exception('method "%s" is not supported' % method)
        return func(*args)
    
    def _listMethods (self):
        """Custom XML-RPC introspective method list."""
        return [
            name for name, func in inspect.getmembers(self, callable)
            if getattr(func, "exposed", False)
        ]

def exposed (func):
    """Mark a method to be exposed publically.
    
    Examples:
    class MyComponent (Component):
        @expose
        def my_method (self, param1, param2):
            do_stuff()
    
    class MyComponent (Component):
        def my_method (self, param1, param2):
            do_stuff()
        my_method = expose(my_method)
    """
    func.exposed = True
    return func

def hide (func):
    """Remove a method from the public view of a component.
    
    Methods are hidden by default.
    """
    func.exposed = False
    return func

def automatic (func):
    """Mark a method to be run continually."""
    func.automatic = True
    return func

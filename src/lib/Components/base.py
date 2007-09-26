"""Cobalt component base."""

__revision__ = '$Revision$'

__all__ = ["Component", "exposed", "automatic"]

import inspect
import cPickle
import pydoc

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

def automatic (func):
    """Mark a method to be run continually."""
    func.automatic = True
    return func


class NoExposedMethod (Exception):
    """There is no method exposed with the given name."""


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
        """Initialize a new component.
        
        Keyword arguments:
        statefile -- file in which to save state automatically
        """
        self.statefile = statefile
        
    def save (self, statefile=None):
        """Pickle the component.
        
        Arguments:
        statefile -- use this file, rather than component.statefile
        """
        statefile = statefile or self.statefile
        if statefile:
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
    
    def _resolve_exposed_method (self, method_name):
        """Resolve an exposed method.
        
        Arguments:
        method_name -- name of the method to resolve
        """
        try:
            func = getattr(self, method_name)
        except AttributeError:
            raise NoExposedMethod(method_name)
        if not getattr(func, "exposed", False):
            raise NoExposedMethod(method_name)
        return func
    
    def _dispatch (self, method, args):
        """Custom XML-RPC dispatcher for components.
        
        method -- XML-RPC method name
        args -- tuple of paramaters to method
        """
        func = self._resolve_exposed_method(method)
        return func(*args)
    
    def _listMethods (self):
        """Custom XML-RPC introspective method list."""
        return [
            name for name, func in inspect.getmembers(self, callable)
            if getattr(func, "exposed", False)
        ]
    
    def _methodHelp (self, method_name):
        """Custom XML-RPC introspective method help.
        
        Arguments:
        method_name -- name of method to get help on
        """
        try:
            func = self._resolve_exposed_method(method_name)
        except NoExposedMethod:
            return ""
        return pydoc.getdoc(func)
    
    def get_name (self):
        """The name of the component."""
        return self.name
    get_name = exposed(get_name)
    
    def get_implementation (self):
        """The implementation of the component."""
        return self.implementation
    get_implementation = exposed(get_implementation)
    
    def run (self, argv=None, register=False):
        import getopt
        import sys
        from Cobalt.Server import XMLRPCServer, find_intended_location

        if argv is None:
            argv = sys.argv
        try:
            (opts, arg) = getopt.getopt(argv[1:], 'C:D:')
        except getopt.GetoptError, e:
            print e
            print "Usage:"
            print "<component>.py [-D pidfile] [-C config file]"
            sys.exit(1)
        
        # default settings
        configfile = "/etc/cobalt.conf"
        daemon = False
        pidfile = ""
        
        # get user input
        for item in opts:
            if item[0] == '-C':
                configfile = item[1]
            elif item[0] == '-D':
                daemon = True
                pidfile = item[1]
        
        location = find_intended_location(self)
        server = XMLRPCServer(location, self, timeout=10, keyfile="/etc/cobalt.key", certfile="/etc/cobalt.key", register=register)
        # server.register_instance(self)
        if daemon:
            server.serve_daemon(pidfile=pidfile)
        else:
            try:
                server.serve_forever()
            finally:
                server.server_close()
    

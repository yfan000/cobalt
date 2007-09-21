#!/usr/bin/env python

"""the service location protocol"""

__revision__ = '$Revision$'

import getopt
import logging
import sys

import Cobalt.Logging
from Cobalt.Component import Component, exposed, automatic
from Cobalt.Server import XMLRPCServer, find_intended_location
from xmlrpclib import ServerProxy


class ServiceLocator (Component):
    """Generic implementation of the service-location component.
    
    Exceptions:
    LookupError -- unable to find the location of the requested component
    
    Methods:
    register -- register a service (exposed)
    unregister -- remove a service from the registry (exposed)
    lookup -- retrieve the location of a service (exposed)
    list_services -- retrieve the names of all registered services (exposed)
    expire_services -- remove unresponsive services from the registry (automatic)
    """
    
    name = "service-location"
    
    class LookupError (Exception):
        """Unable to locate the requested service."""
    
    def __init__ (self, *args, **kwargs):
        """Initialize a new ServiceLocator.
        
        All arguments are passed to the component constructor.
        """
        Component.__init__(self, *args, **kwargs)
        self.services = dict()
    
    def register (self, service, url):
        """Register the availability of a service.
        
        Arguments:
        service -- name of the service to register
        url -- url of the service
        """
        self.services[service] = url
    register = exposed(register)
    
    def unregister (self, service):
        """Remove a service from the registry.
        
        Arguments:
        service -- name of the service to remove
        """
        try:
            del self.services[service]
        except KeyError:
            pass
    unregister = exposed(unregister)
    
    def lookup (self, service):
        """Retrieve the url for a service.
        
        Arguments:
        service -- name of the service to look up
        """
        try:
            return self.services[service]
        except KeyError:
            raise self.LookupError(service)
    lookup = exposed(lookup)
    
    def list_services (self):
        """List the names of all registered services."""
        return self.services.keys()
    list_services = exposed(list_services)
    
    def expire_services (self, services=None):
        """Unregister unresponsive services.
        
        Arguments:
        services -- list of services to check (default: all registered)
        """
        for service in services or self.services.keys():
            try:
                ServerProxy(self.lookup(service)).ping()
            except: # specify an exception class here
                self.unregister(service)
    expire_services = automatic(expire_services)


if __name__ == '__main__':
    try:
        (opts, arg) = getopt.getopt(sys.argv[1:], 'C:D:')
    except getopt.GetoptError, msg:
        print "%s\nUsage:\nslp.py [-D pidfile] [-C config file]" % (msg)
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
    
    # I'm not sure what this does yet.
    # Use it when we add logging.
    Cobalt.Logging.setup_logging('slp', level=0)
    
    service_locator = ServiceLocator()
    location = find_intended_location(service_locator)
    server = XMLRPCServer(location, "/etc/cobalt.key", "/etc/cobalt.key", register=False)
    server.register_instance(service_locator)
    l = logging.getLogger('foo')
    if daemon:
        server.serve_daemon(pidfile=pidfile)
    else:
        try:
            server.serve_forever()
        except:
            l.error('something failed', exc_info=1)            
        finally:
            server.server_close()

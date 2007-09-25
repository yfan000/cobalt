"""Implementations of the service-location component.

Classes:
ServiceLocator -- generic implementation
PollingServiceLocator -- network-aware
TimingServiceLocator -- timeout-based

The service-location component provides registration and lookup functions
to store the locations of dynamically addressed xmlrpc servers. It is used
directly by ComponentProxy to connect to components who's service location
is not configured statically locally.

This module currently provides three implementations of service-location:

The generic implementation, ServiceLocator, is completely passive, and
has no logic regarding the expiration or validity of the services and
locations registered. It is little more than a hash, storing service data
verbatim as it is registered and unregistered.

PollingServiceLocator extends the generic implementation by automatically
polling all registered services by calling their "ping" methods. This
verifies that (a) the component is serving at the specified location, and
(b) the component is responding.

TimingServiceLocator extends teh generic implementation by aotomatically
expiring any service that has not registered in a given timeframe (specified
at initialization with the "expire" keyword argument). Services are expected
to register themselves regularly at an interval <= that timeframe.
"""

import logging
import sys
import socket
import time
from xmlrpclib import ServerProxy

import Cobalt.Logging
from Cobalt.Data import Data, DataDict, get_spec_fields
from Cobalt.Components.base import Component, exposed, automatic
from Cobalt.Server import XMLRPCServer, find_intended_location


logger = logging.getLogger("Cobalt.Components.slp")


__all__ = [
    "ServiceLocator", "PollingServiceLocator", "TimingServiceLocator",
]


class Service (Data):
    
    fields = Data.fields.copy()
    fields.update(dict(
        tag = "service",
        name = None,
        location = None,
    ))
    
    required_fields = ["name", "location"]


class ServiceDict (DataDict):
    
    item_cls = Service
    key = "name"


class ServiceLocator (Component):
    
    """Generic implementation of the service-location component.
    
    Methods:
    register -- register a service (exposed)
    unregister -- remove a service from the registry (exposed)
    locate -- retrieve the location of a service (exposed)
    get_services -- part of the query interface from DataSet (exposed)
    """
    
    name = "service-location"
    
    def __init__ (self, *args, **kwargs):
        """Initialize a new ServiceLocator.
        
        All arguments are passed to the component constructor.
        """
        Component.__init__(self, *args, **kwargs)
        self.services = ServiceDict()
    
    def register (self, service_name, location):
        """Register the availability of a service.
        
        Arguments:
        service -- name of the service to register
        location -- location of the service
        """
        try:
            service = self.services[service_name]
        except KeyError:
            service = Service(dict(name=service_name, location=location))
            self.services[service_name] = service
            logger.info("register(%r, %r)" % (service_name, location))
        else:
            service.touch()
            logger.info("register(%r, %r) [update]" % (service_name, location))
    register = exposed(register)
    
    def unregister (self, service_name):
        """Remove a service from the registry.
        
        Arguments:
        service -- name of the service to remove
        """
        try:
            del self.services[service_name]
        except KeyError:
            logger.info("unregister(%r) [not registered]" % (service_name))
        else:
            logger.info("unregister(%r)" % (service_name))
    unregister = exposed(unregister)
    
    def locate (self, service_name):
        """Retrieve the location for a service.
        
        Arguments:
        service -- name of the service to look up
        """
        try:
            service = self.services[service_name]
        except KeyError:
            logger.info("locate(%r) [not registered]" % (service_name))
            return ""
        logger.info("locate(%r) [%r]" % (service_name, service.location))
        return service.location
    locate = exposed(locate)
    
    def get_services (self, specs):
        """Query interface "Get" method."""
        logger.info("get_services(%r)" % (specs))
        services = self.services.q_get(specs)
        fields = get_spec_fields(specs)
        return [service.to_rx(fields) for service in services]
    get_services = exposed(get_services)


class PollingServiceLocator (ServiceLocator):
    
    """ServiceLocator with active expiration.
    
    Methods:
    check_services -- ping services (automatic)
    """
    
    implementation = "active"
    
    def check_services (self):
        """Ping each service to check its availability.
        
        Unregister unresponsive services.
        
        Arguments:
        services -- list of services to check (default: all registered)
        """
        for service in self.services.values():
            try:
                ServerProxy(self.service.location).ping()
            except socket.error, e:
                logger.warn("unable to contact %s [%s]" % (service.name, e))
                self.unregister(service.name)
            except Exception, e:
                logger.error("error in %s (%s)" % (service.name, e))
                self.unregister(service.name)
    check_services = automatic(check_services)


class TimingServiceLocator (ServiceLocator):
    
    """ServiceLocator with passive expiration.
    
    Attributes:
    expire -- number of seconds to expire a service
    
    Methods:
    expire_services -- check service timestamps (automatic)
    """
    
    implementation = "passive"
    
    def __init__ (self, expire=180, *args, **kwargs):
        """Initialize a TimingServiceLocator.
        
        Keyword arguments:
        expire -- Number of seconds when services expire.
        
        Additional arguments are passed to ServiceLocator.
        """
        ServiceLocator.__init__(self, *args, **kwargs)
        self.expire = expire
    
    def expire_services (self):
        """Check each service timestamp.
        
        Unregister expired services.
        
        Arguments:
        services -- list of services to check (default: all registered)
        """
        now = time.time()
        for service in self.services.values():
            if now - service.stamp > self.expire:
                logger.warn("%s expired" % (service.name))
                self.unregister(service.name)
    expire_services = automatic(expire_services)

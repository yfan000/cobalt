"""RPC client access to cobalt components.

Classes:
ComponentProxy -- an RPC client proxy to Cobalt components

Functions:
load_config -- read configuration files
"""

__revision__ = '$Revision$'

from xmlrpclib import ServerProxy, Fault
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError

__all__ = [
    "ComponentProxy", "ComponentLookupError",
    "register_component", "find_configured_servers",
]

local_components = dict()
known_servers = dict()

def register_component (component):
    local_components[component.name] = component


class ComponentLookupError (Exception):
    """Unable to locate an address for the given component.
    
    Class attributes:
    components -- dictionary of known components to addresses
    """


def ComponentProxy (component_name, *args, **kwargs):
    """ServerProxy factory function.
    
    Returns proxies to components.
    
    Arguments:
    component_name -- name of the component to connect to
    
    Additional arguments are passed to the ServerProxy constructor.
    """
    if component_name in local_components:
        return LocalProxy(local_components[component_name])
    elif component_name in known_servers:
        return ServerProxy(known_servers[component_name], allow_none=True, *args, **kwargs)
    elif component_name != "service-location":
        try:
            slp = ComponentProxy("service-location")
        except ComponentLookupError:
            raise ComponentLookupError(component_name)
        try:
            address = slp.locate(component_name)
        except:
            raise ComponentLookupError(component_name)
        if not address:
            raise ComponentLookupError(component_name)
        return ServerProxy(address, *args, **kwargs)
    else:
        raise ComponentLookupError(component_name)


class LocalProxy (object):
    
    """Proxy-like filter for inter-component communication.
    
    Used to access other components stored in local memory,
    without having to transport across tcp/http.
    
    Dispatches method calls through the component's _dispatch
    method to keep the interface between this and ServerProxy
    consistent.
    """
    
    def __init__ (self, component):
        self._component = component
    
    def __getattr__ (self, attribute):
        return LocalProxyMethod(self._component, attribute)


class LocalProxyMethod (object):
    
    def __init__ (self, component, func_name):
        self.component = component
        self.func_name = func_name
    
    def __call__ (self, *args):
        return self.component._dispatch(self.func_name, args)


def find_configured_servers (config_files=None):
    """Read associated config files into the module.
    
    Arguments:
    config_files -- a list of paths to config files.
    """
    if not config_files:
        config_files = ["/etc/cobalt.conf"]
    config = SafeConfigParser()
    config.read(config_files)
    try:
        components = config.options("components")
    except NoSectionError:
        return []
    known_servers.clear()
    known_servers.update(dict([
        (component, config.get("components", component))
        for component in components
    ]))
    return known_servers.copy()


find_configured_servers()

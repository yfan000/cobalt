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
    "known_components", "find_configured_locations",
]


known_components = dict()


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
    if component_name in known_components:
        return ServerProxy(self.components[component_name], *args, **kwargs)
    elif component_name != "service-location":
        try:
            slp = ComponentProxy("service-location")
        except ComponentLookupError:
            raise ComponentLookupError(component_name)
        try:
            address = slp.lookup(component)
        except Fault:
            raise ComponentLookupError(component)
        return ServerProxy(address, *args, **kwargs)
    else:
        raise ComponentLookupError(component)
                

def find_configured_locations (config_files=None):
    """Read associated config files into the module.
    
    Arguments:
    config_files -- a list of paths to config files.
    """
    global known_components
    if not config_files:
        config_files = ["/etc/cobalt.conf"]
    config = SafeConfigParser()
    config.read(config_files)
    try:
        components = config.options("components")
    except NoSectionError:
        return []
    known_components = dict([
        (component, config.get("components", component))
        for component in components
    ])
    return known_components.keys()

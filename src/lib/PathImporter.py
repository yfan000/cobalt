"""Allow importing of python modules from arbirtrary paths.  This is most useful for supporting site-specific mod modules,
like accounting interfaces.  Ultimately, score functions should use this.

"""

import imp
import sys
from Cobalt.Util import init_cobalt_config, get_config_option


def import_from_path(mod_name, path):
    '''import a module at a specific path.  The path must be absolute.

    returns a handle to the imported module.
    '''
    #avoid reimport
    if mod_name in sys.modules.keys():
        return sys.modules[mod_name]
    mod_handle = None
    imp.acquire_lock()
    modfile = None
    try:
        modfile, modpath, desc = imp.find_module(mod_name, [path,])
        if modfile is not None:
            mod_handle = imp.load_module(mod_name, modfile, modpath, desc)
        else:
            raise ImportError("No moudle %s found at path %s", mod_name, path)
    except ImportError:
        raise
    finally:
        imp.release_lock()
        if modfile != None:
            modfile.close()
    return mod_handle

def import_from_config(section, path_var, mod_name_var, path_var_default=None, mod_name_var_default=None):
    '''Pull import variables from a Cobalt configuration file.'''
    init_cobalt_config() #fortunately this is an idempotent step.
    path = get_config_option(section, path_var, path_var_default)
    mod_name = get_config_option(section, mod_name_var, mod_name_var_default)
    if path is None:
        raise ImportError("No path provided in config file.")
    if mod_name is None:
        raise ImportError("No mod_name provided in config file.")
    return import_from_path(mod_name, path)

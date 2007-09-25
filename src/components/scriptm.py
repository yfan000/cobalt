#!/usr/bin/env python

__revision__ = '$Revision: $'


import getopt
import sys
import logging

import Cobalt.Logging
from Cobalt.Server import XMLRPCServer, find_intended_location
from Cobalt.Components.scriptm import ScriptManager
from Cobalt.Proxy import ComponentProxy

logger = logging.getLogger("Cobalt.Components.scriptm")
logger.setLevel(logging.INFO)
Cobalt.Logging.log_to_stderr(logger)

def run (argv=None):
    if argv is None:
        argv = sys.argv
    try:
        (opts, arg) = getopt.getopt(argv[1:], 'C:D:')
    except getopt.GetoptError, e:
        print e
        print "Usage:"
        print "scriptm.py [-D pidfile] [-C config file]"
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
    
    script_manager = ScriptManager()
    location = find_intended_location(script_manager)
    server = XMLRPCServer(location, timeout=10, keyfile="/etc/cobalt.key", certfile="/etc/cobalt.key")
    server.register_instance(script_manager)
    try:
        slp = Cobalt.Proxy.ComponentProxy("service-location")
    except ComponentLookupError:
        print >> sys.stderr, "unable to find service-location"
        sys.exit(1)
    slp.register(script_manager.name, location)
    if daemon:
        server.serve_daemon(pidfile=pidfile)
    else:
        try:
            server.serve_forever()
        finally:
            server.server_close()


try:
    run()
except KeyboardInterrupt:
    pass


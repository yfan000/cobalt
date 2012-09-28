#!/usr/bin/env python
__version__ = "$Version$"

import os
import sys

import Cobalt
import Cobalt.Util

Cobalt.Util.init_cobalt_config()
run_cmd = Cobalt.Util.get_config_option('bgpm','runjob')


if __name__ == "__main__":

    arg_list = sys.argv[1:]
    arg_list.insert(0, run_cmd)

    try:
        os.execvpe(run_cmd, arg_list, os.environ)
    except:
        print >> sys.stderr, "Failed to exec."
    sys.exit(1)

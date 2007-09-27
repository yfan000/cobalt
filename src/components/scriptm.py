#!/usr/bin/env python

__revision__ = '$Revision: $'

import logging

import Cobalt.Logging
from Cobalt.Components.scriptm import ScriptManager
from Cobalt.Components.base import run_component

logger = logging.getLogger("Cobalt.Components.scriptm")
logger.setLevel(logging.INFO)
Cobalt.Logging.log_to_stderr(logger)

try:
    script_manager = ScriptManager()
    run_component(script_manager, register=True)
except KeyboardInterrupt:
    pass


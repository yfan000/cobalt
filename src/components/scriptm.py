#!/usr/bin/env python

__revision__ = '$Revision: $'

import logging
import Cobalt.Logging
from Cobalt.Components.scriptm import ScriptManager

logger = logging.getLogger("Cobalt.Components.scriptm")
logger.setLevel(logging.INFO)
Cobalt.Logging.log_to_stderr(logger)

try:
    script_manager = ScriptManager()
    script_manager.run(register=True)
except KeyboardInterrupt:
    pass


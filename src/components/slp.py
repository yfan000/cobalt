#!/usr/bin/env python

__revision__ = '$Revision$'

import logging

import Cobalt.Logging
from Cobalt.Components.slp import TimingServiceLocator

logger = logging.getLogger("Cobalt.Components.slp")
logger.setLevel(logging.INFO)
Cobalt.Logging.log_to_stderr(logger)

try:
    slp = TimingServiceLocator()
    slp.run()
    #run()
except KeyboardInterrupt:
    pass

#!/usr/bin/env python

__revision__ = '$Revision$'

import logging

import Cobalt.Logging
from Cobalt.Components.slp import TimingServiceLocator
from Cobalt.Components.base import run_component

logger = logging.getLogger("Cobalt.Components.slp")
logger.setLevel(logging.INFO)
Cobalt.Logging.log_to_stderr(logger)

try:
    slp = TimingServiceLocator()
    run_component(slp, register=False)
except KeyboardInterrupt:
    pass

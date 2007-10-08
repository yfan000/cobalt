#!/usr/bin/env python

__revision__ = '$Revision$'

from Cobalt.Components.slp import TimingServiceLocator
from Cobalt.Components.base import run_component

slp = TimingServiceLocator()
run_component(slp, register=False, trace=True)

